"""T7 contract tests: workflow DAG engine.

Requires Postgres at KEELFLOW_TEST_DSN.
"""
from __future__ import annotations

import asyncio
import os

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy, Queue, Worker
from pydantic import BaseModel

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.engine.workflow import WorkflowService, eval_guard, resolve_inputs
from keelflow.postgres.schema import apply_async
from keelflow.sdk import (
    Actor,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
    StepSpec,
    WorkflowDefinition,
)

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
CHANCY_PREFIX = "keelflow_test_"

RUNS: list[dict] = []


class In(BaseModel):
    x: int = 0


class OkH(JobHandler):
    async def run(self, ctx, payload: In):
        RUNS.append({"type": "ok", "x": payload.x})
        return JobResult(status="done", outputs={"n": payload.x + 1})


class FailOnceH(JobHandler):
    calls = 0

    async def run(self, ctx, payload: In):
        type(self).calls += 1
        if type(self).calls == 1:
            return JobResult(status="failed", error="first try")
        return JobResult(status="done", outputs={"n": 99})


async def _conn(autocommit: bool = True) -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(DSN, autocommit=autocommit)


@pytest_asyncio.fixture
async def db():
    try:
        conn = await _conn()
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE hd_flow_jobs, hd_flow_outbox, hd_flow_audit,"
            " hd_flow_step_runs, hd_flow_workflow_runs, hd_flow_chunks,"
            " hd_flow_row_status, hd_flow_dlq CASCADE"
        )
        await conn.execute(
            f"TRUNCATE {CHANCY_PREFIX}jobs, {CHANCY_PREFIX}workers,"
            f" {CHANCY_PREFIX}queues CASCADE"
        )
        yield conn


@pytest_asyncio.fixture
async def env(db):
    async with Chancy(DSN, prefix=CHANCY_PREFIX) as kernel:
        await kernel.migrate()
        await kernel.declare(
            Queue("default", concurrency=4, executor=Chancy.Executor.Async,
                  eager_polling=True),
            upsert=True,
        )
        reg = InMemoryRegistry()
        reg.register_job_type(JobTypeSpec(type="t.ok@v1", input_schema=In, handler=OkH))
        reg.register_job_type(JobTypeSpec(type="t.flaky@v1", input_schema=In, handler=FailOnceH))
        reg.register_job_type(JobTypeSpec(type="human.review@v1", input_schema=In, handler=OkH, is_human=True))
        jobs = JobService(reg, kernel)
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        wf = WorkflowService(reg, jobs)
        yield {"reg": reg, "jobs": jobs, "wf": wf, "kernel": kernel}


async def _start(env, name, inputs, ctx=None):
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        return await env["wf"].start(
            name, inputs, ctx or JobContext(actor=Actor.user("u1")), cur
        )


async def _run_status(db, run_id):
    row = await (await db.execute(
        "SELECT status FROM hd_flow_workflow_runs WHERE id=%s", (run_id,)
    )).fetchone()
    return row[0] if row else None


async def _step_statuses(db, run_id):
    rows = await (await db.execute(
        "SELECT step_id, status, job_id, outputs FROM hd_flow_step_runs WHERE run_id=%s ORDER BY created_at",
        (run_id,),
    )).fetchall()
    return {r[0]: {"status": r[1], "job_id": r[2], "outputs": r[3]} for r in rows}


async def _wait_run(db, run_id, want, timeout=15):
    for _ in range(int(timeout / 0.1)):
        if await _run_status(db, run_id) == want:
            return want
        await asyncio.sleep(0.1)
    return await _run_status(db, run_id)


@pytest.mark.asyncio
async def test_dag_runs_to_completion(env, db):
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.linear@v1",
        steps=(
            StepSpec(id="a", type="t.ok@v1", input={"x": 1}),
            StepSpec(id="b", type="t.ok@v1", needs=("a",),
                     input={"x": "${{ steps.a.outputs.n }}"}),
            StepSpec(id="c", type="t.ok@v1", needs=("b",),
                     input={"x": "${{ steps.b.outputs.n }}"}),
        ),
    ))
    ref = await _start(env, "wf.linear@v1", {})
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        assert await _wait_run(db, ref.id, "done") == "done"
    finally:
        await w.stop(timeout=5)
    steps = await _step_statuses(db, ref.id)
    assert steps["a"]["status"] == "done"
    assert steps["b"]["status"] == "done"
    assert steps["c"]["status"] == "done"
    # a: x=1 → n=2; b: x=${{steps.a.n}}=2 → n=3; c: x=3 → n=4
    assert steps["b"]["outputs"]["n"] == 3
    assert steps["c"]["outputs"]["n"] == 4


@pytest.mark.asyncio
async def test_if_guard_skips_step(env, db):
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.guard@v1",
        steps=(
            StepSpec(id="a", type="t.ok@v1", input={"x": 1}),
            StepSpec(id="b", type="t.ok@v1", needs=("a",),
                     if_expr="${{ steps.a.outputs.n }} > 100"),
            StepSpec(id="c", type="t.ok@v1", needs=("a", "b"),
                     input={"x": 5}),
        ),
    ))
    ref = await _start(env, "wf.guard@v1", {})
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        assert await _wait_run(db, ref.id, "done") == "done"
    finally:
        await w.stop(timeout=5)
    steps = await _step_statuses(db, ref.id)
    assert steps["b"]["status"] == "skipped"
    assert steps["c"]["status"] == "done"
    assert steps["c"]["outputs"]["n"] == 6


@pytest.mark.asyncio
async def test_resume_from_failed_step(env, db):
    FailOnceH.calls = 0
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.flaky@v1",
        steps=(
            StepSpec(id="a", type="t.ok@v1", input={"x": 1}),
            StepSpec(id="b", type="t.flaky@v1", needs=("a",)),
            StepSpec(id="c", type="t.ok@v1", needs=("b",)),
        ),
    ))
    ref = await _start(env, "wf.flaky@v1", {})
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        # b fails once (kernel retries then fails after max_attempts=3? our
        # handler returns failed on first call — kernel marks job failed only
        # when handler raises; a returned 'failed' raises _JobFailedError so
        # kernel retries. max_attempts=3 → 3 calls, all 'failed').
        assert await _wait_run(db, ref.id, "failed", timeout=20) == "failed"
        # resume the failed step — handler now succeeds (call #4+)
        conn = await _conn()
        async with conn:
            await env["wf"].resume_step(conn, ref.id, "b",
                                        JobContext(actor=Actor.user("u1")))
        assert await _wait_run(db, ref.id, "done", timeout=20) == "done"
    finally:
        await w.stop(timeout=5)
    steps = await _step_statuses(db, ref.id)
    assert steps["b"]["status"] == "done"
    assert steps["c"]["status"] == "done"


@pytest.mark.asyncio
async def test_guard_evaluator():
    steps = [{"step_id": "a", "outputs": {"n": 5}}]
    assert eval_guard("${{ steps.a.outputs.n }} > 3", {}, steps)
    assert not eval_guard("${{ steps.a.outputs.n }} > 10", {}, steps)
    assert eval_guard("${{ steps.a.n }} == 5 and ${{ inputs.ok }}",
                      {"ok": True}, steps)
    with pytest.raises(TypeError):
        eval_guard("import os", {}, steps)
    with pytest.raises(TypeError):
        eval_guard("foo()", {}, steps)
    with pytest.raises(TypeError):
        eval_guard("steps.a.outputs.__class__", {}, steps)


@pytest.mark.asyncio
async def test_input_resolution():
    steps = [{"step_id": "a", "outputs": {"user_id": "u_1", "n": 3}}]
    out = resolve_inputs(
        {
            "who": "${{ steps.a.outputs.user_id }}",
            "nested": {"count": "${{ steps.a.outputs.n }}"},
            "lit": "keep",
            "num": 7,
        },
        {},
        steps,
    )
    assert out == {"who": "u_1", "nested": {"count": 3}, "lit": "keep", "num": 7}
