"""T11 + T12 contract tests: human task inbox + entity state machine."""
from __future__ import annotations

import os

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy, Queue, Worker
from pydantic import BaseModel

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.engine.statemachine import EngineStateMachineService
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.schema import apply_async
from keelflow.sdk import (
    Actor,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
    StateMachineDefinition,
    StepSpec,
    TransitionSpec,
    WorkflowDefinition,
)

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
CHANCY_PREFIX = "keelflow_test_"


class P(BaseModel):
    pass


class H(JobHandler):
    async def run(self, ctx, payload):
        return JobResult(status="done", outputs={"ok": True})


async def _conn(autocommit: bool = True):
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
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_dlq, keelflow_entity_state CASCADE"
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
        reg.register_job_type(JobTypeSpec(type="wf.step@v1", input_schema=P, handler=H))
        reg.register_job_type(JobTypeSpec(
            type="human.review@v1", input_schema=P, handler=H, is_human=True))
        jobs = JobService(reg, kernel)
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        sm = EngineStateMachineService(reg, kernel)
        await sm.ensure_schema(db)
        human = HumanTaskService(reg, kernel)
        wf = WorkflowService(reg, jobs)
        yield {"reg": reg, "jobs": jobs, "sm": sm, "human": human, "wf": wf,
               "kernel": kernel}


@pytest.mark.asyncio
async def test_transition_valid_and_guard(env, db):
    env["reg"].register_state_machine(StateMachineDefinition(
        name="invoice@v1", entity="invoice",
        states=("draft", "in_review", "approved"),
        transitions=(
            TransitionSpec("draft", "in_review", "submit",
                           guard={"permission": "inv.submit"}),
            TransitionSpec("in_review", "approved", "approve",
                           guard={"permission": "inv.approve"}),
        ),
        initial="draft",
    ))
    ctx = JobContext(actor=Actor.user("u1"),
                     permissions=frozenset({"inv.submit"}))
    entity = type("E", (), {"type": "invoice", "id": "inv-1"})()
    from keelflow.sdk import EntityRef
    entity = EntityRef(type="invoice", id="inv-1")

    async with await _conn(autocommit=False) as conn:
        # missing permission → rejected
        with pytest.raises(PermissionError):
            async with conn.transaction(), conn.cursor() as cur:
                await env["sm"].transition(entity, "submit",
                                           JobContext(actor=Actor.user("u2")), cur)
        await conn.rollback()
        # invalid action → rejected
        with pytest.raises(ValueError, match="cannot approve"):
            async with conn.transaction(), conn.cursor() as cur:
                await env["sm"].transition(entity, "approve", ctx, cur)
        await conn.rollback()
        # valid
        async with conn.transaction(), conn.cursor() as cur:
            await env["sm"].transition(entity, "submit", ctx, cur)
        assert await env["sm"].current(entity, conn) == "in_review"
        actions = await env["sm"].allowed_actions(entity, ctx.actor, conn)
        # u1 lacks inv.approve → filtered out
        assert actions == []


@pytest.mark.asyncio
async def test_creates_job_and_workflow(env, db):
    env["reg"].register_state_machine(StateMachineDefinition(
        name="invoice@v1", entity="invoice",
        states=("draft", "in_review", "approved"),
        transitions=(
            TransitionSpec("draft", "in_review", "submit",
                           creates_job="human.review@v1",
                           guard={"assignee_role": "finance"}),
            TransitionSpec("in_review", "approved", "approve",
                           triggers_workflow="post.wf@v1"),
        ),
        initial="draft",
    ))
    env["reg"].register_workflow(WorkflowDefinition(
        name="post.wf@v1",
        steps=(StepSpec(id="s1", type="wf.step@v1"),),
    ))
    ctx = JobContext(actor=Actor.user("u1"), permissions=frozenset({"*"}))
    from keelflow.sdk import EntityRef
    entity = EntityRef(type="invoice", id="inv-2")

    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            await env["sm"].transition(entity, "submit", ctx, cur)
    # human task created, assigned to finance role
    tasks = await env["human"].my_tasks(db, Actor.user("anyone"),
                                        roles=["finance"])
    assert len(tasks) == 1
    assert tasks[0]["type"] == "human.review@v1"
    assert tasks[0]["assignee"] == "role:finance"

    # claim as a finance user then complete → approve transition + workflow
    job_id = str(tasks[0]["id"])
    await env["human"].claim(db, job_id, Actor.user("u_fin"))
    with pytest.raises(PermissionError):
        await env["human"].claim(db, job_id, Actor.user("other"))
    await env["human"].complete(db, job_id, Actor.user("u_fin"),
                                {"decision": "ok"})
    job = await (await db.execute(
        "SELECT status FROM keelflow_jobs WHERE id=%s", (job_id,)
    )).fetchone()
    assert job[0] == "done"

    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            await env["sm"].transition(entity, "approve", ctx, cur)
    # workflow run started + step job enqueued
    run = await (await db.execute(
        "SELECT status FROM keelflow_workflow_runs"
    )).fetchone()
    assert run[0] in ("running", "done")
    step_job = await (await db.execute(
        "SELECT count(*) FROM keelflow_jobs WHERE type='wf.step@v1'"
    )).fetchone()
    assert step_job[0] == 1


@pytest.mark.asyncio
async def test_human_step_parks_then_unblocks_run(env, db):
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.human@v1",
        steps=(
            StepSpec(id="a", type="wf.step@v1"),
            StepSpec(id="review", type="human.review@v1", needs=("a",)),
            StepSpec(id="b", type="wf.step@v1", needs=("review",)),
        ),
    ))
    ctx = JobContext(actor=Actor.user("u1"))
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            ref = await env["wf"].start("wf.human@v1", {}, ctx, cur)

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        # wait for 'a' to finish and human job to appear
        human_job_id = None
        for _ in range(150):
            row = await (await db.execute(
                "SELECT id FROM keelflow_jobs WHERE type='human.review@v1'"
            )).fetchone()
            if row:
                human_job_id = str(row[0])
                break
            import asyncio
            await asyncio.sleep(0.1)
        assert human_job_id
        # run is still active (human step pending)
        status = await (await db.execute(
            "SELECT status FROM keelflow_workflow_runs WHERE id=%s", (ref.id,)
        )).fetchone()
        assert status[0] == "running"

        # human completes the task → workflow unblocks → 'b' runs → done
        await env["human"].complete(db, human_job_id, Actor.user("u1"))
        for _ in range(150):
            row = await (await db.execute(
                "SELECT status FROM keelflow_workflow_runs WHERE id=%s", (ref.id,)
            )).fetchone()
            if row[0] == "done":
                break
            import asyncio
            await asyncio.sleep(0.1)
        assert row[0] == "done"
    finally:
        await w.stop(timeout=5)
