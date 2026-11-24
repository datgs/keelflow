"""External wait/signal contract tests (G1).

`wait.*` job types materialize as blocked workflow steps with no kernel
claim. An application-owned callback resolves them through
`JobService.signal`/`signal_failure` — atomic with the step update and
run advance.

Requires Postgres at KEELFLOW_TEST_DSN.
"""
from __future__ import annotations

import asyncio
import os
import random
import time

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy, Queue, Worker
from pydantic import BaseModel

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import EnqueueError, JobService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
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
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow_test"
)
CHANCY_PREFIX = "keelflow_test_"


class In(BaseModel):
    x: int = 0


class OkH(JobHandler):
    async def run(self, ctx, payload: In):
        return JobResult(status="done", outputs={"n": payload.x + 1})


async def _conn(autocommit: bool = True):
    return await psycopg.AsyncConnection.connect(DSN, autocommit=autocommit)


@pytest_asyncio.fixture
async def db():
    try:
        conn = await _conn()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq CASCADE"
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
        reg.register_job_type(JobTypeSpec(
            type="t.ok@v1", input_schema=In, handler=OkH))
        reg.register_job_type(JobTypeSpec(
            type="wait.vendor@v1", input_schema=In, is_external_wait=True))
        jobs = JobService(reg, kernel, PostgresStorage(DSN))
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        yield {"reg": reg, "jobs": jobs,
               "wf": WorkflowService(reg, jobs), "kernel": kernel}


def _sig_ctx() -> JobContext:
    return JobContext(actor=Actor.service("vendor-webhook"),
                      permissions=frozenset({"job.signal"}))


async def _start(env, steps, inputs=None):
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.wait@v1", steps=steps))
    ctx = JobContext(actor=Actor.user("u1"))
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        return await env["wf"].start("wf.wait@v1", inputs or {}, ctx, cur)


async def _wait_for(db, sql, params, want, timeout=10):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        row = await (await db.execute(sql, params)).fetchone()
        if row and row[0] == want:
            return row[0]
        await asyncio.sleep(0.1)
    raise AssertionError(f"{sql} not {want} after {timeout}s (last={row!r})")


@pytest.mark.asyncio
async def test_is_wait_registration_rules():
    reg = InMemoryRegistry()
    with pytest.raises(ValueError, match="wait\\."):
        reg.register_job_type(JobTypeSpec(
            type="ext.vendor@v1", input_schema=In, is_external_wait=True))
    with pytest.raises(ValueError, match="both"):
        reg.register_job_type(JobTypeSpec(
            type="wait.bad@v1", input_schema=In,
            is_external_wait=True, is_human=True))
    with pytest.raises(ValueError, match="handler"):
        reg.register_job_type(JobTypeSpec(
            type="wait.bad2@v1", input_schema=In,
            is_external_wait=True, handler=OkH))


@pytest.mark.asyncio
async def test_builder_rejects_direct_wait_enqueue(env, db):
    ctx = JobContext(actor=Actor.user("u1"))
    with pytest.raises(EnqueueError, match="external-wait"):
        async with (await _conn(False)) as c, c.transaction(), \
                c.cursor() as cur:
            await env["jobs"].builder("wait.vendor@v1") \
                .payload({}).context(ctx).enqueue(cur)


@pytest.mark.asyncio
async def test_wait_step_parks_then_signal_advances(env, db):
    """wait.* → blocked job, no kernel claim; signal → done → advance."""
    run = await _start(env, (
        StepSpec(id="a", type="t.ok@v1"),
        StepSpec(id="hook", type="wait.vendor@v1", needs=("a",)),
        StepSpec(id="b", type="t.ok@v1", needs=("hook",),
                 input={"x": "${{ steps.hook.outputs.n }}"}),
    ))
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        hook = await _wait_for(
            db,
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'",
            (run.id,), "blocked")
        assert hook == "blocked"
        # no kernel claim was created for the wait job
        wait_job = await (await db.execute(
            "SELECT job_id FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'", (run.id,))).fetchone()
        kernel_rows = await (await db.execute(
            f"SELECT count(*) FROM {CHANCY_PREFIX}jobs"
            " WHERE kwargs->>'job_id'=%s", (str(wait_job[0]),))).fetchone()
        assert kernel_rows[0] == 0
        # wait.opened outbox intent exists for the app integration layer
        outbox = await (await db.execute(
            "SELECT payload FROM keelflow_outbox"
            " WHERE event_type='wait.opened'")).fetchone()
        assert outbox[0]["job_id"] == str(wait_job[0])

        await asyncio.sleep(0.5)  # prove no worker drains it on its own
        async with (await _conn(False)) as conn:
            await env["jobs"].signal(
                conn, str(wait_job[0]), {"n": 41}, _sig_ctx())
            await conn.commit()

        await _wait_for(
            db,
            "SELECT status FROM keelflow_workflow_runs WHERE id=%s",
            (run.id,), "done")
    finally:
        await w.stop(timeout=5)
    steps = await (await db.execute(
        "SELECT step_id, status, outputs FROM keelflow_step_runs"
        " WHERE run_id=%s ORDER BY created_at", (run.id,))).fetchall()
    by_id = {s[0]: (s[1], s[2]) for s in steps}
    assert by_id["hook"][0] == "done" and by_id["hook"][1]["n"] == 41
    assert by_id["b"][0] == "done" and by_id["b"][1]["n"] == 42


@pytest.mark.asyncio
async def test_signal_failure_fails_step_and_run(env, db):
    run = await _start(env, (
        StepSpec(id="hook", type="wait.vendor@v1"),
        StepSpec(id="b", type="t.ok@v1", needs=("hook",)),
    ))
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        await _wait_for(
            db,
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'",
            (run.id,), "blocked")
        wait_job = await (await db.execute(
            "SELECT job_id FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'", (run.id,))).fetchone()
        async with (await _conn(False)) as conn:
            await env["jobs"].signal_failure(
                conn, str(wait_job[0]), "vendor declined",
                _sig_ctx(), result={"reason": "declined"})
            await conn.commit()
        await _wait_for(
            db,
            "SELECT status FROM keelflow_workflow_runs WHERE id=%s",
            (run.id,), "failed")
    finally:
        await w.stop(timeout=5)
    step = await (await db.execute(
        "SELECT status FROM keelflow_step_runs"
        " WHERE run_id=%s AND step_id='hook'", (run.id,))).fetchone()
    assert step[0] == "failed"
    job = await (await db.execute(
        "SELECT status, error FROM keelflow_jobs WHERE id=%s",
        (wait_job[0],))).fetchone()
    assert job == ("failed", "vendor declined")


@pytest.mark.asyncio
async def test_signal_guards(env, db):
    """Permission, non-wait type, terminal wait, first-wins."""
    run = await _start(env, (StepSpec(id="hook", type="wait.vendor@v1"),))
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        await _wait_for(
            db,
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'",
            (run.id,), "blocked")
        wait_job = (await (await db.execute(
            "SELECT job_id FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'", (run.id,))).fetchone())[0]
        jid = str(wait_job)

        # permission required
        with pytest.raises(PermissionError):
            async with (await _conn(False)) as conn:
                await env["jobs"].signal(
                    conn, jid, {}, JobContext(actor=Actor.user("u2")))

        # non-wait job id rejected
        machine = await (await db.execute(
            "SELECT id FROM keelflow_jobs WHERE type='t.ok@v1' LIMIT 1")
        ).fetchone()
        if machine:
            with pytest.raises(ValueError, match="not a wait"):
                async with (await _conn(False)) as conn:
                    await env["jobs"].signal(conn, str(machine[0]), {}, _sig_ctx())

        # first wins; second raises
        async with (await _conn(False)) as conn:
            await env["jobs"].signal(conn, jid, {"first": True}, _sig_ctx())
            await conn.commit()
        with pytest.raises(ValueError, match="not open"):
            async with (await _conn(False)) as conn:
                await env["jobs"].signal(conn, jid, {"second": True}, _sig_ctx())

        # cancelled wait rejects a signal
        run2 = await _start(env, (StepSpec(id="hook", type="wait.vendor@v1"),))
        await _wait_for(
            db,
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'",
            (run2.id,), "blocked")
        wait2 = (await (await db.execute(
            "SELECT job_id FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'", (run2.id,))).fetchone())[0]
        cancel_ctx = JobContext(actor=Actor.user("u1"),
                                permissions=frozenset({"job.cancel"}))
        assert await env["jobs"].cancel(str(wait2), cancel_ctx) is True
        with pytest.raises(ValueError, match="not open"):
            async with (await _conn(False)) as conn:
                await env["jobs"].signal(conn, str(wait2), {}, _sig_ctx())
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_concurrent_signals_single_winner(env, db):
    """Two races to resolve the same wait — exactly one writes."""
    run = await _start(env, (StepSpec(id="hook", type="wait.vendor@v1"),))
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        await _wait_for(
            db,
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'",
            (run.id,), "blocked")
        wait_job = (await (await db.execute(
            "SELECT job_id FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'", (run.id,))).fetchone())[0]

        async def attempt(n):
            try:
                async with (await _conn(False)) as conn:
                    await env["jobs"].signal(
                        conn, str(wait_job), {"winner": n}, _sig_ctx())
                    await conn.commit()
                return n
            except ValueError:
                return None

        results = await asyncio.gather(*(attempt(i) for i in range(3)))
        winners = [r for r in results if r is not None]
        assert len(winners) == 1
        job = await (await db.execute(
            "SELECT status, result->>'winner' FROM keelflow_jobs"
            " WHERE id=%s", (wait_job,))).fetchone()
        assert job == ("done", str(winners[0]))
    finally:
        await w.stop(timeout=5)


# ── randomized event-sequence invariants ────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(12))
async def test_wait_random_event_sequences(env, db, seed):
    """Property-style: a run with 2-4 parallel `wait.*` steps receives a
    random sequence of signals/failures/cancels/duplicates. Whatever the
    ordering, the first successful resolution wins, every wait ends in a
    status consistent with its job, and the run's terminal outcome is
    deterministic (failed > cancelled > done). Runs with an unresolved
    wait stay legitimately blocked — they never self-close."""
    rng = random.Random(seed)
    n_waits = rng.randint(2, 4)
    wait_ids = [f"w{i}" for i in range(n_waits)]
    steps = [StepSpec(id=w, type="wait.vendor@v1") for w in wait_ids]
    steps.append(StepSpec(
        id="tail", type="t.ok@v1",
        needs=tuple(wait_ids),
        input={"x": "${{ steps.w0.outputs.n }}" if "w0" in wait_ids else "{}"}))
    run = await _start(env, tuple(steps))

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    cancel_ctx = JobContext(actor=Actor.user("u1"),
                            permissions=frozenset({"job.cancel"}))
    # expected outcome per wait: first successful mutating event wins
    expected: dict[str, str] = {}          # wait_id -> job status
    try:
        # every wait (no needs) materializes blocked at start
        for wid in wait_ids:
            await _wait_for(
                db,
                "SELECT status FROM keelflow_step_runs"
                " WHERE run_id=%s AND step_id=%s",
                (run.id, wid), "blocked")

        # build a randomized event plan per wait — ops within a wait
        # keep their order (a 'dup' only ever follows a resolving op);
        # the global sequence is an order-preserving interleave
        per_wait: dict[str, list[str]] = {}
        for wid in wait_ids:
            plan = rng.choices(
                ["ok", "ok+dup", "fail", "cancel", "leave"],
                weights=[40, 15, 15, 15, 15])[0]
            if plan == "ok":
                per_wait[wid] = ["ok"]
            elif plan == "ok+dup":
                per_wait[wid] = ["ok", "dup"]
            elif plan == "fail":
                per_wait[wid] = ["fail"]
            elif plan == "cancel":
                per_wait[wid] = ["cancel"] + (
                    ["dup"] if rng.random() < 0.5 else [])
            else:
                per_wait[wid] = []           # 'leave' stays open
        ops: list[tuple[str, str]] = []
        remaining = {w: list(o) for w, o in per_wait.items()}
        while any(remaining.values()):
            wid = rng.choice([w for w, o in remaining.items() if o])
            ops.append((wid, remaining[wid].pop(0)))

        wait_jobs = {}
        for wid in wait_ids:
            wait_jobs[wid] = (await (await db.execute(
                "SELECT job_id FROM keelflow_step_runs"
                " WHERE run_id=%s AND step_id=%s", (run.id, wid)
            )).fetchone())[0]

        for wid, op in ops:
            jid = str(wait_jobs[wid])
            if op == "ok":
                async with (await _conn(False)) as conn:
                    await env["jobs"].signal(
                        conn, jid, {"n": rng.randint(1, 9)}, _sig_ctx())
                    await conn.commit()
                expected.setdefault(wid, "done")
            elif op == "fail":
                async with (await _conn(False)) as conn:
                    await env["jobs"].signal_failure(
                        conn, jid, "vendor error", _sig_ctx())
                    await conn.commit()
                expected.setdefault(wid, "failed")
            elif op == "cancel":
                assert await env["jobs"].cancel(jid, cancel_ctx)
                expected.setdefault(wid, "cancelled")
            else:  # dup — wait already resolved; must raise, never overwrite
                with pytest.raises(ValueError):
                    async with (await _conn(False)) as conn:
                        await env["jobs"].signal(
                            conn, jid, {"late": True}, _sig_ctx())

        # waits left alone stay blocked — the run must not self-close
        open_waits = [w for w in wait_ids if w not in expected]
        run_row = await (await db.execute(
            "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
            " WHERE id=%s", (run.id,))).fetchone()
        # deterministic outcome: any cancelled step cancels the run
        # (even with open waits), any failed step fails it; an untouched
        # wait keeps the run legitimately blocked otherwise.
        want_run = ("failed" if "failed" in expected.values()
                    else "cancelled" if "cancelled" in expected.values()
                    else "done" if not open_waits else "running")
        t0 = time.monotonic()
        while run_row[0] == "running" and want_run != "running" and \
                time.monotonic() - t0 < 20:
            await asyncio.sleep(0.15)
            run_row = await (await db.execute(
                "SELECT status, terminal_outcome"
                " FROM keelflow_workflow_runs WHERE id=%s",
                (run.id,))).fetchone()
        assert run_row[0] == want_run, (seed, expected, run_row)
        for wid in open_waits:
            step = await (await db.execute(
                "SELECT status FROM keelflow_step_runs"
                " WHERE run_id=%s AND step_id=%s", (run.id, wid)
            )).fetchone()
            assert step[0] == "blocked"    # untouched waits stay open

        # job and step statuses agree; first resolution won the payload
        for wid, want in expected.items():
            step = await (await db.execute(
                "SELECT status FROM keelflow_step_runs"
                " WHERE run_id=%s AND step_id=%s", (run.id, wid)
            )).fetchone()
            job = await (await db.execute(
                "SELECT status FROM keelflow_jobs WHERE id=%s",
                (wait_jobs[wid],))).fetchone()
            assert step[0] == want and job[0] == want
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(6))
async def test_wait_random_concurrent_storm(env, db, seed):
    """Concurrent storm on ONE wait: shuffled mix of signal/signal_failure/
    cancel fired together — exactly one mutating event wins, the wait's
    final status is one of the attempted kinds, and losers all raise/no-op
    without corrupting the step."""
    rng = random.Random(seed)
    run = await _start(env, (
        StepSpec(id="hook", type="wait.vendor@v1"),
        StepSpec(id="tail", type="t.ok@v1", needs=("hook",)),
    ))
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        await _wait_for(
            db,
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'",
            (run.id,), "blocked")
        wait_job = (await (await db.execute(
            "SELECT job_id FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'", (run.id,))).fetchone())[0]
        jid = str(wait_job)
        cancel_ctx = JobContext(actor=Actor.user("u1"),
                                permissions=frozenset({"job.cancel"}))

        attempts = rng.sample(["ok", "ok", "fail", "cancel", "ok"],
                              k=rng.randint(3, 5))

        async def fire(kind, i):
            try:
                if kind == "cancel":
                    return ("cancelled",
                            await env["jobs"].cancel(jid, cancel_ctx))
                async with (await _conn(False)) as conn:
                    if kind == "fail":
                        await env["jobs"].signal_failure(
                            conn, jid, f"e{i}", _sig_ctx())
                    else:
                        await env["jobs"].signal(
                            conn, jid, {"i": i}, _sig_ctx())
                    await conn.commit()
                return ("failed" if kind == "fail" else "done", True)
            except (ValueError, KeyError):
                return (kind, False)

        results = await asyncio.gather(
            *(fire(k, i) for i, k in enumerate(attempts)))
        landed = [r for r in results if r[1]]
        # cancel() returns True on success; signal paths return True on
        # their commit — at most ONE mutating resolution per outcome
        mutating = [r for r in landed if r[0] in
                    ("done", "failed", "cancelled")]
        # cancel may co-land with a signal only if both raced the same
        # 'pending' read — the JOB row decides the truth, not the calls
        final = await (await db.execute(
            "SELECT status FROM keelflow_jobs WHERE id=%s", (jid,)
        )).fetchone()
        step = await (await db.execute(
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='hook'", (run.id,))).fetchone()
        assert final[0] in ("done", "failed", "cancelled")
        assert step[0] == final[0]      # step mirrors job, never diverges
        assert mutating                 # something resolved it
        # any later signal is still rejected — terminal is sticky
        with pytest.raises(ValueError):
            async with (await _conn(False)) as conn:
                await env["jobs"].signal(conn, jid, {"late": 1}, _sig_ctx())
    finally:
        await w.stop(timeout=5)
