"""Engine hardening regression tests — the classic durable-workflow bugs.

Covers: cancelled-step run outcome, workflow-definition drift mid-run,
poisoned guard/enqueue inside a finish TX, cancel_requested honored at
claim time, handler timeouts + RetryPolicy backoff + DLQ, human task
release/due_at, and outbox subscriber isolation.

Requires Postgres at KEELFLOW_TEST_DSN (skipped when unavailable).
"""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy, Queue, Worker
from pydantic import BaseModel

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.monitoring import MonitoringService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import (
    Actor,
    HumanTaskOptions,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
    StepSpec,
    WorkflowDefinition,
)

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
CHANCY_PREFIX = "keelflow_test_"

CALLS: list[str] = []


class In(BaseModel):
    x: int = 0


class OkH(JobHandler):
    async def run(self, ctx, payload: In):
        CALLS.append("ok")
        return JobResult(status="done", outputs={"n": payload.x + 1})


class SlowOnceH(JobHandler):
    calls = 0

    async def run(self, ctx, payload: In):
        type(self).calls += 1
        if type(self).calls == 1:
            await asyncio.sleep(30)
        return JobResult(status="done", outputs={"n": 1})


class BoomH(JobHandler):
    async def run(self, ctx, payload: In):
        raise RuntimeError("always boom")


class BigOutH(JobHandler):
    """Returns a large outputs dict for size-visibility tests."""

    async def run(self, ctx, payload: In):
        return JobResult(status="done",
                         outputs={"blob": "x" * 40000})


class CancelDrainH(JobHandler):
    """Sleeps so a cancel request can land mid-run — the job must
    still drain to `done` (cooperative, pre-start-only cancel)."""

    async def run(self, ctx, payload: In):
        await asyncio.sleep(0.4)
        return JobResult(status="done", outputs={"n": 1})


class CheckpointH(JobHandler):
    """Opted-in cooperative cancel: writes a probe row inside the
    handler TX, requests cancellation from a separate connection at a
    controlled point, then checkpoints. JobCancelled must propagate,
    rolling back the probe row.

    Events live on the instance — a new handler is instantiated per
    dispatch and class-level asyncio primitives bind to the first
    event loop they touch (cross-loop breakage between tests).
    """

    last: CheckpointH | None = None

    def __init__(self) -> None:
        self.flag_set = asyncio.Event()
        self.resume = asyncio.Event()
        type(self).last = self

    async def run(self, ctx, payload: In):
        await ctx.uow.execute(
            "INSERT INTO keelflow_outbox (event_type, payload)"
            " VALUES ('probe.cancel', '{}')")
        async with await psycopg.AsyncConnection.connect(
                DSN, autocommit=True) as c2:
            await c2.execute(
                "UPDATE keelflow_jobs SET cancel_requested=true WHERE id=%s",
                (ctx.execution.job_id,))
        self.flag_set.set()
        # Wait for the test to confirm the flag committed, then hit the
        # checkpoint — deterministic mid-run cancel (no sleeps).
        await self.resume.wait()
        await ctx.execution.checkpoint()
        raise AssertionError("checkpoint did not raise JobCancelled")


class BoomThenOkH(JobHandler):
    """Fails while `armed`, succeeds once disarmed — simulates an ops fix
    between DLQ write and replay."""

    armed = True

    async def run(self, ctx, payload: In):
        if type(self).armed:
            raise RuntimeError("boom (armed)")
        return JobResult(status="done", outputs={"n": 1})


async def _conn(autocommit: bool = True) -> psycopg.AsyncConnection:
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
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq, keelflow_entity_state,"
            " keelflow_outbox_deliveries CASCADE"
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
        reg.register_job_type(JobTypeSpec(type="t.ok@v1", input_schema=In,
                                          handler=OkH))
        reg.register_job_type(JobTypeSpec(
            type="t.warn@v1", input_schema=In, handler=BigOutH))
        reg.register_job_type(JobTypeSpec(
            type="t.slow@v1", input_schema=In, handler=SlowOnceH,
            retry=RetryPolicy(max_attempts=2, base_seconds=0.05,
                              jitter=False),
            timeout_seconds=0.2))
        reg.register_job_type(JobTypeSpec(
            type="t.boom@v1", input_schema=In, handler=BoomH,
            retry=RetryPolicy(max_attempts=2, base_seconds=0.05,
                              jitter=False)))
        reg.register_job_type(JobTypeSpec(
            type="t.drain@v1", input_schema=In, handler=CancelDrainH))
        reg.register_job_type(JobTypeSpec(
            type="t.checkpoint@v1", input_schema=In, handler=CheckpointH,
            cooperative_cancel=True,
            retry=RetryPolicy(max_attempts=2, base_seconds=0.05,
                              jitter=False)))
        reg.register_job_type(JobTypeSpec(
            type="human.review@v1", input_schema=In, handler=OkH,
            is_human=True,
            human_task=HumanTaskOptions(assignee_role="finance", due_in_seconds=60)))
        jobs = JobService(reg, kernel, PostgresStorage(DSN))
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        wf = WorkflowService(reg, jobs)
        human = HumanTaskService(reg, kernel)
        yield {"reg": reg, "jobs": jobs, "wf": wf, "kernel": kernel,
               "human": human}


async def _start(env, name, inputs=None):
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        return await env["wf"].start(
            name, inputs or {}, JobContext(actor=Actor.user("u1")), cur)


async def _run_row(db, run_id):
    return await (await db.execute(
        "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
        " WHERE id=%s", (run_id,))).fetchone()


async def _wait_run(db, run_id, want, timeout=15):
    for _ in range(int(timeout / 0.1)):
        row = await _run_row(db, run_id)
        if row and row[0] == want:
            return row
        await asyncio.sleep(0.1)
    return await _run_row(db, run_id)


# ── cancelled-step run outcome ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancelled_step_produces_cancelled_run(env, db):
    """A step whose job is cancelled before execution must NOT count as a
    successful run (was: mislabeled 'succeeded')."""
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.cancel@v1",
        steps=(
            StepSpec(id="a", type="t.ok@v1", input={"x": 1}),
            StepSpec(id="b", type="t.ok@v1", needs=("a",)),
        ),
    ))
    ref = await _start(env, "wf.cancel@v1")
    # cancel step a's job before any worker runs it
    step = await (await db.execute(
        "SELECT id, job_id FROM keelflow_step_runs"
        " WHERE run_id=%s AND step_id='a'", (ref.id,))).fetchone()
    await db.execute(
        "UPDATE keelflow_jobs SET cancel_requested=true WHERE id=%s",
        (step[1],))
    # cancel is honored at claim: dispatch finishes it as cancelled and
    # advances the run through on_job_finished.
    await worker_mod.dispatch(str(step[1]), str(step[0]))
    status, outcome = await _run_row(db, ref.id)
    assert (status, outcome) == ("cancelled", "cancelled")


# ── definition drift mid-run ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_definition_drift_fails_run(env, db):
    """Editing a workflow def without a version bump while a run is in
    flight must fail loudly, not park the run 'running' forever."""
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.drift@v1",
        steps=(
            StepSpec(id="h", type="human.review@v1"),
            StepSpec(id="b", type="t.ok@v1", needs=("h",)),
        ),
    ))
    ref = await _start(env, "wf.drift@v1")
    # def changes mid-run: step 'h' removed
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.drift@v1",
        steps=(StepSpec(id="b", type="t.ok@v1"),),
    ))
    # completing the human task triggers advance → drift detected
    job_id = str((await (await db.execute(
        "SELECT id FROM keelflow_jobs WHERE type='human.review@v1'"
        " ORDER BY created_at DESC LIMIT 1")).fetchone())[0])
    await env["human"].complete(db, job_id, Actor.user("fin1"),
                                {"decision": "approve"},
                                roles=["finance"])
    status, outcome = await _run_row(db, ref.id)
    assert status == "failed"
    assert outcome == "failed"


# ── poisoned guard inside a finish TX ─────────────────────────────────────


@pytest.mark.asyncio
async def test_poisoned_guard_fails_step_not_stuck(env, db):
    """A guard that raises during advance must mark the step failed
    (failing the run) instead of rolling back the finish TX forever."""
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.poison@v1",
        steps=(
            StepSpec(id="a", type="t.ok@v1", input={"x": 1}),
            # division by zero — eval_guard raises
            StepSpec(id="b", type="t.ok@v1", needs=("a",),
                     if_expr="${{ steps.a.outputs.n }} % 0 == 0"),
        ),
    ))
    ref = await _start(env, "wf.poison@v1")
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        status, _outcome = await _wait_run(db, ref.id, "failed", timeout=20)
    finally:
        await w.stop(timeout=5)
    assert status == "failed"
    row = await (await db.execute(
        "SELECT status FROM keelflow_step_runs"
        " WHERE run_id=%s AND step_id='b'", (ref.id,))).fetchone()
    assert row[0] == "failed"


# ── cancel_requested honored at claim ─────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_request_honored_at_claim(env, db):
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            ref = await env["jobs"].builder("t.ok@v1") \
                .context(JobContext(actor=Actor.user("u1"))) \
                .payload({"x": 1}) \
                .enqueue(cur)
    await db.execute(
        "UPDATE keelflow_jobs SET cancel_requested=true WHERE id=%s",
        (ref.id,))
    await worker_mod.dispatch(ref.id)
    row = await (await db.execute(
        "SELECT status FROM keelflow_jobs WHERE id=%s", (ref.id,))).fetchone()
    assert row[0] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_request_does_not_interrupt_running_job(env, db):
    """Flagging cancel_requested while the handler runs must not stop
    it — an in-flight job drains to `done` (cancel is pre-start only)."""
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            ref = await env["jobs"].builder("t.drain@v1") \
                .context(JobContext(actor=Actor.user("u1"))) \
                .payload({"x": 1}) \
                .enqueue(cur)
    task = asyncio.create_task(worker_mod.dispatch(ref.id))
    try:
        # wait until the handler is actually running, then request cancel
        for _ in range(100):
            row = await (await db.execute(
                "SELECT status FROM keelflow_jobs WHERE id=%s",
                (ref.id,))).fetchone()
            if row and row[0] == "running":
                break
            await asyncio.sleep(0.05)
        assert row[0] == "running"
        await db.execute(
            "UPDATE keelflow_jobs SET cancel_requested=true WHERE id=%s",
            (ref.id,))
        await task
    finally:
        if not task.done():
            task.cancel()
    row = await (await db.execute(
        "SELECT status, cancel_requested FROM keelflow_jobs WHERE id=%s",
        (ref.id,))).fetchone()
    assert row[0] == "done"          # ran to completion despite the flag
    assert row[1] is True            # the request was recorded


# ── cooperative checkpoints + public cancel API ───────────────────────────


async def _enqueue_ok(env, type_name="t.ok@v1"):
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            return await env["jobs"].builder(type_name) \
                .context(JobContext(actor=Actor.user("u1"))) \
                .payload({"x": 1}) \
                .enqueue(cur)


@pytest.mark.asyncio
async def test_checkpoint_observes_flag_and_rolls_back(env, db):
    """Opted-in handler checkpoints: committed cancel_requested raises
    JobCancelled → handler TX rolls back → job cancelled (no retry/DLQ)."""
    ref = await _enqueue_ok(env, "t.checkpoint@v1")
    task = asyncio.create_task(worker_mod.dispatch(ref.id))
    try:
        for _ in range(100):
            if CheckpointH.last and CheckpointH.last.flag_set.is_set():
                break
            await asyncio.sleep(0.05)
        h = CheckpointH.last
        assert h is not None and h.flag_set.is_set()
        row = await (await db.execute(
            "SELECT status, cancel_requested FROM keelflow_jobs WHERE id=%s",
            (ref.id,))).fetchone()
        assert row == ("running", True)
        h.resume.set()
        await asyncio.wait_for(task, timeout=10)
    finally:
        if CheckpointH.last:
            CheckpointH.last.resume.set()
        if not task.done():
            task.cancel()
    row = await (await db.execute(
        "SELECT status, attempts FROM keelflow_jobs WHERE id=%s",
        (ref.id,))).fetchone()
    assert row[0] == "cancelled"
    assert row[1] == 1                 # no retry attempt scheduled
    probe = await (await db.execute(
        "SELECT count(*) FROM keelflow_outbox WHERE event_type='probe.cancel'"
    )).fetchone()
    assert probe[0] == 0               # handler writes rolled back
    dlq = await (await db.execute(
        "SELECT count(*) FROM keelflow_dlq WHERE id=%s", (ref.id,)
    )).fetchone()
    assert dlq[0] == 0                 # cancellation never hits the DLQ


@pytest.mark.asyncio
async def test_cancel_api_pending_flags_running_and_noops_terminal(env, db):
    perms = JobContext(actor=Actor.user("u1"),
                       permissions=frozenset({"job.cancel"}))
    # pending → terminalized cancelled
    p = await _enqueue_ok(env)
    assert await env["jobs"].cancel(p.id, perms) is True
    row = await (await db.execute(
        "SELECT status, cancel_requested FROM keelflow_jobs WHERE id=%s",
        (p.id,))).fetchone()
    assert row == ("cancelled", True)
    # terminal → no-op
    assert await env["jobs"].cancel(p.id, perms) is False
    # running → flagged, status preserved (drain or checkpoint decides)
    r = await _enqueue_ok(env)
    await db.execute(
        "UPDATE keelflow_jobs SET status='running' WHERE id=%s", (r.id,))
    assert await env["jobs"].cancel(r.id, perms) is True
    row = await (await db.execute(
        "SELECT status, cancel_requested FROM keelflow_jobs WHERE id=%s",
        (r.id,))).fetchone()
    assert row == ("running", True)


@pytest.mark.asyncio
async def test_cancel_api_requires_permission(env, db):
    ref = await _enqueue_ok(env)
    with pytest.raises(PermissionError):
        await env["jobs"].cancel(
            ref.id, JobContext(actor=Actor.user("u1")))
    row = await (await db.execute(
        "SELECT status FROM keelflow_jobs WHERE id=%s", (ref.id,))).fetchone()
    assert row[0] == "pending"


@pytest.mark.asyncio
async def test_cancel_wins_start_race_handler_never_runs(env, db, monkeypatch):
    """Cancel commits between the load-time flag check and the
    pending→running transition: the conditional UPDATE must lose to it
    and the handler must never be invoked."""
    CALLS.clear()
    ref = await _enqueue_ok(env)
    from keelflow.postgres.storage import PostgresStorage
    orig = PostgresStorage.job_mark_running

    async def injected(self, uow, job_id):
        await db.execute(
            "UPDATE keelflow_jobs SET cancel_requested=true WHERE id=%s",
            (ref.id,))
        return await orig(self, uow, job_id)

    monkeypatch.setattr(PostgresStorage, "job_mark_running", injected)
    await worker_mod.dispatch(ref.id)
    row = await (await db.execute(
        "SELECT status, cancel_requested FROM keelflow_jobs WHERE id=%s",
        (ref.id,))).fetchone()
    assert row == ("cancelled", True)
    assert "ok" not in CALLS           # handler never invoked


@pytest.mark.asyncio
async def test_checkpoint_cancel_advances_workflow_step(env, db):
    """A step job cancelled at a checkpoint terminalizes its step and
    run as cancelled (not failed)."""
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.checkpointcancel@v1",
        steps=(StepSpec(id="a", type="t.checkpoint@v1", input={"x": 1}),),
    ))
    CheckpointH.last = None
    ref = await _start(env, "wf.checkpointcancel@v1")
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        for _ in range(100):
            if CheckpointH.last and CheckpointH.last.flag_set.is_set():
                break
            await asyncio.sleep(0.05)
        h = CheckpointH.last
        assert h is not None and h.flag_set.is_set()
        h.resume.set()
        row = await _wait_run(db, ref.id, "cancelled")
    finally:
        if CheckpointH.last:
            CheckpointH.last.resume.set()
        await w.stop(timeout=5)
    assert (row[0], row[1]) == ("cancelled", "cancelled")


# ── timeout + engine-scheduled backoff + DLQ ──────────────────────────────


@pytest.mark.asyncio
async def test_timeout_retry_and_dlq(env, db):
    """Slow handler → TimeoutError attempt 1 → engine-scheduled retry
    (fast backoff for the test) → succeeds on attempt 2."""
    SlowOnceH.calls = 0
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            ref = await env["jobs"].builder("t.slow@v1") \
                .context(JobContext(actor=Actor.user("u1"))) \
                .payload({"x": 1}) \
                .enqueue(cur)
    # attempt 1: hits timeout_seconds → retryable, stays 'running',
    # run_at deferred by the backoff delay
    await worker_mod.dispatch(ref.id)
    row = await (await db.execute(
        "SELECT status, attempts, error FROM keelflow_jobs WHERE id=%s",
        (ref.id,))).fetchone()
    assert row[0] == "running" and row[1] == 1
    assert "TimeoutError" in (row[2] or "")
    # a redelivery kernel job was scheduled for later
    kjobs = await (await db.execute(
        f"SELECT count(*) FROM {CHANCY_PREFIX}jobs"
        " WHERE kwargs->>'job_id'=%s", (ref.id,))).fetchone()
    assert kjobs[0] == 2  # original + retry push
    # attempt 2: handler returns fast → done
    await worker_mod.dispatch(ref.id)
    row = await (await db.execute(
        "SELECT status, attempts FROM keelflow_jobs WHERE id=%s",
        (ref.id,))).fetchone()
    assert row[0] == "done" and row[1] == 2


@pytest.mark.asyncio
async def test_exhausted_failure_lands_in_dlq(env, db):
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            ref = await env["jobs"].builder("t.boom@v1") \
                .context(JobContext(actor=Actor.user("u1"))) \
                .payload({"x": 1}) \
                .enqueue(cur)
    await worker_mod.dispatch(ref.id)   # attempt 1 → retryable
    await worker_mod.dispatch(ref.id)   # attempt 2 → exhausted → DLQ
    row = await (await db.execute(
        "SELECT status, attempts FROM keelflow_jobs WHERE id=%s",
        (ref.id,))).fetchone()
    assert row[0] == "failed" and row[1] == 2
    mon = MonitoringService(env["jobs"])
    dlq = await mon.dlq_list(db)
    assert [str(d["id"]) for d in dlq] == [ref.id]
    assert "boom" in dlq[0]["error"]
    # replay enqueues a fresh job of the same type/payload
    new_id = await mon.dlq_replay(db, ref.id, JobContext(actor=Actor.user("ops")))
    row = await (await db.execute(
        "SELECT type, status, payload FROM keelflow_jobs WHERE id=%s",
        (new_id,))).fetchone()
    assert row[0] == "t.boom@v1" and row[1] == "pending"


@pytest.mark.asyncio
async def test_dlq_replay_resumes_owning_run(env, db):
    """A dead DAG-step job replays INTO its run via resume_step — the run
    completes, instead of an orphaned standalone re-run."""
    BoomThenOkH.armed = True
    env["reg"].register_job_type(JobTypeSpec(
        type="t.boomthenok@v1", input_schema=In, handler=BoomThenOkH,
        retry=RetryPolicy(max_attempts=2, base_seconds=0.05,
                          jitter=False)))
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.dlqresume@v1",
        steps=(
            StepSpec(id="a", type="t.ok@v1", input={"x": 1}),
            StepSpec(id="b", type="t.boomthenok@v1",
                     needs=("a",), input={"x": 1}),
        ),
    ))
    ref = await _start(env, "wf.dlqresume@v1")
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        run = await _wait_run(db, ref.id, "failed")
        assert run[1] == "failed"                    # terminal_outcome
    finally:
        await w.stop(timeout=5)

    dlq = await MonitoringService(env["jobs"]).dlq_list(db)
    assert len(dlq) == 1 and dlq[0]["type"] == "t.boomthenok@v1"
    entry = await (await db.execute(
        "SELECT run_id, step_run_id FROM keelflow_dlq WHERE id=%s",
        (dlq[0]["id"],))).fetchone()
    assert str(entry[0]) == ref.id and entry[1] is not None

    # ops fixes the root cause, then replays — run resumes, not standalone
    BoomThenOkH.armed = False
    mon = MonitoringService(env["jobs"], registry=env["reg"])
    resumed = await mon.dlq_replay(
        db, str(dlq[0]["id"]), JobContext(actor=Actor.user("ops")))
    assert resumed == ref.id                      # returns run id

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        run = await _wait_run(db, ref.id, "done")
        assert run[1] == "succeeded"
        step = await (await db.execute(
            "SELECT status, job_id FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='b'", (ref.id,))).fetchone()
        assert step[0] == "done"
        new_job = await (await db.execute(
            "SELECT status FROM keelflow_jobs WHERE id=%s",
            (step[1],))).fetchone()
        assert new_job[0] == "done"              # re-enqueued job finished
    finally:
        await w.stop(timeout=5)
        BoomThenOkH.armed = True


# ── result size visibility: warning + opt-in hard cap ────────────────────


@pytest.mark.asyncio
async def test_result_warning_still_succeeds_and_records_size(env, db):
    """t.warn@v1 returns ~40KB outputs: over the 32KB soft threshold →
    warning logged, size recorded, but job still done (no hard cap)."""
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        ref = await env["jobs"].builder("t.warn@v1") \
            .context(JobContext(actor=Actor.user("u1"))) \
            .payload({"x": 1}) \
            .enqueue(cur)
    await worker_mod.dispatch(ref.id)
    row = await (await db.execute(
        "SELECT status, result_bytes FROM keelflow_jobs WHERE id=%s",
        (ref.id,))).fetchone()
    assert row[0] == "done"
    assert row[1] and row[1] > 32768           # warning threshold passed

    oversized = await MonitoringService().results_oversized(
        db, threshold=32768)
    entry = next(r for r in oversized if r["type"] == "t.warn@v1")
    assert entry["oversized"] == 1 and entry["worst"] > 32768


@pytest.mark.asyncio
async def test_result_max_bytes_fails_loudly(env, db):
    """Opt-in hard cap: a result over result_max_bytes fails the job
    with a diagnostic naming the cap and type."""
    env["reg"].register_job_type(JobTypeSpec(
        type="t.capped@v1", input_schema=In, handler=BigOutH,
        result_max_bytes=1000))
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        ref = await env["jobs"].builder("t.capped@v1") \
            .context(JobContext(actor=Actor.user("u1"))) \
            .payload({"x": 1}) \
            .enqueue(cur)
    # failed result propagates _JobFailedError so the kernel marks its job
    with pytest.raises(worker_mod._JobFailedError):
        await worker_mod.dispatch(ref.id)
    row = await (await db.execute(
        "SELECT status, error FROM keelflow_jobs WHERE id=%s",
        (ref.id,))).fetchone()
    assert row[0] == "failed"
    assert "result_max_bytes" in row[1] and "t.capped@v1" in row[1]


# ── human tasks: release + due_at ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_release_returns_task_to_pending(env, db):
    job_id = uuid.uuid4()
    await db.execute(
        "INSERT INTO keelflow_jobs (id, type, status, payload, context,"
        " assignee, claimed_by) VALUES (%s,'human.review@v1','claimed',"
        " '{}','{}','role:finance','user:fin1')",
        (job_id,))
    with pytest.raises(PermissionError):
        await env["human"].release(db, str(job_id), Actor.user("other"))
    await env["human"].release(db, str(job_id), Actor.user("fin1"),
                               roles=["finance"])
    row = await (await db.execute(
        "SELECT status, claimed_by FROM keelflow_jobs WHERE id=%s",
        (job_id,))).fetchone()
    assert row == ("pending", None)


@pytest.mark.asyncio
async def test_due_at_set_and_escalated(env, db):
    """ui.due_in_seconds sets due_at; escalate_overdue picks up overdue
    tasks regardless of older_than_days."""
    env["reg"].register_workflow(WorkflowDefinition(
        name="wf.due@v1",
        steps=(StepSpec(id="h", type="human.review@v1"),),
    ))
    await _start(env, "wf.due@v1")
    row = await (await db.execute(
        "SELECT due_at FROM keelflow_jobs WHERE type='human.review@v1'"
        " ORDER BY created_at DESC LIMIT 1")).fetchone()
    assert row[0] is not None
    # force it overdue → escalate reassigns even for older_than_days=99
    await db.execute(
        "UPDATE keelflow_jobs SET due_at=%s WHERE type='human.review@v1'",
        (datetime.now(UTC) - timedelta(seconds=1),))
    n = await env["human"].escalate_overdue(
        db, older_than_days=99, to_role="lead")
    assert n == 1
    row = await (await db.execute(
        "SELECT assignee FROM keelflow_jobs WHERE type='human.review@v1'"
    )).fetchone()
    assert row[0] == "role:lead"


# ── outbox subscriber isolation ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_outbox_bad_subscriber_does_not_block_others(db):
    from keelflow.engine.events import OutboxEventBus, dispatch_once
    from keelflow.sdk.services import DomainEvent

    calls = []

    async def bad(event, ctx):
        raise RuntimeError("subscriber down")

    async def good(event, ctx):
        calls.append(event.payload)

    bus = OutboxEventBus(PostgresStorage(DSN))
    bus.subscribe("e.t@v1", bad)
    bus.subscribe("e.t@v1", good)

    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            await bus.publish(DomainEvent(type="e.t@v1", payload={"a": 1}), cur)

    sess = await PostgresStorage(DSN).connect()
    n = await dispatch_once(sess, PostgresStorage(DSN), bus)
    await sess.close()
    assert n == 0  # event stays 'failed' — one subscriber raised
    assert calls == [{"a": 1}]  # but the good subscriber still ran
