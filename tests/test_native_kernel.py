"""Native kernel conformance tests (T3).

Runs the same claim/dispatch contract as the Chancy adapter against the
native Postgres backend (`NativeKernel`): `keelflow_jobs` is the single
queue/lifecycle record.

Requires Postgres at KEELFLOW_TEST_DSN.
"""
from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
import pytest_asyncio
from pydantic import BaseModel

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.native_kernel import NativeKernel
from keelflow.engine.registry import InMemoryRegistry
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import (
    Actor,
    JobContext,
    JobHandler,
    JobResult,
    JobStatus,
    JobTypeSpec,
    RetryPolicy,
)

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow_test"
)

CALLS: list[tuple[str, dict]] = []


class Pay(BaseModel):
    invoice_id: str


class OkHandler(JobHandler):
    async def run(self, ctx, payload: Pay):
        CALLS.append(("ok", {"actor": ctx.actor.id}))
        return JobResult(status="done", outputs={"matched": 7})


class FailHandler(JobHandler):
    async def run(self, ctx, payload: Pay):
        raise RuntimeError("boom")


class SlowHandler(JobHandler):
    async def run(self, ctx, payload: Pay):
        await asyncio.sleep(30)
        return JobResult(status="done", outputs={})


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
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq CASCADE"
        )
        yield conn


@pytest_asyncio.fixture
async def reg():
    reg = InMemoryRegistry()
    reg.register_job_type(
        JobTypeSpec(type="test.ok@v1", input_schema=Pay, handler=OkHandler)
    )
    reg.register_job_type(
        JobTypeSpec(
            type="test.fail@v1",
            input_schema=Pay,
            handler=FailHandler,
            retry=RetryPolicy(
                max_attempts=2, base_seconds=0.2, backoff="fixed",
                jitter=False),
        )
    )
    reg.register_job_type(
        JobTypeSpec(type="test.slow@v1", input_schema=Pay,
                    handler=SlowHandler)
    )
    reg.register_job_type(
        JobTypeSpec(type="human.rev@v1", input_schema=Pay,
                    handler=OkHandler, is_human=True)
    )
    return reg


@pytest_asyncio.fixture
async def env(db, reg):
    CALLS.clear()
    kernel = NativeKernel(DSN)
    worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
    w = kernel.create_worker(
        concurrency=4, poll_interval=0.05,
        lease_seconds=1.0, heartbeat_interval=0.2)
    await w.start()
    yield {"kernel": kernel, "worker": w,
           "jobs": JobService(reg, kernel, PostgresStorage(DSN)), "reg": reg}
    await w.stop(timeout=2)
    worker_mod.configure(registry=None, kernel=None)


async def _enqueue(service, type_name: str, payload: dict,
                   run_at=None) -> str:
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        b = service.builder(type_name).context(
            JobContext(actor=Actor.user("u1"))).payload(payload)
        if run_at is not None:
            b = b.run_at(run_at)
        ref = await b.enqueue(cur)
    return ref.id


async def _wait_status(db, job_id: str, want: str,
                       timeout: float = 15.0) -> str:
    row = None
    for _ in range(int(timeout / 0.05)):
        row = await (
            await db.execute(
                "SELECT status FROM keelflow_jobs WHERE id=%s", (job_id,))
        ).fetchone()
        if row and row[0] == want:
            return row[0]
        await asyncio.sleep(0.05)
    return row[0] if row else "missing"


@pytest.mark.asyncio
async def test_native_claims_and_completes(env, db):
    job_id = await _enqueue(env["jobs"], "test.ok@v1",
                            {"invoice_id": "i-1"})
    assert await _wait_status(db, job_id, "done") == "done"
    assert CALLS == [("ok", {"actor": "u1"})]
    row = await (await db.execute(
        "SELECT status, result, attempts, claimed_by FROM keelflow_jobs"
        " WHERE id=%s", (job_id,))).fetchone()
    assert row[0] == JobStatus.DONE.value
    assert row[1]["matched"] == 7
    assert row[2] == 1
    assert row[3] == env["worker"].worker_id


@pytest.mark.asyncio
async def test_native_retry_then_done(env, db, reg, monkeypatch):
    """Handler fails once, engine-scheduled retry re-dispatches."""
    attempts = {"n": 0}

    class FlakyH(JobHandler):
        async def run(self, ctx, payload):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("flaky")
            return JobResult(status="done", outputs={"attempts": attempts["n"]})

    reg.register_job_type(JobTypeSpec(
        type="test.flaky@v1", input_schema=Pay, handler=FlakyH,
        retry=RetryPolicy(max_attempts=3, base_seconds=0.1,
                          backoff="fixed", jitter=False)))
    job_id = await _enqueue(env["jobs"], "test.flaky@v1",
                            {"invoice_id": "i-9"})
    assert await _wait_status(db, job_id, "done", timeout=15) == "done"
    row = await (await db.execute(
        "SELECT attempts FROM keelflow_jobs WHERE id=%s", (job_id,))
    ).fetchone()
    assert attempts["n"] == 2 and row[0] == 2


@pytest.mark.asyncio
async def test_native_handler_fails_permanently(env, db):
    job_id = await _enqueue(env["jobs"], "test.fail@v1",
                            {"invoice_id": "i-2"})
    assert await _wait_status(db, job_id, "failed", timeout=20) == "failed"
    row = await (await db.execute(
        "SELECT attempts, error FROM keelflow_jobs WHERE id=%s",
        (job_id,))).fetchone()
    assert row[0] == 2  # max_attempts=2
    assert "boom" in row[1]


@pytest.mark.asyncio
async def test_native_non_dispatch_row_not_claimed(env, db):
    """A pending engine row that never went through push_ex (e.g. a
    workflow-created human task — the workflow path materializes the row
    without a kernel push) has dispatch_queued=false and must never be
    claimed by the worker."""
    import json
    import uuid

    job_id = uuid.uuid4()
    await db.execute(
        "INSERT INTO keelflow_jobs"
        " (id, type, status, payload, context)"
        " VALUES (%s, 'human.rev@v1', 'pending', %s, %s)",
        (job_id, json.dumps({"invoice_id": "i-h"}), json.dumps({})),
    )
    await asyncio.sleep(1.0)
    row = await (await db.execute(
        "SELECT status, claimed_by, dispatch_queued"
        " FROM keelflow_jobs WHERE id=%s", (job_id,))).fetchone()
    assert row[0] == "pending" and row[1] is None and row[2] is False
    assert CALLS == []


@pytest.mark.asyncio
async def test_native_future_run_at_not_claimed(env, db):
    job_id = await _enqueue(env["jobs"], "test.ok@v1", {"invoice_id": "i-3"},
                            run_at=datetime.now(UTC) + timedelta(minutes=10))
    await asyncio.sleep(1.0)
    row = await (await db.execute(
        "SELECT status FROM keelflow_jobs WHERE id=%s", (job_id,))).fetchone()
    assert row[0] == "pending"


@pytest.mark.asyncio
async def test_native_enqueue_rollback_no_dispatch(env, db):
    """Aborted caller tx must leave nothing claimable."""
    async with await _conn(autocommit=False) as conn:
        async with conn.cursor() as cur:
            await conn.execute("BEGIN")
            ref = await env["jobs"].builder("test.ok@v1").context(
                JobContext(actor=Actor.user("u1"))).payload(
                {"invoice_id": "i-rb"}).enqueue(cur)
            await conn.execute("ROLLBACK")
        await asyncio.sleep(0.7)
        row = await (await db.execute(
            "SELECT count(*) FROM keelflow_jobs WHERE id=%s",
            (ref.id,))).fetchone()
        assert row[0] == 0
        assert CALLS == []


@pytest.mark.asyncio
async def test_native_cancel_pending(env, db):
    # Park the job far in the future, cancel, then confirm cancelled.
    job_id = await _enqueue(env["jobs"], "test.ok@v1", {"invoice_id": "i-c"},
                            run_at=datetime.now(UTC) + timedelta(minutes=10))
    ok = await env["jobs"].cancel(
        job_id, JobContext(actor=Actor.user("u1"),
                           permissions=["job.cancel"]))
    assert ok is True
    row = await (await db.execute(
        "SELECT status FROM keelflow_jobs WHERE id=%s", (job_id,))).fetchone()
    assert row[0] == "cancelled"
    await asyncio.sleep(0.6)
    assert CALLS == []


@pytest.mark.asyncio
async def test_native_reclaim_after_lease_expiry(env, db):
    """A claim whose owner dies (lease expiry) is reclaimed and re-run."""
    job_id = await _enqueue(env["jobs"], "test.ok@v1",
                            {"invoice_id": "i-k"})
    # Simulate a dead owner's abandoned claim.
    await db.execute(
        "UPDATE keelflow_jobs SET status='claimed', claimed_by='dead',"
        " lease_until=now()-interval '1 second' WHERE id=%s", (job_id,))
    await asyncio.sleep(0.8)  # heartbeat_interval reaper passes
    assert await _wait_status(db, job_id, "done", timeout=15) == "done"
    assert ("ok", {"actor": "u1"}) in CALLS


@pytest.mark.asyncio
async def test_native_stale_worker_cannot_finish(env, db):
    """Fencing: once the row belongs to another owner (reclaimed and
    finished elsewhere), the stale worker's heartbeat detects the lost
    lease and cancels its local dispatch task."""
    job_id = await _enqueue(env["jobs"], "test.slow@v1",
                            {"invoice_id": "i-s"})
    # Wait until claimed by our worker.
    for _ in range(100):
        row = await (await db.execute(
            "SELECT status FROM keelflow_jobs WHERE id=%s",
            (job_id,))).fetchone()
        if row[0] in ("claimed", "running"):
            break
        await asyncio.sleep(0.05)
    assert row[0] in ("claimed", "running")
    # Reclaim + finish elsewhere: the row is terminal and owned by
    # someone else — our running dispatch must not outlive it.
    await db.execute(
        "UPDATE keelflow_jobs SET status='done', claimed_by='other'"
        " WHERE id=%s", (job_id,))
    for _ in range(100):
        task = env["worker"]._inflight.get(job_id)
        if task is None or task.done():
            break
        await asyncio.sleep(0.05)
    task = env["worker"]._inflight.get(job_id)
    assert task is None or task.done()
