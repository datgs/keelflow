"""Reliability pass proofs — real-worker fault injection on Postgres.

Covers the follow-up hardening plan (Native + Chancy):

- A: claim ownership fencing — a dispatch whose claim was reclaimed
  before it ran can never mint a token over the newer claim; repeated
  crash-abandoned claims exhaust the attempt budget and settle
  (failed + DLQ) instead of looping forever.
- B: graceful stop renews in-flight leases through the drain window
  (a sibling must observe nothing reappable), and an idle claim loop
  waits for wakeup/poll instead of busy-spinning.
- C: both workers keep consuming after their DB connections are
  severed (auto-reconnect), and stopping leaks no LISTEN connections.
- D: concurrent duplicate enqueue while running stays one live job /
  one execution, and `pending_due` surfaces an alive-but-not-consuming
  pipeline.
"""

from __future__ import annotations

import asyncio
import logging

import psycopg
import pytest
import pytest_asyncio
from pydantic import BaseModel

from conftest import DSN, declare_default_queue, make_worker, open_kernel
from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import (
    Actor,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
)

log = logging.getLogger(__name__)

CALLS: dict[str, int] = {}
EFFECTS: list[str] = []


class P(BaseModel):
    x: int = 0


class OkH(JobHandler):
    async def run(self, ctx, payload: P):
        EFFECTS.append("ok")
        return JobResult(status="done", outputs={"n": payload.x + 1})


class StallOnceH(JobHandler):
    """First dispatch blocks on `gate` (set per test); later dispatches
    complete immediately."""

    gate: asyncio.Event | None = None

    async def run(self, ctx, payload: P):
        n = CALLS["stall"] = CALLS.get("stall", 0) + 1
        if n == 1 and type(self).gate is not None:
            await type(self).gate.wait()
        EFFECTS.append(f"stall-{n}")
        return JobResult(status="done")


class DrainH(JobHandler):
    entered: asyncio.Event | None = None
    release: asyncio.Event | None = None

    async def run(self, ctx, payload: P):
        type(self).entered.set()
        await type(self).release.wait()
        EFFECTS.append("drain")
        return JobResult(status="done")


@pytest_asyncio.fixture
async def env(kernel_kind):
    if kernel_kind == "sqlite":
        pytest.skip("reliability proofs are Postgres-kernel scoped")
    try:
        conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox,"
            " keelflow_outbox_deliveries CASCADE"
        )
    conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    async with conn, open_kernel(kernel_kind) as kernel:
        await declare_default_queue(kernel, kernel_kind, concurrency=4)
        reg = InMemoryRegistry()
        reg.register_job_type(
            JobTypeSpec(
                type="rel.ok@v1",
                input_schema=P,
                handler=OkH,
                retry=RetryPolicy(max_attempts=3, base_seconds=0.05, jitter=False),
            )
        )
        reg.register_job_type(
            JobTypeSpec(
                type="rel.stall@v1",
                input_schema=P,
                handler=StallOnceH,
                retry=RetryPolicy(max_attempts=1, base_seconds=0.05, jitter=False),
            )
        )
        reg.register_job_type(
            JobTypeSpec(type="rel.drain@v1", input_schema=P, handler=DrainH)
        )
        storage = PostgresStorage(DSN)
        jobs = JobService(reg, kernel, storage)
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel, storage=storage)
        CALLS.clear()
        EFFECTS.clear()
        StallOnceH.gate = None
        DrainH.entered = DrainH.release = None
        yield {
            "reg": reg,
            "jobs": jobs,
            "kernel": kernel,
            "storage": storage,
            "db": conn,
            "kind": kernel_kind,
        }


async def _enqueue(env, type_name="rel.ok@v1", **kwargs):
    sess = await env["storage"].connect()
    try:
        async with sess.begin() as uow:
            b = (
                env["jobs"]
                .builder(type_name)
                .payload(kwargs.pop("payload", {}))
                .context(JobContext(actor=Actor.user("u")))
            )
            for k, v in kwargs.items():
                b = getattr(b, k)(v)
            ref = await b.enqueue(uow)
    finally:
        await sess.close()
    return ref


async def _wait_status(env, job_id, want, timeout=15):
    for _ in range(int(timeout * 10)):
        row = await (
            await env["db"].execute(
                "SELECT status, attempts FROM keelflow_jobs WHERE id=%s", (job_id,)
            )
        ).fetchone()
        if row and row[0] == want:
            return row
        await asyncio.sleep(0.1)
    raise AssertionError(f"job {job_id} never reached {want}: {row}")


# ── A. claim ownership + bounded crash recovery ─────────────────────────


@pytest.mark.asyncio
async def test_stale_dispatch_cannot_steal_reclaimed_claim(env):
    """Claim gap window: claim1's dispatch is stalled before
    mark_running; the lease lapses, a sibling reclaims (token2), and
    when claim1's dispatch finally runs it must lose — its token is
    dead, so it can never mint its own token over claim2."""
    if env["kind"] != "native":
        pytest.skip("kernel-owned claims are a native mechanism")
    ref = await _enqueue(env)
    w = make_worker(env["kernel"], env["kind"], lease_seconds=1)
    await w._open()

    ran: list[tuple] = []
    spy_release = asyncio.Event()
    real = worker_mod.dispatch

    async def spy(job_id, step_run_id=None, *, execution_token=None):
        ran.append((job_id, execution_token))
        if len(ran) == 1:  # claim1's dispatch stalls
            await spy_release.wait()
        return await real(job_id, step_run_id, execution_token=execution_token)

    w._dispatch = spy
    assert await w._claim_once(1) == 1
    token1 = ran[0][1] if ran else None
    await asyncio.sleep(0.2)  # claim1 dispatch is stalled
    task1 = next(iter(w._inflight.values()))
    token1 = task1 and ran[0][1]

    # claim1 abandoned pre-dispatch → row reset + token retired
    await env["db"].execute(
        "UPDATE keelflow_jobs SET status='pending', claimed_by=NULL,"
        " lease_until=NULL, execution_token=NULL WHERE id=%s",
        (ref.id,),
    )
    assert await w._claim_once(1) == 1  # claim2 mints token2
    await asyncio.wait_for(w._inflight[ref.id], timeout=15)
    assert ran[1][1] != token1  # fresh token per claim

    spy_release.set()
    await task1  # stale dispatch loses
    assert len(ran) == 2
    row = await _wait_status(env, ref.id, "done")
    assert row[1] == 2  # both claims consumed budget
    assert EFFECTS == ["ok"]  # handler ran once
    await w._conn.close()


@pytest.mark.asyncio
async def test_repeated_crash_abandoned_claims_exhaust_to_dlq(env):
    """Each reclaim consumes one attempt. With max_attempts=1, the
    second claim's dispatch observes attempts > max and settles the job
    permanently — failed + DLQ in one unit — without re-running the
    handler."""
    if env["kind"] != "native":
        pytest.skip("kernel-owned claims are a native mechanism")
    StallOnceH.gate = asyncio.Event()
    ref = await _enqueue(env, "rel.stall@v1")
    w = make_worker(env["kernel"], env["kind"], lease_seconds=1)
    await w._open()

    assert await w._claim_once(1) == 1  # claim1 → attempts=1
    for _ in range(50):
        if CALLS.get("stall"):
            break
        await asyncio.sleep(0.05)
    assert CALLS["stall"] == 1
    task1 = w._inflight[ref.id]

    # crash-abandon: claim1 never finishes — row reset, token retired
    await env["db"].execute(
        "UPDATE keelflow_jobs SET status='pending', claimed_by=NULL,"
        " lease_until=NULL, execution_token=NULL WHERE id=%s",
        (ref.id,),
    )
    assert await w._claim_once(1) == 1  # claim2 → attempts=2 > max=1
    await asyncio.wait_for(w._inflight[ref.id], timeout=15)

    row = await _wait_status(env, ref.id, "failed")
    assert row[1] == 2
    dlq = await (
        await env["db"].execute(
            "SELECT type, attempts FROM keelflow_dlq WHERE id=%s", (ref.id,)
        )
    ).fetchone()
    assert dlq == ("rel.stall@v1", 2)
    StallOnceH.gate.set()
    await task1  # stale finish is fenced
    row = await (
        await env["db"].execute(
            "SELECT status FROM keelflow_jobs WHERE id=%s", (ref.id,)
        )
    ).fetchone()
    assert row[0] == "failed"  # settle survives the straggler
    await w._conn.close()


@pytest.mark.asyncio
async def test_chancy_redelivery_settles_exhausted_claim(env):
    """Engine-owned claim path: every dispatch (including a chancy
    recovery redelivery) consumes an attempt at mark_running. A second
    dispatch while the first is still running owns the row, sees the
    budget spent, and settles failed + DLQ."""
    StallOnceH.gate = asyncio.Event()
    ref = await _enqueue(env, "rel.stall@v1")
    task1 = asyncio.create_task(worker_mod.dispatch(ref.id))
    for _ in range(50):
        if CALLS.get("stall"):
            break
        await asyncio.sleep(0.05)
    assert CALLS["stall"] == 1  # first dispatch is mid-handler

    await worker_mod.dispatch(ref.id)  # recovery-style redelivery

    row = await _wait_status(env, ref.id, "failed")
    assert row[1] == 2
    dlq = await (
        await env["db"].execute("SELECT type FROM keelflow_dlq WHERE id=%s", (ref.id,))
    ).fetchone()
    assert dlq[0] == "rel.stall@v1"
    StallOnceH.gate.set()
    await task1
    await asyncio.sleep(0.3)
    row = await (
        await env["db"].execute(
            "SELECT status FROM keelflow_jobs WHERE id=%s", (ref.id,)
        )
    ).fetchone()
    assert row[0] == "failed"  # stale finish fenced


# ── B. lease-safe drain + paced idle polling ────────────────────────────


@pytest.mark.asyncio
async def test_graceful_stop_renews_lease_through_drain(env):
    """stop() drains in-flight work while maintenance keeps renewing
    its lease — a sibling reaper must observe nothing to reclaim for
    the whole drain window, or it would double-run the job."""
    if env["kind"] != "native":
        pytest.skip("lease drain is a native mechanism")
    DrainH.entered = asyncio.Event()
    DrainH.release = asyncio.Event()
    ref = await _enqueue(env, "rel.drain@v1")
    w = make_worker(
        env["kernel"],
        env["kind"],
        concurrency=1,
        lease_seconds=1,
        heartbeat_interval=0.2,
    )
    await w.start()
    await asyncio.wait_for(DrainH.entered.wait(), timeout=15)

    async def _hold_release():
        await asyncio.sleep(1.6)  # > lease: renewal must hold
        DrainH.release.set()

    holding = asyncio.create_task(_hold_release())

    # sibling reaper shares no state with w — only the row's lease
    wB = make_worker(env["kernel"], env["kind"])
    await wB._open()
    try:
        stop_task = asyncio.create_task(w.stop(timeout=30))
        await asyncio.sleep(1.4)  # drain underway, lease overdue
        assert await wB._reap() == 0, (
            "lease was not renewed during drain — sibling would steal"
        )
        await asyncio.wait_for(stop_task, timeout=30)
    finally:
        await holding
        await wB._conn.close()

    row = await _wait_status(env, ref.id, "done")
    assert row[0] == "done"
    assert EFFECTS == ["drain"]


@pytest.mark.asyncio
async def test_idle_claim_loop_paces_polling(env):
    """Idle worker must wait for a wakeup or the poll interval — not
    re-run the claim query in a hot loop."""
    if env["kind"] not in ("native", "sqlite"):
        pytest.skip("claim pacing is a native mechanism")
    w = make_worker(env["kernel"], env["kind"], poll_interval=0.1)
    calls = 0
    real = w._claim_once

    async def counting(limit):
        nonlocal calls
        calls += 1
        return await real(limit)

    w._claim_once = counting
    await w.start()
    try:
        await asyncio.sleep(0.65)
        assert calls <= 12, f"busy-poll: {calls} claims in 0.65s idle"
    finally:
        await w.stop(timeout=5)


# ── C. DB recovery ──────────────────────────────────────────────────────


async def _listener_pids(conn):
    """Worker claim/notify connections — identified by application_name
    (LISTEN is not retained as pg_stat_activity.query)."""
    rows = await (
        await conn.execute(
            "SELECT pid FROM pg_stat_activity"
            " WHERE datname=current_database()"
            "   AND application_name = 'keelflow-worker-claim'"
        )
    ).fetchall()
    return {r[0] for r in rows}


@pytest.mark.asyncio
async def test_native_worker_recovers_after_connection_drop(env):
    """Terminate the worker's claim/LISTEN connection: the loops log
    the failure, drop the dead conn, reconnect, and keep consuming —
    including LISTEN, so the next job is claimed on wakeup."""
    if env["kind"] != "native":
        pytest.skip("native claim-loop reconnect proof")
    w = make_worker(env["kernel"], env["kind"], poll_interval=0.2)
    await w.start()
    try:
        before = await _listener_pids(env["db"])
        assert before, "worker LISTEN connection not found"
        for pid in before:
            await env["db"].execute("SELECT pg_terminate_backend(%s)", (pid,))
        await asyncio.sleep(1.0)  # let the loops notice + heal

        ref = await _enqueue(env)
        await _wait_status(env, ref.id, "done", timeout=15)
        assert EFFECTS == ["ok"]
        # reconnect re-established LISTEN for the next wakeup
        await asyncio.sleep(0.5)
        assert await _listener_pids(env["db"])
    finally:
        await w.stop(timeout=10)


@pytest.mark.asyncio
async def test_chancy_pipeline_recovers_after_connection_drop(env):
    """Terminate every backend except ours (worker pool, dispatcher):
    the pipeline must still complete new work — either chancy heals its
    own connections or our supervision replaces the worker."""
    if env["kind"] != "chancy":
        pytest.skip("chancy recovery proof")
    import chancy

    stop = asyncio.Event()
    kernel = env["kernel"]
    beat = asyncio.ensure_future(
        worker_mod.worker_heartbeat_loop(env["storage"], interval=0.5, stop=stop)
    )

    async def supervise():
        while not stop.is_set():
            w = chancy.Worker(kernel, shutdown_timeout=5)
            try:
                await w.start()
                await w.wait_for_shutdown()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - restart on any death
                await asyncio.sleep(0.3)  # test-scaled restart delay
                continue
            finally:
                try:
                    await w.stop()
                except Exception:  # stopping a dead worker is best-effort
                    log.debug("test worker stop failed", exc_info=True)
            break

    sup = asyncio.ensure_future(supervise())
    try:
        await asyncio.sleep(1.5)  # worker booted, heartbeat up
        ref = await _enqueue(env)
        await _wait_status(env, ref.id, "done", timeout=20)
        EFFECTS.clear()

        # one-way loss: every client connection dies at once
        await env["db"].execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname=current_database() AND pid<>pg_backend_pid()"
        )
        await asyncio.sleep(2.0)  # outage window

        ref2 = await _enqueue(env)
        await _wait_status(env, ref2.id, "done", timeout=30)
        assert EFFECTS == ["ok"]
    finally:
        stop.set()
        sup.cancel()  # wait_for_shutdown ignores
        # stop — cancel is the abort
        await asyncio.gather(sup, beat, return_exceptions=True)


@pytest.mark.asyncio
async def test_no_listener_connection_leak_after_stop(env):
    """Stopping the worker must close its LISTEN connection — no
    leaked backends in pg_stat_activity."""
    if env["kind"] != "native":
        pytest.skip("native LISTEN lifecycle proof")
    baseline = await _listener_pids(env["db"])
    w = make_worker(env["kernel"], env["kind"])
    await w.start()
    await asyncio.sleep(0.3)
    assert len(await _listener_pids(env["db"])) == len(baseline) + 1
    await w.stop(timeout=10)
    await asyncio.sleep(0.5)
    assert await _listener_pids(env["db"]) == baseline


# ── D. dedup + consumption observability ────────────────────────────────


@pytest.mark.asyncio
async def test_concurrent_duplicate_enqueue_single_execution(env):
    """Two concurrent enqueues racing the same deduplication key while
    the job is mid-flight: one job row, one kernel delivery, one
    handler execution."""
    StallOnceH.gate = asyncio.Event()
    ref = await _enqueue(env, "rel.stall@v1", deduplication_key="dup-race")

    async def enqueue_dup():
        sess = await env["storage"].connect()
        try:
            async with sess.begin() as uow:
                return (
                    await env["jobs"]
                    .builder("rel.stall@v1")
                    .payload({})
                    .context(JobContext(actor=Actor.user("u")))
                    .deduplication_key("dup-race")
                    .enqueue(uow)
                )
        finally:
            await sess.close()

    w = make_worker(env["kernel"], env["kind"])
    await w.start()
    try:
        for _ in range(50):  # job is mid-handler now
            if CALLS.get("stall"):
                break
            await asyncio.sleep(0.05)
        assert CALLS.get("stall") == 1
        dup = await asyncio.gather(enqueue_dup(), enqueue_dup())
        assert dup[0].id == ref.id == dup[1].id
        StallOnceH.gate.set()
        await _wait_status(env, ref.id, "done", timeout=15)
    finally:
        await w.stop(timeout=10)
    assert EFFECTS == ["stall-1"]
    rows = await (
        await env["db"].execute(
            "SELECT count(*) FROM keelflow_jobs WHERE unique_key='dup-race'"
        )
    ).fetchone()
    assert rows[0] == 1


@pytest.mark.asyncio
async def test_pending_due_tracks_claimable_backlog(env):
    """pending_due counts only pending rows whose run_at has arrived —
    the consumption signal. Scheduled-future pending rows are backlog
    but not due."""
    from datetime import UTC, datetime, timedelta

    await _enqueue(env)  # due now
    await _enqueue(env, run_at=datetime.now(UTC) + timedelta(hours=1))
    sess = await env["storage"].connect()
    try:
        async with sess.begin() as uow:
            snap = await env["storage"].ops_metrics_snapshot(uow)
    finally:
        await sess.close()
    assert snap["queue_depth"] == 2
    assert snap["pending_due"] == 1
    assert 0 <= snap["oldest_pending_s"] < 60
