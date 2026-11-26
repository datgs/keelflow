"""Kernel-port contract suite (sqlite portability proof).

Runs the same dispatch-port semantics against every backend via
`--kernel=chancy|native|sqlite|both|all`:

- `push_ex`/`push_many_ex` make a dispatch row claimable on commit only.
- Claim is exclusive under concurrency; `run_at`/`scheduled_at` gates.
- Leases, reclaim, and fencing for the single-row backends.
- `unique_key` dedup is never dispatched twice.

The suite injects a stub dispatch (`conftest.kernel_contract_dispatch`)
so it exercises the KERNEL boundary only — `keelflow.engine.worker.dispatch`
and `JobService` remain Postgres-bound and are out of scope here.

Postgres backends skip cleanly when the DB is unavailable; sqlite runs
anywhere (temp file — :memory: cannot be shared across connections).
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio

from conftest import (
    CONTRACT_FUNC,
    DSN,
    KERNEL_DISPATCH_RECORD,
    declare_default_queue,
    make_worker,
    open_kernel,
)
from keelflow.engine.kernel import as_kernel

# ── backend fixtures ────────────────────────────────────────────────────


async def _pg_conn():
    import psycopg

    try:
        conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"postgres unavailable: {exc}")
    return conn


@pytest_asyncio.fixture
async def kenv(kernel_kind, tmp_path):
    """Yield {kind, kernel(raw), port, conn} for the selected backend."""
    KERNEL_DISPATCH_RECORD.clear()
    if kernel_kind == "sqlite":
        path = str(tmp_path / "kernel.db")
        async with open_kernel("sqlite", dsn=path) as kernel:
            db = await kernel._connect()  # busy_timeout included
            try:
                yield {"kind": "sqlite", "kernel": kernel,
                       "port": kernel, "conn": db}
            finally:
                await db.close()
        return

    conn = await _pg_conn()
    try:
        from keelflow.postgres.schema import apply_async

        await apply_async(conn)
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_step_runs CASCADE")
        if kernel_kind == "chancy":
            await conn.execute(
                "TRUNCATE keelflow_test_jobs, keelflow_test_workers,"
                " keelflow_test_queues CASCADE")
        async with open_kernel(kernel_kind) as kernel:
            if kernel_kind == "chancy":
                await declare_default_queue(kernel, kernel_kind,
                                            concurrency=8)
            yield {"kind": kernel_kind, "kernel": kernel,
                   "port": as_kernel(kernel), "conn": conn}
    finally:
        await conn.close()


# ── helpers ─────────────────────────────────────────────────────────────


def _spec(kenv, job_id, *, scheduled_at=None, unique_key=None):
    """Backend-neutral dispatch spec via kernel.job()."""
    return kenv["port"].job(
        func=CONTRACT_FUNC,
        queue="default",
        kwargs={"job_id": str(job_id), "step_run_id": None},
        max_attempts=3,
        scheduled_at=scheduled_at,
        unique_key=unique_key,
        meta={"job_id": str(job_id)},
    )


async def _insert_job(kenv, job_id, cursor=None) -> None:
    """Insert the engine row (native/sqlite own the only record)."""
    target = cursor if cursor is not None else kenv["conn"]
    if kenv["kind"] == "sqlite":
        await target.execute(
            "INSERT INTO keelflow_jobs (id, type, status, payload, context,"
            " run_at, created_at, updated_at)"
            " VALUES (?, 'test.job@v1', 'pending', '{}', '{}', ?, ?, ?)",
            (str(job_id), time.time(), time.time(), time.time()))
        return
    await target.execute(
        "INSERT INTO keelflow_jobs (id, type, status, payload, context)"
        " VALUES (%s, 'test.job@v1', 'pending', %s, %s)",
        (job_id, json.dumps({}), json.dumps({})))


async def _push(kenv, job_id, *, scheduled_at=None, unique_key=None) -> None:
    """Insert engine row + push spec inside one caller transaction."""
    conn = kenv["conn"]
    if kenv["kind"] == "sqlite":
        await _insert_job(kenv, job_id)
        await kenv["port"].push_ex(
            conn, _spec(kenv, job_id, scheduled_at=scheduled_at,
                        unique_key=unique_key))
        await conn.commit()
        return
    from psycopg.rows import dict_row

    async with (conn.transaction(),
                conn.cursor(row_factory=dict_row) as cur):
        await _insert_job(kenv, job_id, cursor=cur)
        await kenv["port"].push_ex(
            cur, _spec(kenv, job_id, scheduled_at=scheduled_at,
                       unique_key=unique_key))


async def _wait_called(want=1, timeout=10.0) -> list:
    for _ in range(int(timeout / 0.05)):
        if len(KERNEL_DISPATCH_RECORD) >= want:
            return list(KERNEL_DISPATCH_RECORD)
        await asyncio.sleep(0.05)
    return list(KERNEL_DISPATCH_RECORD)


async def _status(kenv, job_id):
    if kenv["kind"] == "sqlite":
        cur = await kenv["conn"].execute(
            "SELECT status FROM keelflow_jobs WHERE id=?", (str(job_id),))
        row = await cur.fetchone()
        return row[0] if row else None
    row = await (await kenv["conn"].execute(
        "SELECT status FROM keelflow_jobs WHERE id=%s",
        (job_id,))).fetchone()
    return row[0] if row else None


# ── shared contract cases ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_push_commit_dispatches(kenv):
    w = make_worker(kenv["kernel"], kenv["kind"], concurrency=4,
                    dispatch=_record)
    await w.start()
    try:
        job_id = uuid.uuid4()
        await _push(kenv, job_id)
        calls = await _wait_called(1, timeout=15)
        assert calls and str(job_id) in {str(c[0]) for c in calls}
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_push_rollback_no_dispatch(kenv):
    conn = kenv["conn"]
    job_id = uuid.uuid4()
    if kenv["kind"] == "sqlite":
        await conn.execute("BEGIN")
        await _insert_job(kenv, job_id)
        await kenv["port"].push_ex(conn, _spec(kenv, job_id))
        await conn.execute("ROLLBACK")
    else:
        from psycopg.rows import dict_row

        try:
            async with (conn.transaction(),
                        conn.cursor(row_factory=dict_row) as cur):
                await _insert_job(kenv, job_id, cursor=cur)
                await kenv["port"].push_ex(cur, _spec(kenv, job_id))
                raise _Abort()
        except _Abort:
            pass
    w = make_worker(kenv["kernel"], kenv["kind"], concurrency=2,
                    dispatch=_record)
    await w.start()
    try:
        await asyncio.sleep(1.0)
    finally:
        await w.stop(timeout=5)
    assert KERNEL_DISPATCH_RECORD == []


class _Abort(Exception):
    pass


@pytest.mark.asyncio
async def test_scheduled_future_not_claimed(kenv):
    w = make_worker(kenv["kernel"], kenv["kind"], concurrency=2,
                    dispatch=_record)
    await w.start()
    try:
        job_id = uuid.uuid4()
        future = (datetime.now(UTC) + timedelta(minutes=10)
                  if kenv["kind"] != "sqlite"
                  else time.time() + 600)
        await _push(kenv, job_id, scheduled_at=future)
        await asyncio.sleep(1.0)
        assert KERNEL_DISPATCH_RECORD == []
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_concurrent_workers_single_claim(kenv):
    w1 = make_worker(kenv["kernel"], kenv["kind"], concurrency=4,
                     dispatch=_record)
    w2 = make_worker(kenv["kernel"], kenv["kind"], concurrency=4,
                     dispatch=_record)
    await w1.start()
    await w2.start()
    try:
        ids = [uuid.uuid4() for _ in range(6)]
        for job_id in ids:
            await _push(kenv, job_id)
        calls = await _wait_called(6, timeout=15)
        got = sorted(str(c[0]) for c in calls)
        assert got == sorted(str(i) for i in ids)
        # each job invoked exactly once — no double claim
        assert len(calls) == len({str(c[0]) for c in calls})
    finally:
        await w1.stop(timeout=5)
        await w2.stop(timeout=5)


@pytest.mark.asyncio
async def test_push_many_dispatches_each(kenv):
    w = make_worker(kenv["kernel"], kenv["kind"], concurrency=8,
                    dispatch=_record)
    await w.start()
    try:
        ids = [uuid.uuid4() for _ in range(10)]
        conn = kenv["conn"]
        if kenv["kind"] == "sqlite":
            for job_id in ids:
                await _insert_job(kenv, job_id)
            await kenv["port"].push_many_ex(
                conn, [_spec(kenv, j) for j in ids])
            await conn.commit()
        else:
            from psycopg.rows import dict_row

            async with (conn.transaction(),
                        conn.cursor(row_factory=dict_row) as cur):
                for job_id in ids:
                    await _insert_job(kenv, job_id, cursor=cur)
                await kenv["port"].push_many_ex(
                    cur, [_spec(kenv, j) for j in ids])
        calls = await _wait_called(10, timeout=15)
        assert sorted(str(c[0]) for c in calls) == sorted(
            str(i) for i in ids)
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_unique_key_never_dispatched_twice(kenv):
    w = make_worker(kenv["kernel"], kenv["kind"], concurrency=4,
                    dispatch=_record)
    await w.start()
    try:
        key = f"uk-{uuid.uuid4().hex[:8]}"
        j1, j2 = uuid.uuid4(), uuid.uuid4()
        await _push(kenv, j1, unique_key=key)
        # second push with the same unique_key on a different row —
        # chancy no-ops the kernel insert; native/sqlite never claim the row
        await _push(kenv, j2, unique_key=key)
        calls = await _wait_called(1, timeout=15)
        await asyncio.sleep(0.5)
        assert len(KERNEL_DISPATCH_RECORD) == 1
        assert str(calls[0][0]) == str(j1)
    finally:
        await w.stop(timeout=5)


# ── single-row backends only (chancy owns lease/reclaim internals) ──────


def _single_row(kenv):
    if kenv["kind"] == "chancy":
        pytest.skip("lease/reclaim are chancy internals; covered by e2e")


@pytest.mark.asyncio
async def test_expired_lease_reclaimed(kenv):
    _single_row(kenv)
    w = make_worker(kenv["kernel"], kenv["kind"], concurrency=4,
                    lease_seconds=1.0, heartbeat_interval=0.2,
                    dispatch=_record)
    await w.start()
    try:
        job_id = uuid.uuid4()
        await _push(kenv, job_id)
        # dead owner's abandoned claim
        if kenv["kind"] == "sqlite":
            await kenv["conn"].execute(
                "UPDATE keelflow_jobs SET status='claimed',"
                " claimed_by='dead', lease_until=? WHERE id=?",
                (time.time() - 1, str(job_id)))
        else:
            await kenv["conn"].execute(
                "UPDATE keelflow_jobs SET status='claimed',"
                " claimed_by='dead',"
                " lease_until=now()-interval '1 second' WHERE id=%s",
                (job_id,))
        await kenv["conn"].commit()
        calls = await _wait_called(1, timeout=15)
        assert calls and str(calls[0][0]) == str(job_id)
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_fence_lost_lease_cancels_local(kenv):
    _single_row(kenv)
    job_id = uuid.uuid4()
    await _push(kenv, job_id)
    w = make_worker(kenv["kernel"], kenv["kind"], concurrency=4,
                    lease_seconds=1.0, heartbeat_interval=0.2,
                    dispatch=_slow)
    await w.start()
    try:
        for _ in range(100):
            st = await _status(kenv, job_id)
            if st in ("claimed", "running"):
                break
            await asyncio.sleep(0.05)
        assert st in ("claimed", "running")
        # finish elsewhere → heartbeat must cancel local dispatch
        if kenv["kind"] == "sqlite":
            await kenv["conn"].execute(
                "UPDATE keelflow_jobs SET status='done',"
                " claimed_by='other' WHERE id=?", (str(job_id),))
        else:
            await kenv["conn"].execute(
                "UPDATE keelflow_jobs SET status='done',"
                " claimed_by='other' WHERE id=%s", (job_id,))
        await kenv["conn"].commit()
        for _ in range(100):
            task = w._inflight.get(str(job_id))
            if task is None or task.done():
                break
            await asyncio.sleep(0.05)
        task = w._inflight.get(str(job_id))
        assert task is None or task.done()
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_non_queued_row_not_claimed(kenv):
    """A pending engine row that never went through push_ex
    (dispatch_queued=false) must never be claimed."""
    _single_row(kenv)
    job_id = uuid.uuid4()
    await _insert_job(kenv, job_id)  # no push → no flag
    await kenv["conn"].commit()
    w = make_worker(kenv["kernel"], kenv["kind"], concurrency=2,
                    dispatch=_record)
    await w.start()
    try:
        await asyncio.sleep(1.0)
        assert KERNEL_DISPATCH_RECORD == []
        assert await _status(kenv, job_id) == "pending"
    finally:
        await w.stop(timeout=5)


# ── dispatch stubs (injected; chancy resolves CONTRACT_FUNC by path) ────


async def _record(job_id, step_run_id=None, *,
                  execution_token=None):
    KERNEL_DISPATCH_RECORD.append((job_id, step_run_id))


async def _slow(job_id, step_run_id=None):
    KERNEL_DISPATCH_RECORD.append((job_id, step_run_id))
    await asyncio.sleep(60)
