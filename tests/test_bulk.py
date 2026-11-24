"""T8 contract tests: bulk chunk + row processing.

Requires Postgres at KEELFLOW_TEST_DSN.
"""
from __future__ import annotations

import asyncio
import os
from typing import ClassVar

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy, Queue, Worker

from keelflow.engine import worker as worker_mod
from keelflow.engine.bulk import (
    BulkJobHandler,
    BulkJobPayload,
    ChunkJobHandler,
)
from keelflow.engine.bulk import (
    register as register_bulk,
)
from keelflow.engine.enqueue import JobService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import (
    Actor,
    BatchRowResult,
    JobContext,
    JobTypeSpec,
    RetryPolicy,
)

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow_test"
)
CHANCY_PREFIX = "keelflow_test_"

APPLIED: list[str] = []


class _ImportH(BulkJobHandler):
    """App bulk handler: 'applies' each row into a side list + domain table."""

    async def process_row(self, ctx, row, tx):
        APPLIED.append(row["row_key"])
        await tx.execute(
            "INSERT INTO test_domain (row_key, job_id) VALUES (%s, %s)",
            (row["row_key"], ctx.extra["job_id"]),
        )
        return {"ok": True}


class _FlakyH(BulkJobHandler):
    """Fails on row_key 'bad' — for error-queue checks."""

    async def process_row(self, ctx, row, tx):
        if row["row_key"] == "bad":
            raise ValueError("invalid row")
        APPLIED.append(row["row_key"])
        return {"ok": True}


class _SeedFailH(BulkJobHandler):
    """Fails exactly on row_keys listed in `FAIL_KEYS` (set per test)."""

    FAIL_KEYS: ClassVar[set[str]] = set()

    async def process_row(self, ctx, row, tx):
        if row["row_key"] in _SeedFailH.FAIL_KEYS:
            raise ValueError(f"bad row {row['row_key']}")
        APPLIED.append(row["row_key"])
        return {"ok": True}


class _CancelGateH(BulkJobHandler):
    """Gates the FIRST row processed globally (any chunk): sets
    `started`, waits for `release`, then applies — so the test can
    commit a cancel flag before the next row's checkpoint."""

    instances: ClassVar[list[_CancelGateH]] = []
    gate_taken = False

    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        type(self).instances.append(self)

    async def process_row(self, ctx, row, tx):
        if not _CancelGateH.gate_taken:
            _CancelGateH.gate_taken = True
            self.started.set()
            await self.release.wait()
        APPLIED.append(row["row_key"])
        return {"ok": True}


class Payload(BulkJobPayload):
    pass


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
        await conn.execute("DROP TABLE IF EXISTS test_domain")
        await conn.execute(
            "CREATE TABLE test_domain (row_key text, job_id uuid)"
        )
        # fresh DB (CI): kernel tables must exist before we truncate them.
        async with Chancy(DSN, prefix=CHANCY_PREFIX) as _k:
            await _k.migrate()
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
        register_bulk(reg, retry=RetryPolicy(max_attempts=3, base_seconds=1))
        reg.register_job_type(
            JobTypeSpec(type="imp.rows@v1", input_schema=Payload, handler=_ImportH)
        )
        reg.register_job_type(
            JobTypeSpec(type="imp.flaky@v1", input_schema=Payload, handler=_FlakyH)
        )
        jobs = JobService(reg, kernel, PostgresStorage(DSN))
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        yield {"reg": reg, "jobs": jobs, "kernel": kernel}


async def _stage(db, job_id: str, keys: list[str]):
    for k in keys:
        await db.execute(
            "INSERT INTO keelflow_row_status (job_id, row_key, status, data)"
            " VALUES (%s, %s, 'pending', %s)",
            (job_id, k, '{"v": 1}'),
        )


async def _enqueue(env, type_name, payload: dict):
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        ref = await env["jobs"].builder(type_name).context(
            JobContext(actor=Actor.user("u1"), correlation_id="c-bulk")
        ).payload(payload).enqueue(cur)
    return ref.id


async def _wait(db, sql, want, timeout=20):
    for _ in range(int(timeout / 0.1)):
        row = await (await db.execute(sql)).fetchone()
        if row and row[0] == want:
            return row[0]
        await asyncio.sleep(0.1)
    return row[0] if row else None


@pytest.mark.asyncio
async def test_partition_exact_membership_and_shape(env, db):
    """Set-based partition must produce exactly ceil(n/size) chunks,
    assign each pending row to exactly one chunk honoring chunk_size,
    and create one engine+kernel job per chunk — same contract as the
    old per-chunk loop."""
    for n, size, want_chunks in ((0, 10, 0), (1, 10, 1), (10, 10, 1),
                                 (11, 10, 2), (25, 10, 3), (7, 3, 3)):
        keys = [f"k{i}" for i in range(n)]
        parent = await _enqueue(
            env, "imp.rows@v1", {"chunk_size": size, "atomic": "row"})
        await _stage(db, parent, keys)

        w = Worker(env["kernel"], shutdown_timeout=5)
        await w.start()
        try:
            assert await _wait(
                db,
                f"SELECT status FROM keelflow_jobs WHERE id='{parent}'",
                "done") == "done"
        finally:
            await w.stop(timeout=5)

        chunks = await (await db.execute(
            "SELECT id, idx FROM keelflow_chunks WHERE job_id=%s"
            " ORDER BY idx", (parent,))).fetchall()
        assert len(chunks) == want_chunks
        assert [c[1] for c in chunks] == list(range(want_chunks))
        # every pending row claimed exactly once; counts honor size.
        # row_disposition=archive moves done rows at fan-in, so
        # membership = live rows + archived rows per chunk.
        members = await (await db.execute(
            "SELECT chunk_id, count(*) FROM keelflow_row_status"
            " WHERE job_id=%s AND chunk_id IS NOT NULL GROUP BY chunk_id",
            (parent,))).fetchall()
        archived = await (await db.execute(
            "SELECT chunk_id, count(*) FROM keelflow_row_archive"
            " WHERE job_id=%s GROUP BY chunk_id", (parent,))).fetchall()
        live = dict(members)
        for m_chunk, cnt in archived:
            live[m_chunk] = live.get(m_chunk, 0) + cnt
        assert set(live) == {c[0] for c in chunks}
        assert sum(live.values()) == n
        for i, (cid, _) in enumerate(chunks):
            want_n = min(size, n - i * size)
            assert live[cid] == want_n
        # one engine child job + one kernel job per chunk
        kids = await (await db.execute(
            "SELECT count(*) FROM keelflow_jobs"
            " WHERE parent_job_id=%s", (parent,))).fetchone()
        assert kids[0] == want_chunks
        await db.execute(
            f"DELETE FROM {CHANCY_PREFIX}jobs")
        await db.execute(
            "DELETE FROM keelflow_row_status WHERE job_id=%s", (parent,))
        await db.execute(
            "DELETE FROM keelflow_row_archive WHERE job_id=%s", (parent,))
        await db.execute(
            "DELETE FROM keelflow_chunks WHERE job_id=%s", (parent,))
        await db.execute(
            "DELETE FROM keelflow_jobs WHERE id=%s"
            " OR parent_job_id=%s", (parent, parent))


@pytest.mark.asyncio
async def test_bulk_partitions_and_applies(env, db):
    keys = [f"row-{i}" for i in range(25)]
    parent = await _enqueue(
        env, "imp.rows@v1", {"chunk_size": 10, "atomic": "row"}
    )
    await _stage(db, parent, keys)

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        assert await _wait(
            db, f"SELECT status FROM keelflow_jobs WHERE id='{parent}'", "done"
        ) == "done"
    finally:
        await w.stop(timeout=5)

    chunks = await (
        await db.execute("SELECT status, count(*) FROM keelflow_chunks GROUP BY status")
    ).fetchall()
    assert dict(chunks) == {"done": 3}

    # default row_disposition='archive': done rows moved out of the live
    # table into keelflow_row_archive at fan-in
    rows = await (
        await db.execute("SELECT status, count(*) FROM keelflow_row_status GROUP BY status")
    ).fetchall()
    assert dict(rows) == {}

    archived = await (
        await db.execute(
            "SELECT count(*) FROM keelflow_row_archive WHERE job_id=%s",
            (parent,))
    ).fetchone()
    assert archived[0] == 25

    applied = await (
        await db.execute("SELECT count(*) FROM test_domain")
    ).fetchone()
    assert applied[0] == 25

    parent_row = await (
        await db.execute("SELECT status, result FROM keelflow_jobs WHERE id=%s", (parent,))
    ).fetchone()
    assert parent_row[0] == "done"
    assert parent_row[1]["chunks"] == {"done": 3}
    assert parent_row[1]["row_disposition"] == "archive"
    assert parent_row[1]["archived_rows"] == 25


@pytest.mark.asyncio
async def test_error_queue_marks_bad_row(env, db):
    keys = ["ok-1", "bad", "ok-2"]
    parent = await _enqueue(
        env, "imp.flaky@v1",
        {"chunk_size": 10, "atomic": "row", "row_disposition": "keep"}
    )
    await _stage(db, parent, keys)

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        assert await _wait(
            db, f"SELECT status FROM keelflow_jobs WHERE id='{parent}'", "done"
        ) == "done"
    finally:
        await w.stop(timeout=5)

    rows = await (
        await db.execute(
            "SELECT row_key, status, error FROM keelflow_row_status ORDER BY row_key"
        )
    ).fetchall()
    by_key = {r[0]: (r[1], r[2]) for r in rows}
    assert by_key["bad"][0] == "failed"
    assert "invalid row" in by_key["bad"][1]
    assert by_key["ok-1"][0] == "done"
    assert by_key["ok-2"][0] == "done"


@pytest.mark.asyncio
async def test_resume_pending_only_after_crash(env, db):
    """Row-atomic: a 'dead' chunk mid-run leaves done rows done; re-run
    processes only pending rows (must-prove #1, simulated kill)."""
    keys = [f"k-{i}" for i in range(5)]
    parent = await _enqueue(
        env, "imp.rows@v1",
        {"chunk_size": 5, "atomic": "row", "row_disposition": "keep"}
    )
    await _stage(db, parent, keys)

    # Build the chunk the way the partition step would, then mark 2 rows
    # done as if a worker had processed them before dying mid-chunk.
    import uuid as _uuid

    chunk_id = _uuid.uuid4()
    await db.execute(
        "INSERT INTO keelflow_chunks (id, job_id, idx, status) VALUES (%s,%s,0,'pending')",
        (chunk_id, parent),
    )
    await db.execute(
        "UPDATE keelflow_row_status SET chunk_id=%s WHERE job_id=%s",
        (chunk_id, parent),
    )
    rows = await (
        await db.execute(
            "SELECT id, row_key FROM keelflow_row_status WHERE chunk_id=%s"
            " ORDER BY row_key", (chunk_id,)
        )
    ).fetchall()
    for r in rows[:2]:
        await db.execute(
            "UPDATE keelflow_row_status SET status='done' WHERE id=%s", (r[0],)
        )
        await db.execute(
            "INSERT INTO test_domain (row_key, job_id) VALUES (%s, %s)",
            (r[1], parent),
        )

    # Now run the real chunk handler — must process only the 3 pending rows.
    chunk_handler = ChunkJobHandler()
    from keelflow.engine.bulk import ChunkPayload

    await chunk_handler.run(
        JobContext(actor=Actor.system(), extra={"job_id": str(parent)}),
        ChunkPayload(parent_job_id=str(parent), chunk_id=str(chunk_id)),
    )

    done_rows = await (
        await db.execute(
            "SELECT count(*) FROM keelflow_row_status WHERE chunk_id=%s"
            " AND status='done'", (chunk_id,)
        )
    ).fetchone()
    assert done_rows[0] == 5
    # only the 3 pending rows were processed (the 2 'pre-applied' were not re-run)
    assert APPLIED.count("k-0") == 0
    assert APPLIED.count("k-1") == 0
    assert APPLIED.count("k-2") == 1
    assert APPLIED.count("k-3") == 1
    assert APPLIED.count("k-4") == 1


# ── cooperative cancellation through a bulk job ────────────────────────────


def _reset_gate():
    APPLIED.clear()
    _CancelGateH.instances.clear()
    _CancelGateH.gate_taken = False


async def _wait_gate():
    """Wait for the gated row's handler instance (any instance — the
    parent-handler instance and the chunk's instance are distinct)."""
    for _ in range(200):
        for h in _CancelGateH.instances:
            if h.started.is_set():
                return h
        await asyncio.sleep(0.05)
    raise AssertionError("no gated process_row call after 10s")


@pytest.mark.asyncio
async def test_bulk_cancel_pre_partition_no_children(env, db):
    """Cancelling a still-pending parent: it terminalizes cancelled and
    spawns no chunks — dispatch's terminal skip settles cleanly."""
    APPLIED.clear()
    parent = await _enqueue(
        env, "imp.rows@v1", {"chunk_size": 5, "atomic": "row"})
    await _stage(db, parent, [f"row-{i}" for i in range(10)])
    perms = JobContext(actor=Actor.user("u1"),
                       permissions=frozenset({"job.cancel"}))
    assert await env["jobs"].cancel(parent, perms) is True
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        row = await _wait(
            db,
            f"SELECT status FROM keelflow_jobs WHERE id='{parent}'",
            "cancelled")
        assert row == "cancelled"
    finally:
        await w.stop(timeout=5)
    kids = await (await db.execute(
        "SELECT count(*) FROM keelflow_jobs WHERE parent_job_id=%s",
        (parent,))).fetchone()
    assert kids[0] == 0
    assert APPLIED == []


@pytest.mark.asyncio
async def test_bulk_cancel_cascades_and_checkpoints(env, db):
    """jobs.cancel(parent) on a partitioned job: the flag cascades to
    the running child; its next row checkpoint raises JobCancelled,
    the chunk settles, and fan-in terminalizes the parent 'cancelled'
    (not 'done'). Single chunk keeps the timing deterministic."""
    _reset_gate()
    env["reg"].register_job_type(JobTypeSpec(
        type="imp.gate@v1", input_schema=Payload, handler=_CancelGateH))
    parent = await _enqueue(
        env, "imp.gate@v1", {"chunk_size": 10, "atomic": "row"})
    await _stage(db, parent, [f"row-{i}" for i in range(10)])
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        gate = await _wait_gate()        # a chunk is running row 1
        perms = JobContext(actor=Actor.user("u1"),
                           permissions=frozenset({"job.cancel"}))
        assert await env["jobs"].cancel(parent, perms) is True
        kids = await (await db.execute(
            "SELECT status, cancel_requested FROM keelflow_jobs"
            " WHERE parent_job_id=%s", (parent,))).fetchall()
        assert all(flag for _, flag in kids)     # cascade landed
        gate.release.set()
        await _wait(
            db,
            f"SELECT status FROM keelflow_jobs WHERE id='{parent}'",
            "cancelled", timeout=30)
    finally:
        await w.stop(timeout=5)
    parent_row = await (await db.execute(
        "SELECT status, cancel_requested FROM keelflow_jobs WHERE id=%s",
        (parent,))).fetchone()
    assert parent_row == ("cancelled", True)
    cancelled_chunks = await (await db.execute(
        "SELECT count(*) FROM keelflow_chunks WHERE status='cancelled'"
    )).fetchone()
    assert cancelled_chunks[0] == 1
    assert len(APPLIED) == 1             # only the gated row completed


@pytest.mark.asyncio
async def test_bulk_chunk_checkpoint_stops_mid_run(env, db):
    """A running chunk honours a flag set on it alone: rows processed
    before the flag commits keep their work; remaining rows stay
    pending. The unrequested parent terminalizes 'cancelled' — its
    rows are not fully processed, so the pass is not a success."""
    _reset_gate()
    env["reg"].register_job_type(JobTypeSpec(
        type="imp.gate@v1", input_schema=Payload, handler=_CancelGateH))
    parent = await _enqueue(
        env, "imp.gate@v1", {"chunk_size": 10, "atomic": "row"})
    await _stage(db, parent, [f"row-{i}" for i in range(10)])
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        gate = await _wait_gate()
        chunk_id = await (await db.execute(
            "SELECT chunk_id FROM keelflow_row_status"
            " WHERE job_id=%s LIMIT 1", (parent,))).fetchone()
        child = await (await db.execute(
            "SELECT id FROM keelflow_jobs"
            " WHERE parent_job_id=%s AND status='running'",
            (parent,))).fetchone()
        await db.execute(
            "UPDATE keelflow_jobs SET cancel_requested=true WHERE id=%s",
            (child[0],))
        gate.release.set()
        for _ in range(200):
            row = await (await db.execute(
                "SELECT status FROM keelflow_jobs WHERE id=%s",
                (child[0],))).fetchone()
            if row[0] == "cancelled":
                break
            await asyncio.sleep(0.05)
        assert row[0] == "cancelled"
    finally:
        await w.stop(timeout=5)
    chunk = await (await db.execute(
        "SELECT status FROM keelflow_chunks WHERE id=%s",
        (chunk_id[0],))).fetchone()
    assert chunk[0] == "cancelled"
    assert len(APPLIED) == 1          # only the gated row completed
    parent_row = await (await db.execute(
        "SELECT status FROM keelflow_jobs WHERE id=%s", (parent,)
    )).fetchone()
    # child-only cancellation leaves rows unprocessed → incomplete,
    # not an ordinary success even though the parent never requested it
    assert parent_row[0] == "cancelled"


# ── randomized row-failure matrix ───────────────────────────────────────────

import random


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(10))
async def test_bulk_random_row_failure_matrix(env, db, seed):
    """N rows, random subset fails per-row; chunk_size random. Row
    failures are error-queue data — chunks and parent stay 'done',
    `rows_failed`/`rows_done` counts match the seed exactly, done rows
    are archived out of the live table, failed rows keep their error."""
    rng = random.Random(seed)
    n_rows = rng.randint(10, 40)
    keys = [f"r-{i}" for i in range(n_rows)]
    fail_keys = set(rng.sample(keys, rng.randint(0, n_rows // 3)))
    _SeedFailH.FAIL_KEYS = fail_keys
    APPLIED.clear()
    env["reg"].register_job_type(JobTypeSpec(
        type="imp.seedfail@v1", input_schema=Payload,
        handler=_SeedFailH))
    chunk_size = rng.randint(1, max(1, n_rows // 2))
    parent = await _enqueue(
        env, "imp.seedfail@v1",
        {"chunk_size": chunk_size, "atomic": "row"})
    await _stage(db, parent, keys)
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        row = await _wait(
            db,
            f"SELECT status FROM keelflow_jobs WHERE id='{parent}'",
            "done", timeout=60)
        assert row == "done"
    finally:
        await w.stop(timeout=5)

    # chunks all terminal-done — row failures don't fail chunks
    chunks = await (await db.execute(
        "SELECT status, count(*) FROM keelflow_chunks GROUP BY status")
    ).fetchall()
    assert dict(chunks) == {"done": sum(dict(chunks).values())}

    # parent summary counts match the seed
    summary = await (await db.execute(
        "SELECT result->>'rows_done', result->>'rows_failed'"
        " FROM keelflow_jobs WHERE id=%s", (parent,))).fetchone()
    assert int(summary[0]) == n_rows - len(fail_keys)
    assert int(summary[1]) == len(fail_keys)

    # live table keeps only the error queue; done rows are archived
    live = await (await db.execute(
        "SELECT count(*) FROM keelflow_row_status WHERE job_id=%s",
        (parent,))).fetchone()
    assert live[0] == len(fail_keys)
    errors = await (await db.execute(
        "SELECT count(*) FROM keelflow_row_status"
        " WHERE job_id=%s AND status='failed' AND error IS NOT NULL",
        (parent,))).fetchone()
    assert errors[0] == len(fail_keys)
    archived = await (await db.execute(
        "SELECT count(*) FROM keelflow_row_archive WHERE job_id=%s",
        (parent,))).fetchone()
    assert archived[0] == n_rows - len(fail_keys)
    assert set(APPLIED) == set(keys) - fail_keys


# ── chunk-level batch callback (process_batch) ──────────────────────────────


class _BatchH(BulkJobHandler):
    """Batch handler: upserts rows set-based; fails rows keyed in FAIL_KEYS."""

    FAIL_KEYS: ClassVar[set[str]] = set()
    CALLS: ClassVar[int] = 0
    RAISE_ONCE: ClassVar[bool] = False

    async def process_row(self, ctx, row, tx):  # pragma: no cover - fallback
        raise AssertionError("process_row must not run in batch mode")

    async def process_batch(self, ctx, rows, tx):
        _BatchH.CALLS += 1
        if _BatchH.RAISE_ONCE and _BatchH.CALLS == 1:
            raise RuntimeError("transient batch failure")
        out = []
        for r in rows:
            if r["row_key"] in _BatchH.FAIL_KEYS:
                out.append(BatchRowResult(row_id=r["id"],
                                          error=f"bad {r['row_key']}"))
                continue
            APPLIED.append(r["row_key"])
            await tx.execute(
                "INSERT INTO test_domain (row_key, job_id) VALUES (%s, %s)",
                (r["row_key"], ctx.extra["job_id"]),
            )
            out.append(BatchRowResult(row_id=r["id"], outputs={"ok": True}))
        return out


class _MalformedH(BulkJobHandler):
    """Returns a result list with a missing row (malformed → rollback+retry)."""

    MODE: ClassVar[str] = "missing"   # missing | duplicate | unknown | type

    async def process_row(self, ctx, row, tx):  # pragma: no cover
        raise AssertionError("process_row must not run in batch mode")

    async def process_batch(self, ctx, rows, tx):
        if _MalformedH.MODE == "missing":
            return [BatchRowResult(row_id=r["id"]) for r in rows[:-1]]
        if _MalformedH.MODE == "duplicate":
            return [BatchRowResult(row_id=r["id"]) for r in rows] + [
                BatchRowResult(row_id=rows[0]["id"])]
        if _MalformedH.MODE == "unknown":
            return [BatchRowResult(row_id=r["id"]) for r in rows] + [
                BatchRowResult(row_id=999999)]
        return [{"row_id": r["id"]} for r in rows]      # wrong type


class _BatchOnlyH(BulkJobHandler):
    """Implements only process_batch — must be rejected with atomic='row'."""

    async def process_batch(self, ctx, rows, tx):  # pragma: no cover
        return [BatchRowResult(row_id=r["id"]) for r in rows]


async def _run_parent(env, db, type_name, payload, keys, timeout=30):
    parent = await _enqueue(env, type_name, payload)
    await _stage(db, parent, keys)
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        status = await _wait(
            db, f"SELECT status FROM keelflow_jobs WHERE id='{parent}'",
            "done", timeout=timeout)
    finally:
        await w.stop(timeout=5)
    return parent, status


@pytest.mark.asyncio
async def test_batch_commits_all_rows_and_error_queue(env, db):
    """atomic='chunk' + process_batch: whole chunk one TX; explicit row
    errors land in the queue while siblings commit; duplicate row_key
    works (correlation by engine row id)."""
    _BatchH.FAIL_KEYS = {"dup", "bad"}
    _BatchH.CALLS = 0
    _BatchH.RAISE_ONCE = False
    APPLIED.clear()
    env["reg"].register_job_type(JobTypeSpec(
        type="imp.batch@v1", input_schema=Payload, handler=_BatchH))
    keys = ["ok-1", "bad", "dup", "dup", "ok-2"]     # dup row_key x2
    parent, status = await _run_parent(
        env, db, "imp.batch@v1",
        {"chunk_size": 10, "atomic": "chunk", "row_disposition": "keep"},
        keys)
    assert status == "done"

    rows = await (await db.execute(
        "SELECT row_key, status, error FROM keelflow_row_status"
        " ORDER BY id")).fetchall()
    by = {}
    for r in rows:
        by.setdefault(r[0], []).append((r[1], r[2]))
    assert [s for s, _ in by["ok-1"]] == ["done"]
    assert [s for s, _ in by["ok-2"]] == ["done"]
    assert all(s == "failed" and "bad" in e for s, e in by["bad"])
    assert all(s == "failed" for s, _ in by["dup"])
    assert len(by["dup"]) == 2            # both dup rows resolved by id
    assert sorted(APPLIED) == ["ok-1", "ok-2"]

    # one batch call for the whole chunk; chunk + parent done
    assert _BatchH.CALLS == 1
    chunks = await (await db.execute(
        "SELECT status, count(*) FROM keelflow_chunks"
        " WHERE job_id=%s GROUP BY status", (parent,))).fetchall()
    assert dict(chunks) == {"done": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["missing", "duplicate", "unknown", "type"])
async def test_batch_malformed_result_rolls_back_and_retries(env, db, mode):
    """Malformed result → whole chunk rolls back (no row/domain state),
    chunk retried; after exhausting attempts the chunk fails via DLQ
    path and the parent fails — no partial commits."""
    _MalformedH.MODE = mode
    APPLIED.clear()
    env["reg"].register_job_type(JobTypeSpec(
        type="imp.malformed@v1", input_schema=Payload, handler=_MalformedH,
        retry=RetryPolicy(max_attempts=1, base_seconds=1)))
    parent, status = await _run_parent(
        env, db, "imp.malformed@v1",
        {"chunk_size": 10, "atomic": "chunk", "row_disposition": "keep"},
        ["a", "b", "c"])
    assert status == "failed"

    # nothing committed: rows still pending, no domain writes
    rows = await (await db.execute(
        "SELECT status, count(*) FROM keelflow_row_status"
        " WHERE job_id=%s GROUP BY status", (parent,))).fetchall()
    assert dict(rows) == {"pending": 3}
    applied = await (await db.execute(
        "SELECT count(*) FROM test_domain")).fetchone()
    assert applied[0] == 0
    chunk = await (await db.execute(
        "SELECT status FROM keelflow_chunks WHERE job_id=%s",
        (parent,))).fetchone()
    assert chunk[0] == "failed"


@pytest.mark.asyncio
async def test_batch_transient_exception_retries_then_succeeds(env, db):
    """Callback exception on attempt 1 → rollback + retry; attempt 2
    commits all rows exactly once."""
    _BatchH.FAIL_KEYS = set()
    _BatchH.CALLS = 0
    _BatchH.RAISE_ONCE = True
    APPLIED.clear()
    env["reg"].register_job_type(JobTypeSpec(
        type="imp.batchretry@v1", input_schema=Payload, handler=_BatchH,
        retry=RetryPolicy(max_attempts=3, base_seconds=1)))
    _parent, status = await _run_parent(
        env, db, "imp.batchretry@v1",
        {"chunk_size": 10, "atomic": "chunk", "row_disposition": "keep"},
        ["x", "y", "z"])
    assert status == "done"
    assert _BatchH.CALLS == 2            # failed once, retried once
    assert sorted(APPLIED) == ["x", "y", "z"]
    applied = await (await db.execute(
        "SELECT count(*) FROM test_domain")).fetchone()
    assert applied[0] == 3               # retried TX did not duplicate


@pytest.mark.asyncio
async def test_batch_only_handler_rejected_with_row_atomic(env, db):
    """Batch-only handler + atomic='row' → config error, chunk fails."""
    APPLIED.clear()
    env["reg"].register_job_type(JobTypeSpec(
        type="imp.batchonly@v1", input_schema=Payload, handler=_BatchOnlyH,
        retry=RetryPolicy(max_attempts=1, base_seconds=1)))
    parent, status = await _run_parent(
        env, db, "imp.batchonly@v1",
        {"chunk_size": 10, "atomic": "row", "row_disposition": "keep"},
        ["a", "b"])
    assert status == "failed"
    chunk = await (await db.execute(
        "SELECT status FROM keelflow_chunks WHERE job_id=%s",
        (parent,))).fetchone()
    assert chunk[0] == "failed"
    rows = await (await db.execute(
        "SELECT status, count(*) FROM keelflow_row_status"
        " WHERE job_id=%s GROUP BY status", (parent,))).fetchall()
    assert dict(rows) == {"pending": 2}
