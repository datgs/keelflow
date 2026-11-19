"""T8 contract tests: bulk chunk + row processing.

Requires Postgres at KEELFLOW_TEST_DSN.
"""
from __future__ import annotations

import asyncio
import os

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
from keelflow.sdk import Actor, JobContext, JobTypeSpec

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
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
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_dlq CASCADE"
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
        register_bulk(reg)
        reg.register_job_type(
            JobTypeSpec(type="imp.rows@v1", input_schema=Payload, handler=_ImportH)
        )
        reg.register_job_type(
            JobTypeSpec(type="imp.flaky@v1", input_schema=Payload, handler=_FlakyH)
        )
        jobs = JobService(reg, kernel)
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

    rows = await (
        await db.execute("SELECT status, count(*) FROM keelflow_row_status GROUP BY status")
    ).fetchall()
    assert dict(rows) == {"done": 25}

    applied = await (
        await db.execute("SELECT count(*) FROM test_domain")
    ).fetchone()
    assert applied[0] == 25

    parent_row = await (
        await db.execute("SELECT status, result FROM keelflow_jobs WHERE id=%s", (parent,))
    ).fetchone()
    assert parent_row[0] == "done"
    assert parent_row[1]["chunks"] == {"done": 3}


@pytest.mark.asyncio
async def test_error_queue_marks_bad_row(env, db):
    keys = ["ok-1", "bad", "ok-2"]
    parent = await _enqueue(
        env, "imp.flaky@v1", {"chunk_size": 10, "atomic": "row"}
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
        env, "imp.rows@v1", {"chunk_size": 5, "atomic": "row"}
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
