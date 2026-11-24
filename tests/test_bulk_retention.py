"""Bulk row retention — row_disposition archive/keep/delete (T5).

Direct chunk-handler calls (no kernel) like test_resume_pending_only_after_crash:
stage rows → build one chunk → ChunkJobHandler.run → fan-in → cleanup.
"""
from __future__ import annotations

import json
import uuid

import psycopg
import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from pydantic import BaseModel

from keelflow.engine import worker as worker_mod
from keelflow.engine.bulk import (
    BulkJobHandler,
    ChunkJobHandler,
    ChunkPayload,
    progress,
)
from keelflow.engine.bulk import register as register_bulk
from keelflow.engine.monitoring import MonitoringService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import Actor, JobContext, JobTypeSpec

DSN = "postgresql://postgres:postgres@localhost:5544/keelflow_test"

STORAGE = PostgresStorage(DSN)

APPLIED: list[str] = []


class _P(BaseModel):
    chunk_size: int = 1000
    atomic: str = "row"
    row_disposition: str = "archive"


class _ImportH(BulkJobHandler):
    async def process_row(self, ctx, row, tx):
        APPLIED.append(row["row_key"])
        return {"ok": row["row_key"]}


class _FlakyH(BulkJobHandler):
    async def process_row(self, ctx, row, tx):
        if row["row_key"] == "bad":
            raise ValueError("invalid row")
        APPLIED.append(row["row_key"])
        return {"ok": row["row_key"]}


async def _conn():
    return await psycopg.AsyncConnection.connect(
        DSN, autocommit=True, row_factory=dict_row)


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
        reg = InMemoryRegistry()
        register_bulk(reg)
        reg.register_job_type(
            JobTypeSpec(type="ret.rows@v1", input_schema=_P, handler=_ImportH)
        )
        reg.register_job_type(
            JobTypeSpec(type="ret.flaky@v1", input_schema=_P, handler=_FlakyH)
        )
        worker_mod.configure(registry=reg, dsn=DSN, kernel=None)
        yield conn


async def _parent(db, type_name, disposition="archive"):
    pid = str(uuid.uuid4())
    await db.execute(
        "INSERT INTO keelflow_jobs (id, type, status, payload)"
        " VALUES (%s, %s, 'running', %s)",
        (pid, type_name, json.dumps({
            "chunk_size": 1000, "atomic": "row",
            "row_disposition": disposition})),
    )
    return pid


async def _stage(db, job_id, keys, statuses=None):
    for i, k in enumerate(keys):
        st = (statuses or {}).get(k, "pending")
        await db.execute(
            "INSERT INTO keelflow_row_status (job_id, row_key, status, data)"
            " VALUES (%s, %s, %s, %s)",
            (job_id, k, st, json.dumps({"v": i})),
        )


async def _run_chunk(db, parent, keys=None):
    """Partition pending rows into one chunk and run the chunk handler."""
    cid = uuid.uuid4()
    await db.execute(
        "INSERT INTO keelflow_chunks (id, job_id, idx, status)"
        " VALUES (%s,%s,0,'pending')", (cid, parent))
    await db.execute(
        "UPDATE keelflow_row_status SET chunk_id=%s WHERE job_id=%s"
        " AND status='pending'", (cid, parent))
    await ChunkJobHandler().run(
        JobContext(actor=Actor.system(), extra={"job_id": parent}),
        ChunkPayload(parent_job_id=str(parent), chunk_id=str(cid)),
    )
    return cid


async def _counts(db, table, job_id):
    if table == "keelflow_row_status":
        cur = await db.execute(
            "SELECT status, count(*) FROM keelflow_row_status"
            " WHERE job_id=%s GROUP BY status", (job_id,))
        return {r["status"]: r["count"] for r in await cur.fetchall()}
    cur = await db.execute(
        "SELECT count(*) AS n FROM keelflow_row_archive WHERE job_id=%s",
        (job_id,))
    n = (await cur.fetchone())["n"]
    return {"archived": n} if n else {}


@pytest.mark.asyncio
async def test_archive_default_moves_done_rows(db):
    APPLIED.clear()
    parent = await _parent(db, "ret.rows@v1")  # archive
    await _stage(db, parent, ["r1", "r2", "r3"])
    await _run_chunk(db, parent)

    job = await (await db.execute(
        "SELECT status, result FROM keelflow_jobs WHERE id=%s",
        (parent,))).fetchone()
    assert job["status"] == "done"
    assert job["result"]["row_disposition"] == "archive"
    assert job["result"]["archived_rows"] == 3
    assert job["result"]["rows_done"] == 3

    assert await _counts(db, "keelflow_row_status", parent) == {}
    archived = await _counts(db, "keelflow_row_archive", parent)
    assert archived["archived"] == 3

    rows = await MonitoringService().rows_list_archived(db, parent)
    assert [r["row_key"] for r in rows] == ["r1", "r2", "r3"]
    assert rows[0]["data"] == {"v": 0}


@pytest.mark.asyncio
async def test_keep_retains_done_rows(db):
    APPLIED.clear()
    parent = await _parent(db, "ret.rows@v1", "keep")
    await _stage(db, parent, ["a", "b"])
    await _run_chunk(db, parent)

    assert await _counts(db, "keelflow_row_status", parent) == {"done": 2}
    assert await _counts(db, "keelflow_row_archive", parent) == {}
    job = await (await db.execute(
        "SELECT result FROM keelflow_jobs WHERE id=%s", (parent,))).fetchone()
    assert job["result"]["row_disposition"] == "keep"
    assert job["result"]["kept_rows"] == 2


@pytest.mark.asyncio
async def test_delete_drops_done_rows(db):
    APPLIED.clear()
    parent = await _parent(db, "ret.rows@v1", "delete")
    await _stage(db, parent, ["a", "b", "c"])
    await _run_chunk(db, parent)

    assert await _counts(db, "keelflow_row_status", parent) == {}
    assert await _counts(db, "keelflow_row_archive", parent) == {}
    job = await (await db.execute(
        "SELECT result FROM keelflow_jobs WHERE id=%s", (parent,))).fetchone()
    assert job["result"]["row_disposition"] == "delete"
    assert job["result"]["deleted_rows"] == 3


@pytest.mark.asyncio
async def test_failed_rows_never_moved(db):
    APPLIED.clear()
    parent = await _parent(db, "ret.flaky@v1")  # archive
    await _stage(db, parent, ["ok-1", "bad", "ok-2"])
    await _run_chunk(db, parent)

    live = await _counts(db, "keelflow_row_status", parent)
    assert live == {"failed": 1}
    assert await _counts(db, "keelflow_row_archive", parent) == {
        "archived": 2}
    err = await MonitoringService().rows_list_failed(db, parent)
    assert err[0]["row_key"] == "bad" and "invalid row" in err[0]["error"]


@pytest.mark.asyncio
async def test_fan_in_rerun_is_idempotent(db):
    """Re-entering _fan_in (e.g. retry of the final write) must not
    duplicate archive rows or re-delete."""
    APPLIED.clear()
    parent = await _parent(db, "ret.rows@v1")
    await _stage(db, parent, ["x", "y"])
    await _run_chunk(db, parent)

    handler = ChunkJobHandler()
    async with await _conn() as conn:
        await handler._fan_in(conn, STORAGE, parent)  # re-run path
    assert await _counts(db, "keelflow_row_archive", parent) == {
        "archived": 2}


@pytest.mark.asyncio
async def test_archive_failure_rolls_back(db):
    """If the archive move fails, live rows + parent status must be
    untouched — the parent must not go terminal losing its rows."""
    APPLIED.clear()
    parent = await _parent(db, "ret.rows@v1")
    await _stage(db, parent, ["p", "q"])
    cid = uuid.uuid4()
    await db.execute(
        "INSERT INTO keelflow_chunks (id, job_id, idx, status)"
        " VALUES (%s,%s,0,'done')", (cid, parent))
    await db.execute(
        "UPDATE keelflow_row_status SET chunk_id=%s, status='done'"
        " WHERE job_id=%s", (cid, parent))

    async with await _conn() as conn:
        await conn.execute("DROP TABLE keelflow_row_archive")
        try:
            with pytest.raises(psycopg.errors.UndefinedTable):
                await ChunkJobHandler()._fan_in(conn, STORAGE, parent)
        finally:
            await conn.execute(
                "CREATE TABLE keelflow_row_archive ("
                " id BIGSERIAL PRIMARY KEY, job_id UUID NOT NULL,"
                " row_id BIGINT NOT NULL, row_key TEXT NOT NULL,"
                " chunk_id UUID, data JSONB,"
                " archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
                " UNIQUE (job_id, row_id))")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_row_archive_job"
                " ON keelflow_row_archive (job_id, id)")

    job = await (await db.execute(
        "SELECT status FROM keelflow_jobs WHERE id=%s", (parent,))).fetchone()
    assert job["status"] == "running"          # not terminal on failure
    assert await _counts(db, "keelflow_row_status", parent) == {"done": 2}


@pytest.mark.asyncio
async def test_archive_survives_job_delete(db):
    """Archive has no FK to live jobs: rows stay inspectable after the
    parent job (and its live row_status via CASCADE) is removed."""
    APPLIED.clear()
    parent = await _parent(db, "ret.rows@v1")
    await _stage(db, parent, ["gone-1", "gone-2"])
    await _run_chunk(db, parent)

    await db.execute("DELETE FROM keelflow_jobs WHERE id=%s", (parent,))
    assert await _counts(db, "keelflow_row_status", parent) == {}
    rows = await MonitoringService().rows_list_archived(db, parent)
    assert [r["row_key"] for r in rows] == ["gone-1", "gone-2"]


@pytest.mark.asyncio
async def test_progress_terminal_uses_summary(db):
    APPLIED.clear()
    parent = await _parent(db, "ret.flaky@v1")
    await _stage(db, parent, ["a", "bad", "b"])
    await _run_chunk(db, parent)

    st = await progress(db, STORAGE, parent)
    assert st["finished"] is True
    assert st["rows_total"] == 3
    assert st["done"] == 2
    assert st["failed"] == 1
    assert st["pct"] == 100.0
    assert st["row_disposition"] == "archive"
    assert st["archived_rows"] == 2


@pytest.mark.asyncio
async def test_archive_pagination(db):
    APPLIED.clear()
    parent = await _parent(db, "ret.rows@v1")
    await _stage(db, parent, [f"k{i}" for i in range(5)])
    await _run_chunk(db, parent)

    mon = MonitoringService()
    page1 = await mon.rows_list_archived(db, parent, limit=3)
    assert len(page1) == 3
    page2 = await mon.rows_list_archived(
        db, parent, limit=3, after_id=page1[-1]["id"])
    assert [r["row_key"] for r in page1 + page2] == [
        "k0", "k1", "k2", "k3", "k4"]
