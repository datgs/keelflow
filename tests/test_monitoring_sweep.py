"""MonitoringService.purge_expired_records — time-based retention for engine tables."""
from __future__ import annotations

import os
import uuid

import psycopg
import pytest
import pytest_asyncio

from keelflow.engine.monitoring import MonitoringService
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)


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
            "TRUNCATE keelflow_jobs, keelflow_step_runs,"
            " keelflow_workflow_runs, keelflow_chunks, keelflow_row_status,"
            " keelflow_row_archive, keelflow_outbox,"
            " keelflow_outbox_deliveries, keelflow_dlq CASCADE"
        )
        yield conn


async def _job(db, status="done", days_old=40, parent=None):
    jid = str(uuid.uuid4())
    await db.execute(
        "INSERT INTO keelflow_jobs"
        " (id, type, status, payload, context, parent_job_id, updated_at)"
        " VALUES (%s, 'x@v1', %s, '{}', '{}', %s,"
        "         now() - make_interval(days => %s))",
        (jid, status, parent, days_old),
    )
    return jid


async def _count(db, table, where=""):
    cur = await db.execute(                      # test tables only
        f"SELECT count(*) FROM {table} {where}")
    return (await cur.fetchone())[0]


@pytest.mark.asyncio
async def test_sweep_terminal_jobs_cascades_and_is_idempotent(db):
    svc = MonitoringService(storage=PostgresStorage(DSN))
    old = await _job(db, "done", 40)
    await db.execute(
        "INSERT INTO keelflow_row_status (job_id, row_key, status)"
        " VALUES (%s, 'r1', 'failed')", (old,))
    await db.execute(
        "INSERT INTO keelflow_chunks (job_id, idx, status)"
        " VALUES (%s, 0, 'done')", (old,))
    await _job(db, "done", 2)          # recent terminal — kept
    await _job(db, "running", 40)      # in flight — kept
    await _job(db, "pending", 40)      # never touched

    out = await svc.purge_expired_records(db, jobs_days=30)
    assert out["jobs"] == 1
    assert await _count(db, "keelflow_row_status") == 0      # cascaded
    assert await _count(db, "keelflow_chunks") == 0
    assert await _count(db, "keelflow_jobs",
                        "WHERE status='done'") == 1
    assert await _count(db, "keelflow_jobs",
                        "WHERE status IN ('running','pending')") == 2

    again = await svc.purge_expired_records(db, jobs_days=30)                # idempotent
    assert all(v == 0 for v in again.values())


@pytest.mark.asyncio
async def test_sweep_children_before_parent(db):
    """A parent can't be deleted while a child job still references it —
    repeated sweeps converge child-first."""
    svc = MonitoringService(storage=PostgresStorage(DSN))
    parent = await _job(db, "done", 40)
    child = await _job(db, "done", 40, parent=parent)

    out = await svc.purge_expired_records(db, jobs_days=30, batch=1)
    assert out["jobs"] == 1                                   # one per batch
    await svc.purge_expired_records(db, jobs_days=30)
    assert await _count(db, "keelflow_jobs") == 0
    assert not await (
        await db.execute("SELECT 1 FROM keelflow_jobs WHERE id=%s",
                         (child,))).fetchone()


@pytest.mark.asyncio
async def test_sweep_runs_release_step_linked_jobs(db):
    """Deleting a terminal run cascades its step_runs; the step's job then
    becomes sweepable. A job still referenced by a live run is skipped."""
    svc = MonitoringService(storage=PostgresStorage(DSN))
    run_id, job_id = str(uuid.uuid4()), str(uuid.uuid4())
    await db.execute(
        "INSERT INTO keelflow_workflow_runs (id, workflow, status, updated_at)"
        " VALUES (%s, 'wf@v1', 'done', now() - make_interval(days => 40))",
        (run_id,))
    await db.execute(
        "INSERT INTO keelflow_jobs (id, type, status, payload, context,"
        " updated_at) VALUES (%s, 'x@v1', 'done', '{}', '{}',"
        " now() - make_interval(days => 40))", (job_id,))
    await db.execute(
        "INSERT INTO keelflow_step_runs (run_id, step_id, type, status, job_id)"
        " VALUES (%s, 's1', 'x@v1', 'done', %s)", (run_id, job_id))

    out = await svc.purge_expired_records(db, jobs_days=30)
    assert out["workflow_runs"] == 1
    assert out["jobs"] == 1
    assert await _count(db, "keelflow_step_runs") == 0
    assert await _count(db, "keelflow_jobs") == 0

    # same shape, run still 'running' → nothing deleted
    run_id2, job_id2 = str(uuid.uuid4()), str(uuid.uuid4())
    await db.execute(
        "INSERT INTO keelflow_workflow_runs (id, workflow, status, updated_at)"
        " VALUES (%s, 'wf@v1', 'running', now() - make_interval(days => 40))",
        (run_id2,))
    await db.execute(
        "INSERT INTO keelflow_jobs (id, type, status, payload, context,"
        " updated_at) VALUES (%s, 'x@v1', 'done', '{}', '{}',"
        " now() - make_interval(days => 40))", (job_id2,))
    await db.execute(
        "INSERT INTO keelflow_step_runs (run_id, step_id, type, status, job_id)"
        " VALUES (%s, 's1', 'x@v1', 'done', %s)", (run_id2, job_id2))
    out = await svc.purge_expired_records(db, jobs_days=30)
    assert out["jobs"] == 0 and out["workflow_runs"] == 0
    assert await _count(db, "keelflow_jobs") == 1


@pytest.mark.asyncio
async def test_sweep_outbox_archive_dlq(db):
    svc = MonitoringService(storage=PostgresStorage(DSN))
    await db.execute(
        "INSERT INTO keelflow_outbox (event_type, payload, status,"
        " published_at, created_at) VALUES"
        " ('e1', '{}', 'published', now() - make_interval(days => 10), now()),"
        " ('e2', '{}', 'published', now() - make_interval(days => 1), now()),"
        " ('e3', '{}', 'pending', NULL, now()),"
        " ('e4', '{}', 'failed', now() - make_interval(days => 10), now())")
    jid = str(uuid.uuid4())
    await db.execute(
        "INSERT INTO keelflow_row_archive"
        " (job_id, row_id, row_key, archived_at) VALUES"
        " (%s, 1, 'k', now() - make_interval(days => 120)),"
        " (%s, 2, 'k2', now() - make_interval(days => 3))",
        (jid, jid))
    await db.execute(
        "INSERT INTO keelflow_dlq (id, type, payload, context, attempts,"
        " dead_at) VALUES (%s, 'x@v1', '{}', '{}', 5,"
        " now() - make_interval(days => 120))",
        (uuid.uuid4(),))

    out = await svc.purge_expired_records(db, outbox_days=7, archive_days=90, dlq_days=90,
                          jobs_days=None)
    assert out["outbox"] == 1        # only old + published
    assert out["row_archive"] == 1
    assert out["dlq"] == 1
    assert "jobs" not in out         # jobs_days=None disables
    assert await _count(db, "keelflow_outbox") == 3


@pytest.mark.asyncio
async def test_sweep_none_disables_everything(db):
    svc = MonitoringService(storage=PostgresStorage(DSN))
    await _job(db, "done", 100)
    out = await svc.purge_expired_records(db, jobs_days=None, outbox_days=None,
                          archive_days=None, dlq_days=None)
    assert out == {}
    assert await _count(db, "keelflow_jobs") == 1
