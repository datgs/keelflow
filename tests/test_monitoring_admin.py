"""T15 contract tests: monitoring APIs + SuperAdmin smoke."""
from __future__ import annotations

import os

import psycopg
import pytest
import pytest_asyncio

from keelflow.engine.monitoring import MonitoringService
from keelflow.postgres.schema import apply_async

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
            "TRUNCATE keelflow_jobs, keelflow_step_runs, keelflow_workflow_runs,"
            " keelflow_row_status CASCADE"
        )
        yield conn


@pytest.mark.asyncio
async def test_jobs_list_and_filter(db):
    svc = MonitoringService()
    for i, (t, s) in enumerate([("a@v1", "done"), ("b@v1", "failed"), ("a@v1", "pending")]):
        await db.execute(
            "INSERT INTO keelflow_jobs (id, type, status, payload, context)"
            " VALUES (gen_random_uuid(), %s, %s, '{}', '{}')",
            (t, s),
        )
    res = await svc.jobs_list(db, types=["a@v1"])
    assert {r["type"] for r in res["items"]} == {"a@v1"}
    res = await svc.jobs_list(db, statuses=["failed"])
    assert [r["status"] for r in res["items"]] == ["failed"]


@pytest.mark.asyncio
async def test_metrics_snapshot(db):
    svc = MonitoringService()
    await db.execute(
        "INSERT INTO keelflow_jobs (id, type, status, payload, context)"
        " VALUES (gen_random_uuid(), 'x@v1', 'pending', '{}', '{}'),"
        "        (gen_random_uuid(), 'x@v1', 'failed', '{}', '{}')"
    )
    m = await svc.metrics_snapshot(db)
    assert m["queue_depth"] == 1
    assert m["failed_24h"] == 1


@pytest.mark.asyncio
async def test_rows_list_failed(db):
    svc = MonitoringService()
    job_id = "00000000-0000-0000-0000-0000000000aa"
    await db.execute(
        "INSERT INTO keelflow_jobs (id, type, status, payload, context)"
        " VALUES (%s, 'imp@v1', 'running', '{}', '{}')",
        (job_id,),
    )
    await db.execute(
        "INSERT INTO keelflow_row_status (job_id, row_key, status, error)"
        " VALUES (%s, 'r1', 'failed', 'bad row'), (%s, 'r2', 'done', NULL)",
        (job_id, job_id),
    )
    rows = await svc.rows_list_failed(db, job_id)
    assert len(rows) == 1 and rows[0]["row_key"] == "r1"
    assert rows[0]["error"] == "bad row"


def test_admin_serves():
    """SuperAdmin serves a real page on an ephemeral port (sync test)."""
    import socket
    import threading
    import time
    import urllib.request

    import psycopg as _p

    try:
        c = _p.connect(DSN)
        c.close()
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"postgres unavailable: {exc}")

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    proc = threading.Thread(target=_serve, args=(port,), daemon=True)
    proc.start()
    body = b""
    for _ in range(50):
        try:
            body = urllib.request.urlopen(
                f"http://127.0.0.1:{port}/", timeout=5).read()
            break
        except Exception:  # noqa: BLE001 - server still starting
            time.sleep(0.1)
    assert b"queue depth" in body


def _serve(port):
    from keelflow.admin.app import make_app

    make_app(DSN, port=port)
