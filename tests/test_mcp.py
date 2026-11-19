"""T16 contract test: read-only MCP tool surface."""
from __future__ import annotations

import os

import psycopg
import pytest
import pytest_asyncio

from keelflow.mcp.server import make_server
from keelflow.postgres.schema import apply_async

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)


@pytest_asyncio.fixture
async def db():
    try:
        conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_audit CASCADE"
        )
        yield conn


@pytest.mark.asyncio
async def test_tool_surface_registered(db):
    app = make_server(DSN)
    tools = await app.list_tools()
    names = {t.name for t in tools}
    assert {"jobs_list", "job_get", "run_get", "rows_list_failed",
            "audit_trace", "metrics_snapshot"} <= names


@pytest.mark.asyncio
async def test_call_tool_returns_json(db, monkeypatch):
    monkeypatch.setenv("KEELFLOW_DSN", DSN)
    await db.execute(
        "INSERT INTO keelflow_jobs (id, type, status, payload, context)"
        " VALUES (gen_random_uuid(), 'x@v1', 'pending', '{}', '{}')"
    )
    app = make_server(DSN)
    res = await app.call_tool("metrics_snapshot", {})
    import json
    # CallToolResult.content[0].text
    m = json.loads(res.content[0].text)
    assert "queue_depth" in m and "failed_24h" in m
