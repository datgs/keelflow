"""MCP server exposing read-only pyerp data APIs."""
from __future__ import annotations

import json
import os
from typing import Any

import psycopg
from mcp.server.mcpserver import MCPServer

from ..engine.audit import PostgresAuditService
from ..engine.monitoring import MonitoringService


def make_server(dsn: str | None = None) -> MCPServer:
    """Build the MCP server. `dsn` defaults to $PYERP_DSN."""
    dsn = dsn or os.environ["PYERP_DSN"]
    svc = MonitoringService()
    audit = PostgresAuditService()
    app = MCPServer("pyerp")

    async def _conn():
        return await psycopg.AsyncConnection.connect(dsn, autocommit=True)

    async def _json(data: Any) -> str:
        return json.dumps(data, default=str)

    @app.tool()
    async def jobs_list(
        types: list[str] | None = None,
        statuses: list[str] | None = None,
        limit: int = 50,
    ) -> str:
        """List jobs (durable units of work). Filter by `types`, `statuses`."""
        async with await _conn() as c:
            return await _json(await svc.jobs_list(
                c, types=types, statuses=statuses, limit=limit))

    @app.tool()
    async def job_get(job_id: str) -> str:
        """Full detail for one job (status, payload, context, error)."""
        async with await _conn() as c:
            return await _json(await svc.job_get(c, job_id))

    @app.tool()
    async def run_get(run_id: str) -> str:
        """Workflow run detail incl. per-step status."""
        async with await _conn() as c:
            return await _json(await svc.runs_get(c, run_id))

    @app.tool()
    async def rows_list_failed(job_id: str) -> str:
        """Failed rows for a bulk job (retry UI / debugging)."""
        async with await _conn() as c:
            return await _json(await svc.rows_list_failed(c, job_id))

    @app.tool()
    async def audit_trace(entity_type: str, entity_id: str) -> str:
        """Audit events for an entity (in record order)."""
        from ..sdk import EntityRef

        async with await _conn() as c:
            events = await audit.trace(
                c, EntityRef(type=entity_type, id=entity_id))
            return await _json(events)

    @app.tool()
    async def metrics_snapshot() -> str:
        """Queue depth, running count, 24h failures, oldest pending."""
        async with await _conn() as c:
            return await _json(await svc.metrics_snapshot(c))

    return app


if __name__ == "__main__":  # pragma: no cover
    make_server().run("stdio")
