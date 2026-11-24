"""MCP server exposing read-only keelflow data APIs."""
from __future__ import annotations

import json
import os
from typing import Any

from mcp.server.mcpserver import MCPServer

from ..engine.audit import PostgresAuditService
from ..engine.monitoring import MonitoringService
from ..postgres.storage import PostgresStorage


def make_server(dsn: str | None = None) -> MCPServer:
    """Build the MCP server. `dsn` defaults to $KEELFLOW_DSN."""
    dsn = dsn or os.environ["KEELFLOW_DSN"]
    storage = PostgresStorage(dsn)
    svc = MonitoringService(storage=storage)
    audit = PostgresAuditService(storage)
    app = MCPServer("keelflow")

    async def _session():
        return await storage.connect()

    async def _json(data: Any) -> str:
        return json.dumps(data, default=str)

    @app.tool()
    async def jobs_list(
        types: list[str] | None = None,
        statuses: list[str] | None = None,
        limit: int = 50,
    ) -> str:
        """List jobs (durable units of work). Filter by `types`, `statuses`."""
        s = await _session()
        try:
            async with s.begin() as uow:
                return await _json(await svc.list_jobs(
                    uow, types=types, statuses=statuses, limit=limit))
        finally:
            await s.close()

    @app.tool()
    async def job_get(job_id: str) -> str:
        """Full detail for one job (status, payload, context, error)."""
        s = await _session()
        try:
            async with s.begin() as uow:
                return await _json(await svc.get_job(uow, job_id))
        finally:
            await s.close()

    @app.tool()
    async def run_get(run_id: str) -> str:
        """Workflow run detail incl. per-step status."""
        s = await _session()
        try:
            async with s.begin() as uow:
                return await _json(await svc.get_run(uow, run_id))
        finally:
            await s.close()

    @app.tool()
    async def rows_list_failed(job_id: str) -> str:
        """Failed rows for a bulk job (retry UI / debugging)."""
        s = await _session()
        try:
            async with s.begin() as uow:
                return await _json(await svc.rows_list_failed(uow, job_id))
        finally:
            await s.close()

    @app.tool()
    async def audit_trace(entity_type: str, entity_id: str) -> str:
        """Audit events for an entity (in record order)."""
        from ..sdk import EntityRef

        s = await _session()
        try:
            async with s.begin() as uow:
                events = await audit.trace(
                    uow, EntityRef(type=entity_type, id=entity_id))
                return await _json(events)
        finally:
            await s.close()

    @app.tool()
    async def metrics_snapshot() -> str:
        """Queue depth, running count, 24h failures, oldest pending."""
        s = await _session()
        try:
            async with s.begin() as uow:
                return await _json(await svc.metrics_snapshot(uow))
        finally:
            await s.close()

    return app


if __name__ == "__main__":  # pragma: no cover
    make_server().run("stdio")
