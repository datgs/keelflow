"""Read-only MCP adapter (T16) — exposes keelflow data APIs to agents.

Tools: `jobs.list`, `job.get`, `run.get`, `rows.list_failed`,
`audit.trace`, `metrics.snapshot`. Each wraps a MonitoringService /
AuditService call; writes are explicitly out of scope.

Run: `python -m keelflow.mcp` (stdio) or `make_server(dsn).run("sse")`.
"""
from .server import make_server

__all__ = ["make_server"]
