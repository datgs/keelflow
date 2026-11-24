"""Observability seam — structured log fields, W3C trace context,
optional OpenTelemetry spans, and a Prometheus text formatter.

No hard dependencies: OTel is used only when installed
(`pip install keelflow[otel]`). Everything degrades to no-ops
otherwise, so instrumentation costs nothing for apps that don't use it.

Log-field contract (engine `extra=` keys — never payload/context):

    job_id, job_type, run_id, step_id, correlation_id, tenant_id,
    worker_id, attempt

Trace context: `traceparent` rides in `JobContext.extra`, so enqueue →
worker → retry keeps one trace chain through any context-propagation
scheme — OTel when installed, manual propagation otherwise.
"""
from __future__ import annotations

import logging
import uuid
from contextlib import contextmanager
from typing import Any

try:  # pragma: no cover - optional dependency
    from opentelemetry import trace as _otel_trace

    _OTEL = True
except ImportError:  # pragma: no cover
    _otel_trace = None  # type: ignore[assignment]
    _OTEL = False

LOG_FIELDS = (
    "job_id", "job_type", "run_id", "step_id",
    "correlation_id", "tenant_id", "worker_id", "attempt",
)


def _new_traceparent() -> str:
    """W3C traceparent: version-traceid-spanid-flags (not sampled)."""
    return f"00-{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-00"


def _child_traceparent(parent: str) -> str:
    """New span id under the parent's trace id."""
    parts = parent.split("-")
    trace_id = parts[1] if len(parts) == 4 else uuid.uuid4().hex
    return f"00-{trace_id}-{uuid.uuid4().hex[:16]}-00"


def inject_traceparent(ctx) -> None:
    """Seed `ctx.extra['traceparent']` when absent — call at enqueue and
    dispatch so the chain survives retries and child jobs."""
    tp = ctx.extra.get("traceparent")
    ctx.extra["traceparent"] = (
        _child_traceparent(tp) if tp else _new_traceparent())


def extract_traceparent(ctx) -> str | None:
    return ctx.extra.get("traceparent")


def tracer():
    """The app-configured OTel tracer, or a no-op tracer."""
    if _OTEL:
        return _otel_trace.get_tracer("keelflow")
    return _NoOpTracer()


class _NoOpTracer:
    def start_as_current_span(self, *_a, **_kw):
        @contextmanager
        def _span():
            yield None
        return _span()


def log_fields(job_id=None, job_type=None, run_id=None, step_id=None,
               correlation_id=None, tenant_id=None, worker_id=None,
               attempt=None) -> dict[str, Any]:
    """Structured `extra=` payload for engine log lines — only fields
    that are set are included; payload/context never allowed."""
    out = {}
    for k, v in {
        "job_id": job_id, "job_type": job_type, "run_id": run_id,
        "step_id": step_id, "correlation_id": correlation_id,
        "tenant_id": tenant_id, "worker_id": worker_id,
        "attempt": attempt,
    }.items():
        if v is not None:
            out[k] = v
    return out


# ── Prometheus exposition ─────────────────────────────────────────────


def format_metrics_prometheus(snapshot: dict[str, Any]) -> str:
    """Render `MonitoringService.metrics_snapshot` output as Prometheus
    exposition text. Scalar entries become gauge metrics; app mounts the
    text on its own /metrics endpoint — no built-in listener."""
    lines = [
        "# HELP keelflow_queue_depth Jobs waiting to be claimed.",
        "# TYPE keelflow_queue_depth gauge",
        "# HELP keelflow_claimed Jobs claimed but not yet running.",
        "# TYPE keelflow_claimed gauge",
        "# HELP keelflow_running Jobs currently executing.",
        "# TYPE keelflow_running gauge",
        "# HELP keelflow_waiting Blocked jobs (human tasks, external waits) — legitimate waits, not stuck work.",
        "# TYPE keelflow_waiting gauge",
        "# HELP keelflow_failed_24h Jobs failed in the last 24 hours.",
        "# TYPE keelflow_failed_24h gauge",
        "# HELP keelflow_oldest_pending_seconds Age of the oldest pending job.",
        "# TYPE keelflow_oldest_pending_seconds gauge",
        "# HELP keelflow_avg_done_duration_seconds_24h Mean end-to-end duration of jobs finished in 24h.",
        "# TYPE keelflow_avg_done_duration_seconds_24h gauge",
        "# HELP keelflow_dlq_depth Dead-letter entries awaiting replay.",
        "# TYPE keelflow_dlq_depth gauge",
        "# HELP keelflow_workers_alive Workers with a heartbeat row.",
        "# TYPE keelflow_workers_alive gauge",
        "# HELP keelflow_scheduler_last_seen_age_seconds Age of the latest scheduler heartbeat.",
        "# TYPE keelflow_scheduler_last_seen_age_seconds gauge",
        "# HELP keelflow_db_connections Connections to this database (pg_stat_activity).",
        "# TYPE keelflow_db_connections gauge",
    ]
    mapping = {
        "queue_depth": "keelflow_queue_depth",
        "claimed": "keelflow_claimed",
        "running": "keelflow_running",
        "waiting": "keelflow_waiting",
        "failed_24h": "keelflow_failed_24h",
        "oldest_pending_s": "keelflow_oldest_pending_seconds",
        "avg_done_duration_s_24h": "keelflow_avg_done_duration_seconds_24h",
        "dlq_depth": "keelflow_dlq_depth",
        "workers_alive": "keelflow_workers_alive",
        "scheduler_last_seen_age_s": "keelflow_scheduler_last_seen_age_seconds",
        "db_connections": "keelflow_db_connections",
    }
    for key, metric in mapping.items():
        v = snapshot.get(key)
        lines.append(f"{metric} {0 if v is None else v}")
    return "\n".join(lines) + "\n"


def job_logger(base: logging.Logger, **fields) -> logging.LoggerAdapter:
    """LoggerAdapter enforcing the engine field contract."""
    extra = {k: v for k, v in fields.items()
             if k in LOG_FIELDS and v is not None}
    return logging.LoggerAdapter(base, extra)
