"""Worker runtime (T5).

The chancy kernel claims `pyerp.engine.worker.dispatch` for every enqueued
pyerp job (`DISPATCH_FUNC`). Dispatch resolves the handler from the
configured registry, re-validates the payload at execution time (jobs may
have been written by older code), rebuilds the JobContext, and writes the
result back to `hd_flow_jobs`.

Leases/recovery are the kernel's job: a dead worker's in-flight job is
re-queued by chancy's Recovery plugin and `dispatch` runs again —
handlers must be idempotent.

Configuration (process-global, set by `configure` before the worker starts):
    PYERP_DSN, PYERP_REGISTRY   — env fallbacks used by `dispatch`
"""
from __future__ import annotations

import logging
import os
import traceback
from typing import Any

import chancy
import psycopg
from pydantic import ValidationError

from ..sdk import JobResult, JobStatus, Registry
from .context import ctx_from_dict

log = logging.getLogger("pyerp.worker")

DSN_ENV = "PYERP_DSN"
REGISTRY_ENV = "PYERP_REGISTRY"  # dotted path, e.g. "myapp.jobs:registry"

_REGISTRY: Registry | None = None
_kernel_client = None  # chancy.Chancy, set by configure()/run_worker


def configure(registry: Registry | None = None, dsn: str | None = None,
              kernel=None) -> None:
    """Configure dispatch for this process. Called by `run_worker`; apps may
    also call it directly before starting their own chancy worker."""
    global _REGISTRY, _kernel_client
    if registry is not None:
        _REGISTRY = registry
    if dsn is not None:
        os.environ[DSN_ENV] = dsn
    if kernel is not None:
        _kernel_client = kernel


def _get_registry() -> Registry:
    global _REGISTRY
    if _REGISTRY is not None:
        return _REGISTRY
    ref = os.environ.get(REGISTRY_ENV)
    if not ref:
        raise RuntimeError(
            f"no registry configured; call configure() or set {REGISTRY_ENV}"
        )
    module_name, _, attr = ref.partition(":")
    import importlib

    _REGISTRY = getattr(importlib.import_module(module_name), attr or "registry")
    return _REGISTRY


async def dispatch(job_id: str, step_run_id: str | None = None) -> None:
    """Execute one engine job. Called by the kernel claim loop.

    `step_run_id` links a kernel job back to a workflow step (T7); when the
    job finishes, `workflow.on_job_finished` advances the run.
    """
    dsn = os.environ[DSN_ENV]
    registry = _get_registry()

    conn = await psycopg.AsyncConnection.connect(dsn)
    try:
        row = await _load_job(conn, job_id)
        if row is None:
            log.error("job %s not found in hd_flow_jobs", job_id)
            return
        _, type_name, status, payload_json, ctx_json = row
        # Re-claim of an already-terminal job (e.g. double execution):
        # mark kernel-side as done — but still advance the workflow run if
        # this job feeds a step (the first attempt may have died mid-advance).
        if status in (JobStatus.DONE.value, JobStatus.CANCELLED.value):
            log.info("job %s already %s; skipping", job_id, status)
            if step_run_id and _kernel_client is not None:
                from .workflow import on_job_finished

                sr = await conn.execute(
                    "SELECT outputs FROM hd_flow_step_runs WHERE id=%s",
                    (step_run_id,),
                )
                sr_row = await sr.fetchone()
                await on_job_finished(
                    conn, _kernel_client, registry, job_id, step_run_id,
                    JobStatus(status),
                    (sr_row[0] or {}) if sr_row else {},
                )
            return
        try:
            spec = registry.job_type(type_name)
        except KeyError:
            await _finish(
                conn, job_id, JobStatus.FAILED, error=f"unregistered job type {type_name}"
            )
            log.error("job %s has unregistered type %s", job_id, type_name)
            return

        ctx = ctx_from_dict(ctx_json)
        jlog = _job_logger(job_id, type_name, ctx.correlation_id)
        jlog.info("claimed")

        # Execution-time schema validation (payload may be from older code).
        try:
            payload = spec.input_schema.model_validate(payload_json)  # type: ignore[attr-defined]
        except ValidationError as exc:
            await _finish(conn, job_id, JobStatus.FAILED, error=f"payload invalid: {exc}")
            jlog.error("payload invalid: %s", exc)
            return

        await _set_status(conn, job_id, JobStatus.RUNNING, increment_attempts=True)
        handler = spec.handler()  # type: ignore[misc]
        try:
            result: JobResult = await handler.run(ctx, payload)
        except Exception as exc:
            tb = traceback.format_exc(limit=10)
            await _finish(conn, job_id, JobStatus.FAILED, error=str(exc), trace=tb)
            jlog.error("handler failed: %s", exc)
            raise

        status = JobStatus.DONE if result.status == "done" else JobStatus.FAILED
        await _finish(
            conn, job_id, status, result=result.outputs, error=result.error
        )
        jlog.info("%s", result.status)

        if step_run_id:
            # Step bookkeeping + advance; failures here retry via the kernel
            # (re-delivery then takes the 'already done' path above and
            # completes the advance idempotently).
            kernel = _kernel_client
            if kernel is not None:
                from .workflow import on_job_finished

                try:
                    await on_job_finished(
                        conn, kernel, registry, job_id, step_run_id,
                        status, result.outputs,
                    )
                except Exception:
                    jlog.exception("workflow advance failed for step_run %s",
                                   step_run_id)
                    raise

        if result.status != "done":
            raise _JobFailedError(result.error or "handler returned failed")
    finally:
        await conn.close()


def run_worker(
    registry: Registry,
    dsn: str | None = None,
    *,
    queues: list[chancy.Queue] | None = None,
    prefix: str = "pyerp_",
) -> None:
    """Run a kernel worker claiming pyerp jobs. Blocks until SIGINT/SIGTERM.

    Equivalent of `python -m pyerp.engine.worker`.
    """
    import asyncio

    dsn = dsn or os.environ[DSN_ENV]
    configure(registry=registry, dsn=dsn)

    async def _run() -> None:
        async with chancy.Chancy(dsn, prefix=prefix) as client:
            configure(registry=registry, dsn=dsn, kernel=client)
            await client.migrate()
            for q in queues or [
                chancy.Queue(
                    "default",
                    concurrency=16,
                    executor=chancy.Chancy.Executor.Async,
                    eager_polling=True,
                )
            ]:
                await client.declare(q, upsert=True)
            worker = chancy.Worker(client)
            await worker.start()
            await worker.wait_for_shutdown()

    asyncio.run(_run())


class _JobFailedError(RuntimeError):
    """Internal: handler returned status='failed' → kernel records failure."""


# ── db helpers ────────────────────────────────────────────────────────────


async def _load_job(conn, job_id: str):
    cur = await conn.execute(
        "SELECT id, type, status, payload, context FROM hd_flow_jobs WHERE id=%s",
        (job_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    payload = row[3]
    if isinstance(payload, str):
        import json

        payload = json.loads(payload)
    context = row[4]
    if isinstance(context, str):
        import json

        context = json.loads(context)
    return (row[0], row[1], row[2], payload, context)


async def _set_status(
    conn, job_id: str, status: JobStatus, *, increment_attempts: bool = False
) -> None:
    await conn.execute(
        "UPDATE hd_flow_jobs SET status=%s, attempts = attempts + %s,"
        " updated_at=now() WHERE id=%s",
        (status.value, 1 if increment_attempts else 0, job_id),
    )
    await conn.commit()


async def _finish(
    conn,
    job_id: str,
    status: JobStatus,
    *,
    result: dict[str, Any] | None = None,
    error: str | None = None,
    trace: str | None = None,
) -> None:
    import json

    await conn.execute(
        "UPDATE hd_flow_jobs SET status=%s, result=%s, error=%s,"
        " updated_at=now() WHERE id=%s",
        (status.value, json.dumps(result) if result else None,
         (error or "") + (("\n" + trace) if trace else "") or None, job_id),
    )
    await conn.commit()


def _job_logger(job_id: str, type_name: str, correlation_id: str) -> logging.LoggerAdapter:
    return logging.LoggerAdapter(
        log, {"job_id": job_id, "type": type_name, "correlation_id": correlation_id}
    )


if __name__ == "__main__":  # pragma: no cover
    from .registry import InMemoryRegistry

    run_worker(registry=InMemoryRegistry())
