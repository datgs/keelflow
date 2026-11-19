"""Worker runtime (T5).

The chancy kernel claims `keelflow.engine.worker.dispatch` for every enqueued
keelflow job (`DISPATCH_FUNC`). Dispatch resolves the handler from the
configured registry, re-validates the payload at execution time (jobs may
have been written by older code), rebuilds the JobContext, and writes the
result back to `keelflow_jobs`.

Leases/recovery are the kernel's job: a dead worker's in-flight job is
re-queued by chancy's Recovery plugin and `dispatch` runs again —
handlers must be idempotent.

Configuration (process-global, set by `configure` before the worker starts):
    KEELFLOW_DSN, KEELFLOW_REGISTRY   — env fallbacks used by `dispatch`
"""
from __future__ import annotations

import logging
import os
import traceback
from typing import Any

import chancy
import psycopg
from pydantic import ValidationError

from ..sdk import JobStatus, Registry
from .context import ctx_from_dict

log = logging.getLogger("keelflow.worker")

DSN_ENV = "KEELFLOW_DSN"
REGISTRY_ENV = "KEELFLOW_REGISTRY"  # dotted path, e.g. "myapp.jobs:registry"

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
            log.error("job %s not found in keelflow_jobs", job_id)
            return
        _, type_name, status, payload_json, ctx_json, _ck = row
        # Re-claim of an already-terminal job (e.g. double execution or
        # kernel retry of a declared-failed job): mark kernel-side done and
        # idempotently advance the run — never re-run the handler, or
        # domain effects would execute twice.
        if status in (
            JobStatus.DONE.value, JobStatus.CANCELLED.value,
            JobStatus.FAILED.value,
        ):
            log.info("job %s already %s; skipping", job_id, status)
            if step_run_id and _kernel_client is not None:
                async with conn.transaction():
                    from .workflow import on_job_finished

                    sr = await conn.execute(
                        "SELECT outputs FROM keelflow_step_runs WHERE id=%s",
                        (step_run_id,),
                    )
                    sr_row = await sr.fetchone()
                    await on_job_finished(
                        conn, _kernel_client, registry, job_id, step_run_id,
                        JobStatus(status),
                        (sr_row[0] or {}) if sr_row else {},
                    )
                await conn.commit()
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
        # Handlers get job_id via extra (context stays read-only elsewhere).
        ctx.extra["job_id"] = job_id
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

        # Key-serialized execution (T10): jobs sharing a resolved
        # concurrency_key never run in parallel. SKIP LOCKED in the claim
        # query already serializes claim; this advisory lock covers the run.
        lock = _KeyLock(conn, _ck) if _ck else _NullLock()
        await lock.acquire()

        handler = spec.handler()  # type: ignore[misc]
        # One TX: handler domain writes (via ctx.extra["tx"]) + job finish
        # + workflow advance commit or roll back together.
        status = None
        result = None
        try:
            async with conn.transaction(), conn.cursor() as tx_cur:
                object.__setattr__(ctx, "tx", tx_cur)
                result = await handler.run(ctx, payload)
                object.__setattr__(ctx, "tx", None)
                status = JobStatus.DONE if result.status == "done" else JobStatus.FAILED
                if result.outputs.get("_bulk_parent"):
                    # bulk parent: partitions are out; fan-in flips to done/failed.
                    status = JobStatus.RUNNING
                    result.outputs.pop("_bulk_parent", None)
                await _write_finish(
                    tx_cur, job_id, status,
                    result=result.outputs, error=result.error,
                )
                if step_run_id:
                    kernel = _kernel_client
                    if kernel is not None:
                        from .workflow import on_job_finished

                        await on_job_finished(
                            conn, kernel, registry, job_id, step_run_id,
                            status, result.outputs,
                        )
            # conn.transaction() above is a savepoint when an implicit TX
            # is already open (the advisory-lock SELECT opened one) —
            # explicit commit is required or close() rolls everything back.
            await conn.commit()
        except Exception as exc:
            tb = traceback.format_exc(limit=10)
            cur = await conn.execute(
                "SELECT attempts, max_attempts FROM keelflow_jobs WHERE id=%s",
                (job_id,),
            )
            att = await cur.fetchone()
            exhausted = bool(att and att[0] >= att[1])
            if exhausted:
                # Kernel retries are done → terminal failure.
                await _finish(
                    conn, job_id, JobStatus.FAILED,
                    error=str(exc), trace=tb)
                jlog.error("handler failed permanently: %s", exc)
                if step_run_id and _kernel_client is not None:
                    async with conn.transaction():
                        from .workflow import on_job_finished

                        await on_job_finished(
                            conn, _kernel_client, registry, job_id,
                            step_run_id, JobStatus.FAILED, {},
                        )
                    await conn.commit()
            else:
                # Kernel will re-deliver → stay 'running'; error is
                # recorded on the row but status must not be 'failed'
                # or the redelivered dispatch would skip the handler.
                await conn.execute(
                    "UPDATE keelflow_jobs SET error=%s, updated_at=now()"
                    " WHERE id=%s",
                    (str(exc), job_id),
                )
                await conn.commit()
                jlog.error("handler failed (retryable): %s", exc)
            await lock.release()
            raise
        await lock.release()
        jlog.info("%s", result.status)

        if result.status != "done":
            raise _JobFailedError(result.error or "handler returned failed")
    finally:
        await conn.close()


def run_worker(
    registry: Registry,
    dsn: str | None = None,
    *,
    queues: list[chancy.Queue] | None = None,
    prefix: str = "keelflow_kernel_",
    event_bus=None,
) -> None:
    """Run a kernel worker claiming keelflow jobs. Blocks until SIGINT/SIGTERM.

    `prefix` namespaces the KERNEL tables (chancy's `*_jobs`, `*_queues`…).
    It MUST differ from the engine's `keelflow_` schema prefix or kernel
    tables collide with `keelflow_jobs`/`keelflow_queues` (chancy also
    creates `<prefix>jobs`).

    Equivalent of `python -m keelflow.engine.worker`.
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
            tasks = [worker.wait_for_shutdown()]
            if event_bus is not None:
                from .events import dispatch_events

                tasks.append(dispatch_events(dsn, event_bus))
            await asyncio.gather(*tasks)

    asyncio.run(_run())


class _JobFailedError(RuntimeError):
    """Internal: handler returned status='failed' → kernel records failure."""


# ── db helpers ────────────────────────────────────────────────────────────


async def _load_job(conn, job_id: str):
    cur = await conn.execute(
        "SELECT id, type, status, payload, context, concurrency_key"
        " FROM keelflow_jobs WHERE id=%s",
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
    return (row[0], row[1], row[2], payload, context, row[5])


async def _set_status(
    conn, job_id: str, status: JobStatus, *, increment_attempts: bool = False
) -> None:
    await conn.execute(
        "UPDATE keelflow_jobs SET status=%s, attempts = attempts + %s,"
        " updated_at=now() WHERE id=%s",
        (status.value, 1 if increment_attempts else 0, job_id),
    )
    await conn.commit()


async def _write_finish(
    cursor,
    job_id: str,
    status: JobStatus,
    *,
    result: dict[str, Any] | None = None,
    error: str | None = None,
    trace: str | None = None,
) -> None:
    """Finish write inside the caller's transaction — no commit."""
    import json

    await cursor.execute(
        "UPDATE keelflow_jobs SET status=%s, result=%s, error=%s,"
        " updated_at=now() WHERE id=%s",
        (status.value, json.dumps(result) if result else None,
         (error or "") + (("\n" + trace) if trace else "") or None, job_id),
    )


async def _finish(
    conn,
    job_id: str,
    status: JobStatus,
    *,
    result: dict[str, Any] | None = None,
    error: str | None = None,
    trace: str | None = None,
) -> None:
    """Standalone finish — own TX (used outside the handler-run TX)."""
    async with conn.transaction(), conn.cursor() as cur:
        await _write_finish(
            cur, job_id, status, result=result, error=error, trace=trace)
    await conn.commit()


class _KeyLock:
    """pg advisory session lock on a concurrency key."""

    def __init__(self, conn, key: str) -> None:
        self._conn = conn
        self._key = key

    async def acquire(self) -> None:
        await self._conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (self._key,))

    async def release(self) -> None:
        await self._conn.execute(
            "SELECT pg_advisory_unlock(hashtext(%s))", (self._key,)
        )


class _NullLock:
    async def acquire(self) -> None:
        return None

    async def release(self) -> None:
        return None


def _job_logger(job_id: str, type_name: str, correlation_id: str) -> logging.LoggerAdapter:
    return logging.LoggerAdapter(
        log, {"job_id": job_id, "type": type_name, "correlation_id": correlation_id}
    )


if __name__ == "__main__":  # pragma: no cover
    from .registry import InMemoryRegistry

    run_worker(registry=InMemoryRegistry())
