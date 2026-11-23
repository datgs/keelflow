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

import asyncio
import json
import logging
import os
import random
import traceback
from datetime import UTC, datetime, timedelta
from typing import Any

import chancy
import psycopg
from pydantic import ValidationError

from ..sdk import ExecutionContext, JobCancelled, JobResult, JobStatus, Registry
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
    # Always reset: a stale client from an earlier configure() would
    # silently route retry pushes into the wrong kernel (test pollution
    # and a real bug when re-configuring between registries).
    from .kernel import as_kernel

    _kernel_client = as_kernel(kernel) if kernel is not None else None


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
        _, type_name, status, payload_json, ctx_json, _ck, cancel_req = row
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
            if status == JobStatus.CANCELLED.value:
                # A pre-cancelled bulk chunk still has to settle its
                # chunk row + run fan-in or the parent never finishes.
                await _settle_cancelled_chunk(
                    conn, type_name, payload_json)
            return
        # Cooperative cancel (JobService.cancel, kill_previous):
        # honored at claim time — an in-flight job drains to completion
        # unless it checkpoints (cooperative_cancel opt-in).
        if cancel_req:
            await _cancel_terminalize(
                conn, registry, job_id, step_run_id,
                _job_logger(job_id, type_name, ""),
                type_name, payload_json)
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
        object.__setattr__(
            ctx, "execution",
            ExecutionContext(
                job_id=job_id,
                cooperative_cancel=bool(getattr(spec, "cooperative_cancel", False)),
            ),
        )
        jlog = _job_logger(job_id, type_name, ctx.correlation_id)
        jlog.info("claimed")

        # Execution-time schema validation (payload may be from older code).
        try:
            payload = spec.input_schema.model_validate(payload_json)  # type: ignore[attr-defined]
        except ValidationError as exc:
            await _finish(conn, job_id, JobStatus.FAILED, error=f"payload invalid: {exc}")
            jlog.error("payload invalid: %s", exc)
            return

        # Atomic start/cancel decision (T-cancel): the transition into
        # `running` is conditioned on cancel_requested=false, so a
        # cancellation committed between the load above and this update
        # wins — a separate check + unconditional write would lose the
        # race and run a cancelled job.
        if not await _transition_to_running(conn, job_id):
            if await _cancel_requested(conn, job_id):
                await _cancel_terminalize(
                    conn, registry, job_id, step_run_id, jlog,
                    type_name, payload_json)
            else:
                log.warning(
                    "job %s not startable (status changed); skipping", job_id)
            return

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
                ctx.execution.tx = tx_cur
                run = handler.run(ctx, payload)
                if spec.timeout_seconds:
                    result = await asyncio.wait_for(
                        run, timeout=spec.timeout_seconds)
                else:
                    result = await run
                object.__setattr__(ctx, "tx", None)
                ctx.execution.tx = None
                status = JobStatus.DONE if result.status == "done" else JobStatus.FAILED
                bulk_hold = bool(result.outputs.get("_bulk_parent"))
                if bulk_hold:
                    # bulk parent: partitions are out; fan-in flips to
                    # done/failed AND advances the step. Reporting RUNNING
                    # here would terminalize the step as 'cancelled'.
                    status = JobStatus.RUNNING
                    result.outputs.pop("_bulk_parent", None)

                # Size visibility (two-tier): measure once, always record
                # result_bytes; warn past the soft threshold; fail only
                # when a job type opts into result_max_bytes.
                _res_json = json.dumps(result.outputs)
                _res_bytes = len(_res_json.encode())
                if (spec.result_warning_bytes is not None
                        and _res_bytes > spec.result_warning_bytes):
                    jlog.warning(
                        "result %d bytes exceeds warning threshold %d for"
                        " %s — return references, not blobs",
                        _res_bytes, spec.result_warning_bytes, type_name)
                if (spec.result_max_bytes is not None
                        and _res_bytes > spec.result_max_bytes):
                    status = JobStatus.FAILED
                    result = JobResult(
                        status="failed",
                        error=(f"result {_res_bytes} bytes exceeds"
                               f" result_max_bytes"
                               f" {spec.result_max_bytes} for {type_name}"),
                    )
                await _write_finish(
                    tx_cur, job_id, status,
                    result=result.outputs, error=result.error,
                    result_bytes=_res_bytes,
                )
                if step_run_id and not bulk_hold:
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
        except JobCancelled:
            # A checkpoint observed a committed cancel_requested: the
            # handler TX (savepoint) already rolled back its domain
            # writes. Terminalize as cancelled in a NEW transaction —
            # never through the retry/DLQ path.
            jlog.info("cancelled at checkpoint")
            await _cancel_terminalize(conn, registry, job_id, step_run_id,
                                      jlog, type_name, payload_json)
            await lock.release()
            return
        except Exception as exc:
            tb = traceback.format_exc(limit=10)
            exc_text = str(exc) or type(exc).__name__  # TimeoutError → ''
            cur = await conn.execute(
                "SELECT attempts, max_attempts FROM keelflow_jobs WHERE id=%s",
                (job_id,),
            )
            att = await cur.fetchone()
            exhausted = bool(att and att[0] >= att[1])
            if exhausted:
                # Retries are done → terminal failure + DLQ copy.
                await _finish(
                    conn, job_id, JobStatus.FAILED,
                    error=exc_text, trace=tb)
                # keep the workflow link so dlq_replay can resume the run
                _dlq_run_id = None
                if step_run_id:
                    _sr = await conn.execute(
                        "SELECT run_id FROM keelflow_step_runs WHERE id=%s",
                        (step_run_id,),
                    )
                    _sr_row = await _sr.fetchone()
                    _dlq_run_id = str(_sr_row[0]) if _sr_row else None
                await _write_dlq(
                    conn, job_id, type_name, payload_json, ctx_json,
                    exc_text, att[0] if att else 0,
                    run_id=_dlq_run_id, step_run_id=step_run_id,
                )
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
                # Engine-scheduled retry: keep the engine job 'running',
                # record the error, and push a replacement kernel job at
                # the spec's backoff delay inside one TX (push rolls back
                # with the error write if either fails). Kernel crash
                # recovery still redelivers the original kernel job — the
                # attempts>=max_attempts check above converges both.
                delay = _retry_delay(spec, att[0] if att else 1)
                from psycopg.rows import dict_row

                async with (
                    conn.transaction(),
                    conn.cursor(row_factory=dict_row) as rtx_cur,
                ):
                    await rtx_cur.execute(
                        "UPDATE keelflow_jobs SET error=%s,"
                        " run_at=run_at + interval '1 second' * %s,"
                        " updated_at=now() WHERE id=%s",
                        (exc_text, delay, job_id),
                    )
                    if _kernel_client is not None:
                        await _kernel_client.push_ex(
                            rtx_cur, _retry_kernel_job(
                                job_id, type_name, spec, step_run_id, delay))
                await conn.commit()
                jlog.error(
                    "handler failed (retryable in %.1fs): %s", delay, exc)
            await lock.release()
            if _kernel_client is None and not exhausted:
                # No kernel client to push a redelivery through — fall
                # back to kernel retry semantics.
                raise
        await lock.release()
        if result is not None:
            jlog.info("%s", result.status)
            if result.status != "done":
                raise _JobFailedError(
                    result.error or "handler returned failed")
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

    from .kernel import ChancyKernel

    dsn = dsn or os.environ[DSN_ENV]
    configure(registry=registry, dsn=dsn)

    async def _run() -> None:
        async with chancy.Chancy(dsn, prefix=prefix) as client:
            kernel = ChancyKernel(client)
            configure(registry=registry, dsn=dsn, kernel=kernel)
            await kernel.migrate()
            for q in queues or [
                chancy.Queue(
                    "default",
                    concurrency=16,
                    executor=chancy.Chancy.Executor.Async,
                    eager_polling=True,
                )
            ]:
                await kernel.declare_queue(q, upsert=True)
            worker = kernel.create_worker()
            await worker.start()
            tasks = [worker.wait_for_shutdown()]
            if event_bus is not None:
                from .events import dispatch_events

                tasks.append(dispatch_events(dsn, event_bus))
            await asyncio.gather(*tasks)

    asyncio.run(_run())


class _JobFailedError(RuntimeError):
    """Internal: handler returned status='failed' → kernel records failure."""


def _retry_delay(spec, attempt: int) -> float:
    """RetryPolicy backoff in seconds (jitter = uniform 0.5–1.5×)."""
    retry = getattr(spec, "retry", None)
    base = getattr(retry, "base_seconds", 30.0)
    backoff = getattr(retry, "backoff", "exponential")
    delay = base if backoff == "fixed" else base * (2 ** (attempt - 1))
    if getattr(retry, "jitter", False):
        delay *= random.uniform(0.5, 1.5)
    return delay


def _retry_kernel_job(job_id, type_name: str, spec, step_run_id, delay: float):
    """Replacement dispatch spec for an engine-scheduled retry."""
    from .kernel import dispatch_spec

    return dispatch_spec(
        job_id=job_id,
        max_attempts=spec.retry.max_attempts if spec.retry else 3,
        scheduled_at=datetime.now(UTC) + timedelta(seconds=delay),
        meta={"job_id": str(job_id), "type": type_name,
              "step_run_id": str(step_run_id) if step_run_id else None},
        step_run_id=step_run_id,
    )


async def _write_dlq(conn, job_id, type_name: str, payload, context,
                     error: str, attempts: int, *,
                     run_id: str | None = None,
                     step_run_id: str | None = None) -> None:
    """Copy a terminally-failed job to keelflow_dlq for ops replay.

    `run_id`/`step_run_id` link a dead DAG-step job back to its run so
    `dlq_replay` can resume the workflow rather than orphan the run.
    """
    async with conn.transaction():
        await conn.execute(
            "INSERT INTO keelflow_dlq (id, type, payload, context, error,"
            " attempts, run_id, step_run_id)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (id) DO UPDATE"
            " SET error=EXCLUDED.error, attempts=EXCLUDED.attempts,"
            " dead_at=now()",
            (job_id, type_name,
             json.dumps(payload) if not isinstance(payload, str) else payload,
             json.dumps(context) if not isinstance(context, str) else context,
             error, attempts, run_id, step_run_id),
        )
    await conn.commit()


# ── db helpers ────────────────────────────────────────────────────────────


async def _load_job(conn, job_id: str):
    cur = await conn.execute(
        "SELECT id, type, status, payload, context, concurrency_key,"
        " cancel_requested FROM keelflow_jobs WHERE id=%s",
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
    return (row[0], row[1], row[2], payload, context, row[5], row[6])


async def _transition_to_running(conn, job_id) -> bool:
    """Atomic pending/claimed/running → running + attempts increment.

    Conditioned on `cancel_requested=false`: a cancel request that
    commits first makes this a no-op (returns False) and the job is
    never started. `running` stays eligible so engine-scheduled retries
    (status kept 'running') can re-enter dispatch.
    """
    cur = await conn.execute(
        "UPDATE keelflow_jobs SET status=%s, attempts = attempts + 1,"
        " updated_at=now()"
        " WHERE id=%s"
        "   AND status IN ('pending','claimed','running')"
        "   AND cancel_requested = false",
        (JobStatus.RUNNING.value, job_id),
    )
    if cur.rowcount:
        await conn.commit()
        return True
    return False


async def _cancel_requested(conn, job_id) -> bool:
    cur = await conn.execute(
        "SELECT cancel_requested FROM keelflow_jobs WHERE id=%s", (job_id,))
    row = await cur.fetchone()
    return bool(row and row[0])


async def _cancel_terminalize(conn, registry, job_id, step_run_id, jlog,
                              type_name=None, payload_json=None) -> None:
    """Terminalize a job as cancelled and advance its owning step — the
    same path the claim-time cancel check uses."""
    await _finish(conn, job_id, JobStatus.CANCELLED)
    if step_run_id and _kernel_client is not None:
        async with conn.transaction():
            from .workflow import on_job_finished

            await on_job_finished(
                conn, _kernel_client, registry, job_id, step_run_id,
                JobStatus.CANCELLED, {},
            )
        await conn.commit()
    await _settle_cancelled_chunk(conn, type_name, payload_json)


async def _settle_cancelled_chunk(conn, type_name, payload_json) -> None:
    """A cancelled `bulk.chunk@v1` job must still settle bookkeeping:
    mark its chunk cancelled (if not already terminal) and run fan-in
    so the parent bulk job can terminalize. Idempotent."""
    if type_name != "bulk.chunk@v1" or not isinstance(payload_json, dict):
        return
    from . import bulk as _bulk

    chunk_id = payload_json.get("chunk_id")
    parent_id = payload_json.get("parent_job_id")
    if not chunk_id or not parent_id:
        return
    await conn.execute(
        "UPDATE keelflow_chunks SET status='cancelled', updated_at=now()"
        " WHERE id=%s AND status NOT IN ('done','failed','cancelled')",
        (chunk_id,),
    )
    await conn.commit()
    await _bulk.ChunkJobHandler()._fan_in(conn, parent_id)





async def _write_finish(
    cursor,
    job_id: str,
    status: JobStatus,
    *,
    result: dict[str, Any] | None = None,
    error: str | None = None,
    trace: str | None = None,
    result_bytes: int | None = None,
) -> None:
    """Finish write inside the caller's transaction — no commit."""
    import json

    await cursor.execute(
        "UPDATE keelflow_jobs SET status=%s, result=%s, result_bytes=%s,"
        " error=%s, updated_at=now() WHERE id=%s",
        (status.value, json.dumps(result) if result else None,
         result_bytes,
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
