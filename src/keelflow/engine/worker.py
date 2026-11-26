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
import contextlib
import json
import logging
import os
import random
import time
import traceback
from datetime import UTC, datetime, timedelta

import chancy
from pydantic import ValidationError

from ..sdk import (
    DlqInsert,
    ExecutionContext,
    ExecutionLostError,
    JobCancelled,
    JobFinish,
    JobResult,
    JobStatus,
    Registry,
    Storage,
)
from .context import ctx_from_dict

log = logging.getLogger("keelflow.worker")

DSN_ENV = "KEELFLOW_DSN"
REGISTRY_ENV = "KEELFLOW_REGISTRY"  # dotted path, e.g. "myapp.jobs:registry"

_REGISTRY: Registry | None = None
_kernel_client = None  # Kernel adapter, set by configure()/run_worker
_storage: Storage | None = None  # set by configure(), lazy-built on demand


def configure(registry: Registry | None = None, dsn: str | None = None,
              kernel=None, storage: Storage | None = None) -> None:
    """Configure dispatch for this process. Called by `run_worker`; apps may
    also call it directly before starting their own kernel worker."""
    global _REGISTRY, _kernel_client, _storage
    if registry is not None:
        _REGISTRY = registry
    if dsn is not None:
        os.environ[DSN_ENV] = dsn
    # Always reset: a stale client from an earlier configure() would
    # silently route retry pushes into the wrong kernel (test pollution
    # and a real bug when re-configuring between registries).
    from .kernel import as_kernel

    _kernel_client = as_kernel(kernel) if kernel is not None else None
    _storage = storage


def _get_storage() -> Storage:
    """Configured storage, lazily built from the DSN when unset.

    Postgres remains the default backend — an app passing a different
    adapter calls `configure(storage=…)` first.
    """
    global _storage
    if _storage is None:
        from ..postgres.storage import PostgresStorage

        _storage = PostgresStorage(os.environ[DSN_ENV])
    return _storage


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


async def dispatch(
    job_id: str,
    step_run_id: str | None = None,
    *,
    execution_token: str | None = None,
) -> None:
    """Execute one engine job. Called by the kernel claim loop.

    `step_run_id` links a kernel job back to a workflow step (T7); when the
    job finishes, `workflow.on_job_finished` advances the run.

    `execution_token` is the claim generation minted by a kernel-owned
    claim (native): `job_mark_running` validates it so a dispatch whose
    claim was reclaimed in the gap can never mint its own token over a
    newer claim. None means the engine owns the claim boundary
    (chancy/direct) and the token is minted here.
    """
    registry = _get_registry()
    storage = _get_storage()

    session = await storage.connect()
    try:
        async with session.begin() as uow:
            # worker liveness: every dispatch upserts a heartbeat row so
            # ops can tell "no work" from "dead worker".
            await storage.heartbeat_upsert(uow, _worker_name(), "worker")
            row = await storage.job_load(uow, job_id)
            if row is None:
                log.error("job %s not found in keelflow_jobs", job_id)
                return
            type_name = row["type"]
            status = row["status"]
            payload_json = row["payload"]
            ctx_json = row["context"]
            _ck = row["concurrency_key"]
            cancel_req = row["cancel_requested"]
            # Re-claim of an already-terminal job (e.g. double execution or
            # kernel retry of a declared-failed job): mark kernel-side done
            # and idempotently advance the run — never re-run the handler,
            # or domain effects would execute twice.
            if status in (
                JobStatus.DONE.value, JobStatus.CANCELLED.value,
                JobStatus.FAILED.value,
            ):
                log.info("job %s already %s; skipping", job_id, status)
                if step_run_id and _kernel_client is not None:
                    from .workflow import on_job_finished

                    sr = await storage.step_run_get(uow, step_run_id)
                    await on_job_finished(
                        uow, storage, _kernel_client, registry, job_id,
                        step_run_id, JobStatus(status),
                        (sr["outputs"] or {}) if sr else {},
                    )
                if status == JobStatus.CANCELLED.value:
                    # A pre-cancelled bulk chunk still has to settle its
                    # chunk row + run fan-in or the parent never finishes.
                    await _settle_cancelled_chunk(
                        uow, session, storage, type_name, payload_json)
                return
            # Cooperative cancel (JobService.cancel, kill_previous):
            # honored at claim time — an in-flight job drains to completion
            # unless it checkpoints (cooperative_cancel opt-in).
            if cancel_req:
                await _cancel_terminalize(
                    uow, session, storage, registry, job_id, step_run_id,
                    _job_logger(job_id, type_name, ""),
                    type_name, payload_json)
                return
            try:
                spec = registry.job_type(type_name)
            except KeyError:
                await storage.job_finish(uow, job_id, JobFinish(
                    status=JobStatus.FAILED,
                    error=f"unregistered job type {type_name}"))
                log.error("job %s has unregistered type %s",
                          job_id, type_name)
                return

            ctx = ctx_from_dict(ctx_json)
            # Handlers get job_id via extra (context stays read-only elsewhere).
            ctx.extra["job_id"] = job_id
            from ..observability import inject_traceparent
            inject_traceparent(ctx)
            object.__setattr__(
                ctx, "execution",
                ExecutionContext(
                    job_id=job_id,
                    cooperative_cancel=bool(
                        getattr(spec, "cooperative_cancel", False)),
                    progress_enabled=bool(
                        getattr(spec, "progress_enabled", False)
                        or getattr(spec, "progress_report_timeout_seconds",
                                   None) is not None),
                ),
            )
            jlog = _job_logger(job_id, type_name, ctx.correlation_id,
                               ctx.tenant_id)
            jlog.info("claimed")

            # Execution-time schema validation (payload may be from
            # older code).
            try:
                payload = spec.input_schema.model_validate(payload_json)  # type: ignore[attr-defined]
            except ValidationError as exc:
                await storage.job_finish(uow, job_id, JobFinish(
                    status=JobStatus.FAILED,
                    error=f"payload invalid: {exc}"))
                jlog.error("payload invalid: %s", exc)
                return

            # Atomic start/cancel decision (T-cancel): the transition
            # into `running` is conditioned on cancel_requested=false,
            # so a cancellation committed between the load above and
            # this update wins — a separate check + unconditional write
            # would lose the race and run a cancelled job.
            # The claim mints a fresh non-reusable execution_token in
            # the same atomic UPDATE: fenced progress/finish/reschedule
            # writes reject anything older than this claim, and ops
            # retry (attempts=0) can never collide with a live token.
            execution_token = await storage.job_mark_running(
                uow, job_id,
                progress_timeout_seconds=spec.progress_report_timeout_seconds,
                claim_token=execution_token)
            if execution_token is None:
                if await storage.job_cancel_requested(uow, job_id):
                    await _cancel_terminalize(
                        uow, session, storage, registry, job_id,
                        step_run_id, jlog, type_name, payload_json)
                else:
                    log.warning(
                        "job %s not startable (claim lost or status "
                        "changed); skipping", job_id)
                return
            ctx.execution._execution_token = execution_token
            # Claim budget: every claim (kernel or engine) consumed one
            # attempt — a job claimed more times than max_attempts is
            # settled permanently here (failed + DLQ + step settle in
            # this unit) instead of running the handler again.
            att = await storage.job_attempts(uow, job_id)
            if att and att[0] > att[1]:
                jlog.error(
                    "attempt budget exhausted (%d > %d); "
                    "settling without running the handler",
                    att[0], att[1])
                await _settle_exhausted(
                    uow, storage, registry, job_id, step_run_id,
                    type_name, payload_json, ctx_json, att,
                    error=(f"attempt budget exhausted: claimed {att[0]}"
                           f" times > max_attempts {att[1]}"),
                    jlog=jlog, execution_token=execution_token)
                return
        # uow boundary 1 committed: job is 'running' durably — a crash
        # mid-handler leaves it for kernel recovery instead of re-claim.

        # Key-serialized execution (T10): jobs sharing a resolved
        # concurrency_key never run in parallel. The kernel claim query
        # already serializes claim; this session lock covers the run.
        lock_cm = (session.key_lock(_ck) if _ck
                   else contextlib.nullcontext())
        handler = spec.handler()  # type: ignore[misc]
        status = None
        result = None
        async with lock_cm:
            try:
                # One UoW: handler domain writes (via ctx.uow) + job
                # finish + workflow advance commit or roll back together.
                async with session.begin() as uow:
                    object.__setattr__(ctx, "uow", uow)
                    ctx.execution._uow = uow
                    ctx.execution._storage = storage
                    from ..observability import tracer
                    with tracer().start_as_current_span(
                            "keelflow.dispatch"):
                        ctx.execution._last_progress_at = time.monotonic()
                        run = handler.run(ctx, payload)
                        timeout = (spec.progress_report_timeout_seconds
                                   or spec.timeout_seconds)
                        if timeout:
                            result = await _run_with_progress_timeout(
                                run, ctx.execution, timeout,
                                absolute_timeout=spec.timeout_seconds,
                                progress_timeout=(
                                    spec.progress_report_timeout_seconds is not None))
                        else:
                            result = await run
                    object.__setattr__(ctx, "uow", None)
                    ctx.execution._uow = None
                    ctx.execution._storage = None
                    status = (JobStatus.DONE if result.status == "done"
                              else JobStatus.FAILED)
                    bulk_hold = bool(result.outputs.get("_bulk_parent"))
                    if bulk_hold:
                        # bulk parent: partitions are out; fan-in flips
                        # to done/failed AND advances the step. Reporting
                        # RUNNING would terminalize the step as
                        # 'cancelled'.
                        status = JobStatus.RUNNING
                        result.outputs.pop("_bulk_parent", None)

                    # Size visibility (two-tier): measure once, always
                    # record result_bytes; warn past the soft threshold;
                    # fail only when a job type opts into result_max_bytes.
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
                    await storage.job_finish(
                        uow, job_id, JobFinish(
                            status=status,
                            result=result.outputs,
                            error=result.error,
                            result_bytes=_res_bytes,
                        ), execution_token=ctx.execution._execution_token)
                    if step_run_id and not bulk_hold:
                        kernel = _kernel_client
                        if kernel is not None:
                            from .workflow import on_job_finished

                            await on_job_finished(
                                uow, storage, kernel, registry, job_id,
                                step_run_id, status, result.outputs,
                            )
            except (JobCancelled, ExecutionLostError) as ctrl:
                # JobCancelled: a checkpoint observed a committed
                # cancel_requested → terminalize 'cancelled' in a new
                # unit (bypasses retry/DLQ).
                # ExecutionLostError: this dispatch lost its claim
                # (retry takeover, ops reset, newer claim) → the fenced
                # write already rolled the UoW back; abandon.
                if isinstance(ctrl, ExecutionLostError):
                    jlog.warning(
                        "execution lost before completion; abandoning "
                        "stale dispatch")
                    return
                jlog.info("cancelled at checkpoint")
                try:
                    async with session.begin() as uow:
                        await _cancel_terminalize(
                            uow, session, storage, registry, job_id,
                            step_run_id, jlog, type_name, payload_json,
                            execution_token=ctx.execution._execution_token)
                except ExecutionLostError:
                    jlog.warning(
                        "execution lost during cancel settle; "
                        "abandoning stale dispatch")
                return
            except Exception as exc:
                tb = traceback.format_exc(limit=10)
                exc_text = str(exc) or type(exc).__name__  # TimeoutError → ''
                try:
                    async with session.begin() as uow:
                        att = await storage.job_attempts(uow, job_id)
                        exhausted = bool(att and att[0] >= att[1])
                        if exhausted:
                            await _settle_exhausted(
                                uow, storage, registry, job_id,
                                step_run_id, type_name, payload_json,
                                ctx_json, att,
                                error=(exc_text
                                       + ("\n" + tb if tb else "")),
                                dlq_error=exc_text,
                                jlog=jlog,
                                execution_token=(
                                    ctx.execution._execution_token))
                            jlog.error(
                                "handler failed permanently: %s", exc)
                        else:
                            # Engine-scheduled retry: keep the engine job
                            # 'running', record the error, and push a
                            # replacement kernel job at the spec's backoff
                            # delay inside one UoW (push rolls back with
                            # the error write if either fails). Kernel
                            # crash recovery still redelivers the
                            # original kernel job — the
                            # attempts>=max_attempts check above
                            # converges both.
                            delay = _retry_delay(spec, att[0] if att else 1)
                            await storage.job_error_and_reschedule(
                                uow, job_id, exc_text, delay,
                                execution_token=ctx.execution._execution_token)
                            if _kernel_client is not None:
                                await _kernel_client.push_ex(
                                    uow, _retry_kernel_job(
                                        job_id, type_name, spec,
                                        step_run_id, delay))
                            jlog.error(
                                "handler failed (retryable in %.1fs): %s",
                                delay, exc)
                except ExecutionLostError:
                    # The failure-recording UoW hit the fence: another
                    # claim or ops action owns the job now. Never
                    # reschedule from a stale dispatch.
                    jlog.warning(
                        "execution lost while recording failure; "
                        "abandoning stale dispatch")
                    return
                if _kernel_client is None and not exhausted:
                    # No kernel client to push a redelivery through —
                    # fall back to kernel retry semantics.
                    raise
        if result is not None:
            jlog.info("%s", result.status)
            if result.status != "done":
                raise _JobFailedError(
                    result.error or "handler returned failed")
    finally:
        await session.close()


def run_worker(
    registry: Registry,
    dsn: str | None = None,
    *,
    kernel=None,
    storage: Storage | None = None,
    queues: list[chancy.Queue] | None = None,
    prefix: str = "keelflow_kernel_",
    event_bus=None,
) -> None:
    """Run a kernel worker claiming keelflow jobs. Blocks until SIGINT/SIGTERM.

    `kernel=None` (default) bootstraps a Chancy backend, creating and
    owning the `chancy.Chancy` client for the process. `prefix`
    namespaces the KERNEL tables (chancy's `*_jobs`, `*_queues`…). It
    MUST differ from the engine's `keelflow_` schema prefix or kernel
    tables collide with `keelflow_jobs`/`keelflow_queues` (chancy also
    creates `<prefix>jobs`).

    Pass `kernel=` to run a caller-owned Kernel adapter (e.g.
    `NativeKernel`, `SQLiteKernel`): the helper configures dispatch,
    migrates and starts the worker through the port — it never closes a
    caller-owned kernel. `queues` is Chancy-only configuration and is
    rejected for other backends.

    Equivalent of `python -m keelflow.engine.worker`.
    """
    import asyncio

    from .chancy_kernel import ChancyKernel
    from .kernel import as_kernel

    injected = as_kernel(kernel) if kernel is not None else None
    if injected is not None and queues is not None \
            and not isinstance(injected, ChancyKernel):
        raise ValueError(
            "queues= is chancy-specific; configure concurrency on the"
            " kernel's create_worker instead")

    dsn = dsn or os.environ.get(DSN_ENV) \
        or getattr(injected, "dsn", None) or os.environ[DSN_ENV]
    if storage is None:
        from ..postgres.storage import PostgresStorage

        storage = PostgresStorage(dsn)
    configure(registry=registry, dsn=dsn, storage=storage)

    if injected is not None:
        async def _run_injected() -> None:
            configure(registry=registry, dsn=dsn, kernel=injected,
                      storage=storage)
            await injected.migrate()
            await _run_supervised(storage, event_bus,
                                  injected.create_worker)

        asyncio.run(_run_injected())
        return

    async def _run() -> None:
        async with chancy.Chancy(dsn, prefix=prefix) as client:
            kernel = ChancyKernel(client)
            configure(registry=registry, dsn=dsn, kernel=kernel,
                      storage=storage)
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
            await _run_supervised(storage, event_bus,
                                  kernel.create_worker)

    asyncio.run(_run())


async def _run_supervised(storage, event_bus, worker_factory,
                          restart_delay: float = 2.0) -> None:
    """Run a kernel worker under supervision: a dead worker (transient
    DB loss, broker fault, plugin crash) is stopped and replaced by a
    fresh `worker_factory()` instance after `restart_delay`, forever —
    the heartbeat task keeps declaring the process alive through the
    gap. A clean `wait_for_shutdown` return ends the loop.
    """
    import asyncio

    stop = asyncio.Event()

    async def _supervise() -> None:
        try:
            while True:
                worker = worker_factory()
                try:
                    await worker.start()
                    await worker.wait_for_shutdown()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception(
                        "kernel worker lost; restarting in %.1fs",
                        restart_delay)
                    await asyncio.sleep(restart_delay)
                    continue
                finally:
                    try:
                        await worker.stop()
                    except Exception:
                        log.exception("worker.stop failed during "
                                      "supervision")
                break  # clean shutdown
        finally:
            stop.set()

    tasks = [asyncio.ensure_future(_supervise()),
             asyncio.ensure_future(
                 worker_heartbeat_loop(storage, stop=stop))]
    if event_bus is not None:
        from .events import dispatch_events

        tasks.append(asyncio.ensure_future(
            dispatch_events(event_bus, storage)))
    # wait (not gather): any task dying — supervision returns, the
    # heartbeat crashes — ends the set, then remaining tasks are
    # cancelled and their exception (if any) propagates.
    try:
        done, _ = await asyncio.wait(tasks,
                                     return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    for t in done:
        t.result()


async def _run_with_progress_timeout(
    awaitable,
    execution,
    timeout: float,
    *,
    absolute_timeout: float | None,
    progress_timeout: bool,
):
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    task = asyncio.ensure_future(awaitable)
    try:
        while True:
            elapsed = time.monotonic() - execution._last_progress_at
            remaining = timeout - elapsed
            if absolute_timeout is not None:
                remaining = min(
                    remaining, absolute_timeout - (loop.time() - started_at)
                )
            if remaining <= 0:
                if progress_timeout and elapsed >= timeout:
                    raise TimeoutError("no progress reported before timeout")
                raise TimeoutError
            waiter = asyncio.create_task(execution._progress_event.wait())
            done, _ = await asyncio.wait(
                {task, waiter}, timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED)
            if task in done:
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
                return task.result()
            if waiter in done:
                execution._progress_event.clear()
                if (absolute_timeout is not None
                        and loop.time() - started_at >= absolute_timeout):
                    raise TimeoutError
                continue
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            elapsed = time.monotonic() - execution._last_progress_at
            if progress_timeout and elapsed >= timeout:
                raise TimeoutError("no progress reported before timeout")
            raise TimeoutError
    except BaseException:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise


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


# ── cancel helpers ──────────────────────────────────────────────────────


async def _settle_exhausted(
    uow, storage, registry, job_id, step_run_id,
    type_name, payload_json, ctx_json, att,
    *, error: str, jlog, execution_token,
    dlq_error: str | None = None,
) -> None:
    """Permanent terminalization in ONE unit: job_finish('failed') +
    DLQ copy + workflow step settle. Used both by handler-exception
    exhaustion and by a dispatch observing the claim budget already
    spent (repeated crashes never ran the handler). Caller commits."""
    await storage.job_finish(uow, job_id, JobFinish(
        status=JobStatus.FAILED, error=error),
        execution_token=execution_token)
    # keep the workflow link so dlq_replay can resume the owning run
    _dlq_run_id = None
    if step_run_id:
        _sr = await storage.step_run_get(uow, step_run_id)
        _dlq_run_id = str(_sr["run_id"]) if _sr else None
    await storage.dlq_insert(uow, DlqInsert(
        id=job_id, type=type_name,
        payload=payload_json, context=ctx_json,
        error=dlq_error if dlq_error is not None else error,
        attempts=att[0] if att else 0,
        run_id=_dlq_run_id, step_run_id=step_run_id,
    ))
    if step_run_id and _kernel_client is not None:
        from .workflow import on_job_finished

        await on_job_finished(
            uow, storage, _kernel_client, registry,
            job_id, step_run_id, JobStatus.FAILED, {})



async def _cancel_terminalize(uow, session, storage, registry, job_id,
                              step_run_id, jlog,
                              type_name=None, payload_json=None,
                              execution_token=None) -> None:
    """Terminalize a job as cancelled and advance its owning step — the
    same path the claim-time cancel check uses. Caller commits."""
    await storage.job_finish(
        uow, job_id, JobFinish(status=JobStatus.CANCELLED),
        execution_token=execution_token)
    if step_run_id and _kernel_client is not None:
        from .workflow import on_job_finished

        await on_job_finished(
            uow, storage, _kernel_client, registry, job_id, step_run_id,
            JobStatus.CANCELLED, {},
        )
    await _settle_cancelled_chunk(
        uow, session, storage, type_name, payload_json)


async def _settle_cancelled_chunk(uow, session, storage, type_name,
                                  payload_json) -> None:
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
    await storage.chunk_cancel(uow, str(chunk_id))
    await _bulk.ChunkJobHandler()._fan_in(uow, storage, parent_id)

_worker_id: str | None = None

#: Liveness contract: workers upsert a heartbeat every
#: HEARTBEAT_INTERVAL_S (dispatch and idle); a process is alive while
#: its heartbeat is fresher than STALE_AFTER_S.
HEARTBEAT_INTERVAL_S = 10.0
STALE_AFTER_S = 30.0


def _worker_name() -> str:
    """Stable per-process worker identity for heartbeat rows."""
    global _worker_id
    if _worker_id is None:
        import socket
        _worker_id = f"worker-{socket.gethostname()}-{os.getpid()}"
    return _worker_id


async def worker_heartbeat_loop(
    storage: Storage,
    *,
    interval: float = HEARTBEAT_INTERVAL_S,
    stop: asyncio.Event | None = None,
) -> None:
    """Periodic worker heartbeat — keeps a live worker visible even when
    it dispatches no jobs. Each upsert commits in its own session unit
    so a failed cycle reconnects cleanly on the next one.

    The declared freshness window derives from `interval` —
    `max(interval*3, STALE_AFTER_S)`, the same missed-ticks margin the
    scheduler uses — so a caller that heartbeats less often than the
    default is not reported stale while healthy.
    """
    stale_after = max(interval * 3.0, STALE_AFTER_S)
    while not (stop and stop.is_set()):
        try:
            session = await storage.connect()
            try:
                async with session.begin() as uow:
                    await storage.heartbeat_upsert(
                        uow, _worker_name(), "worker",
                        stale_after_s=stale_after)
            finally:
                await session.close()
        except Exception:  # liveness must never kill the worker
            log.debug("worker heartbeat failed", exc_info=True)
        if stop is None:
            await asyncio.sleep(interval)
        else:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except TimeoutError:
                pass


def _job_logger(job_id: str, type_name: str, correlation_id: str,
                tenant_id: str | None = None) -> logging.LoggerAdapter:
    from ..observability import job_logger
    return job_logger(
        log, job_id=job_id, job_type=type_name,
        correlation_id=correlation_id, tenant_id=tenant_id)


if __name__ == "__main__":  # pragma: no cover
    from .registry import InMemoryRegistry

    run_worker(registry=InMemoryRegistry())
