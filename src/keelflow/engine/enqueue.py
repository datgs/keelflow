"""Transactional enqueue pipeline (T4).

`JobService.builder(type)` returns a fluent `JobBuilder`; `.context()` is
mandatory, payload is validated at enqueue time (fail fast), and the write
happens inside the caller's transaction:

    INSERT keelflow_jobs          — engine-visible job record
    chancy.push_ex(cursor, ...)  — kernel job claimed by workers
    INSERT keelflow_outbox        — 'job.enqueued' event intent

All three share the caller's transaction: commit → job becomes visible and
a `pg_notify` wakes workers; rollback → nothing exists.
"""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ValidationError

from ..sdk import (
    Actor,
    JobContext,
    JobInsert,
    JobRef,
    JobStatus,
    Registry,
    Storage,
)
from ..sdk.storage import json_default
from ..sdk.uow import nested_scope
from .context import ctx_to_dict

#: Re-exported from kernel.py (canonical home) for existing callers.
from .kernel import DEFAULT_QUEUE, DISPATCH_FUNC  # noqa: F401


class EnqueueError(ValueError):
    """Raised for un-enqueueable jobs (missing context, bad payload, …)."""


class JobBuilder:
    """Fluent builder; one instance per enqueue."""

    def __init__(self, registry: Registry, type_name: str) -> None:
        self._registry = registry
        self._type = type_name
        self._payload: dict[str, Any] | None = None
        self._ctx: JobContext | None = None
        self._run_at: datetime | None = None
        self._concurrency_key: str | None = None
        self._unique_key: str | None = None
        self._assignee: str | None = None

    # ── fluent setters ───────────────────────────────────────────────

    def payload(self, p: BaseModel | dict[str, Any]) -> JobBuilder:
        if isinstance(p, BaseModel):
            self._payload = p.model_dump(mode="json")
        elif isinstance(p, dict):
            self._payload = p
        else:
            raise EnqueueError(f"payload must be a BaseModel or dict, got {type(p)}")
        return self

    def context(self, ctx: JobContext) -> JobBuilder:
        self._ctx = ctx
        return self

    def run_at(self, ts: datetime) -> JobBuilder:
        self._run_at = ts
        return self

    def concurrency_key(self, key: str) -> JobBuilder:
        self._concurrency_key = key
        return self

    def deduplication_key(self, key: str) -> JobBuilder:
        """Dedup enqueue: at most one non-terminal job per
        (type, deduplication_key). A duplicate returns the existing job's
        JobRef — no second kernel push, no second outbox row."""
        self._unique_key = key
        return self

    def assignee(self, actor: Actor | str) -> JobBuilder:
        if isinstance(actor, Actor):
            self._assignee = f"{actor.type.value}:{actor.id}"
        else:
            self._assignee = actor
        return self

    # ── enqueue ──────────────────────────────────────────────────────

    async def enqueue(self, uow, *, kernel, storage: Storage) -> JobRef:
        spec = self._registry.job_type(self._type)  # KeyError → unknown type

        if spec.is_external_wait:
            raise EnqueueError(
                f"{self._type} is an external-wait type — it is materialized"
                " by workflow steps only, not enqueued directly")
        if self._ctx is None:
            raise EnqueueError(".context(ctx) is required before enqueue()")

        # Enqueue-time validation: fail fast on badly-typed payloads.
        payload_dict = self._payload or {}
        try:
            spec.input_schema.model_validate(payload_dict)  # type: ignore[attr-defined]
        except ValidationError as exc:
            raise EnqueueError(
                f"payload for {self._type} failed schema validation: {exc}"
            ) from exc

        body = json.dumps(payload_dict, default=json_default)
        if len(body.encode()) > spec.payload_max_bytes:
            raise EnqueueError(
                f"payload for {self._type} exceeds {spec.payload_max_bytes} bytes"
            )

        # Spec-declared serialization key: a payload path like
        # "payload.sku_id" resolves to the job's concurrency_key so
        # same-key jobs never run in parallel. An explicit
        # .concurrency_key() call wins over the spec path.
        concurrency_key = self._concurrency_key
        if concurrency_key is None and spec.concurrency.key:
            resolved = _resolve_path(
                spec.concurrency.key, {"payload": payload_dict})
            if resolved is None:
                raise EnqueueError(
                    f"{self._type} declares concurrency.key "
                    f"{spec.concurrency.key!r} but the payload has no "
                    "value there")
            concurrency_key = str(resolved)

        job_id = uuid.uuid4()
        now = datetime.now(UTC)

        # Trace context propagation: seed ctx.extra['traceparent'] so the
        # chain carries from enqueue → dispatch → retry (and to children
        # that inherit context).
        from ..observability import inject_traceparent
        inject_traceparent(self._ctx)

        # 1. engine-visible record — the (type, unique_key) dedup makes
        #    enqueue idempotent; a duplicate returns the live job's id
        #    without a second kernel push or outbox row.
        landed = await storage.job_insert(uow, JobInsert(
            id=str(job_id),
            type=self._type,
            payload=payload_dict,
            context=ctx_to_dict(self._ctx),
            max_attempts=spec.retry.max_attempts,
            run_at=self._run_at or now,
            concurrency_key=concurrency_key,
            assignee=self._assignee,
            unique_key=self._unique_key,
        ))
        if landed != str(job_id):
            # dedup collision → the live job's id (None is defensive).
            return JobRef(id=landed or str(job_id))

        # 2. kernel job (claimed by workers). Same transaction → notify
        #    fires only on commit.
        from .kernel import dispatch_spec

        await kernel.push_ex(
            uow,
            # NOTE: concurrency_key is NOT mapped to unique_key — it
            # serializes *execution* (advisory lock in dispatch), not
            # enqueue-time dedup.
            dispatch_spec(
                job_id=job_id,
                max_attempts=spec.retry.max_attempts,
                scheduled_at=self._run_at or now,
                meta={"job_id": str(job_id), "type": self._type},
            ),
        )

        # 3. outbox intent — event dispatcher publishes on commit.
        await storage.outbox_insert(uow, "job.enqueued", {
            "job_id": str(job_id),
            "type": self._type,
            "correlation_id": self._ctx.correlation_id,
        })

        return JobRef(id=str(job_id))


class JobService:
    """SDK-facing service; construct with registry + kernel + storage."""

    def __init__(self, registry: Registry, kernel, storage: Storage) -> None:
        from .kernel import as_kernel

        self._registry = registry
        self._kernel = as_kernel(kernel)
        self._storage = storage

    @property
    def registry(self) -> Registry:
        return self._registry

    @property
    def kernel(self):
        return self._kernel

    @property
    def storage(self) -> Storage:
        return self._storage

    def builder(self, type: str) -> JobBuilder:
        return _BoundJobBuilder(
            self._registry, type, self._kernel, self._storage)

    async def cancel(self, job_id: str, ctx: JobContext) -> bool:
        """Request cooperative cancellation of a live job.

        Requires the `job.cancel` permission. Pending jobs are
        terminalized `cancelled` immediately (the worker's terminal
        skip advances any owning workflow step on delivery). Claimed or
        running jobs only get `cancel_requested=true` and keep their
        status: default jobs drain; job types with
        `cooperative_cancel=True` can stop at a checkpoint. Terminal
        jobs and repeated requests are idempotent no-ops.

        The request cascades to non-terminal children (e.g. bulk
        chunks), which follow the same rule. For `bulk.chunk@v1` the
        worker additionally settles the chunk row and runs fan-in so
        the parent can terminalize even when every chunk was cancelled
        before starting. Commits in its own unit of work — callers
        never see a transaction handle.

        Returns True when the request was recorded (job was live).
        """
        from .permissions import DefaultPermissionService
        from .workflow import on_job_finished

        DefaultPermissionService().require(ctx, "job.cancel")
        session = await self._storage.connect()
        try:
            async with session.begin() as uow:
                landed = await self._storage.job_cancel_request(
                    uow, job_id)
                if landed is not None:
                    await self._storage.job_cancel_request_descendants(
                        uow, job_id)
                # Only a job that actually terminalized here
                # (pending→cancelled) may advance its owning step —
                # flagged-but-live jobs must not. Non-kernel lifecycles
                # (wait.*, human.*) have no dispatch delivery to do this
                # later; for machine jobs the worker's terminal skip
                # performs the same idempotent write.
                if landed == "cancelled":
                    step = await self._storage.step_run_by_job(
                        uow, job_id)
                    if step:
                        await on_job_finished(
                            uow, self._storage, self._kernel,
                            self._registry, job_id, step,
                            JobStatus.CANCELLED, {})
                return landed is not None
        finally:
            await session.close()

    async def signal(self, uow, job_id: str, result: dict[str, Any],
                     ctx: JobContext) -> None:
        """Resolve an open `wait.*` job with a success result.

        This is the durable callback primitive: the application receives
        an external callback (its own auth, signature checks and
        correlation), then calls this inside a caller-owned unit of
        work. The wait resolution, owning step update and run advance
        commit together. Requires `job.signal`; first signal wins — a
        later signal on a terminal wait raises.
        """
        await self._signal(uow, job_id, ctx,
                           status=JobStatus.DONE, result=result or {})

    async def signal_failure(self, uow, job_id: str, error: str,
                             ctx: JobContext,
                             result: dict[str, Any] | None = None) -> None:
        """Resolve an open `wait.*` job as failed (external system
        reported a terminal error). The step fails and the run follows
        the normal `on_failed` path."""
        await self._signal(uow, job_id, ctx,
                           status=JobStatus.FAILED, result=result or {},
                           error=error)

    async def _signal(self, uow, job_id: str, ctx: JobContext, *,
                      status: JobStatus, result: dict[str, Any],
                      error: str | None = None) -> None:
        from .permissions import DefaultPermissionService
        from .workflow import on_job_finished

        DefaultPermissionService().require(ctx, "job.signal")
        async with nested_scope(uow) as nuow:
            row = await self._storage.job_lock(nuow, job_id)
            if row is None:
                raise KeyError(f"job {job_id} not found")
            spec = self._registry.job_type(row["type"])
            if not spec.is_external_wait:
                raise ValueError(
                    f"job {job_id} is {row['type']}, not a wait.* type")
            if row["status"] not in ("pending", "claimed"):
                raise ValueError(
                    f"wait {job_id} is not open (status={row['status']})")
            await self._storage.job_signal_wait(
                nuow, job_id, status, result, error)
            step = await self._storage.step_run_by_job(nuow, job_id)
            if step:
                await on_job_finished(
                    nuow, self._storage, self._kernel, self._registry,
                    job_id, step, status, result)


class _BoundJobBuilder(JobBuilder):
    """JobBuilder bound to kernel + storage so enqueue(uow) matches SDK."""

    def __init__(self, registry: Registry, type_name: str, kernel,
                 storage: Storage) -> None:
        super().__init__(registry, type_name)
        self._kernel = kernel
        self._storage = storage

    async def enqueue(self, uow) -> JobRef:  # type: ignore[override]
        return await super().enqueue(
            uow, kernel=self._kernel, storage=self._storage)


# ── helpers ─────────────────────────────────────────────────────────────



def _resolve_path(path: str, scope: dict) -> Any:
    """Resolve a dotted path like 'payload.sku_id' against the enqueue
    scope {payload: {...}}. Returns None when any segment is missing."""
    cur: Any = scope
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur
