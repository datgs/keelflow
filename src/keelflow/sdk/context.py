"""JobContext — ambient execution context for every job."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from .actor import Actor


class JobCancelled(BaseException):
    """Raised by `ExecutionContext.checkpoint()` when the job's durable
    `cancel_requested` flag is set.

    Subclasses `BaseException` (not `Exception`) so ordinary
    `except Exception` blocks in handlers cannot swallow it. Handlers that
    catch `BaseException` or `asyncio.CancelledError`-style control flow
    MUST re-raise this signal — it is how the worker rolls back the
    handler transaction and terminalizes the job as `cancelled`.
    """


@dataclass
class ExecutionContext:
    """Worker-injected runtime for the current dispatch.

    Engine-owned, never serialized into `jobs.context`. `uow` is the
    handler's unit of work (also available as `ctx.uow`) and `storage`
    the engine's semantic storage adapter.

    `checkpoint()` lets a handler cooperatively stop at a safe point:
    if the job's `cancel_requested` flag is set it raises
    `JobCancelled`, which unwinds the handler transaction. Only takes
    effect when the job type opts in via
    `JobTypeSpec(cooperative_cancel=True)`.
    """

    job_id: str
    cooperative_cancel: bool = False
    progress_enabled: bool = False
    attempt: int = 0
    progress_event: asyncio.Event = field(default_factory=asyncio.Event)
    last_progress_at: float = field(default_factory=time.monotonic)
    uow: Any = None
    storage: Any = None

    async def report_progress(
        self,
        current: int,
        total: int,
        *,
        message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Persist bounded application progress outside the handler UoW."""
        import json

        if not self.progress_enabled:
            raise RuntimeError(
                "report_progress() requires JobTypeSpec(progress_enabled=True)"
            )
        if (isinstance(current, bool) or isinstance(total, bool)
                or not isinstance(current, int) or not isinstance(total, int)
                or current < 0 or total < 0 or current > total):
            raise ValueError("progress current/total must be integers with 0 <= current <= total")
        if message is not None and (not isinstance(message, str) or len(message) > 256):
            raise ValueError("progress message must be at most 256 characters")
        if metadata is not None:
            if not isinstance(metadata, dict):
                raise ValueError("progress metadata must be a JSON object")
            try:
                encoded = json.dumps(
                    metadata, separators=(",", ":"), allow_nan=False)
            except (TypeError, ValueError) as exc:
                raise ValueError("progress metadata must contain JSON values") from exc
            if len(encoded.encode()) > 8192:
                raise ValueError("progress metadata exceeds 8192 bytes")
        if self.storage is None:
            raise RuntimeError("report_progress() is only valid inside a handler")
        self.progress_event.clear()
        persisted = await self.storage.job_progress_report(
            self.job_id, self.attempt, {
                "current": current, "total": total,
                "message": message, "metadata": metadata,
            })
        if persisted:
            self.last_progress_at = time.monotonic()
            self.progress_event.set()

    async def checkpoint(self) -> None:
        """Observe a cancellation request at a safe point in the handler.

        Raises `JobCancelled` when the durable flag is set; the request
        must have committed before this statement for it to be observed
        (READ COMMITTED statement visibility). No background polling —
        a checkpoint that runs before a cancel request commits returns
        normally and the job keeps running.
        """
        if not self.cooperative_cancel:
            raise RuntimeError(
                "ctx.execution.checkpoint() requires the job type to opt in "
                "via JobTypeSpec(cooperative_cancel=True)"
            )
        if self.uow is None or self.storage is None:
            raise RuntimeError("checkpoint() is only valid inside the handler transaction")
        if await self.storage.job_cancel_requested(self.uow, self.job_id):
            raise JobCancelled(self.job_id)


@dataclass(frozen=True)
class JobContext:
    """Ambient data carried with every job, serialized into jobs.context.

    Payload carries business arguments (schema-validated); context carries
    who/where/trace. They are never mixed. Context is read-only in handlers —
    handlers may not mutate it; downstream data goes through step outputs.
    """

    actor: Actor
    tenant_id: str | None = None
    correlation_id: str = ""
    permissions: frozenset[str] = frozenset()
    request_id: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    # In-run unit of work set by the dispatcher when the handler executes
    # inside the finish TX. Never serialized (excluded from ctx_to_dict).
    uow: Any = field(default=None, repr=False, compare=False)
    # Worker-injected execution runtime (job id, checkpoint API). Set by
    # dispatch before handler.run; never serialized.
    execution: Any = field(default=None, repr=False, compare=False)

    def require(self, permission: str) -> None:
        """Raise if the actor lacks a permission (service-layer check)."""
        if "*" not in self.permissions and permission not in self.permissions:
            raise PermissionError(
                f"actor {self.actor.type.value}:{self.actor.id} lacks {permission}"
            )
