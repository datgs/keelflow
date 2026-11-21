"""JobContext — ambient execution context for every job."""
from __future__ import annotations

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

    Engine-owned, never serialized into `jobs.context`. `tx` is the
    handler-transaction cursor (also available as `ctx.tx`).

    `checkpoint()` lets a handler cooperatively stop at a safe point:
    if the job's `cancel_requested` flag is set it raises
    `JobCancelled`, which unwinds the handler transaction. Only takes
    effect when the job type opts in via
    `JobTypeSpec(cooperative_cancel=True)`.
    """

    job_id: str
    cooperative_cancel: bool = False
    tx: Any = None

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
        if self.tx is None:
            raise RuntimeError("checkpoint() is only valid inside the handler transaction")
        cur = await self.tx.execute(
            "SELECT cancel_requested FROM keelflow_jobs WHERE id=%s",
            (self.job_id,),
        )
        row = await cur.fetchone()
        if row and row[0]:
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
    # In-run cursor set by the dispatcher when the handler executes inside
    # the finish TX. Never serialized (excluded from ctx_to_dict).
    tx: Any = field(default=None, repr=False, compare=False)
    # Worker-injected execution runtime (job id, checkpoint API). Set by
    # dispatch before handler.run; never serialized.
    execution: Any = field(default=None, repr=False, compare=False)

    def require(self, permission: str) -> None:
        """Raise if the actor lacks a permission (service-layer check)."""
        if "*" not in self.permissions and permission not in self.permissions:
            raise PermissionError(
                f"actor {self.actor.type.value}:{self.actor.id} lacks {permission}"
            )
