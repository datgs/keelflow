"""JobHandler — the interface domain modules implement."""
from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import BaseModel

from .context import JobContext
from .specs import JobResult


class JobHandler(ABC):
    """Business logic for one job type. Engine owns lifecycle.

    Handlers receive a read-only JobContext and a validated payload.
    They return a JobResult with small reference outputs (ids, counts).
    """

    @abstractmethod
    async def run(self, ctx: JobContext, payload: BaseModel) -> JobResult:
        """Execute the job. Raise or return failed status on error."""
        ...

    async def on_cancel(self, ctx: JobContext) -> None:
        """Cooperative cancel hook. Default: no-op.

        Experimental — the current worker never invokes this hook; it is
        a placeholder for a future cleanup protocol and must not carry
        business invariants. Actual cooperative cancellation happens
        through `ctx.execution.checkpoint()` on job types that opt in
        via `JobTypeSpec(cooperative_cancel=True)`.
        """
        return
