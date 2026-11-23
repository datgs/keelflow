"""Engine kernel port (T-kernel): abstract dispatch backend.

The engine never constructs backend-specific job objects outside this
module. A `Kernel` implementation supplies:

- `job()` / `push_ex()` / `push_many_ex()` — dispatch rows inside the
  caller's transaction; the worker may claim them on commit.
- `create_worker()` — a backend-owned claim/invoke loop that calls
  `keelflow.engine.worker.dispatch(job_id, step_run_id)`. The engine
  never claims dispatch work itself.
- `migrate()` / `declare_queue()` — backend schema/queue setup.

Chancy is the default backend (`chancy_kernel.ChancyKernel`); native
Postgres (`native_kernel`) and SQLite (`sqlite_kernel`) adapters
implement the same port.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

#: Kernel job func that dispatches into our handler registry.
#: Re-exported by `enqueue` for existing callers.
DISPATCH_FUNC = "keelflow.engine.worker.dispatch"

#: Queue all engine dispatch rows go to.
DEFAULT_QUEUE = "default"


@dataclass(frozen=True)
class KernelJobSpec:
    """Backend-neutral dispatch-row spec. Only `job()` consumes it."""

    func: str
    queue: str
    kwargs: dict[str, Any]
    max_attempts: int
    scheduled_at: Any
    unique_key: str | None = None
    meta: dict[str, Any] | None = None
    priority: int = 0
    limits: tuple = ()


class KernelWorker(Protocol):
    """Backend-owned worker lifecycle (claim/invoke loop)."""

    async def start(self) -> None: ...

    async def stop(self, *, timeout: float | None = None) -> bool: ...

    async def wait_for_shutdown(self) -> Any: ...


@runtime_checkable
class Kernel(Protocol):
    """Backend port for engine dispatch + worker lifecycle."""

    dsn: str

    def job(self, **kwargs: Any) -> KernelJobSpec: ...

    async def push_ex(self, cursor, spec: KernelJobSpec) -> Any: ...

    async def push_many_ex(self, cursor, specs: list[KernelJobSpec]) -> Any: ...

    async def notify_pushed(self, cursor, queue: str) -> None: ...

    def create_worker(self, **kwargs: Any) -> KernelWorker: ...

    async def migrate(self) -> None: ...

    async def declare_queue(self, queue: Any, *, upsert: bool = False) -> Any: ...


def dispatch_spec(*, job_id, queue: str = DEFAULT_QUEUE,
                  max_attempts: int = 3, scheduled_at=None,
                  unique_key: str | None = None,
                  meta: dict[str, Any] | None = None,
                  step_run_id=None) -> KernelJobSpec:
    """Shared spec builder for engine dispatch rows.

    `func` is fixed to the engine dispatch entry point; `job_id`/`type`
    meta lets ops correlate kernel rows to `keelflow_jobs`.
    """
    return KernelJobSpec(
        func=DISPATCH_FUNC,
        queue=queue,
        kwargs={"job_id": str(job_id),
                "step_run_id": str(step_run_id) if step_run_id else None},
        max_attempts=max_attempts,
        scheduled_at=scheduled_at,
        unique_key=unique_key,
        meta=meta,
    )


def as_kernel(obj: Any) -> Kernel:
    """Accept a Kernel, a raw `chancy.Chancy`, or raise.

    Raw `chancy.Chancy` objects are wrapped — the test suite and
    `run_worker` bootstrap pass the client directly today.
    """
    from .chancy_kernel import ChancyKernel

    if isinstance(obj, ChancyKernel):
        return obj
    if isinstance(obj, Kernel):
        return obj
    if hasattr(obj, "push_ex") and hasattr(obj, "migrate"):
        return ChancyKernel(obj)
    raise TypeError(f"expected Kernel or chancy.Chancy, got {type(obj)}")
