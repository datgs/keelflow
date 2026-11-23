"""Chancy kernel adapter — the production default backend.

Wraps a `chancy.Chancy` client behind the `Kernel` port: spec →
`chancy.Job` conversion, `push_ex`/`push_many_ex` delegation, queue
declaration, worker lifecycle, and `queue.pushed` notifications.
"""
from __future__ import annotations

from typing import Any

from .kernel import DEFAULT_QUEUE, KernelJobSpec, KernelWorker


class ChancyKernel:
    """Kernel backed by a `chancy.Chancy` client (production default)."""

    def __init__(self, client) -> None:
        self.client = client
        self.dsn = client.dsn

    # ── spec factory ────────────────────────────────────────────────────

    def job(self, **kwargs: Any) -> KernelJobSpec:
        return KernelJobSpec(**kwargs)

    # ── dispatch push ───────────────────────────────────────────────────

    async def push_ex(self, cursor, spec: KernelJobSpec):
        return await self.client.push_ex(cursor, _to_chancy_job(spec))

    async def push_many_ex(self, cursor, specs: list[KernelJobSpec]):
        return await self.client.push_many_ex(
            cursor, [_to_chancy_job(s) for s in specs])

    async def notify_pushed(self, cursor, queue: str = DEFAULT_QUEUE) -> None:
        await self.client.notify(cursor, "queue.pushed", {"q": queue})

    # ── lifecycle ───────────────────────────────────────────────────────

    def create_worker(self, **kwargs: Any) -> KernelWorker:
        import chancy

        return chancy.Worker(self.client, **kwargs)

    async def migrate(self) -> None:
        await self.client.migrate()

    async def declare_queue(self, queue: Any, *, upsert: bool = False) -> Any:
        return await self.client.declare(queue, upsert=upsert)


def _to_chancy_job(spec: KernelJobSpec):
    from datetime import UTC, datetime

    import chancy

    return chancy.Job(
        func=spec.func,
        queue=spec.queue,
        kwargs=spec.kwargs,
        priority=spec.priority,
        max_attempts=spec.max_attempts,
        scheduled_at=spec.scheduled_at or datetime.now(UTC),
        limits=list(spec.limits),
        unique_key=spec.unique_key,
        meta=spec.meta or {},
    )
