"""Chancy kernel adapter — the production default backend.

Wraps a `chancy.Chancy` client behind the `Kernel` port: spec →
`chancy.Job` conversion, `push_ex`/`push_many_ex` delegation, queue
declaration, worker lifecycle, and `queue.pushed` notifications.
"""
from __future__ import annotations

import logging
from typing import Any

from .kernel import DEFAULT_QUEUE, KernelJobSpec, KernelWorker

log = logging.getLogger("keelflow.chancy_kernel")


def warn_best_effort() -> None:
    log.warning(
        "Chancy is selected as an opt-in best-effort backend; Keelflow "
        "cannot guarantee claim fencing or bounded engine attempt "
        "accounting for crashes before dispatch starts")


def _cur(uow):
    """Chancy needs a dict_row cursor on the uow's psycopg connection.

    A caller-supplied cursor is swapped for a dict_row cursor on the
    same connection — `client.push_ex` reads columns by name."""
    from psycopg.rows import dict_row

    native = getattr(uow, "native", uow)  # tests may pass a raw conn
    conn = getattr(native, "connection", native)  # cursor/tx → conn
    return conn.cursor(row_factory=dict_row)


class ChancyKernel:
    """Kernel backed by a `chancy.Chancy` client (opt-in, best-effort)."""

    def __init__(self, client) -> None:
        self.client = client
        self.dsn = client.dsn

    # ── spec factory ────────────────────────────────────────────────────

    def job(self, **kwargs: Any) -> KernelJobSpec:
        return KernelJobSpec(**kwargs)

    # ── dispatch push ───────────────────────────────────────────────────

    async def push_ex(self, uow, spec: KernelJobSpec):
        return await self.client.push_ex(_cur(uow), _to_chancy_job(spec))

    async def push_many_ex(self, uow, specs: list[KernelJobSpec]):
        return await self.client.push_many_ex(
            _cur(uow), [_to_chancy_job(s) for s in specs])

    async def notify_pushed(self, uow, queue: str = DEFAULT_QUEUE) -> None:
        await self.client.notify(_cur(uow), "queue.pushed", {"q": queue})

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
