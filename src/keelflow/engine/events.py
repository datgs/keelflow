"""EventBus + outbox dispatcher (T13).

`EventBus.publish` writes an outbox row inside the caller's unit of work
— if the UoW rolls back, no event exists (must-prove #2).

A background `dispatch_events` loop (run alongside the worker) claims
pending outbox rows one unit of work at a time and calls each
subscriber. Delivery is at-least-once: `keelflow_outbox_deliveries`
records (outbox_id, subscriber) so a crash between handler success and
the mark-published write doesn't redeliver to the same subscriber.

Subscribers are keyed by `f"{module}.{qualname}"`; two different handlers
registered under the same key share a delivery record.
"""
from __future__ import annotations

import asyncio
import logging

from ..sdk import DomainEvent, EventBus, EventHandler, JobContext, Storage

log = logging.getLogger("keelflow.events")


class OutboxEventBus(EventBus):
    def __init__(self, storage: Storage) -> None:
        self._storage = storage
        self._subscribers: dict[str, dict[str, EventHandler]] = {}

    @property
    def storage(self) -> Storage:
        return self._storage

    async def publish(self, event: DomainEvent, uow) -> None:
        await self._storage.outbox_insert(uow, event.type, {
            "payload": event.payload,
            "occurred_at": (
                event.occurred_at.isoformat() if event.occurred_at else None
            ),
        })

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        key = f"{getattr(handler, '__module__', '')}.{getattr(handler, '__qualname__', repr(handler))}"
        self._subscribers.setdefault(event_type, {})[key] = handler

    def subscribers_for(self, event_type: str) -> dict[str, EventHandler]:
        subs = dict(self._subscribers.get(event_type, {}))
        subs.update(self._subscribers.get("*", {}))
        return subs


async def dispatch_once(
    session, storage: Storage, bus: OutboxEventBus, *, limit: int = 50
) -> int:
    """Claim and deliver up to `limit` pending outbox rows.

    Each row is claimed in its own unit of work so locks are released
    per delivery and multiple dispatchers can run concurrently without
    double-delivering.
    """
    delivered = 0
    for _ in range(limit):
        async with session.begin() as uow:
            row = await storage.outbox_claim_next(uow)
        if row is None:
            break
        delivered += await _deliver_row(session, storage, bus, row)
    return delivered


async def _deliver_row(session, storage: Storage, bus: OutboxEventBus,
                       row) -> int:
    subs = bus.subscribers_for(row["event_type"])
    event = DomainEvent(
        type=row["event_type"],
        payload=(row["payload"] or {}).get("payload", row["payload"] or {}),
    )
    ctx = JobContext(actor=_system_actor())
    failed = False
    for key, handler in subs.items():
        async with session.begin() as uow:
            seen = await storage.delivery_seen(uow, row["id"], key)
        if seen:
            continue
        try:
            await handler(event, ctx)
        except Exception:
            # Continue to other subscribers: one bad subscriber must not
            # head-of-line-block the rest (retry handles it next pass).
            log.exception("event %s delivery to %s failed", row["id"], key)
            failed = True
            continue
        async with session.begin() as uow:
            await storage.delivery_mark(uow, row["id"], key)
    async with session.begin() as uow:
        await storage.outbox_settle(
            uow, row["id"], "failed" if failed else "published")
    return 0 if failed else 1


async def dispatch_events(
    bus: OutboxEventBus, storage: Storage, *, interval: float = 2.0,
    stop=None,
) -> None:
    """Poll pending outbox rows and deliver to subscribers. Runs forever;
    stop via the optional asyncio.Event or task cancellation."""
    session = await storage.connect()
    try:
        while not (stop and stop.is_set()):
            try:
                n = await dispatch_once(session, storage, bus)
            except Exception:
                log.exception("outbox dispatch failed")
                n = 0
            await asyncio.sleep(interval if n == 0 else 0)
    finally:
        await session.close()


def _system_actor():
    from ..sdk import Actor

    return Actor.system()
