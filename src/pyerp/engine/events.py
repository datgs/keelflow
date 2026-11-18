"""EventBus + outbox dispatcher (T13).

`EventBus.publish` writes a `hd_flow_outbox` row inside the caller's
transaction — if the TX rolls back, no event exists (must-prove #2).

A background `dispatch_events` loop (run alongside the worker) claims
pending outbox rows with `FOR UPDATE SKIP LOCKED` and calls each
subscriber. Delivery is at-least-once: `hd_flow_outbox_deliveries`
records (outbox_id, subscriber) so a crash between handler success and
the mark-published write doesn't redeliver to the same subscriber.

Subscribers are keyed by `f"{module}.{qualname}"`; two different handlers
registered under the same key share a delivery record.
"""
from __future__ import annotations

import asyncio
import logging

import psycopg
from psycopg.rows import dict_row

from ..sdk import DomainEvent, EventBus, EventHandler, JobContext, Transaction
from .enqueue import _as_cursor

log = logging.getLogger("pyerp.events")


class OutboxEventBus(EventBus):
    def __init__(self) -> None:
        self._subscribers: dict[str, dict[str, EventHandler]] = {}

    async def publish(self, event: DomainEvent, tx: Transaction) -> None:
        cursor = _as_cursor(tx)
        import json

        await cursor.execute(
            "INSERT INTO hd_flow_outbox (event_type, payload) VALUES (%s, %s)",
            (
                event.type,
                json.dumps(
                    {
                        "payload": event.payload,
                        "occurred_at": (
                            event.occurred_at.isoformat()
                            if event.occurred_at
                            else None
                        ),
                    }
                ),
            ),
        )

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        key = f"{getattr(handler, '__module__', '')}.{getattr(handler, '__qualname__', repr(handler))}"
        self._subscribers.setdefault(event_type, {})[key] = handler

    def subscribers_for(self, event_type: str) -> dict[str, EventHandler]:
        subs = dict(self._subscribers.get(event_type, {}))
        subs.update(self._subscribers.get("*", {}))
        return subs


async def dispatch_once(
    conn, bus: OutboxEventBus, *, limit: int = 50
) -> int:
    """Claim and deliver up to `limit` pending outbox rows.

    Each row is claimed in its own transaction (FOR UPDATE SKIP LOCKED), so
    locks are released per delivery and multiple dispatchers can run
    concurrently without double-delivering.
    """
    delivered = 0
    for _ in range(limit):
        async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, event_type, payload, attempts FROM hd_flow_outbox"
                " WHERE status='pending' ORDER BY id LIMIT 1"
                " FOR UPDATE SKIP LOCKED",
            )
            row = await cur.fetchone()
        if row is None:
            break
        delivered += await _deliver_row(conn, bus, row)
    return delivered


async def _deliver_row(conn, bus: OutboxEventBus, row) -> int:
    subs = bus.subscribers_for(row["event_type"])
    event = DomainEvent(
        type=row["event_type"],
        payload=(row["payload"] or {}).get("payload", row["payload"] or {}),
    )
    ctx = JobContext(actor=_system_actor())
    failed = False
    for key, handler in subs.items():
        seen = await conn.execute(
            "SELECT 1 FROM hd_flow_outbox_deliveries"
            " WHERE outbox_id=%s AND subscriber=%s",
            (row["id"], key),
        )
        if await seen.fetchone():
            continue
        try:
            await handler(event, ctx)
        except Exception:
            log.exception("event %s delivery to %s failed", row["id"], key)
            failed = True
            break
        await conn.execute(
            "INSERT INTO hd_flow_outbox_deliveries (outbox_id, subscriber)"
            " VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (row["id"], key),
        )
    if failed:
        await conn.execute(
            "UPDATE hd_flow_outbox SET status='failed',"
            " attempts=attempts+1 WHERE id=%s",
            (row["id"],),
        )
    else:
        await conn.execute(
            "UPDATE hd_flow_outbox SET status='published',"
            " published_at=now() WHERE id=%s",
            (row["id"],),
        )
    await conn.commit()
    return 0 if failed else 1


async def dispatch_events(
    dsn: str, bus: OutboxEventBus, *, interval: float = 2.0, stop=None
) -> None:
    """Poll pending outbox rows and deliver to subscribers. Runs forever;
    stop via the optional asyncio.Event or task cancellation."""
    conn = await psycopg.AsyncConnection.connect(dsn)
    try:
        while not (stop and stop.is_set()):
            try:
                n = await dispatch_once(conn, bus)
            except Exception:
                log.exception("outbox dispatch failed")
                n = 0
            await asyncio.sleep(interval if n == 0 else 0)
    finally:
        await conn.close()


def _system_actor():
    from ..sdk import Actor

    return Actor.system()
