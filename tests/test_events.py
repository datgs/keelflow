"""T13 contract tests: EventBus + outbox dispatcher.

Requires Postgres at PYERP_TEST_DSN.
"""
from __future__ import annotations

import os

import psycopg
import pytest
import pytest_asyncio

from pyerp.engine.events import OutboxEventBus, dispatch_once
from pyerp.postgres.schema import apply_async
from pyerp.sdk import DomainEvent

DSN = os.environ.get(
    "PYERP_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/pyerp"
)

RECEIVED: list[tuple[str, dict]] = []


async def handler_ok(event, ctx):
    RECEIVED.append(("ok", event.payload))


async def handler_flaky(event, ctx):
    RECEIVED.append(("flaky", event.payload))
    raise RuntimeError("subscriber blew up")


async def _conn(autocommit: bool = True):
    return await psycopg.AsyncConnection.connect(DSN, autocommit=autocommit)


@pytest_asyncio.fixture
async def db():
    try:
        conn = await _conn()
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE hd_flow_outbox, hd_flow_outbox_deliveries CASCADE"
        )
        RECEIVED.clear()
        yield conn


@pytest.mark.asyncio
async def test_publish_inside_tx_and_dispatch(db):
    bus = OutboxEventBus()
    bus.subscribe("order.paid@v1", handler_ok)

    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            await bus.publish(
                DomainEvent(type="order.paid@v1", payload={"order_id": "o-1"}),
                cur,
            )

    n = await dispatch_once(db, bus)
    assert n == 1
    assert RECEIVED == [("ok", {"order_id": "o-1"})]
    row = await (await db.execute(
        "SELECT status FROM hd_flow_outbox"
    )).fetchone()
    assert row[0] == "published"


@pytest.mark.asyncio
async def test_rollback_produces_no_event(db):
    bus = OutboxEventBus()
    bus.subscribe("order.paid@v1", handler_ok)

    async with await _conn(autocommit=False) as conn:
        with pytest.raises(RuntimeError):
            async with conn.transaction(), conn.cursor() as cur:
                await bus.publish(
                    DomainEvent(type="order.paid@v1", payload={"x": 1}), cur
                )
                raise RuntimeError("abort")

    assert await dispatch_once(db, bus) == 0
    assert RECEIVED == []
    assert (await (await db.execute(
        "SELECT count(*) FROM hd_flow_outbox"
    )).fetchone())[0] == 0


@pytest.mark.asyncio
async def test_at_least_once_dedup_per_subscriber(db):
    """Simulate crash: handler succeeds, mark-published fails → redispatch
    must not re-deliver to the same subscriber."""
    bus = OutboxEventBus()
    bus.subscribe("e.t@v1", handler_ok)

    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            await bus.publish(DomainEvent(type="e.t@v1", payload={"a": 1}), cur)

    # deliver but simulate crash before the outbox row is marked published
    row = await (await db.execute("SELECT id FROM hd_flow_outbox")).fetchone()
    key = f"{handler_ok.__module__}.{handler_ok.__qualname__}"
    await handler_ok(DomainEvent(type="e.t@v1", payload={"a": 1}), None)
    await db.execute(
        "INSERT INTO hd_flow_outbox_deliveries (outbox_id, subscriber)"
        " VALUES (%s, %s)",
        (row[0], key),
    )
    # row still pending → dispatch again; subscriber must not re-fire.
    n = await dispatch_once(db, bus)
    assert n == 1  # marked published
    assert RECEIVED == [("ok", {"a": 1})]  # handler ran only once


@pytest.mark.asyncio
async def test_failed_subscriber_marks_event_failed(db):
    bus = OutboxEventBus()
    bus.subscribe("e.f@v1", handler_flaky)

    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            await bus.publish(DomainEvent(type="e.f@v1", payload={}), cur)

    n = await dispatch_once(db, bus)
    assert n == 0
    row = await (await db.execute(
        "SELECT status, attempts FROM hd_flow_outbox"
    )).fetchone()
    assert row[0] == "failed"
    assert row[1] == 1
