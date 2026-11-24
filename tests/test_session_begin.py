"""Session.begin() idle-contract tests (review finding 1).

`begin()` must raise when the connection has a pending transaction —
silent rollback would discard a caller's uncommitted writes. These use
deterministic fake connections (no database) plus a live Postgres case
in `test_uow_contract.py`.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from psycopg import pq

from keelflow.postgres.storage import PgSession


class _Info:
    def __init__(self, status):
        self.transaction_status = status


class FakeConn:
    """Minimal psycopg conn surface used by PgSession.begin()."""

    def __init__(self, status=pq.TransactionStatus.IDLE):
        self.info = _Info(status)
        self.rollback_calls = 0
        self.transaction_calls = 0

    async def rollback(self):
        self.rollback_calls += 1

    def transaction(self):
        self.transaction_calls += 1

        @asynccontextmanager
        async def _tx():
            self.info.transaction_status = pq.TransactionStatus.INTRANS
            try:
                yield
            finally:
                self.info.transaction_status = pq.TransactionStatus.IDLE

        return _tx()

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_begin_rejects_pending_implicit_work():
    """A connection holding INTRANS (implicit TX from a raw statement)
    fails loudly — begin() must not rollback the caller's work away."""
    conn = FakeConn(pq.TransactionStatus.INTRANS)
    session = PgSession(conn)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="pending transaction"):
        async with session.begin():
            pass  # pragma: no cover
    assert conn.rollback_calls == 0
    assert conn.transaction_calls == 0


@pytest.mark.asyncio
async def test_begin_rejects_error_transaction():
    conn = FakeConn(pq.TransactionStatus.INERROR)
    session = PgSession(conn)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="pending transaction"):
        async with session.begin():
            pass  # pragma: no cover
    assert conn.rollback_calls == 0


@pytest.mark.asyncio
async def test_begin_opens_unit_on_idle_connection():
    conn = FakeConn()
    session = PgSession(conn)  # type: ignore[arg-type]
    async with session.begin() as uow:
        assert uow.native is conn
    assert conn.transaction_calls == 1


@pytest.mark.asyncio
async def test_begin_still_rejects_nested_begin():
    conn = FakeConn()
    session = PgSession(conn)  # type: ignore[arg-type]
    async with session.begin():
        with pytest.raises(RuntimeError, match="open UnitOfWork"):
            async with session.begin():
                pass  # pragma: no cover


# ── live Postgres cases ────────────────────────────────────────────────

DSN_IMPORT = "postgresql://postgres:postgres@localhost:5544/keelflow_test"


@pytest.mark.asyncio
async def test_begin_rejects_pending_work_live():
    """A raw write through session.native left uncommitted makes the
    next begin() raise; an explicit rollback restores the idle session."""
    import os

    from keelflow.postgres.storage import PostgresStorage

    dsn = os.environ.get("KEELFLOW_TEST_DSN", DSN_IMPORT)
    try:
        storage = PostgresStorage(dsn)
        session = await storage.connect()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    try:
        await session.native.execute("SELECT 1")  # implicit TX opens
        with pytest.raises(RuntimeError, match="pending transaction"):
            async with session.begin():
                pass  # pragma: no cover
        await session.native.rollback()
        async with session.begin():
            pass  # idle again — works
    finally:
        await session.close()
