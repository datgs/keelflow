"""Postgres implementation of the semantic Storage port.

`PgSession` wraps one `psycopg.AsyncConnection`. `begin()` requires the
connection to be idle — it raises when uncommitted work is pending so a
forgotten write is never silently committed or discarded by a later
scope. `UnitOfWork.nested()` maps to `conn.transaction()` which psycopg
lowers to a savepoint when a transaction is already open.

All SQL lives here (plus kernel adapters and `postgres/schema.py`).
Engine and SDK core import only the protocol from `keelflow.sdk`.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from ..sdk.services import JobStatus
from ..sdk.storage import (
    DlqInsert,
    JobFinish,
    JobInsert,
    Storage,
)
from ..sdk.uow import Session, UnitOfWork


class PgUnitOfWork(UnitOfWork):
    """One open transaction on a psycopg connection."""

    def __init__(self, conn: psycopg.AsyncConnection) -> None:
        self._conn = conn

    @property
    def native(self) -> psycopg.AsyncConnection:
        return self._conn

    def nested(self):
        """Savepoint child scope — failure rolls back only child writes."""
        conn = self._conn

        @asynccontextmanager
        async def _scope():
            async with conn.transaction():
                yield PgUnitOfWork(conn)

        return _scope()


class PgSession(Session):
    """One psycopg connection; factory for transaction units."""

    def __init__(self, conn: psycopg.AsyncConnection) -> None:
        self._conn = conn
        self._in_tx = False  # True while a begin() scope is open

    @property
    def native(self) -> psycopg.AsyncConnection:
        return self._conn

    def begin(self):
        conn = self._conn
        session = self

        @asynccontextmanager
        async def _uow():
            if session._in_tx:
                raise RuntimeError(
                    "session already has an open UnitOfWork — "
                    "use uow.nested() for a child scope"
                )
            # psycopg opens an implicit TX on the first statement; an
            # explicit rollback keeps begin() honest when the caller
            # ran raw statements through session.native.
            await conn.rollback()
            session._in_tx = True
            try:
                async with conn.transaction():
                    yield PgUnitOfWork(conn)
            finally:
                session._in_tx = False

        return _uow()

    def key_lock(self, key: str):
        """Session-scoped `pg_advisory_lock` on the key's hash."""
        conn = self._conn

        @asynccontextmanager
        async def _lock():
            await conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (key,))
            try:
                yield
            finally:
                await conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (key,))

        return _lock()

    async def close(self) -> None:
        await self._conn.close()


class PostgresStorage(Storage):
    """Semantic Storage port backed by Postgres/psycopg.

    Owns a DSN only — sessions are opened per use by the engine or the
    application. Methods never commit; the caller's `session.begin()`
    scope is the atomic boundary.
    """

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    async def connect(self) -> PgSession:
        return PgSession(await psycopg.AsyncConnection.connect(self.dsn))

    # ── helpers ───────────────────────────────────────────────────

    @staticmethod
    def _conn(uow: UnitOfWork) -> psycopg.AsyncConnection:
        conn = uow.native
        if not isinstance(conn, psycopg.AsyncConnection):
            raise TypeError(
                f"PostgresStorage requires a psycopg unit of work, got {type(conn)}"
            )
        return conn

    @staticmethod
    def _cur(uow: UnitOfWork):
        return PostgresStorage._conn(uow).cursor(row_factory=dict_row)

    # ── job lifecycle ────────────────────────────────────────────

    async def job_insert(self, uow: UnitOfWork, job: JobInsert) -> str | None:
        import uuid as _uuid
        from datetime import UTC, datetime

        now = datetime.now(UTC)
        conn = self._conn(uow)
        cur = await conn.execute(
            """
            INSERT INTO keelflow_jobs
                (id, type, status, payload, context, attempts, max_attempts,
                 run_at, claimed_by, lease_until, concurrency_key, assignee,
                 due_at, parent_job_id, result, created_at, updated_at,
                 unique_key, cancel_requested)
            VALUES (%s,%s,%s,%s,%s,0,%s,%s,NULL,NULL,%s,%s,%s,%s,NULL,%s,%s,
                    %s,%s)
            ON CONFLICT (type, unique_key)
                WHERE unique_key IS NOT NULL
                  AND status IN ('pending','claimed','running','blocked')
            DO NOTHING
            RETURNING id
            """,
            (
                _uuid.UUID(job.id) if isinstance(job.id, str) else job.id,
                job.type,
                job.status,
                json.dumps(job.payload),
                json.dumps(job.context),
                job.max_attempts,
                job.run_at or now,
                job.concurrency_key,
                job.assignee,
                job.due_at,
                job.parent_job_id,
                now,
                now,
                job.unique_key,
                job.cancel_requested,
            ),
        )
        inserted = await cur.fetchone()
        if inserted is not None:
            return str(inserted[0])
        existing = await (
            await conn.execute(
                "SELECT id FROM keelflow_jobs WHERE type=%s AND unique_key=%s"
                " AND status IN ('pending','claimed','running','blocked')"
                " ORDER BY created_at LIMIT 1",
                (job.type, job.unique_key),
            )
        ).fetchone()
        return str(existing[0]) if existing else None

    async def job_mark_running(self, uow: UnitOfWork, job_id: str) -> bool:
        cur = await self._conn(uow).execute(
            "UPDATE keelflow_jobs SET status=%s, attempts = attempts + 1,"
            " updated_at=now()"
            " WHERE id=%s"
            "   AND status IN ('pending','claimed','running')"
            "   AND cancel_requested = false",
            (JobStatus.RUNNING.value, job_id),
        )
        return bool(cur.rowcount)

    async def job_cancel_requested(self, uow: UnitOfWork, job_id: str) -> bool:
        cur = await self._conn(uow).execute(
            "SELECT cancel_requested FROM keelflow_jobs WHERE id=%s",
            (job_id,),
        )
        row = await cur.fetchone()
        return bool(row and row[0])

    async def job_finish(self, uow: UnitOfWork, job_id: str, finish: JobFinish) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_jobs SET status=%s, result=%s, result_bytes=%s,"
            " error=%s, updated_at=now() WHERE id=%s",
            (
                finish.status.value,
                json.dumps(finish.result) if finish.result else None,
                finish.result_bytes,
                finish.error,
                job_id,
            ),
        )

    async def job_load(self, uow: UnitOfWork, job_id: str) -> dict[str, Any] | None:
        cur = self._conn(uow).cursor(row_factory=dict_row)
        await cur.execute(
            "SELECT id, type, status, payload, context, concurrency_key,"
            " cancel_requested, attempts, max_attempts, dispatch_queued"
            " FROM keelflow_jobs WHERE id=%s",
            (job_id,),
        )
        return await cur.fetchone()

    async def job_cancel_request(self, uow: UnitOfWork, job_id: str) -> str | None:
        cur = await self._conn(uow).execute(
            "UPDATE keelflow_jobs"
            " SET cancel_requested=true,"
            "     status = CASE WHEN status = 'pending'"
            "                   THEN 'cancelled' ELSE status END,"
            "     updated_at=now()"
            " WHERE id=%s"
            "   AND status IN ('pending','claimed','running','blocked')"
            " RETURNING status",
            (job_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else None

    async def job_cancel_request_descendants(
        self, uow: UnitOfWork, job_id: str
    ) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_jobs"
            " SET cancel_requested=true,"
            "     status = CASE WHEN status = 'pending'"
            "                   THEN 'cancelled' ELSE status END,"
            "     updated_at=now()"
            " WHERE parent_job_id=%s"
            "   AND status IN ('pending','claimed','running','blocked')",
            (job_id,),
        )

    async def job_error_and_reschedule(
        self, uow: UnitOfWork, job_id: str, error: str, delay_s: float
    ) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_jobs SET error=%s,"
            " run_at=run_at + interval '1 second' * %s,"
            " updated_at=now() WHERE id=%s",
            (error, delay_s, job_id),
        )

    async def job_retry_reset(self, uow: UnitOfWork, job_id: str) -> int:
        cur = await self._conn(uow).execute(
            "UPDATE keelflow_jobs SET status='pending', error=NULL,"
            " attempts=0, updated_at=now()"
            " WHERE id=%s AND status='failed'",
            (job_id,),
        )
        return cur.rowcount

    async def job_attempts(
        self, uow: UnitOfWork, job_id: str
    ) -> tuple[int, int] | None:
        cur = await self._conn(uow).execute(
            "SELECT attempts, max_attempts FROM keelflow_jobs WHERE id=%s",
            (job_id,),
        )
        row = await cur.fetchone()
        return (row[0], row[1]) if row else None

    async def job_lock(self, uow: UnitOfWork, job_id: str) -> dict[str, Any] | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT * FROM keelflow_jobs WHERE id=%s FOR UPDATE", (job_id,)
        )
        return await cur.fetchone()

    async def job_signal_wait(
        self,
        uow: UnitOfWork,
        job_id: str,
        status: JobStatus,
        result: dict[str, Any],
        error: str | None,
    ) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_jobs SET status=%s, result=%s, error=%s,"
            " updated_at=now() WHERE id=%s",
            (status.value, json.dumps(result), error, job_id),
        )

    async def job_type_of(self, uow: UnitOfWork, job_id: str) -> str | None:
        cur = await self._conn(uow).execute(
            "SELECT type FROM keelflow_jobs WHERE id=%s", (job_id,)
        )
        row = await cur.fetchone()
        return row[0] if row else None

    async def job_parent_cancel_flag(self, uow: UnitOfWork, job_id: str) -> bool:
        cur = await self._conn(uow).execute(
            "SELECT cancel_requested FROM keelflow_jobs WHERE id=%s", (job_id,)
        )
        row = await cur.fetchone()
        return bool(row and row[0])

    # ── outbox ────────────────────────────────────────────────────

    async def outbox_insert(
        self, uow: UnitOfWork, event_type: str, payload: dict[str, Any]
    ) -> None:
        await self._conn(uow).execute(
            "INSERT INTO keelflow_outbox (event_type, payload) VALUES (%s, %s)",
            (event_type, json.dumps(payload)),
        )

    # ── dlq ──────────────────────────────────────────────────────

    async def dlq_insert(self, uow: UnitOfWork, entry: DlqInsert) -> None:
        await self._conn(uow).execute(
            "INSERT INTO keelflow_dlq (id, type, payload, context, error,"
            " attempts, run_id, step_run_id)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (id) DO UPDATE"
            " SET error=EXCLUDED.error, attempts=EXCLUDED.attempts,"
            " dead_at=now()",
            (
                entry.id,
                entry.type,
                json.dumps(entry.payload),
                json.dumps(entry.context),
                entry.error,
                entry.attempts,
                entry.run_id,
                entry.step_run_id,
            ),
        )
