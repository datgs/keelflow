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
import uuid
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from ..sdk.services import AuditEvent, JobStatus
from ..sdk.storage import (
    DlqInsert,
    JobFinish,
    JobInsert,
    Storage,
    json_default,
)
from ..sdk.uow import Session, UnitOfWork


class PgUnitOfWork(UnitOfWork):
    """One open transaction on a psycopg connection."""

    def __init__(self, conn: psycopg.AsyncConnection) -> None:
        self._conn = conn

    @property
    def native(self) -> psycopg.AsyncConnection:
        return self._conn

    def __getattr__(self, name: str):
        """Adapter edge: `uow.execute`/`uow.cursor` reach the psycopg
        connection — handlers and repos migrate one call site at a time
        without unwrapping `.native` everywhere."""
        if name == "executemany":
            conn = self._conn

            async def _executemany(query, params, *args, **kwargs):
                cur = conn.cursor()
                await cur.executemany(query, params, *args, **kwargs)
                return cur

            return _executemany
        return getattr(self._conn, name)

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
        # Unit of work → its native psycopg connection. A bare
        # AsyncConnection is accepted (read-only callers and legacy
        # fixtures pass the connection itself as the unit edge).
        conn = getattr(uow, "native", uow)
        if isinstance(conn, (psycopg.AsyncCursor, psycopg.AsyncTransaction)):
            # Legacy fixtures pass a cursor or transaction scope; their
            # connection carries the caller's transaction so writes join
            # the same unit.
            conn = conn.connection
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
                json.dumps(job.payload, default=json_default),
                json.dumps(job.context, default=json_default),
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
            # caller's cursor may be tuple- or dict-shaped
            rid = inserted["id"] if isinstance(inserted, dict) else inserted[0]
            return str(rid)
        existing = await (
            await conn.execute(
                "SELECT id FROM keelflow_jobs WHERE type=%s AND unique_key=%s"
                " AND status IN ('pending','claimed','running','blocked')"
                " ORDER BY created_at LIMIT 1",
                (job.type, job.unique_key),
            )
        ).fetchone()
        if existing is None:
            return None
        rid = existing["id"] if isinstance(existing, dict) else existing[0]
        return str(rid)

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
        cur = self._cur(uow)
        await cur.execute(
            "SELECT cancel_requested FROM keelflow_jobs WHERE id=%s",
            (job_id,),
        )
        row = await cur.fetchone()
        return bool(row and row["cancel_requested"])

    async def job_finish(self, uow: UnitOfWork, job_id: str, finish: JobFinish) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_jobs SET status=%s, result=%s, result_bytes=%s,"
            " error=%s, updated_at=now() WHERE id=%s",
            (
                finish.status.value,
                json.dumps(finish.result, default=json_default) if finish.result else None,
                finish.result_bytes,
                finish.error,
                job_id,
            ),
        )

    async def job_load(self, uow: UnitOfWork, job_id: str) -> dict[str, Any] | None:
        cur = self._conn(uow).cursor(row_factory=dict_row)
        await cur.execute(
            "SELECT * FROM keelflow_jobs WHERE id=%s", (job_id,))
        return await cur.fetchone()

    async def job_cancel_request(self, uow: UnitOfWork, job_id: str) -> str | None:
        cur = self._cur(uow)
        await cur.execute(
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
        return row["status"] if row else None

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
        cur = self._cur(uow)
        await cur.execute(
            "SELECT attempts, max_attempts FROM keelflow_jobs WHERE id=%s",
            (job_id,),
        )
        row = await cur.fetchone()
        return (row["attempts"], row["max_attempts"]) if row else None

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
            (status.value, json.dumps(result, default=json_default), error, job_id),
        )

    async def job_type_of(self, uow: UnitOfWork, job_id: str) -> str | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT type FROM keelflow_jobs WHERE id=%s", (job_id,)
        )
        row = await cur.fetchone()
        return row["type"] if row else None

    async def job_parent_cancel_flag(self, uow: UnitOfWork, job_id: str) -> bool:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT cancel_requested FROM keelflow_jobs WHERE id=%s", (job_id,)
        )
        row = await cur.fetchone()
        return bool(row and row["cancel_requested"])

    # ── outbox ────────────────────────────────────────────────────

    async def outbox_insert(
        self, uow: UnitOfWork, event_type: str, payload: dict[str, Any]
    ) -> None:
        await self._conn(uow).execute(
            "INSERT INTO keelflow_outbox (event_type, payload) VALUES (%s, %s)",
            (event_type, json.dumps(payload, default=json_default)),
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
                json.dumps(entry.payload, default=json_default),
                json.dumps(entry.context, default=json_default),
                entry.error,
                entry.attempts,
                entry.run_id,
                entry.step_run_id,
            ),
        )

    async def job_insert_many(self, uow: UnitOfWork, jobs: list[JobInsert]) -> int:
        if not jobs:
            return 0
        from datetime import UTC, datetime

        conn = self._conn(uow)
        now = datetime.now(UTC)
        cur = await conn.execute(
            """
            INSERT INTO keelflow_jobs
                (id, type, status, payload, context, attempts,
                 max_attempts, run_at, parent_job_id,
                 cancel_requested, created_at, updated_at)
            SELECT j.id, j.type, j.status, j.payload::jsonb,
                   j.context::jsonb, 0, j.max_attempts, %s, j.parent_id,
                   j.cancel_requested, %s, %s
            FROM unnest(
                %s::uuid[], %s::text[], %s::text[], %s::text[],
                %s::text[], %s::int[], %s::uuid[], %s::boolean[])
                AS j(id, type, status, payload, context,
                     max_attempts, parent_id, cancel_requested)
            """,
            (
                now, now, now,
                [uuid.UUID(j.id) for j in jobs],
                [j.type for j in jobs],
                [j.status for j in jobs],
                [json.dumps(j.payload, default=json_default) for j in jobs],
                [json.dumps(j.context, default=json_default) for j in jobs],
                [j.max_attempts for j in jobs],
                [j.parent_job_id for j in jobs],
                [j.cancel_requested for j in jobs],
            ),
        )
        return cur.rowcount

    # ── human tasks ──────────────────────────────────────────────

    async def tasks_open_for(
        self, uow: UnitOfWork, actor_id: str, roles: list[str]
    ) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            """
            SELECT id, type, status, payload, assignee, claimed_by,
                   due_at, created_at
            FROM keelflow_jobs
            WHERE type LIKE 'human.%%'
              AND status IN ('pending','claimed')
              AND (
                    claimed_by = %s
                 OR assignee = %s
                 OR assignee = ANY(%s)
                 OR (assignee IS NULL AND claimed_by IS NULL)
              )
            ORDER BY created_at
            """,
            (
                f"user:{actor_id}",
                f"user:{actor_id}",
                [f"role:{r}" for r in roles],
            ),
        )
        return await cur.fetchall()

    async def task_update(
        self,
        uow: UnitOfWork,
        job_id: str,
        *,
        status: str | None = None,
        claimed_by: str | None = None,
        clear_claim: bool = False,
        result: dict[str, Any] | None = None,
        assignee: str | None = None,
    ) -> None:
        sets = ["updated_at=now()"]
        params: list[Any] = []
        if status is not None:
            sets.append("status=%s")
            params.append(status)
        if claimed_by is not None:
            sets.append("claimed_by=%s")
            params.append(claimed_by)
        if clear_claim:
            sets.append("claimed_by=NULL")
        if result is not None:
            sets.append("result=%s")
            params.append(json.dumps(result, default=json_default))
        if assignee is not None:
            sets.append("assignee=%s")
            params.append(assignee)
        params.append(job_id)
        await self._conn(uow).execute(
            f"UPDATE keelflow_jobs SET {', '.join(sets)} WHERE id=%s",
            params,
        )

    async def tasks_escalate_overdue(
        self, uow: UnitOfWork, older_than_days: int, to_role: str
    ) -> int:
        cur = await self._conn(uow).execute(
            """
            UPDATE keelflow_jobs SET assignee=%s, claimed_by=NULL,
                   status='pending', updated_at=now()
            WHERE type LIKE 'human.%%' AND status IN ('pending','claimed')
              AND (
                    (due_at IS NOT NULL AND due_at < now())
                 OR (due_at IS NULL
                     AND created_at < now() - %s * interval '1 day')
              )
            """,
            (f"role:{to_role}", older_than_days),
        )
        return cur.rowcount

    # ── scheduler ────────────────────────────────────────────────

    async def jobs_live_by_type(
        self, uow: UnitOfWork, type_name: str
    ) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, status FROM keelflow_jobs"
            " WHERE type=%s AND status IN ('pending','claimed','running')"
            " ORDER BY created_at DESC LIMIT 1",
            (type_name,),
        )
        return await cur.fetchall()

    # ── workflow runs / steps ────────────────────────────────────

    async def run_insert(
        self,
        uow: UnitOfWork,
        run_id: str,
        workflow: str,
        inputs: dict[str, Any],
        *,
        triggered_by_entity_type: str | None = None,
        triggered_by_entity_id: str | None = None,
        trigger_config: dict[str, Any] | None = None,
        trigger_ctx: dict[str, Any] | None = None,
    ) -> None:
        await self._conn(uow).execute(
            """
            INSERT INTO keelflow_workflow_runs
                (id, workflow, status, inputs,
                 triggered_by_entity_type, triggered_by_entity_id,
                 trigger_config, trigger_ctx, callback_state)
            VALUES (%s, %s, 'running', %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id, workflow, json.dumps(inputs, default=json_default),
                triggered_by_entity_type, triggered_by_entity_id,
                json.dumps(trigger_config) if trigger_config else None,
                json.dumps(trigger_ctx, default=json_default)
                if trigger_ctx else None,
                "pending" if triggered_by_entity_type else "none",
            ),
        )

    async def step_runs_insert(
        self,
        uow: UnitOfWork,
        run_id: str,
        steps: list[tuple[str, str]],
    ) -> None:
        conn = self._conn(uow)
        for step_id, type_name in steps:
            await conn.execute(
                "INSERT INTO keelflow_step_runs (id, run_id, step_id, type,"
                " status) VALUES (%s, %s, %s, %s, 'pending')",
                (uuid.uuid4(), run_id, step_id, type_name),
            )

    async def run_get(
        self, uow: UnitOfWork, run_id: str, *, for_update: bool = False
    ) -> dict[str, Any] | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT * FROM keelflow_workflow_runs WHERE id=%s"
            f"{' FOR UPDATE' if for_update else ''}",
            (run_id,),
        )
        return await cur.fetchone()

    async def steps_get(self, uow: UnitOfWork, run_id: str) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, step_id, type, status, job_id, outputs, attempts"
            " FROM keelflow_step_runs WHERE run_id=%s ORDER BY created_at",
            (run_id,),
        )
        return await cur.fetchall()

    async def step_set_status(
        self, uow: UnitOfWork, step_run_id: str, status: str
    ) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_step_runs SET status=%s, updated_at=now()"
            " WHERE id=%s",
            (status, step_run_id),
        )

    async def step_attach_job(
        self,
        uow: UnitOfWork,
        step_run_id: str,
        job_id: str,
        status: str,
    ) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_step_runs SET job_id=%s, status=%s,"
            " updated_at=now() WHERE id=%s",
            (job_id, status, step_run_id),
        )

    async def step_settle(
        self,
        uow: UnitOfWork,
        step_run_id: str,
        status: str,
        outputs: dict[str, Any],
    ) -> None:
        payload = json.dumps(outputs, default=json_default)
        await self._conn(uow).execute(
            "UPDATE keelflow_step_runs SET status=%s, outputs=%s,"
            " outputs_bytes=%s, updated_at=now() WHERE id=%s",
            (status, payload, len(payload.encode()), step_run_id),
        )

    async def step_fail(self, uow: UnitOfWork, step_run_id: str, error: str) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_step_runs SET status='failed',"
            " outputs=%s, updated_at=now() WHERE id=%s",
            (json.dumps({"error": error}), step_run_id),
        )

    async def step_reset(self, uow: UnitOfWork, step_run_id: str) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_step_runs SET status='pending', job_id=NULL,"
            " attempts=attempts+1, updated_at=now() WHERE id=%s",
            (step_run_id,),
        )

    async def run_set_status(
        self, uow: UnitOfWork, run_id: str, status: str
    ) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_workflow_runs SET status=%s, updated_at=now()"
            " WHERE id=%s",
            (status, run_id),
        )

    async def run_finish(
        self,
        uow: UnitOfWork,
        run_id: str,
        status: str,
        terminal_outcome: str,
    ) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_workflow_runs SET status=%s, terminal_outcome=%s,"
            " updated_at=now() WHERE id=%s",
            (status, terminal_outcome, run_id),
        )

    async def run_reopen(self, uow: UnitOfWork, run_id: str) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_workflow_runs SET status='running',"
            " terminal_outcome=NULL, callback_state='pending',"
            " updated_at=now() WHERE id=%s",
            (run_id,),
        )

    async def run_mark_callback_done(self, uow: UnitOfWork, run_id: str) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_workflow_runs SET callback_state='done',"
            " updated_at=now() WHERE id=%s",
            (run_id,),
        )

    async def step_run_by_job(self, uow: UnitOfWork, job_id: str) -> str | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id FROM keelflow_step_runs WHERE job_id=%s", (job_id,)
        )
        row = await cur.fetchone()
        return str(row["id"]) if row else None

    async def step_run_get(
        self, uow: UnitOfWork, step_run_id: str
    ) -> dict[str, Any] | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT * FROM keelflow_step_runs WHERE id=%s", (step_run_id,)
        )
        return await cur.fetchone()

    async def step_run_for_update(
        self, uow: UnitOfWork, run_id: str, step_id: str
    ) -> dict[str, Any] | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, type, status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id=%s FOR UPDATE",
            (run_id, step_id),
        )
        return await cur.fetchone()

    async def steps_resumable(
        self, uow: UnitOfWork, run_id: str
    ) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, step_id FROM keelflow_step_runs"
            " WHERE run_id=%s AND (status='failed' OR"
            " (status='done' AND outputs->>'decision'='reject'))",
            (run_id,),
        )
        return await cur.fetchall()

    async def trigger_callback_lock(
        self, uow: UnitOfWork, run_id: str
    ) -> dict[str, Any] | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT triggered_by_entity_type, triggered_by_entity_id,"
            " trigger_config, trigger_ctx, callback_state"
            " FROM keelflow_workflow_runs WHERE id=%s FOR UPDATE",
            (run_id,),
        )
        return await cur.fetchone()

    async def run_parent_ctx(
        self, uow: UnitOfWork, run_id: str
    ) -> dict[str, Any] | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT context FROM keelflow_jobs j"
            " JOIN keelflow_step_runs sr ON sr.job_id = j.id"
            " WHERE sr.run_id=%s ORDER BY j.created_at LIMIT 1",
            (run_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        ctx_d = row["context"]
        if isinstance(ctx_d, str):
            ctx_d = json.loads(ctx_d)
        return ctx_d

    # ── bulk ─────────────────────────────────────────────────────

    async def bulk_pending_count(self, uow: UnitOfWork, job_id: str) -> int:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT count(*) AS n FROM keelflow_row_status"
            " WHERE job_id=%s AND status='pending' AND chunk_id IS NULL",
            (job_id,),
        )
        row = await cur.fetchone()
        return int(row["n"]) if row else 0

    async def bulk_partition(
        self,
        uow: UnitOfWork,
        job_id: str,
        chunk_size: int,
        chunk_ids: list[Any],
    ) -> tuple[int, int]:
        """One statement: claim pending unchunked rows (SKIP LOCKED),
        number them, insert the pre-generated chunks, stamp each row
        with its chunk id. The chunk table is passed via unnest so the
        row→chunk join can hash."""
        n_chunks = len(chunk_ids)
        cur = self._cur(uow)
        await cur.execute(
            """
            WITH claimed AS (
                SELECT id FROM keelflow_row_status
                WHERE job_id=%s AND status='pending' AND chunk_id IS NULL
                ORDER BY id
                FOR UPDATE SKIP LOCKED
            ),
            numbered AS (
                SELECT id,
                       (row_number() OVER () - 1) / %s AS ci
                FROM claimed
            ),
            cids(idx, cid) AS (
                SELECT * FROM unnest(%s::int[], %s::uuid[])
            ),
            ins AS (
                INSERT INTO keelflow_chunks (id, job_id, idx, status)
                SELECT cid, %s, idx, 'pending' FROM cids
                RETURNING id
            ),
            upd AS (
                UPDATE keelflow_row_status rs SET chunk_id = c.cid
                FROM numbered n JOIN cids c ON c.idx = n.ci
                WHERE rs.id = n.id
                RETURNING rs.id
            )
            SELECT (SELECT count(*) FROM upd) AS n_rows,
                   (SELECT count(*) FROM ins) AS n_chunks
            """,
            (job_id, chunk_size, list(range(n_chunks)), chunk_ids, job_id),
        )
        part = await cur.fetchone()
        return int(part["n_rows"]), int(part["n_chunks"])

    async def chunk_claim(
        self, uow: UnitOfWork, chunk_id: str, claimed_by: str
    ) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_chunks SET status='running',"
            " claimed_by=%s, lease_until=now() + interval '15 minutes',"
            " attempts=attempts+1, updated_at=now() WHERE id=%s",
            (claimed_by, chunk_id),
        )

    async def rows_reset_processing(self, uow: UnitOfWork, chunk_id: str) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_row_status SET status='pending'"
            " WHERE chunk_id=%s AND status='processing'",
            (chunk_id,),
        )

    async def rows_fetch_pending(
        self, uow: UnitOfWork, chunk_id: str
    ) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, row_key, data FROM keelflow_row_status"
            " WHERE chunk_id=%s AND status='pending' ORDER BY id",
            (chunk_id,),
        )
        return await cur.fetchall()

    async def rows_mark(
        self,
        uow: UnitOfWork,
        row_ids: list[Any],
        status: str,
        *,
        error_by_id: dict[Any, str] | None = None,
    ) -> None:
        if not row_ids:
            return
        conn = self._conn(uow)
        if error_by_id:
            for rid, err in error_by_id.items():
                await conn.execute(
                    "UPDATE keelflow_row_status SET status=%s, error=%s,"
                    " updated_at=now() WHERE id=%s",
                    (status, err, rid),
                )
        else:
            await conn.execute(
                "UPDATE keelflow_row_status SET status=%s, updated_at=now()"
                " WHERE id = ANY(%s)",
                (status, list(row_ids)),
            )

    async def chunk_mark(
        self,
        uow: UnitOfWork,
        chunk_id: str,
        status: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_chunks SET status=%s, result=%s,"
            " updated_at=now() WHERE id=%s",
            (status,
             json.dumps(result, default=json_default) if result else None,
             chunk_id),
        )

    async def chunk_cancel(self, uow: UnitOfWork, chunk_id: str) -> None:
        await self._conn(uow).execute(
            "UPDATE keelflow_chunks SET status='cancelled', updated_at=now()"
            " WHERE id=%s AND status NOT IN"
            " ('done','failed','cancelled')",
            (chunk_id,),
        )

    async def chunk_counts(self, uow: UnitOfWork, job_id: str) -> dict[str, int]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT status, count(*) AS n FROM keelflow_chunks"
            " WHERE job_id=%s GROUP BY status",
            (job_id,),
        )
        return {r["status"]: int(r["n"]) for r in await cur.fetchall()}

    async def row_counts(self, uow: UnitOfWork, job_id: str) -> dict[str, int]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT status, count(*) AS n FROM keelflow_row_status"
            " WHERE job_id=%s GROUP BY status",
            (job_id,),
        )
        return {r["status"]: int(r["n"]) for r in await cur.fetchall()}

    async def row_disposition(self, uow: UnitOfWork, job_id: str) -> str:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT COALESCE(payload->>'row_disposition','archive') AS d"
            " FROM keelflow_jobs WHERE id=%s",
            (job_id,),
        )
        row = await cur.fetchone()
        return (row["d"] if row else None) or "archive"

    async def rows_archive_done(self, uow: UnitOfWork, job_id: str) -> int:
        cur = await self._conn(uow).execute(
            """
            WITH done AS (
                SELECT id, row_key, chunk_id, data FROM keelflow_row_status
                WHERE job_id=%s AND status='done'
            ),
            archived AS (
                INSERT INTO keelflow_row_archive
                    (job_id, row_id, row_key, chunk_id, data)
                SELECT %s, id, row_key, chunk_id, data FROM done
                ON CONFLICT (job_id, row_id) DO NOTHING
                RETURNING row_id
            )
            DELETE FROM keelflow_row_status r USING done
            WHERE r.id = done.id
            RETURNING r.id
            """,
            (job_id, job_id),
        )
        return cur.rowcount

    async def rows_delete_done(self, uow: UnitOfWork, job_id: str) -> int:
        cur = await self._conn(uow).execute(
            "DELETE FROM keelflow_row_status"
            " WHERE job_id=%s AND status='done'",
            (job_id,),
        )
        return cur.rowcount

    async def job_result_merge(
        self,
        uow: UnitOfWork,
        job_id: str,
        status: str,
        patch: dict[str, Any],
    ) -> dict[str, Any]:
        cur = self._cur(uow)
        await cur.execute(
            "UPDATE keelflow_jobs SET status=%s, updated_at=now(),"
            " result = COALESCE(result,'{}'::jsonb) || %s::jsonb"
            " WHERE id=%s RETURNING result",
            (status, json.dumps(patch, default=json_default), job_id),
        )
        row = await cur.fetchone()
        return (row["result"] if row else None) or {}

    # ── state machine / audit ────────────────────────────────────

    async def entity_ensure_locked(
        self,
        uow: UnitOfWork,
        entity_type: str,
        entity_id: str,
        initial: str,
    ) -> str:
        conn = self._conn(uow)
        await conn.execute(
            "INSERT INTO keelflow_entity_state (entity_type, entity_id,"
            " state) VALUES (%s, %s, %s)"
            " ON CONFLICT (entity_type, entity_id) DO NOTHING",
            (entity_type, entity_id, initial),
        )
        cur = self._cur(uow)
        await cur.execute(
            "SELECT state FROM keelflow_entity_state"
            " WHERE entity_type=%s AND entity_id=%s FOR UPDATE",
            (entity_type, entity_id),
        )
        row = await cur.fetchone()
        return row["state"] if row else initial

    async def entity_transition(
        self,
        uow: UnitOfWork,
        entity_type: str,
        entity_id: str,
        to_state: str,
    ) -> None:
        await self._conn(uow).execute(
            """
            INSERT INTO keelflow_entity_state
                (entity_type, entity_id, state, version)
            VALUES (%s, %s, %s, 1)
            ON CONFLICT (entity_type, entity_id) DO UPDATE
            SET state = EXCLUDED.state,
                version = keelflow_entity_state.version + 1,
                updated_at = now()
            """,
            (entity_type, entity_id, to_state),
        )

    async def entity_state(
        self, uow: UnitOfWork, entity_type: str, entity_id: str
    ) -> str | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT state FROM keelflow_entity_state"
            " WHERE entity_type=%s AND entity_id=%s",
            (entity_type, entity_id),
        )
        row = await cur.fetchone()
        return row["state"] if row else None

    async def audit_record(self, uow: UnitOfWork, event: AuditEvent) -> None:
        def _actor(a) -> dict[str, Any]:
            d = {"type": a.type.value, "id": a.id}
            if a.via:
                d["via"] = _actor(a.via)
            return d

        changes = {
            k: {"before": b, "after": a}
            for k, (b, a) in event.changes.items()
        }
        await self._conn(uow).execute(
            """
            INSERT INTO keelflow_audit
                (actor, action, entity_type, entity_id, changes, source,
                 correlation_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                json.dumps(_actor(event.actor), default=json_default),
                event.action,
                event.entity.type,
                event.entity.id,
                json.dumps(changes, default=json_default),
                event.source,
                event.correlation_id or None,
            ),
        )

    async def audit_trace(
        self, uow: UnitOfWork, entity_type: str, entity_id: str
    ) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT occurred_at, actor, action, changes, source,"
            " correlation_id FROM keelflow_audit"
            " WHERE entity_type=%s AND entity_id=%s ORDER BY id",
            (entity_type, entity_id),
        )
        return await cur.fetchall()

    # ── events / outbox ──────────────────────────────────────────

    async def outbox_claim_next(self, uow: UnitOfWork) -> dict[str, Any] | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, event_type, payload, attempts FROM keelflow_outbox"
            " WHERE status='pending' ORDER BY id LIMIT 1"
            " FOR UPDATE SKIP LOCKED",
        )
        return await cur.fetchone()

    async def delivery_seen(
        self, uow: UnitOfWork, outbox_id: Any, subscriber: str
    ) -> bool:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT 1 FROM keelflow_outbox_deliveries"
            " WHERE outbox_id=%s AND subscriber=%s",
            (outbox_id, subscriber),
        )
        return await cur.fetchone() is not None

    async def delivery_mark(
        self, uow: UnitOfWork, outbox_id: Any, subscriber: str
    ) -> None:
        await self._conn(uow).execute(
            "INSERT INTO keelflow_outbox_deliveries (outbox_id, subscriber)"
            " VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (outbox_id, subscriber),
        )

    async def outbox_settle(
        self, uow: UnitOfWork, outbox_id: Any, status: str
    ) -> None:
        if status == "published":
            await self._conn(uow).execute(
                "UPDATE keelflow_outbox SET status='published',"
                " published_at=now() WHERE id=%s",
                (outbox_id,),
            )
        else:
            await self._conn(uow).execute(
                "UPDATE keelflow_outbox SET status='failed',"
                " attempts=attempts+1 WHERE id=%s",
                (outbox_id,),
            )

    # ── monitoring / ops ─────────────────────────────────────────

    async def ops_jobs_list(
        self,
        uow: UnitOfWork,
        *,
        types: list[str] | None = None,
        statuses: list[str] | None = None,
        limit: int = 50,
        cursor: Any | None = None,
    ) -> dict[str, Any]:
        where = ["1=1"]
        params: list[Any] = []
        if types:
            where.append("type = ANY(%s)")
            params.append(list(types))
        if statuses:
            where.append("status = ANY(%s)")
            params.append(list(statuses))
        if cursor:
            where.append("created_at < %s")
            params.append(cursor)
        params.append(limit)
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, type, status, attempts, run_at, claimed_by,"
            " concurrency_key, assignee, parent_job_id, result_bytes,"
            " created_at, updated_at"
            f" FROM keelflow_jobs WHERE {' AND '.join(where)}"
            " ORDER BY created_at DESC LIMIT %s",
            params,
        )
        items = await cur.fetchall()
        return {
            "items": items,
            "next_cursor": items[-1]["created_at"] if items else None,
        }

    async def ops_runs_list(
        self, uow: UnitOfWork, *, limit: int = 50
    ) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, workflow, status, created_at, updated_at"
            " FROM keelflow_workflow_runs ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        return await cur.fetchall()

    async def ops_rows_failed(
        self, uow: UnitOfWork, job_id: str
    ) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, row_key, chunk_id, error, data, updated_at"
            " FROM keelflow_row_status WHERE job_id=%s AND status='failed'"
            " ORDER BY id",
            (job_id,),
        )
        return await cur.fetchall()

    async def ops_rows_archived(
        self,
        uow: UnitOfWork,
        job_id: str,
        *,
        limit: int = 50,
        after_id: Any | None = None,
    ) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, row_id, row_key, chunk_id, data, archived_at"
            " FROM keelflow_row_archive"
            " WHERE job_id=%s AND (%s::bigint IS NULL OR id > %s::bigint)"
            " ORDER BY id LIMIT %s",
            (job_id, after_id, after_id, limit),
        )
        return await cur.fetchall()

    async def ops_results_oversized(
        self, uow: UnitOfWork, *, threshold: int = 32768, limit: int = 50
    ) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT type,"
            " count(*) FILTER (WHERE result_bytes > %s) AS oversized,"
            " max(result_bytes) AS worst,"
            " count(*) FILTER (WHERE result_bytes IS NOT NULL) AS n"
            " FROM keelflow_jobs GROUP BY type"
            " ORDER BY worst DESC NULLS LAST LIMIT %s",
            (threshold, limit),
        )
        return await cur.fetchall()

    async def ops_metrics_snapshot(self, uow: UnitOfWork) -> dict[str, Any]:
        cur = self._cur(uow)
        await cur.execute(
            """
            SELECT
                count(*) FILTER (WHERE status='pending') AS queue_depth,
                count(*) FILTER (WHERE status='running') AS running,
                count(*) FILTER (WHERE status='failed'
                                 AND created_at > now() - interval '24 hours')
                    AS failed_24h,
                EXTRACT(EPOCH FROM (now() - min(created_at)
                    FILTER (WHERE status='pending'))) AS oldest_pending_s
            FROM keelflow_jobs
            """
        )
        row = await cur.fetchone()
        await cur.execute(
            "SELECT count(*) AS workers FROM "
            "(SELECT 1 FROM pg_stat_activity"
            " WHERE datname=current_database()) x"
        )
        row["db_connections"] = (await cur.fetchone())["workers"]
        return row

    async def ops_sweep(
        self,
        uow: UnitOfWork,
        *,
        jobs_days: float | None,
        outbox_days: float | None,
        archive_days: float | None,
        dlq_days: float | None,
        batch: int,
    ) -> dict[str, int]:
        conn = self._conn(uow)
        out: dict[str, int] = {}
        cur = conn.cursor()
        if jobs_days is not None:
            await cur.execute(
                "DELETE FROM keelflow_workflow_runs WHERE id IN ("
                " SELECT id FROM keelflow_workflow_runs"
                " WHERE status IN ('done','failed','cancelled')"
                " AND updated_at < now() - make_interval(days => %s)"
                " ORDER BY updated_at LIMIT %s)",
                (jobs_days, batch),
            )
            out["workflow_runs"] = cur.rowcount
            await cur.execute(
                "DELETE FROM keelflow_jobs WHERE id IN ("
                " SELECT j.id FROM keelflow_jobs j"
                " WHERE j.status IN ('done','failed','cancelled')"
                " AND j.updated_at < now() - make_interval(days => %s)"
                " AND NOT EXISTS ("
                "   SELECT 1 FROM keelflow_step_runs s"
                "   WHERE s.job_id = j.id)"
                " AND NOT EXISTS ("
                "   SELECT 1 FROM keelflow_jobs c"
                "   WHERE c.parent_job_id = j.id)"
                " ORDER BY j.updated_at LIMIT %s)",
                (jobs_days, batch),
            )
            out["jobs"] = cur.rowcount
        if outbox_days is not None:
            await cur.execute(
                "DELETE FROM keelflow_outbox WHERE id IN ("
                " SELECT id FROM keelflow_outbox"
                " WHERE status='published'"
                " AND published_at < now() - make_interval(days => %s)"
                " ORDER BY published_at LIMIT %s)",
                (outbox_days, batch),
            )
            out["outbox"] = cur.rowcount
        if archive_days is not None:
            await cur.execute(
                "DELETE FROM keelflow_row_archive WHERE id IN ("
                " SELECT id FROM keelflow_row_archive"
                " WHERE archived_at < now() - make_interval(days => %s)"
                " ORDER BY archived_at LIMIT %s)",
                (archive_days, batch),
            )
            out["row_archive"] = cur.rowcount
        if dlq_days is not None:
            await cur.execute(
                "DELETE FROM keelflow_dlq WHERE id IN ("
                " SELECT id FROM keelflow_dlq"
                " WHERE dead_at < now() - make_interval(days => %s)"
                " ORDER BY dead_at LIMIT %s)",
                (dlq_days, batch),
            )
            out["dlq"] = cur.rowcount
        return out

    # ── dlq reads ────────────────────────────────────────────────

    async def dlq_list(
        self, uow: UnitOfWork, *, limit: int = 50
    ) -> list[dict[str, Any]]:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT id, type, payload, error, attempts, dead_at"
            " FROM keelflow_dlq ORDER BY dead_at DESC LIMIT %s",
            (limit,),
        )
        return await cur.fetchall()

    async def dlq_get(
        self, uow: UnitOfWork, dlq_id: str
    ) -> dict[str, Any] | None:
        cur = self._cur(uow)
        await cur.execute(
            "SELECT * FROM keelflow_dlq WHERE id=%s", (dlq_id,)
        )
        return await cur.fetchone()
