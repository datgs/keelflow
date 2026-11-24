"""SQLite kernel — proof that the kernel port survives outside Postgres.

Implements the same `keelflow_jobs` single-record model as
`NativeKernel` in SQLite dialect:

- `push_ex` UPDATEs the engine row inside the caller's transaction
  (aiosqlite connection or cursor; callers manage BEGIN/COMMIT).
- Claim is `UPDATE … WHERE id IN (SELECT …) RETURNING` — atomic under
  SQLite's database-level write serialization (no SKIP LOCKED needed;
  concurrent claimers contend on the write lock instead of skipping).
- Leases are `run_at`-style REAL epoch seconds (no timestamptz).
- Wakeup is an in-process asyncio.Event pub/sub — SQLite has no
  LISTEN/NOTIFY; polling remains the correctness fallback.
- `pg_advisory_lock` / concurrency_key serialization is a Postgres-only
  engine feature and stays unimplemented here (documented gap).

This backend targets extensibility proof and local/dev use; it is not
a production durability target (single-writer, no cross-process notify).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .kernel import DEFAULT_QUEUE, KernelJobSpec
from .native_kernel import NativeWorker

log = logging.getLogger("keelflow.sqlite_kernel")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS keelflow_jobs (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','claimed','running',
                                      'done','failed','cancelled',
                                      'blocked')),
    payload         TEXT NOT NULL DEFAULT '{}',
    context         TEXT NOT NULL DEFAULT '{}',
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    run_at          REAL NOT NULL DEFAULT 0,
    claimed_by      TEXT,
    lease_until     REAL,
    unique_key      TEXT,
    error           TEXT,
    dispatch_queued INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL DEFAULT 0,
    updated_at      REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_jobs_native_claim
    ON keelflow_jobs (run_at, id)
    WHERE status = 'pending' AND dispatch_queued = 1;
CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_unique_key
    ON keelflow_jobs (type, unique_key)
    WHERE unique_key IS NOT NULL
      AND status IN ('pending','claimed','running','blocked');
CREATE TABLE IF NOT EXISTS keelflow_step_runs (
    id     TEXT PRIMARY KEY,
    run_id TEXT,
    job_id TEXT
);
CREATE INDEX IF NOT EXISTS ix_step_runs_job ON keelflow_step_runs (job_id);
"""

# Subscribers for in-process wakeup; each worker registers an Event.
_SUBSCRIBERS: set[asyncio.Event] = set()


class SQLiteKernel:
    """Kernel port on a SQLite `keelflow_jobs` mirror."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn  # path or URI (e.g. file:db?mode=memory&cache=shared)

    def job(self, **kwargs: Any) -> KernelJobSpec:
        return KernelJobSpec(**kwargs)

    # ── dispatch push ───────────────────────────────────────────────────

    async def push_ex(self, uow, spec: KernelJobSpec):
        cursor = _cur(uow)
        await self._push_one(cursor, spec)
        await self.notify_pushed(uow)

    async def _push_one(self, cursor, spec: KernelJobSpec) -> None:
        job_id = spec.kwargs["job_id"]
        run_at = _epoch(spec.scheduled_at)
        cur = await cursor.execute(
            """
            UPDATE keelflow_jobs AS j
               SET status='pending', claimed_by=NULL, lease_until=NULL,
                   run_at=?, dispatch_queued=1,
                   unique_key=COALESCE(j.unique_key, ?),
                   updated_at=?
             WHERE j.id=? AND j.status IN ('pending','claimed','running')
               AND (? IS NULL OR NOT EXISTS (
                   SELECT 1 FROM keelflow_jobs x
                    WHERE x.type=j.type AND x.unique_key=? AND x.id<>j.id
                      AND x.status IN ('pending','claimed','running',
                                       'blocked')))
            """,
            (run_at, spec.unique_key, time.time(), job_id,
             spec.unique_key, spec.unique_key),
        )
        if spec.unique_key is not None and cur.rowcount == 0:
            await cursor.execute(
                """
                UPDATE keelflow_jobs
                   SET status='cancelled', updated_at=?,
                       error='dedup: unique_key already active'
                 WHERE id=? AND status='pending' AND dispatch_queued=1
                """,
                (time.time(), job_id),
            )

    async def push_many_ex(self, uow, specs: list[KernelJobSpec]):
        cursor = _cur(uow)
        for spec in specs:
            await self._push_one(cursor, spec)
        await self.notify_pushed(uow)

    async def notify_pushed(self, uow, queue: str = DEFAULT_QUEUE) -> None:
        for ev in _SUBSCRIBERS:
            ev.set()

    # ── lifecycle ───────────────────────────────────────────────────────

    async def migrate(self) -> None:
        db = await self._connect()
        try:
            await db.executescript(_SCHEMA)
            await db.commit()
        finally:
            await db.close()

    async def _connect(self):
        try:
            import aiosqlite
        except ImportError as exc:
            raise RuntimeError(
                "SQLiteKernel needs the 'sqlite' extra: "
                "pip install keelflow[sqlite]") from exc
        # isolation_level=None → autocommit: kernel-owned statements
        # (claim/lease/reap) commit immediately; caller-managed
        # transactions still work via explicit BEGIN/COMMIT/ROLLBACK.
        db = await aiosqlite.connect(self.dsn, isolation_level=None)
        await db.execute("PRAGMA busy_timeout=5000")
        return db

    async def declare_queue(self, queue: Any, *, upsert: bool = False) -> Any:
        return queue

    def create_worker(self, **kwargs: Any) -> SQLiteWorker:
        return SQLiteWorker(self, **kwargs)


class SQLiteWorker(NativeWorker):
    """NativeWorker skeleton with SQLite dialect statements."""

    def __init__(self, kernel: SQLiteKernel, **kwargs: Any) -> None:
        super().__init__(kernel, **kwargs)
        self._wake = asyncio.Event()
        self.worker_id = f"sqlite-{self.worker_id.split('-', 1)[1]}"

    # ── connection / wakeup ─────────────────────────────────────────────

    async def _connect(self):
        return await self._kernel._connect()

    async def _open(self) -> None:
        self._conn = await self._connect()
        _SUBSCRIBERS.add(self._wake)

    async def _wait_wakeup(self) -> None:
        self._wake.clear()
        try:
            await asyncio.wait_for(self._wake.wait(),
                                   timeout=self.poll_interval)
        except TimeoutError:
            pass

    # ── dialect statements ──────────────────────────────────────────────

    async def _claim_once(self, limit: int) -> None:
        conn = self._conn
        cur = await conn.execute(
            """
            UPDATE keelflow_jobs
               SET status='claimed', claimed_by=?, lease_until=?,
                   updated_at=?
             WHERE id IN (
                SELECT id FROM keelflow_jobs
                 WHERE status='pending' AND run_at<=? AND dispatch_queued=1
                 ORDER BY run_at, id LIMIT ?)
            RETURNING id
            """,
            (self.worker_id, time.time() + self.lease_seconds,
             time.time(), time.time(), limit),
        )
        ids = [r[0] for r in await cur.fetchall()]
        await cur.close()
        rows = []
        for job_id in ids:
            # step-run link lookup is a separate statement: SQLite does
            # not allow scalar subqueries in RETURNING.
            scur = await conn.execute(
                "SELECT id FROM keelflow_step_runs WHERE job_id=?"
                " LIMIT 1", (str(job_id),))
            sr = await scur.fetchone()
            await scur.close()
            rows.append((job_id, sr[0] if sr else None))
        self._spawn(rows)

    async def _heartbeat(self) -> None:
        if not self._inflight:
            return
        ids = list(self._inflight)
        marks = ",".join("?" * len(ids))
        cur = await self._conn.execute(
            f"""
            UPDATE keelflow_jobs
               SET lease_until=?
             WHERE id IN ({marks}) AND claimed_by=?
               AND status IN ('claimed','running')
            RETURNING id
            """,
            (time.time() + self.lease_seconds, *ids, self.worker_id),
        )
        kept = {str(r[0]) for r in await cur.fetchall()}
        await cur.close()
        self._fence(ids, kept)

    async def _reap(self) -> int:
        cur = await self._conn.execute(
            """
            UPDATE keelflow_jobs
               SET status='pending', claimed_by=NULL, lease_until=NULL,
                   updated_at=?
             WHERE status IN ('claimed','running')
               AND lease_until < ?
               AND dispatch_queued=1
            """,
            (time.time(), time.time()),
        )
        n = cur.rowcount
        await cur.close()
        return n

    async def stop(self, *, timeout: float | None = None) -> bool:
        _SUBSCRIBERS.discard(self._wake)
        return await super().stop(timeout=timeout)

    async def recover(self, conn=None) -> int:
        if conn is None:
            return await self._reap()
        cur = await conn.execute(
            """
            UPDATE keelflow_jobs
               SET status='pending', claimed_by=NULL, lease_until=NULL,
                   updated_at=?
             WHERE status IN ('claimed','running')
               AND lease_until < ?
               AND dispatch_queued=1
            """,
            (time.time(), time.time()),
        )
        return cur.rowcount


def _cur(uow):
    """aiosqlite execute lives on the connection — the uow's native
    handle IS the cursor-equivalent. Tests may pass a raw conn/cursor."""
    return getattr(uow, "native", uow)


def _epoch(scheduled_at) -> float:
    if scheduled_at is None:
        return time.time()
    if isinstance(scheduled_at, (int, float)):
        return float(scheduled_at)
    return scheduled_at.timestamp()
