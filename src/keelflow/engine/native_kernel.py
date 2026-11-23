"""Native Postgres kernel — experimental T3 backend.

`keelflow_jobs` is the only queue/lifecycle record; there is no kernel
table. Dispatch rows are made claimable by updating the engine row
itself, so `push_ex`/`push_many_ex` are UPDATEs inside the caller's
transaction (commit makes the row claimable — same atomicity guarantee
as the Chancy adapter).

Claimable = `status='pending' AND run_at<=now() AND type` is a machine
job type (registry `is_human`/`is_wait` types and `human.*` rows are
never claimed — people claim those via Inbox). `bulk.chunk@v1` is
always dispatchable.

Leases are per-job: the worker renews `lease_until` for its in-flight
jobs; the reaper returns expired claims to `pending` (at-least-once,
same contract as chancy recovery — handlers must be idempotent).
Stale-claim fencing: a worker whose heartbeat no longer matches its
`claimed_by` cancels the local dispatch task.

Known capability gaps vs Chancy (native stays experimental):
rate limits, queue pause/resume, worker tags, priority ordering
(claims are FIFO by run_at), chancy metrics plugins, LISTEN/NOTIFY
wakeup (polling only — notify_pushed emits pg_notify for future use).
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import psycopg

from .kernel import DEFAULT_QUEUE, KernelJobSpec

log = logging.getLogger("keelflow.native_kernel")

CHUNK_TYPE = "bulk.chunk@v1"

_ACTIVE = "('pending','claimed','running','blocked')"


class NativeKernel:
    """Kernel port implemented directly on `keelflow_jobs`."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        # Set by configure() → worker bootstrap; the registry decides
        # which types are dispatchable.
        self._registry = None

    def bind(self, registry) -> None:
        self._registry = registry

    def job(self, **kwargs: Any) -> KernelJobSpec:
        return KernelJobSpec(**kwargs)

    # ── dispatch push ───────────────────────────────────────────────────

    async def push_ex(self, cursor, spec: KernelJobSpec):
        """Make the engine row claimable: bump run_at / reset lease.

        `unique_key` dedup mirrors chancy's ON CONFLICT no-op: if another
        active row already carries the key this update matches nothing.
        """
        job_id = spec.kwargs["job_id"]
        run_at = spec.scheduled_at or datetime.now(UTC)
        await cursor.execute(
            """
            UPDATE keelflow_jobs j
               SET status='pending', claimed_by=NULL, lease_until=NULL,
                   run_at=%s,
                   unique_key=COALESCE(j.unique_key, %s),
                   updated_at=now()
             WHERE j.id=%s AND j.status IN ('pending','claimed','running')
               AND (%s::text IS NULL OR NOT EXISTS (
                   SELECT 1 FROM keelflow_jobs x
                    WHERE x.type=j.type AND x.unique_key=%s AND x.id<>j.id
                      AND x.status IN ('pending','claimed','running',
                                       'blocked')))
            """,
            (run_at, spec.unique_key, job_id, spec.unique_key,
             spec.unique_key),
        )

    async def push_many_ex(self, cursor, specs: list[KernelJobSpec]):
        for spec in specs:
            await self.push_ex(cursor, spec)

    async def notify_pushed(self, cursor, queue: str = DEFAULT_QUEUE) -> None:
        await cursor.execute("SELECT pg_notify('keelflow_dispatch', %s)",
                             (queue,))

    # ── lifecycle ───────────────────────────────────────────────────────

    async def migrate(self) -> None:
        from ..postgres.schema import apply_async

        conn = await psycopg.AsyncConnection.connect(self.dsn)
        try:
            await apply_async(conn)
            await conn.commit()
        finally:
            await conn.close()

    async def declare_queue(self, queue: Any, *, upsert: bool = False) -> Any:
        return queue  # no queue objects — single implicit queue

    def create_worker(self, **kwargs: Any) -> NativeWorker:
        return NativeWorker(self, **kwargs)

    # ── claim internals (worker-facing) ─────────────────────────────────

    def _dispatchable_types(self) -> list[str]:
        reg = self._registry
        if reg is None:
            return [CHUNK_TYPE]
        names: dict = getattr(reg, "_job_types", {})
        types = [
            name for name in names
            if not getattr(reg.job_type(name), "is_human", False)
            and not getattr(reg.job_type(name), "is_wait", False)
        ]
        if CHUNK_TYPE not in types:
            types.append(CHUNK_TYPE)
        return types


class NativeWorker:
    """Poll/claim loop over `keelflow_jobs` invoking `dispatch`."""

    def __init__(
        self,
        kernel: NativeKernel,
        *,
        concurrency: int = 8,
        poll_interval: float = 0.5,
        lease_seconds: float = 30.0,
        heartbeat_interval: float = 10.0,
    ) -> None:
        self._kernel = kernel
        self.worker_id = f"native-{uuid.uuid4().hex[:12]}"
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.heartbeat_interval = heartbeat_interval
        self._inflight: dict[str, asyncio.Task] = {}
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._conn: psycopg.AsyncConnection | None = None

    async def start(self) -> None:
        from . import worker as engine_worker

        try:
            engine_worker._get_registry()
        except RuntimeError:
            engine_worker.configure(
                registry=self._kernel._registry,
                dsn=self._kernel.dsn,
                kernel=self._kernel)
        self._conn = await psycopg.AsyncConnection.connect(
            self._kernel.dsn, autocommit=True)
        self._tasks = [
            asyncio.create_task(self._claim_loop()),
            asyncio.create_task(self._maintenance_loop()),
        ]

    async def _claim_loop(self) -> None:
        while not self._stop.is_set():
            try:
                free = self.concurrency - len(self._inflight)
                if free > 0:
                    await self._claim_once(free)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("native claim loop error")
            try:
                await asyncio.wait_for(self._stop.wait(),
                                       timeout=self.poll_interval)
            except TimeoutError:
                pass

    async def _claim_once(self, limit: int) -> None:

        types = self._kernel._dispatchable_types()
        conn = self._conn
        assert conn is not None
        async with conn.cursor() as cur:
            await cur.execute(
                """
                WITH cand AS (
                    SELECT id FROM keelflow_jobs
                     WHERE status='pending' AND run_at<=now()
                       AND type = ANY(%s)
                     ORDER BY run_at, id
                     LIMIT %s
                     FOR UPDATE SKIP LOCKED
                )
                UPDATE keelflow_jobs j
                   SET status='claimed', claimed_by=%s,
                       lease_until=now()+make_interval(secs=>%s),
                       updated_at=now()
                  FROM cand WHERE j.id=cand.id
                RETURNING j.id,
                    (SELECT s.id::text FROM keelflow_step_runs s
                      WHERE s.job_id=j.id LIMIT 1)
                """,
                (types, limit, self.worker_id, self.lease_seconds),
            )
            rows = await cur.fetchall()
        for job_id, step_run_id in rows:
            task = asyncio.create_task(
                self._run(str(job_id), step_run_id))
            self._inflight[str(job_id)] = task

    async def _run(self, job_id: str, step_run_id: str | None) -> None:
        from .worker import dispatch

        try:
            await dispatch(job_id, step_run_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("native dispatch %s failed", job_id)
        finally:
            self._inflight.pop(job_id, None)

    async def _maintenance_loop(self) -> None:
        """Renew in-flight leases (fencing) + reclaim expired claims."""
        while not self._stop.is_set():
            try:
                await self._heartbeat()
                await self._reap()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("native maintenance error")
            try:
                await asyncio.wait_for(self._stop.wait(),
                                       timeout=self.heartbeat_interval)
            except TimeoutError:
                pass

    async def _heartbeat(self) -> None:
        if not self._inflight:
            return
        ids = list(self._inflight)
        conn = self._conn
        assert conn is not None
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE keelflow_jobs
                   SET lease_until=now()+make_interval(secs=>%s)
                 WHERE id = ANY(%s) AND claimed_by=%s
                   AND status IN ('claimed','running')
                RETURNING id
                """,
                (self.lease_seconds, ids, self.worker_id),
            )
            kept = {str(r[0]) for r in await cur.fetchall()}
        for job_id in ids:
            if job_id not in kept:
                task = self._inflight.get(job_id)
                if task is not None and not task.done():
                    log.warning(
                        "job %s lease lost (reclaimed/terminal); "
                        "cancelling local dispatch", job_id)
                    task.cancel()

    async def _reap(self) -> int:
        conn = self._conn
        assert conn is not None
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE keelflow_jobs
                   SET status='pending', claimed_by=NULL, lease_until=NULL,
                       updated_at=now()
                 WHERE status IN ('claimed','running')
                   AND lease_until < now()
                   AND type = ANY(%s)
                """,
                (self._kernel._dispatchable_types(),),
            )
            return cur.rowcount

    async def stop(self, *, timeout: float | None = None) -> bool:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        if self._inflight:
            done = await asyncio.wait(
                self._inflight.values(),
                timeout=timeout if timeout is not None else 30.0,
            )
            for t in done[1]:  # pending → cancel; lease expiry re-queues
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._conn is not None:
            await self._conn.close()
        return not self._inflight

    async def wait_for_shutdown(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def recover(self, conn=None) -> int:
        """Force-reclaim expired claims now (test hook like chancy's
        Recovery plugin)."""
        if conn is not None:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE keelflow_jobs
                       SET status='pending', claimed_by=NULL,
                           lease_until=NULL, updated_at=now()
                     WHERE status IN ('claimed','running')
                       AND lease_until < now()
                       AND type = ANY(%s)
                    """,
                    (self._kernel._dispatchable_types(),),
                )
                return cur.rowcount
        return await self._reap()
