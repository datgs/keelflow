"""Native Postgres kernel — default dispatch backend.

`keelflow_jobs` is the only queue/lifecycle record; there is no kernel
table. Dispatch rows are made claimable by updating the engine row
itself, so `push_ex`/`push_many_ex` are UPDATEs inside the caller's
transaction (commit makes the row claimable — same atomicity guarantee
as the Chancy adapter).

Claimable = `status='pending' AND run_at<=now() AND dispatch_queued`
— `push_ex` sets the flag on the engine row; human/wait rows never
receive a dispatch push, so workers cannot steal them regardless of
registry state.

Leases are per-job: the worker renews `lease_until` for its in-flight
jobs; the reaper returns expired claims to `pending` (at-least-once,
same contract as chancy recovery — handlers must be idempotent).
Stale-claim fencing: a worker whose heartbeat no longer matches its
`claimed_by` cancels the local dispatch task.

Known capability gaps vs Chancy:
rate limits, queue pause/resume, worker tags, priority ordering
(claims are FIFO by run_at), chancy metrics plugins. Wakeup is
LISTEN/NOTIFY via `pg_notify` with a polling fallback.

`sqlite_kernel.SQLiteKernel` reuses `NativeWorker`'s claim/lease/fence
skeleton with SQLite dialect overrides — the port boundary is exercised
by subclassing the worker internals here.
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


def _cur(uow):
    """Cursor over the unit of work's native psycopg connection.

    Test fixtures may still hand in a bare conn/cursor/transaction —
    fall back to the object itself or its connection's cursor."""
    native = getattr(uow, "native", uow)
    if isinstance(native, psycopg.AsyncConnection):
        return native.cursor()
    if isinstance(native, psycopg.AsyncTransaction):
        return native.connection.cursor()
    return native  # already a cursor


class NativeKernel:
    """Kernel port implemented directly on `keelflow_jobs`."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def job(self, **kwargs: Any) -> KernelJobSpec:
        return KernelJobSpec(**kwargs)

    # ── dispatch push ───────────────────────────────────────────────────

    async def push_ex(self, uow, spec: KernelJobSpec):
        """Make the engine row claimable: bump run_at / reset lease.

        `unique_key` dedup mirrors chancy's ON CONFLICT no-op: if another
        active row already carries the key this update matches nothing.
        """
        cursor = _cur(uow)
        await self._push_one(cursor, spec)
        await self.notify_pushed(uow)

    async def _push_one(self, cursor, spec: KernelJobSpec) -> None:
        job_id = spec.kwargs["job_id"]
        run_at = spec.scheduled_at or datetime.now(UTC)
        await cursor.execute(
            """
            UPDATE keelflow_jobs j
               SET status='pending', claimed_by=NULL, lease_until=NULL,
                   execution_token=NULL,
                   run_at=%s, dispatch_queued=true,
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
        # Dedup conflict (rowcount 0 on a live row): mark it cancelled so
        # it can never be claimed — mirrors chancy's silent orphaning of
        # the push while making the outcome observable.
        if spec.unique_key is not None and cursor.rowcount == 0:
            await cursor.execute(
                """
                UPDATE keelflow_jobs
                   SET status='cancelled', updated_at=now(),
                       error='dedup: unique_key already active'
                 WHERE id=%s AND status='pending' AND dispatch_queued
                """,
                (job_id,),
            )

    async def push_many_ex(self, uow, specs: list[KernelJobSpec]):
        cursor = _cur(uow)
        for spec in specs:
            await self._push_one(cursor, spec)
        await self.notify_pushed(uow)

    async def notify_pushed(self, uow, queue: str = DEFAULT_QUEUE) -> None:
        await _cur(uow).execute("SELECT pg_notify('keelflow_dispatch', %s)",
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


class NativeWorker:
    """Poll/LISTEN claim loop over `keelflow_jobs` invoking dispatch.

    Subclass hooks for other dialects (see sqlite_kernel):
    `_connect`, `_wait_wakeup`, `_claim_once`, `_heartbeat`, `_reap`,
    `recover`, and `stop`'s connection close.
    """

    def __init__(
        self,
        kernel,
        *,
        concurrency: int = 8,
        poll_interval: float = 0.5,
        lease_seconds: float = 30.0,
        heartbeat_interval: float = 10.0,
        dispatch=None,
    ) -> None:
        self._kernel = kernel
        self.worker_id = f"native-{uuid.uuid4().hex[:12]}"
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.heartbeat_interval = heartbeat_interval
        self._dispatch = dispatch  # None → engine dispatch
        self._inflight: dict[str, asyncio.Task] = {}
        # _stop_claim ends only the claim loop; _stop ends maintenance
        # too — during graceful drain, maintenance must keep renewing
        # in-flight leases or siblings would reclaim live work.
        self._stop = asyncio.Event()
        self._stop_claim = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._conn: Any = None
        self._conn_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._started = False
        self._stopping = False

    # ── connection / wakeup hooks ───────────────────────────────────────

    async def _connect(self):
        # application_name identifies this claim/notify connection in
        # pg_stat_activity (LISTEN itself is not retained as `query`)
        # — used by ops and by the connection-leak proof.
        return await psycopg.AsyncConnection.connect(
            self._kernel.dsn, autocommit=True,
            application_name="keelflow-worker-claim")

    async def _open(self) -> None:
        conn = await self._connect()
        try:
            await conn.execute("LISTEN keelflow_dispatch")
        except BaseException:
            await conn.close()
            raise
        self._conn = conn

    async def _wait_wakeup(self) -> None:
        """Block until a push notify or poll_interval elapses.

        Postgres: LISTEN on `keelflow_dispatch`. Subclasses override for
        other transports; correctness never depends on wakeup (polling
        fallback bounds delay at `poll_interval`).
        """
        async with self._conn_lock:
            conn = self._conn
            if conn is None:
                return
            async for _ in conn.notifies(
                    timeout=self.poll_interval, stop_after=1):
                break

    async def start(self) -> None:
        async with self._start_lock:
            if self._stopping:
                raise RuntimeError(
                    "NativeWorker still owns running handlers; retry stop "
                    "or await them before restarting")
            if self._started:
                return
            await self._ensure_conn()
            self._stop.clear()
            self._stop_claim.clear()
            self._tasks = [
                asyncio.create_task(self._claim_loop()),
                asyncio.create_task(self._maintenance_loop()),
            ]
            self._started = True

    async def _claim_loop(self) -> None:
        while not self._stop_claim.is_set():
            try:
                await self._ensure_conn()
                free = self.concurrency - len(self._inflight)
                if free > 0 and await self._claim_once(free):
                    continue  # claimed something — drain before sleeping
            except asyncio.CancelledError:
                raise
            except Exception:
                # Transient DB loss must not kill the loop: drop the
                # suspect connection and wait — the next iteration
                # reconnects (auto-reconnect policy).
                await self._discard_conn()
                log.exception("native claim loop error")
            try:
                await self._wait_wakeup()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - wakeup degraded
                # → plain sleep; polling still correct.
                try:
                    await asyncio.wait_for(self._stop_claim.wait(),
                                           timeout=self.poll_interval)
                except TimeoutError:
                    pass

    async def _ensure_conn(self) -> None:
        """Reconnect the worker's claim/lease connection after loss."""
        async with self._conn_lock:
            conn = self._conn
            if conn is not None and not getattr(conn, "closed", False):
                return
            self._conn = None
            try:
                if conn is not None:
                    await conn.close()
            except Exception:  # closing a dead conn is best-effort
                log.debug("closing dead claim connection failed",
                          exc_info=True)
            await self._open()

    # ── dialect statements ──────────────────────────────────────────────

    async def _claim_once(self, limit: int) -> int:
        """Claim up to `limit` pending rows; returns how many were
        claimed (0 → caller waits for wakeup/poll).

        The claim consumes retry budget up front — `attempts` counts
        claims, not handler starts — and mints the execution_token the
        dispatch must present at `job_mark_running` to prove the claim
        was not reclaimed in the gap before dispatch."""
        async with self._conn_lock:
            conn = self._conn
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    WITH cand AS (
                        SELECT id FROM keelflow_jobs
                         WHERE status='pending' AND run_at<=now()
                           AND dispatch_queued
                         ORDER BY run_at, id
                         LIMIT %s
                         FOR UPDATE SKIP LOCKED
                    )
                    UPDATE keelflow_jobs j
                       SET status='claimed', claimed_by=%s,
                           attempts = attempts + 1,
                           execution_token = gen_random_uuid()::text,
                           lease_until=now()+make_interval(secs=>%s),
                           updated_at=now()
                      FROM cand WHERE j.id=cand.id
                    RETURNING j.id, j.execution_token,
                        (SELECT s.id::text FROM keelflow_step_runs s
                          WHERE s.job_id=j.id LIMIT 1)
                    """,
                    (limit, self.worker_id, self.lease_seconds),
                )
                rows = await cur.fetchall()
            self._spawn(rows)
            return len(rows)

    def _spawn(self, rows) -> None:
        for job_id, execution_token, step_run_id in rows:
            task = asyncio.create_task(
                self._run(str(job_id), execution_token,
                          str(step_run_id) if step_run_id else None))
            self._inflight[str(job_id)] = task

    async def _run(self, job_id: str, execution_token: str | None,
                   step_run_id: str | None) -> None:
        dispatch = self._dispatch
        if dispatch is None:
            from .worker import dispatch as _dispatch

            dispatch = _dispatch
        try:
            await dispatch(job_id, step_run_id,
                           execution_token=execution_token)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("native dispatch %s failed", job_id)
        finally:
            # Pop by identity: a reclaim may have replaced this job's
            # inflight entry — an older dispatch finishing last must
            # not clear the newer dispatch's slot.
            if self._inflight.get(job_id) is asyncio.current_task():
                self._inflight.pop(job_id, None)

    async def _maintenance_loop(self) -> None:
        """Renew in-flight leases (fencing) + reclaim expired claims."""
        while not self._stop.is_set():
            try:
                await self._ensure_conn()
                await self._heartbeat()
                await self._reap()
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._discard_conn()
                log.exception("native maintenance error")
            try:
                await asyncio.wait_for(self._stop.wait(),
                                       timeout=self.heartbeat_interval)
            except TimeoutError:
                pass

    async def _discard_conn(self) -> None:
        async with self._conn_lock:
            conn, self._conn = self._conn, None
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    log.debug("closing claim connection failed",
                              exc_info=True)

    async def _heartbeat(self) -> None:
        if not self._inflight:
            return
        ids = list(self._inflight)
        async with self._conn_lock:
            conn = self._conn
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
        self._fence(ids, kept)

    def _fence(self, ids: list[str], kept: set[str]) -> None:
        for job_id in ids:
            if job_id not in kept:
                task = self._inflight.get(job_id)
                if task is not None and not task.done():
                    log.warning(
                        "job %s lease lost (reclaimed/terminal); "
                        "cancelling local dispatch", job_id)
                    task.cancel()

    async def _reap(self) -> int:
        async with self._conn_lock:
            conn = self._conn
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE keelflow_jobs
                       SET status='pending', claimed_by=NULL, lease_until=NULL,
                           execution_token=NULL, updated_at=now()
                     WHERE status IN ('claimed','running')
                       AND lease_until < now()
                       AND dispatch_queued
                    """,
                )
                return cur.rowcount

    async def stop(self, *, timeout: float | None = None) -> bool:
        """Stop claiming and drain handlers within a bounded timeout.

        If cancellation grace expires while a handler remains active,
        return False and keep maintenance, connection, and lease renewals
        running. The caller must retain this worker and may retry stop.
        """
        self._stopping = True
        self._stop_claim.set()
        claim_task = self._tasks[0] if self._tasks else None
        if claim_task is not None:
            claim_task.cancel()
            await asyncio.gather(claim_task, return_exceptions=True)
        tasks = set(self._inflight.values())
        drained = not tasks
        if tasks:
            _done, pending = await asyncio.wait(
                tasks, timeout=timeout if timeout is not None else 30.0)
            drained = not pending
            if pending:
                for task in pending:
                    task.cancel()
                # Give dispatch cancellation time to reach the handler task.
                await asyncio.sleep(0)
                _done, pending = await asyncio.wait(pending, timeout=0.5)
                drained = not pending
                if not drained:
                    await self._ensure_conn()
                    await self._heartbeat()
                    return False
        self._stop.set()
        await asyncio.gather(*self._tasks[1:], return_exceptions=True)
        await self._discard_conn()
        self._started = False
        self._stopping = False
        self._tasks = []
        return drained

    async def wait_for_shutdown(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks)

    async def recover(self, conn=None) -> int:
        """Force-reclaim expired claims now (test hook like chancy's
        Recovery plugin)."""
        if conn is not None:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE keelflow_jobs
                       SET status='pending', claimed_by=NULL,
                           lease_until=NULL, execution_token=NULL,
                           updated_at=now()
                     WHERE status IN ('claimed','running')
                       AND lease_until < now()
                       AND dispatch_queued
                    """,
                )
                return cur.rowcount
        return await self._reap()
