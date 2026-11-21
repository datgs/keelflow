"""Bulk chunking runtime (T8).

Three levels of state (design §3.1):

- `keelflow_jobs`      — parent bulk job: overall status, chunk counts in result
- `keelflow_chunks`    — claimed/retry unit (one kernel job per chunk)
- `keelflow_row_status`— per-row status (reporting + error queue)

Flow:

1. App registers a job type whose handler subclasses `BulkJobHandler` and
   implements `process_row`. Rows are staged into `keelflow_row_status`
   (`job_id` = the parent job's id, `status='pending'`) before enqueueing.
2. The parent job's `run` partitions pending rows into `keelflow_chunks`
   (idempotent — rows already having a `chunk_id` are skipped), inserts one
   `keelflow_jobs` + kernel job per chunk, and marks the parent `running`
   until fan-in. All inside one transaction.
3. `ChunkJobHandler` (registered under `bulk.chunk@v1`) processes its
   chunk's pending/processing rows via the parent handler's `process_row`.
   `atomic='chunk'` (default): the whole chunk is one transaction.
   `atomic='row'`: one transaction per row.
4. Fan-in: when the last chunk settles, the parent `keelflow_jobs` row
   flips to `done` or `failed` with a result summary.

Kill-resume: a dead worker's chunk job is re-queued by the kernel; on retry
the chunk handler resets `processing` rows back to `pending` and continues
— under `atomic='row'`, committed rows stay done, so only pending rows
resume (must-prove #1).
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel

from ..sdk import BatchRowResult, JobCancelled, JobContext, JobResult
from ..sdk.handler import JobHandler
from ..sdk.specs import JobStatus, RetryPolicy
from . import worker as _worker
from .worker import DSN_ENV, _get_registry

log = logging.getLogger("keelflow.bulk")

CHUNK_TYPE = "bulk.chunk@v1"
PARENT_HOLD = "_bulk_parent"


class BulkJobPayload(BaseModel):
    """Payload for a parent bulk job."""

    chunk_size: int = 1000
    atomic: Literal["chunk", "row"] = "chunk"
    # Disposition of 'done' staging rows once the parent reaches terminal
    # fan-in. Failed rows are never moved/deleted — they stay in the live
    # error queue until a separate retention policy clears them.
    #   archive: copy done rows to keelflow_row_archive (durable inspection)
    #   keep:    leave done rows live (debug / strict per-row audit)
    #   delete:  drop done rows (only when the app has a durable source copy)
    row_disposition: Literal["archive", "keep", "delete"] = "archive"


class ChunkPayload(BaseModel):
    """Payload for an engine chunk job."""

    parent_job_id: str
    chunk_id: str


class BulkJobHandler(JobHandler):
    """App's bulk/import handler. Override `process_row` or `process_batch`.

    `run` partitions this job's pending `row_status` rows into chunks and
    enqueues one `bulk.chunk@v1` job per chunk. The parent stays `running`
    until the last chunk settles (fan-in).

    `process_batch` is the optional set-based fast path: it receives ALL
    rows of one chunk (bounded by `chunk_size`) plus the chunk transaction
    cursor, and returns one `BatchRowResult` per engine row id. The whole
    callback and every row-state update commit/roll back together — so it
    must not perform external I/O (a retry would repeat the side effect);
    stage remote calls through durable jobs/outbox instead. Requires
    `atomic='chunk'`; `atomic='row'` keeps using `process_row`.
    """

    async def process_row(
        self, ctx: JobContext, row: dict[str, Any], tx
    ) -> dict[str, Any] | None:
        """Apply one staged row inside the chunk transaction."""
        raise NotImplementedError

    async def process_batch(
        self, ctx: JobContext, rows: list[dict[str, Any]], tx
    ) -> list[BatchRowResult]:
        """Apply one whole chunk set-based inside the chunk transaction.

        Rows carry engine `id`, `row_key`, and `data`. The returned list
        must contain exactly one `BatchRowResult` per input row id (order
        irrelevant; matching by `row_id`). `error=None` → done; a
        non-empty error → that row failed into the error queue while
        siblings commit. Raising or returning a malformed list rolls back
        the whole chunk and retries it under the chunk retry policy.
        """
        raise NotImplementedError

    async def _own_type(self, cur, job_id) -> str:
        """Resolve this handler's own registered job type from its job row."""
        await cur.execute("SELECT type FROM keelflow_jobs WHERE id=%s", (job_id,))
        return (await cur.fetchone())["type"]

    async def run(self, ctx: JobContext, payload: BaseModel) -> JobResult:
        """Partition pending rows into chunks + enqueue chunk jobs.

        Runs on ctx.tx — the handler's own dispatch transaction — so rows
        staged by a subclass's `run` (or pre-staged by the caller) are
        visible, and partition + chunk enqueue commit atomically with the
        job finish. A separate connection would see a stale snapshot.
        """
        job_id = ctx.extra["job_id"]  # injected by dispatch
        assert isinstance(payload, BulkJobPayload)
        kernel = _worker._kernel_client
        if kernel is None:
            raise RuntimeError("no kernel client configured; run via worker")
        cur = (ctx.tx.connection.cursor(row_factory=dict_row)
               if hasattr(ctx.tx, "connection") else ctx.tx)

        size = max(1, payload.chunk_size)

        # Pending count first: chunk ids are generated in Python, so we
        # need the chunk count up front. Rows inserted concurrently
        # between this count and the partition statement simply stay
        # unchunked (same eventual behavior as the previous loop, which
        # also only saw its snapshot of claimed ids).
        await cur.execute(
            "SELECT count(*) FROM keelflow_row_status"
            " WHERE job_id=%s AND status='pending' AND chunk_id IS NULL",
            (job_id,),
        )
        n_pending = (await cur.fetchone())["count"]
        if n_pending == 0:
            return JobResult(status="done", outputs={"chunks": 0})
        n_chunks = -(-n_pending // size)
        chunk_ids = [uuid.uuid4() for _ in range(n_chunks)]

        # Set-based partition — one statement claims the pending,
        # unchunked rows (same SKIP LOCKED contract as before), numbers
        # them in id order, inserts the pre-generated chunks, and stamps
        # each claimed row with its chunk id. The chunk table is passed
        # via unnest so the row→chunk join can hash (joining through a
        # data-modifying CTE forces a nested loop — measured ~4x slower).
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
            (job_id, size, list(range(n_chunks)), chunk_ids, job_id),
        )
        part = await cur.fetchone()
        n_rows = int(part["n_rows"])
        if int(part["n_chunks"]) != n_chunks:
            raise RuntimeError(
                f"partition inserted {part['n_chunks']} chunks,"
                f" expected {n_chunks}")
        # Rows claimed but not mapped (ci beyond the chunk list — can
        # only happen if pending rows appeared between the count and
        # this statement) remain unchunked; flag loudly rather than
        # silently dropping them from this pass.
        if n_rows != n_pending:
            log.warning(
                "bulk %s: claimed %d rows but %d were pending at count"
                " — %d rows stay unchunked for a later pass",
                job_id, n_rows, n_pending, n_pending - n_rows)

        # Parent invariants, fetched once (were per-chunk before).
        # Children inherit a cancel request already committed on the
        # parent — they will be cancelled at claim instead of starting
        # work for a cancelled parent. Retry budget comes from the
        # parent job type's registered RetryPolicy — the app tunes
        # chunk retry behavior on its own bulk type.
        await cur.execute(
            "SELECT cancel_requested FROM keelflow_jobs WHERE id=%s",
            (job_id,),
        )
        parent_cancel = (await cur.fetchone())["cancel_requested"]
        try:
            parent_spec = _get_registry().job_type(
                await self._own_type(cur, job_id))
            chunk_max_attempts = (
                parent_spec.retry.max_attempts if parent_spec.retry else 3)
        except KeyError:
            chunk_max_attempts = 3

        # Build all child jobs in one pass — identical per-job rows,
        # inserted as one multi-row INSERT via unnest.
        now = datetime.now(UTC)
        child_ids = [uuid.uuid4() for _ in chunk_ids]
        payloads, contexts, kernel_jobs = [], [], []
        for child_id, chunk_id in zip(child_ids, chunk_ids):
            child_ctx = _child_ctx(ctx, child_id)
            child_payload = ChunkPayload(
                parent_job_id=job_id, chunk_id=str(chunk_id)
            ).model_dump(mode="json")
            payloads.append(json.dumps(child_payload))
            contexts.append(json.dumps(_ctx_d(child_ctx)))
            kernel_jobs.append(_kernel_job(child_id, str(chunk_id)))
        await cur.execute(
            """
            INSERT INTO keelflow_jobs
                (id, type, status, payload, context, attempts,
                 max_attempts, run_at, parent_job_id,
                 cancel_requested, created_at, updated_at)
            SELECT j.id, %s, 'pending', j.payload::jsonb,
                   j.context::jsonb, 0, %s, %s, %s, %s, %s, %s
            FROM unnest(%s::uuid[], %s::text[], %s::text[])
                 AS j(id, payload, context)
            """,
            (CHUNK_TYPE, chunk_max_attempts, now, job_id,
             parent_cancel, now, now,
             child_ids, payloads, contexts),
        )
        # push_many_ex still inserts one kernel row per job (Chancy
        # 0.26), but sends a single queue notification at the end
        # instead of one per chunk.
        await kernel.push_many_ex(cur, kernel_jobs)

        # parent stays running until fan-in (dispatch honors marker)
        return JobResult(
            status="done",
            outputs={PARENT_HOLD: True, "chunks": n_chunks,
                     "rows": n_rows},
        )


class ChunkJobHandler(JobHandler):
    """Engine-internal chunk worker. Processes rows via the parent handler."""

    async def run(self, ctx: JobContext, payload: BaseModel) -> JobResult:
        assert isinstance(payload, ChunkPayload)
        dsn = os.environ[DSN_ENV]
        registry = _get_registry()
        conn = await psycopg.AsyncConnection.connect(dsn)
        processed = failed_rows = 0
        try:
            # claim the chunk; reset rows stranded in 'processing' by a dead
            # worker (row-atomic resume).
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "UPDATE keelflow_chunks SET status='running',"
                    " claimed_by=%s, lease_until=now() + interval '15 minutes',"
                    " attempts=attempts+1, updated_at=now() WHERE id=%s",
                    (os.environ.get("KEELFLOW_WORKER_ID", "worker-0"),
                     payload.chunk_id),
                )
                await cur.execute(
                    "UPDATE keelflow_row_status SET status='pending'"
                    " WHERE chunk_id=%s AND status='processing'",
                    (payload.chunk_id,),
                )
            await conn.commit()

            parent_type, atomic = await self._parent_type(conn, payload.parent_job_id)
            spec = registry.job_type(parent_type)
            parent_handler: BulkJobHandler = spec.handler()  # type: ignore[misc]

            # Handler selection (spec/04): atomic='chunk' uses the
            # set-based process_batch when the handler overrides it;
            # atomic='row' always uses process_row. A batch-only handler
            # (no process_row override) with atomic='row' is a config
            # error — the whole-batch commit semantics cannot hold.
            has_batch = (type(parent_handler).process_batch
                         is not BulkJobHandler.process_batch)
            has_row = (type(parent_handler).process_row
                       is not BulkJobHandler.process_row)
            if atomic == "row" and has_batch and not has_row:
                raise ValueError(
                    f"bulk handler {type(parent_handler).__name__} only"
                    " implements process_batch, which requires"
                    " atomic='chunk' (the batch commits per chunk);"
                    " either implement process_row or set atomic='chunk'")
            use_batch = atomic == "chunk" and has_batch

            while True:
                rows = await self._fetch_rows(conn, payload.chunk_id)
                if not rows:
                    break
                if use_batch:
                    done_n, failed_n = await self._process_batch_atomic(
                        conn, ctx, parent_handler, rows, payload.chunk_id
                    )
                    processed += done_n
                    failed_rows += failed_n
                    break  # whole chunk in one tx
                elif atomic == "chunk":
                    await self._process_chunk_atomic(
                        conn, ctx, parent_handler, rows, payload.chunk_id
                    )
                    break  # whole batch in one tx
                else:
                    for row in rows:
                        # Cooperative cancel checkpoint — direct
                        # handler.run calls (unit tests) have no
                        # ctx.execution injected; only dispatched jobs
                        # of an opted-in spec observe the flag.
                        if ctx.execution is not None:
                            await ctx.execution.checkpoint()
                        ok = await self._process_one_row(
                            conn, ctx, parent_handler, row
                        )
                        processed += int(ok)
                        failed_rows += int(not ok)

            await self._mark_chunk(conn, payload.chunk_id, "done",
                                   {"processed": processed, "failed": failed_rows})
            await self._fan_in(conn, payload.parent_job_id)
            return JobResult(status="done", outputs={
                "processed": processed, "failed_rows": failed_rows})
        except JobCancelled:
            # Settle bookkeeping outside the rolled-back handler TX so
            # fan-in can terminalize the parent; dispatch then records
            # the job itself as cancelled.
            log.info("chunk %s cancelled at checkpoint", payload.chunk_id)
            await conn.rollback()
            await conn.execute(
                "UPDATE keelflow_chunks SET status='cancelled',"
                " result=%s, updated_at=now() WHERE id=%s",
                (json.dumps({"processed": processed,
                             "failed": failed_rows}),
                 payload.chunk_id),
            )
            await conn.commit()
            await self._fan_in(conn, payload.parent_job_id)
            raise
        except Exception as exc:
            log.exception("chunk %s failed", payload.chunk_id)
            await conn.rollback()
            # Defer chunk-failure + fan-in to retry exhaustion: a
            # transient callback/database failure rolls back and is
            # retried via the kernel redelivery (the engine job stays
            # 'running'); only the last allowed attempt terminalizes the
            # chunk and seals the parent. Direct handler.run calls
            # without dispatch (no own job id) keep the old
            # fail-immediately behavior.
            own_job_id = ctx.extra.get("job_id")
            exhausted = True
            if own_job_id:
                async with conn.cursor(row_factory=dict_row) as xcur:
                    await xcur.execute(
                        "SELECT attempts >= max_attempts AS done_att"
                        " FROM keelflow_jobs WHERE id=%s", (own_job_id,))
                    xrow = await xcur.fetchone()
                    exhausted = bool(xrow and xrow["done_att"])
            if exhausted:
                await self._safe_mark_failed(conn, payload.chunk_id,
                                             payload.parent_job_id, str(exc))
            else:
                log.info(
                    "chunk %s failed transiently (attempt %s); retry"
                    " scheduled by kernel", payload.chunk_id, exc)
            raise
        finally:
            await conn.close()

    # ── internals ─────────────────────────────────────────────────────

    async def _parent_type(self, conn, parent_job_id) -> tuple[str, str]:
        cur = await conn.execute(
            "SELECT type, payload FROM keelflow_jobs WHERE id=%s", (parent_job_id,)
        )
        row = await cur.fetchone()
        if row is None:
            raise KeyError(f"parent job {parent_job_id} not found")
        payload = row[1] if not isinstance(row[1], str) else json.loads(row[1])
        return row[0], payload.get("atomic", "chunk")

    async def _fetch_rows(self, conn, chunk_id) -> list[dict[str, Any]]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, row_key, data FROM keelflow_row_status"
                " WHERE chunk_id=%s AND status='pending'"
                " ORDER BY id",
                (chunk_id,),
            )
            return await cur.fetchall()

    async def _process_batch_atomic(
        self, conn, ctx, handler, rows, chunk_id
    ) -> tuple[int, int]:
        """Set-based path: one callback + all row-state flips in ONE tx.

        The callback receives the whole chunk (bounded by chunk_size) and
        must return exactly one BatchRowResult per engine row id. A raised
        exception or malformed result list rolls the transaction back;
        the chunk job then retries under its retry policy (run()'s except
        path). Explicit per-row errors commit alongside sibling successes.
        Cancellation is observed at the checkpoint before the transaction
        starts — a committed TX cannot be interrupted mid-flight.
        """
        if ctx.execution is not None:
            await ctx.execution.checkpoint()
        async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            results = await handler.process_batch(ctx, rows, cur)
            if not isinstance(results, list) or not all(
                isinstance(r, BatchRowResult) for r in results
            ):
                raise TypeError(
                    "process_batch must return list[BatchRowResult]")
            want = {r["id"] for r in rows}
            if len(results) != len(want):
                raise ValueError(
                    f"process_batch returned {len(results)} results"
                    f" for {len(want)} rows")
            seen: set[int] = set()
            for r in results:
                if r.row_id in seen:
                    raise ValueError(
                        f"process_batch returned duplicate row_id {r.row_id}")
                seen.add(r.row_id)
            unknown = seen - want
            if unknown:
                raise ValueError(
                    "process_batch returned unknown row_ids"
                    f" {sorted(unknown)[:5]}")
            done_ids = [r.row_id for r in results if r.error is None]
            err_rows = [(r.row_id, r.error or "unknown error")
                        for r in results if r.error is not None]
            if done_ids:
                await cur.execute(
                    "UPDATE keelflow_row_status SET status='done',"
                    " updated_at=now() WHERE id = ANY(%s)",
                    (done_ids,),
                )
            for rid, err in err_rows:
                await cur.execute(
                    "UPDATE keelflow_row_status SET status='failed',"
                    " error=%s, updated_at=now() WHERE id=%s",
                    (err, rid),
                )
        return len(done_ids), len(err_rows)

    async def _process_chunk_atomic(
        self, conn, ctx, handler, rows, chunk_id
    ) -> None:
        failing: tuple | None = None
        try:
            async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
                for row in rows:
                    await cur.execute(
                        "UPDATE keelflow_row_status SET status='processing',"
                        " updated_at=now() WHERE id=%s",
                        (row["id"],),
                    )
                    try:
                        await handler.process_row(ctx, row, cur)
                    except Exception as exc:
                        failing = (row["id"], str(exc))
                        raise
                    await cur.execute(
                        "UPDATE keelflow_row_status SET status='done',"
                        " updated_at=now() WHERE id=%s",
                        (row["id"],),
                    )
        finally:
            if failing is not None:
                # record the failing row outside the rolled-back tx so the
                # error queue still shows which row failed (design §3.2).
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE keelflow_row_status SET status='failed',"
                        " error=%s, updated_at=now() WHERE id=%s",
                        (failing[1], failing[0]),
                    )
                await conn.commit()

    async def _process_one_row(self, conn, ctx, handler, row) -> bool:
        async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "UPDATE keelflow_row_status SET status='processing',"
                " updated_at=now() WHERE id=%s",
                (row["id"],),
            )
            try:
                await handler.process_row(ctx, row, cur)
            except Exception as exc:  # noqa: BLE001 - row-level error queue entry
                await cur.execute(
                    "UPDATE keelflow_row_status SET status='failed',"
                    " error=%s, updated_at=now() WHERE id=%s",
                    (str(exc), row["id"]),
                )
                return False
            await cur.execute(
                "UPDATE keelflow_row_status SET status='done',"
                " updated_at=now() WHERE id=%s",
                (row["id"],),
            )
            return True

    async def _mark_chunk(self, conn, chunk_id, status, result=None) -> None:
        await conn.execute(
            "UPDATE keelflow_chunks SET status=%s, result=%s, updated_at=now()"
            " WHERE id=%s",
            (status, json.dumps(result) if result else None, chunk_id),
        )
        await conn.commit()

    async def _fan_in(self, conn, parent_job_id) -> None:
        """Last chunk to settle flips the parent job to its final status.

        If the parent belongs to a workflow step, also advance the run —
        same transaction, so step completion and downstream enqueue are
        atomic with the parent's final status.
        """
        from . import workflow as _wf
        from .worker import _kernel_client

        async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT status, count(*) FROM keelflow_chunks"
                " WHERE job_id=%s GROUP BY status",
                (parent_job_id,),
            )
            counts = {r["status"]: r["count"] for r in await cur.fetchall()}
            if counts.get("pending") or counts.get("claimed") or counts.get("running"):
                return  # still in flight

            # Two chunks can settle near-simultaneously; serialize on the
            # parent row so only the first fan-in computes the summary and
            # moves rows — later ones see the terminal status and return.
            await cur.execute(
                "SELECT status, cancel_requested FROM keelflow_jobs"
                " WHERE id=%s FOR UPDATE",
                (parent_job_id,),
            )
            pj = await cur.fetchone()
            if pj is None or pj["status"] in ("done", "failed", "cancelled"):
                return
            if pj["cancel_requested"]:
                final = "cancelled"
            elif counts.get("failed"):
                final = "failed"
            else:
                final = "done"

            await cur.execute(
                "SELECT status, count(*) FROM keelflow_row_status"
                " WHERE job_id=%s GROUP BY status",
                (parent_job_id,),
            )
            rows = {r["status"]: r["count"] for r in await cur.fetchall()}
            summary = {
                "chunks": counts,
                "rows_total": sum(rows.values()),
                "rows_done": rows.get("done", 0),
                "rows_failed": rows.get("failed", 0),
            }

            # Retention for 'done' staging rows — same TX as the parent's
            # terminal write, so a rollback loses neither the rows nor the
            # step-finish hook. Old jobs lacking the payload field default
            # to 'archive'.
            await cur.execute(
                "SELECT COALESCE(payload->>'row_disposition','archive') AS d"
                " FROM keelflow_jobs WHERE id=%s",
                (parent_job_id,),
            )
            mode = (await cur.fetchone())["d"] or "archive"
            done_rows = rows.get("done", 0)
            moved = {"archive": 0, "delete": 0}
            if done_rows and mode == "archive":
                await cur.execute(
                    "WITH done AS (SELECT id, row_key, chunk_id, data"
                    " FROM keelflow_row_status"
                    " WHERE job_id=%s AND status='done'),"
                    " archived AS ("
                    " INSERT INTO keelflow_row_archive"
                    "     (job_id, row_id, row_key, chunk_id, data)"
                    " SELECT %s, id, row_key, chunk_id, data FROM done"
                    " ON CONFLICT (job_id, row_id) DO NOTHING"
                    " RETURNING row_id"
                    " )"
                    " DELETE FROM keelflow_row_status r USING done"
                    " WHERE r.id = done.id"
                    " RETURNING r.id",
                    (parent_job_id, parent_job_id),
                )
                moved["archive"] = cur.rowcount
            elif done_rows and mode == "delete":
                await cur.execute(
                    "DELETE FROM keelflow_row_status"
                    " WHERE job_id=%s AND status='done'",
                    (parent_job_id,),
                )
                moved["delete"] = cur.rowcount
            summary["row_disposition"] = mode
            summary["archived_rows"] = moved["archive"]
            summary["deleted_rows"] = moved["delete"]
            summary["kept_rows"] = done_rows if mode == "keep" else 0

            await cur.execute(
                "UPDATE keelflow_jobs SET status=%s, updated_at=now(),"
                " result = COALESCE(result,'{}'::jsonb) || %s::jsonb"
                " WHERE id=%s RETURNING result",
                (final, json.dumps(summary), parent_job_id),
            )
            result = (await cur.fetchone())["result"] or {}

            # workflow step link — advance the run in the same TX
            await cur.execute(
                "SELECT id FROM keelflow_step_runs WHERE job_id=%s",
                (parent_job_id,),
            )
            step = await cur.fetchone()
            if step and _kernel_client is not None:
                registry = _get_registry()
                await _wf.on_job_finished(
                    conn, _kernel_client, registry, parent_job_id,
                    str(step["id"]), JobStatus(final), result,
                )
        await conn.commit()

    async def _safe_mark_failed(self, conn, chunk_id, parent_job_id, error) -> None:
        try:
            await conn.rollback()
            await conn.execute(
                "UPDATE keelflow_chunks SET status='failed',"
                " result=%s, updated_at=now() WHERE id=%s",
                (json.dumps({"error": error}), chunk_id),
            )
            await conn.commit()
            await self._fan_in(conn, parent_job_id)
        except Exception:
            log.exception("could not mark chunk %s failed", chunk_id)


async def progress(conn, job_id) -> dict[str, Any]:
    """Live progress snapshot for a bulk parent job.

    Engine-owned counters surfaced as one dict so apps can render
    % / rate / ETA without touching engine tables: rows_total/done/
    failed/pending/processing, chunks per status, elapsed_s,
    rate_rows_per_s, eta_s (None while no row has finished).

    While the job is active, row counters come from live
    `keelflow_row_status`. Once the job is terminal, 'done' rows may
    have been archived or deleted (see `row_disposition`) — counters
    are then read from the parent's fan-in summary in `jobs.result`.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT status, created_at, updated_at, result"
            " FROM keelflow_jobs WHERE id=%s", (job_id,))
        job = await cur.fetchone()
        if job is None:
            raise KeyError(f"bulk job {job_id} not found")
        await cur.execute(
            "SELECT status, count(*) AS n FROM keelflow_row_status"
            " WHERE job_id=%s GROUP BY status", (job_id,))
        rows = {r["status"]: int(r["n"]) for r in await cur.fetchall()}
        await cur.execute(
            "SELECT status, count(*) AS n FROM keelflow_chunks"
            " WHERE job_id=%s GROUP BY status", (job_id,))
        chunks = {r["status"]: int(r["n"]) for r in await cur.fetchall()}

    finished = job["status"] in ("done", "failed", "cancelled")
    result = job["result"] or {}
    if finished and "rows_total" in result:
        # Post-cleanup: live row_status no longer holds 'done' rows.
        done = int(result.get("rows_done", 0))
        failed = int(result.get("rows_failed", 0))
        total = int(result.get("rows_total", done + failed))
        pending = processing = 0
    else:
        total = sum(rows.values())
        done = rows.get("done", 0)
        failed = rows.get("failed", 0)
        pending = rows.get("pending", 0)
        processing = rows.get("processing", 0)
    settled = done + failed
    elapsed = (job["updated_at"] - job["created_at"]).total_seconds()
    rate = settled / elapsed if elapsed > 0 and settled else 0.0
    remaining = total - settled
    return {
        "status": job["status"],
        "rows_total": total,
        "done": done,
        "failed": failed,
        "pending": pending,
        "processing": processing,
        "settled": settled,
        "pct": (100.0 * settled / total) if total else 0.0,
        "chunks": chunks,
        "started_at": job["created_at"],
        "elapsed_s": round(elapsed, 1),
        "rate_rows_per_s": round(rate, 1),
        "eta_s": (round(remaining / rate, 1) if rate else None),
        "finished": finished,
        "row_disposition": result.get("row_disposition"),
        "archived_rows": result.get("archived_rows", 0),
        "deleted_rows": result.get("deleted_rows", 0),
        "kept_rows": result.get("kept_rows", 0),
    }


def register(registry, dsn: str | None = None,
             retry: RetryPolicy | None = None) -> None:
    """Register the internal chunk job type on an InMemoryRegistry.

    `retry` overrides the chunk retry policy (default 3 attempts,
    30s exponential base). Per-chunk-job attempt budgets are copied
    from the parent bulk job type's registered RetryPolicy.
    """
    from ..sdk.specs import JobTypeSpec

    registry.register_job_type(
        JobTypeSpec(
            type=CHUNK_TYPE,
            input_schema=ChunkPayload,
            handler=ChunkJobHandler,
            retry=retry or RetryPolicy(max_attempts=3),
            # Chunks checkpoint between rows so a cancelled import
            # stops promptly at a row boundary; uncommitted row work
            # rolls back and remains 'pending' for a future resume.
            cooperative_cancel=True,
        )
    )


def _kernel_job(job_id, chunk_id: str):
    import chancy

    from .enqueue import DEFAULT_QUEUE, DISPATCH_FUNC

    return chancy.Job(
        func=DISPATCH_FUNC,
        queue=DEFAULT_QUEUE,
        kwargs={"job_id": str(job_id)},
        max_attempts=3,
        meta={"job_id": str(job_id), "type": CHUNK_TYPE, "chunk_id": chunk_id},
    )


def _ctx_d(ctx: JobContext) -> dict[str, Any]:
    from .context import ctx_to_dict

    return ctx_to_dict(ctx)


def _child_ctx(parent: JobContext, job_id) -> JobContext:
    from ..sdk import Actor

    return JobContext(
        actor=Actor.job(str(job_id), via=parent.actor),
        tenant_id=parent.tenant_id,
        correlation_id=parent.correlation_id,
        permissions=parent.permissions,
        request_id=parent.request_id,
        extra=dict(parent.extra),
    )
