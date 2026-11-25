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

from pydantic import BaseModel

from ..sdk import (
    BatchRowResult,
    JobCancelled,
    JobContext,
    JobInsert,
    JobResult,
)
from ..sdk.handler import JobHandler
from ..sdk.specs import JobStatus, RetryPolicy
from ..sdk.uow import nested_scope
from . import worker as _worker
from .worker import _get_registry


def _storage_or_default():
    """Resolve storage outside dispatch (direct handler calls/tests)."""
    from .worker import _get_storage

    return _get_storage()

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

    async def _own_type(self, uow, storage, job_id) -> str:
        """Resolve this handler's own registered job type from its job row."""
        return await storage.job_type_of(uow, job_id)

    async def run(self, ctx: JobContext, payload: BaseModel) -> JobResult:
        """Partition pending rows into chunks + enqueue chunk jobs.

        Runs on ctx.uow — the handler's own dispatch unit of work — so
        rows staged by a subclass's `run` (or pre-staged by the caller)
        are visible, and partition + chunk enqueue commit atomically
        with the job finish. A separate session would see a stale
        snapshot.
        """
        job_id = ctx.extra["job_id"]  # injected by dispatch
        assert isinstance(payload, BulkJobPayload)
        kernel = _worker._kernel_client
        storage = (ctx.execution._storage if ctx.execution._storage is not None
                   else _storage_or_default())
        uow = ctx.uow
        if kernel is None:
            raise RuntimeError("no kernel client configured; run via worker")

        size = max(1, payload.chunk_size)

        # Pending count first: chunk ids are generated in Python, so we
        # need the chunk count up front. Rows inserted concurrently
        # between this count and the partition statement simply stay
        # unchunked (same eventual behavior as the previous loop, which
        # also only saw its snapshot of claimed ids).
        n_pending = await storage.bulk_pending_count(uow, job_id)
        if n_pending == 0:
            return JobResult(status="done", outputs={"chunks": 0})
        n_chunks = -(-n_pending // size)
        chunk_ids = [uuid.uuid4() for _ in range(n_chunks)]

        # Set-based partition — claims the pending, unchunked rows,
        # numbers them in id order, inserts the pre-generated chunks,
        # and stamps each claimed row with its chunk id.
        n_rows, n_chunks_created = await storage.bulk_partition(
            uow, job_id, size, chunk_ids)
        if n_chunks_created != n_chunks:
            raise RuntimeError(
                f"partition inserted {n_chunks_created} chunks,"
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
        parent_cancel = await storage.job_parent_cancel_flag(uow, job_id)
        try:
            parent_spec = _get_registry().job_type(
                await self._own_type(uow, storage, job_id))
            chunk_max_attempts = (
                parent_spec.retry.max_attempts if parent_spec.retry else 3)
        except KeyError:
            chunk_max_attempts = 3

        # Build all child jobs in one pass — identical per-job rows.
        now = datetime.now(UTC)
        child_ids = [uuid.uuid4() for _ in chunk_ids]
        inserts, kernel_jobs = [], []
        for child_id, chunk_id in zip(child_ids, chunk_ids):
            child_ctx = _child_ctx(ctx, child_id)
            inserts.append(JobInsert(
                id=str(child_id), type=CHUNK_TYPE,
                payload=ChunkPayload(
                    parent_job_id=job_id,
                    chunk_id=str(chunk_id)).model_dump(mode="json"),
                context=_ctx_d(child_ctx),
                max_attempts=chunk_max_attempts,
                run_at=now,
                parent_job_id=str(job_id),
                cancel_requested=parent_cancel,
            ))
            kernel_jobs.append(_chunk_dispatch_spec(child_id, str(chunk_id)))
        await storage.job_insert_many(uow, inserts)
        # push_many_ex inserts one kernel row per job (Chancy 0.26),
        # but sends a single queue notification at the end. A set-based
        # direct insert was measured to save only ~1.3s at 1M rows —
        # not worth coupling to the kernel table schema; see
        # docs/plans/completed/bulk-kernel-batch-inserts.md.
        await kernel.push_many_ex(uow, kernel_jobs)

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
        registry = _get_registry()
        storage = (ctx.execution._storage if ctx.execution._storage is not None
                   else _storage_or_default())
        session = await storage.connect()
        processed = failed_rows = 0
        try:
            # claim the chunk; reset rows stranded in 'processing' by a dead
            # worker (row-atomic resume).
            async with session.begin() as uow:
                await storage.chunk_claim(
                    uow, str(payload.chunk_id),
                    os.environ.get("KEELFLOW_WORKER_ID", "worker-0"))
                await storage.rows_reset_processing(
                    uow, str(payload.chunk_id))

            async with session.begin() as uow:
                parent_type, atomic = await self._parent_type(
                    uow, storage, payload.parent_job_id)
                spec = registry.job_type(parent_type)
                parent_handler: BulkJobHandler = spec.handler()  # type: ignore[misc]

                # Handler selection (spec/04): atomic='chunk' uses the
                # set-based process_batch when the handler overrides it;
                # atomic='row' always uses process_row. A batch-only
                # handler (no process_row override) with atomic='row' is
                # a config error — the whole-batch commit semantics
                # cannot hold.
                has_batch = (type(parent_handler).process_batch
                             is not BulkJobHandler.process_batch)
                has_row = (type(parent_handler).process_row
                           is not BulkJobHandler.process_row)
                if atomic == "row" and has_batch and not has_row:
                    raise ValueError(
                        f"bulk handler {type(parent_handler).__name__} only"
                        " implements process_batch, which requires"
                        " atomic='chunk' (the batch commits per chunk);"
                        " either implement process_row or set"
                        " atomic='chunk'")
                use_batch = atomic == "chunk" and has_batch

                while True:
                    rows = await storage.rows_fetch_pending(
                        uow, str(payload.chunk_id))
                    if not rows:
                        break
                    if use_batch:
                        done_n, failed_n = await self._process_batch_atomic(
                            uow, ctx, parent_handler, rows,
                            payload.chunk_id,
                        )
                        processed += done_n
                        failed_rows += failed_n
                        break  # whole chunk in one tx
                    elif atomic == "chunk":
                        await self._process_chunk_atomic(
                            session, uow, ctx, parent_handler, rows,
                            payload.chunk_id,
                        )
                        break  # whole batch in one tx
                    else:
                        for row in rows:
                            # Cooperative cancel checkpoint — direct
                            # handler.run calls (unit tests) have no
                            # ctx.execution injected; only dispatched jobs
                            # of an opted-in spec observe the flag.
                            if ctx.execution._storage is not None:
                                await ctx.execution.checkpoint()
                            ok = await self._process_one_row(
                                uow, ctx, parent_handler, row
                            )
                            processed += int(ok)
                            failed_rows += int(not ok)

                await storage.chunk_mark(
                    uow, str(payload.chunk_id), "done",
                    {"processed": processed, "failed": failed_rows})
            await self._fan_in(session, storage, payload.parent_job_id)
            return JobResult(status="done", outputs={
                "processed": processed, "failed_rows": failed_rows})
        except JobCancelled:
            # Settle bookkeeping outside the rolled-back handler UoW so
            # fan-in can terminalize the parent; dispatch then records
            # the job itself as cancelled.
            log.info("chunk %s cancelled at checkpoint", payload.chunk_id)
            async with session.begin() as uow:
                await storage.chunk_mark(
                    uow, str(payload.chunk_id), "cancelled",
                    {"processed": processed, "failed": failed_rows})
            await self._fan_in(session, storage, payload.parent_job_id)
            raise
        except Exception as exc:
            log.exception("chunk %s failed", payload.chunk_id)
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
                async with session.begin() as uow:
                    att = await storage.job_attempts(uow, own_job_id)
                    exhausted = bool(att and att[0] >= att[1])
            if exhausted:
                await self._safe_mark_failed(session, storage,
                                             payload.chunk_id,
                                             payload.parent_job_id, str(exc))
            else:
                log.info(
                    "chunk %s failed transiently (attempt %s); retry"
                    " scheduled by kernel", payload.chunk_id, exc)
            raise
        finally:
            await session.close()

    # ── internals ─────────────────────────────────────────────────────

    async def _parent_type(self, uow, storage, parent_job_id) -> tuple[str, str]:
        row = await storage.job_load(uow, parent_job_id)
        if row is None:
            raise KeyError(f"parent job {parent_job_id} not found")
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return row["type"], payload.get("atomic", "chunk")

    async def _process_batch_atomic(
        self, uow, ctx, handler, rows, chunk_id
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
        storage = (ctx.execution._storage if ctx.execution._storage is not None
                   else _storage_or_default())
        if ctx.execution._storage is not None:
            await ctx.execution.checkpoint()
        async with nested_scope(uow) as nuow:
            results = await handler.process_batch(ctx, rows, nuow)
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
            err_rows = {r.row_id: (r.error or "unknown error")
                        for r in results if r.error is not None}
            if done_ids:
                await storage.rows_mark(nuow, done_ids, "done")
            if err_rows:
                await storage.rows_mark(
                    nuow, list(err_rows), "failed", error_by_id=err_rows)
        return len(done_ids), len(err_rows)

    async def _process_chunk_atomic(
        self, session, uow, ctx, handler, rows, chunk_id
    ) -> None:
        storage = ctx.execution._storage
        failing: tuple | None = None
        try:
            async with nested_scope(uow) as nuow:
                for row in rows:
                    await storage.rows_mark(nuow, [row["id"]], "processing")
                    try:
                        await handler.process_row(ctx, row, nuow)
                    except Exception as exc:
                        failing = (row["id"], str(exc))
                        raise
                    await storage.rows_mark(nuow, [row["id"]], "done")
        finally:
            if failing is not None:
                # record the failing row outside the rolled-back scope so
                # the error queue still shows which row failed (§3.2).
                async with session.begin() as fuow:
                    await storage.rows_mark(
                        fuow, [failing[0]], "failed",
                        error_by_id={failing[0]: failing[1]})

    async def _process_one_row(self, uow, ctx, handler, row) -> bool:
        storage = (ctx.execution._storage if ctx.execution._storage is not None
                   else _storage_or_default())
        async with nested_scope(uow) as nuow:
            await storage.rows_mark(nuow, [row["id"]], "processing")
            try:
                await handler.process_row(ctx, row, nuow)
            except Exception as exc:  # noqa: BLE001 - row-level error queue entry
                await storage.rows_mark(
                    nuow, [row["id"]], "failed",
                    error_by_id={row["id"]: str(exc)})
                return False
            await storage.rows_mark(nuow, [row["id"]], "done")
            return True

    async def _fan_in(self, scope, storage, parent_job_id) -> None:
        """Last chunk to settle flips the parent job to its final status.

        `scope` is a Session (opens its own unit) or an open UnitOfWork
        (joins as a nested scope) — the caller decides which transaction
        this bookkeeping commits with. If the parent belongs to a
        workflow step, the run advance lands in the same transaction.
        """
        from ..sdk.uow import nested_scope
        from . import workflow as _wf
        from .worker import _kernel_client

        boundary = (scope.begin() if hasattr(scope, "begin")
                    else nested_scope(scope))
        async with boundary as uow:
            counts = await storage.chunk_counts(uow, parent_job_id)
            if counts.get("pending") or counts.get("claimed") or counts.get("running"):
                return  # still in flight

            # Two chunks can settle near-simultaneously; serialize on the
            # parent row so only the first fan-in computes the summary and
            # moves rows — later ones see the terminal status and return.
            pj = await storage.job_lock(uow, parent_job_id)
            if pj is None or pj["status"] in ("done", "failed", "cancelled"):
                return
            if pj["cancel_requested"]:
                final = "cancelled"
            elif counts.get("cancelled"):
                # A cancelled chunk leaves its unprocessed rows pending —
                # the pass did not complete. Parent is non-success so a
                # workflow step does not advance as a success.
                final = "cancelled"
            elif counts.get("failed"):
                final = "failed"
            else:
                final = "done"

            rows = await storage.row_counts(uow, parent_job_id)
            summary = {
                "chunks": counts,
                "rows_total": sum(rows.values()),
                "rows_done": rows.get("done", 0),
                "rows_failed": rows.get("failed", 0),
                # unprocessed input rows (cancelled chunk leaves them
                # pending) — non-zero means the pass did not complete
                "rows_pending": rows.get("pending", 0),
                "rows_processing": rows.get("processing", 0),
            }

            # Retention for 'done' staging rows — same UoW as the parent's
            # terminal write, so a rollback loses neither the rows nor the
            # step-finish hook. Old jobs lacking the payload field default
            # to 'archive'.
            mode = await storage.row_disposition(uow, parent_job_id)
            done_rows = rows.get("done", 0)
            archived = deleted = 0
            if done_rows and mode == "archive":
                archived = await storage.rows_archive_done(
                    uow, parent_job_id)
            elif done_rows and mode == "delete":
                deleted = await storage.rows_delete_done(uow, parent_job_id)
            summary["row_disposition"] = mode
            summary["archived_rows"] = archived
            summary["deleted_rows"] = deleted
            summary["kept_rows"] = done_rows if mode == "keep" else 0

            result = await storage.job_result_merge(
                uow, parent_job_id, final, summary)

            # workflow step link — advance the run in the same UoW
            step = await storage.step_run_by_job(uow, parent_job_id)
            if step and _kernel_client is not None:
                registry = _get_registry()
                await _wf.on_job_finished(
                    uow, storage, _kernel_client, registry, parent_job_id,
                    step, JobStatus(final), result,
                )

    async def _safe_mark_failed(
        self, scope, storage, chunk_id, parent_job_id, error
    ) -> None:
        from ..sdk.uow import nested_scope

        try:
            boundary = (scope.begin() if hasattr(scope, "begin")
                        else nested_scope(scope))
            async with boundary as uow:
                await storage.chunk_mark(
                    uow, str(chunk_id), "failed", {"error": error})
            await self._fan_in(scope, storage, parent_job_id)
        except Exception:
            log.exception("could not mark chunk %s failed", chunk_id)


async def progress(uow, storage, job_id) -> dict[str, Any]:
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
    job = await storage.job_load(uow, job_id)
    if job is None:
        raise KeyError(f"bulk job {job_id} not found")
    rows = await storage.row_counts(uow, job_id)
    chunks = await storage.chunk_counts(uow, job_id)

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


def _chunk_dispatch_spec(job_id, chunk_id: str):
    from .kernel import dispatch_spec

    return dispatch_spec(
        job_id=job_id,
        max_attempts=3,
        meta={"job_id": str(job_id), "type": CHUNK_TYPE,
              "chunk_id": chunk_id},
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
