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

from ..sdk import JobContext, JobResult
from ..sdk.handler import JobHandler
from . import worker as _worker
from .worker import DSN_ENV, _get_registry

log = logging.getLogger("keelflow.bulk")

CHUNK_TYPE = "bulk.chunk@v1"
PARENT_HOLD = "_bulk_parent"


class BulkJobPayload(BaseModel):
    """Payload for a parent bulk job."""

    chunk_size: int = 1000
    atomic: Literal["chunk", "row"] = "chunk"


class ChunkPayload(BaseModel):
    """Payload for an engine chunk job."""

    parent_job_id: str
    chunk_id: str


class BulkJobHandler(JobHandler):
    """App's bulk/import handler. Override `process_row`.

    `run` partitions this job's pending `row_status` rows into chunks and
    enqueues one `bulk.chunk@v1` job per chunk. The parent stays `running`
    until the last chunk settles (fan-in).
    """

    async def process_row(
        self, ctx: JobContext, row: dict[str, Any], tx
    ) -> dict[str, Any] | None:
        """Apply one staged row inside the chunk transaction."""
        raise NotImplementedError

    async def run(self, ctx: JobContext, payload: BaseModel) -> JobResult:
        job_id = ctx.extra["job_id"]  # injected by dispatch
        dsn = os.environ[DSN_ENV]
        assert isinstance(payload, BulkJobPayload)
        kernel = _worker._kernel_client
        if kernel is None:
            raise RuntimeError("no kernel client configured; run via worker")
        conn = await psycopg.AsyncConnection.connect(dsn)
        try:
            async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
                # rows staged for this job that are not yet in a chunk
                await cur.execute(
                    "SELECT id FROM keelflow_row_status"
                    " WHERE job_id=%s AND status='pending' AND chunk_id IS NULL"
                    " ORDER BY id FOR UPDATE SKIP LOCKED",
                    (job_id,),
                )
                row_ids = [r["id"] for r in await cur.fetchall()]
                if not row_ids:
                    return JobResult(status="done", outputs={"chunks": 0})

                size = max(1, payload.chunk_size)
                n_chunks = (len(row_ids) + size - 1) // size
                now = datetime.now(UTC)

                for ci in range(n_chunks):
                    chunk_id = uuid.uuid4()
                    ids = row_ids[ci * size : (ci + 1) * size]
                    await cur.execute(
                        "INSERT INTO keelflow_chunks (id, job_id, idx, status)"
                        " VALUES (%s, %s, %s, 'pending')",
                        (chunk_id, job_id, ci),
                    )
                    await cur.execute(
                        "UPDATE keelflow_row_status SET chunk_id=%s WHERE id = ANY(%s)",
                        (chunk_id, ids),
                    )
                    child_id = uuid.uuid4()
                    child_ctx = _child_ctx(ctx, child_id)
                    child_payload = ChunkPayload(
                        parent_job_id=job_id, chunk_id=str(chunk_id)
                    ).model_dump(mode="json")
                    await cur.execute(
                        """
                        INSERT INTO keelflow_jobs
                            (id, type, status, payload, context, attempts,
                             max_attempts, run_at, parent_job_id,
                             created_at, updated_at)
                        VALUES (%s,%s,'pending',%s,%s,0,3,%s,%s,%s,%s)
                        """,
                        (
                            child_id, CHUNK_TYPE,
                            json.dumps(child_payload),
                            json.dumps(_ctx_d(child_ctx)),
                            now, job_id, now, now,
                        ),
                    )
                    await kernel.push_ex(
                        cur,
                        _kernel_job(child_id, str(chunk_id)),
                    )

                # parent stays running until fan-in (dispatch honors marker)
                return JobResult(
                    status="done",
                    outputs={PARENT_HOLD: True, "chunks": n_chunks,
                             "rows": len(row_ids)},
                )
        finally:
            await conn.close()


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

            while True:
                rows = await self._fetch_rows(conn, payload.chunk_id)
                if not rows:
                    break
                if atomic == "chunk":
                    await self._process_chunk_atomic(
                        conn, ctx, parent_handler, rows, payload.chunk_id
                    )
                    break  # whole batch in one tx
                else:
                    for row in rows:
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
        except Exception as exc:
            log.exception("chunk %s failed", payload.chunk_id)
            await self._safe_mark_failed(conn, payload.chunk_id,
                                         payload.parent_job_id, str(exc))
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
        """Last chunk to settle flips the parent job to its final status."""
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT status, count(*) FROM keelflow_chunks"
                " WHERE job_id=%s GROUP BY status",
                (parent_job_id,),
            )
            counts = {r["status"]: r["count"] for r in await cur.fetchall()}
            if counts.get("pending") or counts.get("claimed") or counts.get("running"):
                return  # still in flight
            final = "failed" if counts.get("failed") else "done"
            await cur.execute(
                "UPDATE keelflow_jobs SET status=%s, updated_at=now(),"
                " result = COALESCE(result,'{}'::jsonb) || %s::jsonb"
                " WHERE id=%s",
                (final, json.dumps({"chunks": counts}), parent_job_id),
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


def register(registry, dsn: str | None = None) -> None:
    """Register the internal chunk job type on an InMemoryRegistry."""
    from ..sdk.specs import JobTypeSpec, RetryPolicy

    registry.register_job_type(
        JobTypeSpec(
            type=CHUNK_TYPE,
            input_schema=ChunkPayload,
            handler=ChunkJobHandler,
            retry=RetryPolicy(max_attempts=3),
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
