"""Monitoring data APIs (T15) — read side for ops UIs / SuperAdmin.

All methods take an open async connection; callers own permissions.
"""
from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row


class MonitoringService:
    def __init__(self, jobs=None, registry=None) -> None:
        # Optional JobService — required only for dlq_replay().
        # Optional registry — required to resume a workflow run from a
        # DLQ entry that links to a step (otherwise replay stays standalone).
        self._jobs = jobs
        self._registry = registry

    async def jobs_list(
        self, conn, *, types=None, statuses=None, limit: int = 50, cursor: str | None = None
    ) -> dict[str, Any]:
        where = ["1=1"]
        params: list = []
        if types:
            where.append("type = ANY(%s)")
            params.append(list(types))
        if statuses:
            where.append("status = ANY(%s)")
            params.append(list(statuses))
        if cursor:
            where.append("created_at < %s")
            params.append(cursor)
        sql = (
            "SELECT id, type, status, attempts, run_at, claimed_by,"
            " concurrency_key, assignee, parent_job_id, result_bytes,"
            " created_at, updated_at"
            f" FROM keelflow_jobs WHERE {' AND '.join(where)}"
            " ORDER BY created_at DESC LIMIT %s"
        )
        params.append(limit)
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            items = await cur.fetchall()
        return {"items": items, "next_cursor": items[-1]["created_at"] if items else None}

    async def job_get(self, conn, job_id: str) -> dict[str, Any] | None:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM keelflow_jobs WHERE id=%s", (job_id,)
            )
            return await cur.fetchone()

    async def runs_get(self, conn, run_id: str) -> dict[str, Any] | None:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM keelflow_workflow_runs WHERE id=%s", (run_id,)
            )
            run = await cur.fetchone()
            if not run:
                return None
            await cur.execute(
                "SELECT step_id, type, status, job_id, outputs, attempts"
                " FROM keelflow_step_runs WHERE run_id=%s ORDER BY created_at",
                (run_id,),
            )
            run["steps"] = await cur.fetchall()
            return run

    async def runs_list(self, conn, *, limit: int = 50) -> list[dict[str, Any]]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, workflow, status, created_at, updated_at"
                " FROM keelflow_workflow_runs ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
            return await cur.fetchall()

    async def rows_list_failed(self, conn, job_id: str) -> list[dict[str, Any]]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, row_key, chunk_id, error, data, updated_at"
                " FROM keelflow_row_status WHERE job_id=%s AND status='failed'"
                " ORDER BY id",
                (job_id,),
            )
            return await cur.fetchall()

    async def rows_list_archived(
        self,
        conn,
        job_id: str,
        *,
        limit: int = 50,
        after_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Completed rows moved to `keelflow_row_archive` at fan-in.

        Keyset pagination over the archive's (job_id, id) index — pass the
        last returned `id` as `after_id` for the next page. Archive rows
        have no FK to live jobs, so this stays readable after job retention.
        """
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, row_id, row_key, chunk_id, data, archived_at"
                " FROM keelflow_row_archive"
                " WHERE job_id=%s AND (%s::bigint IS NULL OR id > %s::bigint)"
                " ORDER BY id LIMIT %s",
                (job_id, after_id, after_id, limit),
            )
            return await cur.fetchall()

    async def results_oversized(
        self, conn, *, threshold: int = 32768, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Job types ranked by worst result size — the `result_bytes`
        signal in action so ops can find hot job types to optimize."""
        async with conn.cursor(row_factory=dict_row) as cur:
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

    async def metrics_snapshot(self, conn) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
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
                "(SELECT 1 FROM pg_stat_activity WHERE datname=current_database()) x"
            )
            row["db_connections"] = (await cur.fetchone())["workers"]
            return row

    async def retry_job(self, conn, job_id: str) -> None:
        """Ops action: put a failed job back to pending (kernel re-claims)."""
        await conn.execute(
            "UPDATE keelflow_jobs SET status='pending', error=NULL,"
            " attempts=0, updated_at=now()"
            " WHERE id=%s AND status='failed'",
            (job_id,),
        )
        await conn.commit()

    async def sweep(
        self,
        conn,
        *,
        jobs_days: float | None = 30,
        outbox_days: float | None = 7,
        archive_days: float | None = 90,
        dlq_days: float | None = 90,
        batch: int = 500,
    ) -> dict[str, int]:
        """Time-based retention for engine tables — **caller-invoked**.

        Deletes, oldest-first and bounded by `batch` per table per call:

        - terminal `keelflow_workflow_runs` older than `jobs_days`
          (cascades to their step_runs, releasing the job_id link);
        - terminal `keelflow_jobs` older than `jobs_days` (cascades to
          chunks and row_status — failed rows share the parent's
          lifetime); jobs still referenced by a step_run are skipped;
        - `keelflow_outbox` rows `status='published'` older than
          `outbox_days`;
        - `keelflow_row_archive` rows older than `archive_days`;
        - `keelflow_dlq` entries older than `dlq_days`.

        Never touched: non-terminal jobs/runs, pending or failed outbox
        rows, `keelflow_audit`, `keelflow_entity_state`. `None` disables
        a category. Returns per-table deleted counts; callers typically
        run it periodically (cron job / ops command) until counts reach 0.
        """
        out: dict[str, int] = {}
        async with conn.cursor() as cur:
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

    async def dlq_list(self, conn, *, limit: int = 50) -> list[dict[str, Any]]:
        """Terminal-failure copies in keelflow_dlq, newest first."""
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, type, payload, error, attempts, dead_at"
                " FROM keelflow_dlq ORDER BY dead_at DESC LIMIT %s",
                (limit,),
            )
            return await cur.fetchall()

    async def dlq_replay(self, conn, dlq_id: str, ctx) -> str:
        """Replay a DLQ entry.

        If the dead job was a DAG step (`run_id`/`step_run_id` populated,
        the step_run still exists, and a registry is available), resume
        the owning run via `resume_step` — the step is re-enqueued in
        place and the run advances when it finishes. Returns the run id.
        Otherwise enqueue a fresh standalone job and return its job id.
        """
        if self._jobs is None:
            raise RuntimeError(
                "dlq_replay requires MonitoringService(jobs=JobService(...))"
            )
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT type, payload, run_id, step_run_id"
                " FROM keelflow_dlq WHERE id=%s", (dlq_id,))
            row = await cur.fetchone()
        if row is None:
            raise KeyError(f"no dlq entry {dlq_id}")

        step = None
        if row["step_run_id"] and self._registry is not None:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT run_id, step_id, status FROM keelflow_step_runs"
                    " WHERE id=%s", (row["step_run_id"],))
                step = await cur.fetchone()
        if step is not None and step["status"] == "failed":
            from .workflow import WorkflowService

            wf = WorkflowService(self._registry, self._jobs)
            async with conn.transaction():
                await wf.resume_step(conn, str(step["run_id"]),
                                     step["step_id"], ctx)
            await conn.commit()
            return str(step["run_id"])

        ref = await self._jobs.builder(row["type"]) \
            .payload(row["payload"] or {}) \
            .context(ctx) \
            .enqueue(conn)
        await conn.commit()
        return str(ref.id)
