"""Monitoring data APIs (T15) — read side for ops UIs / SuperAdmin.

All methods take an open async connection; callers own permissions.
"""
from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row


class MonitoringService:
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
            " concurrency_key, assignee, parent_job_id, created_at, updated_at"
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
