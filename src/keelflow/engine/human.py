"""Human task layer (T11) — Inbox data API over the jobs table.

Human jobs (`human.*`) live in `keelflow_jobs` like machine jobs but are
claimed/completed by people instead of workers. `assignee` is `user:<id>`
or `role:<name>`; `claimed_by` records the first user to open the task
(`FOR UPDATE` + null check — first claim wins).

`complete` writes the result and, if the job came from a workflow step,
unblocks the run via `workflow.on_job_finished`.
"""
from __future__ import annotations

import json
from typing import Any

from psycopg.rows import dict_row

from ..sdk import Actor, JobStatus
from .workflow import on_job_finished


class HumanTaskService:
    """Inbox queries + claim/complete. All methods take an open conn."""

    def __init__(self, registry, kernel=None) -> None:
        self._registry = registry
        self._kernel = kernel

    async def my_tasks(self, conn, actor: Actor, roles: list[str] | None = None) -> list[dict[str, Any]]:
        """Open tasks assigned to this user or any of their roles."""
        roles = roles or []
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT id, type, status, payload, assignee, claimed_by,
                       created_at
                FROM keelflow_jobs
                WHERE type LIKE 'human.%%'
                  AND status IN ('pending','claimed')
                  AND (
                        assignee = %s
                     OR assignee = ANY(%s)
                     OR (assignee IS NULL AND claimed_by IS NULL)
                  )
                ORDER BY created_at
                """,
                (
                    f"user:{actor.id}",
                    [f"role:{r}" for r in roles],
                ),
            )
            return await cur.fetchall()

    async def claim(self, conn, job_id: str, actor: Actor) -> None:
        """First claim wins. Raises if already claimed by someone else."""
        async with conn.transaction(), conn.cursor() as cur:
            await cur.execute(
                "SELECT claimed_by FROM keelflow_jobs WHERE id=%s FOR UPDATE",
                (job_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise KeyError(f"job {job_id} not found")
            who = f"user:{actor.id}"
            if row[0] and row[0] != who:
                raise PermissionError(f"task already claimed by {row[0]}")
            await cur.execute(
                "UPDATE keelflow_jobs SET status='claimed', claimed_by=%s,"
                " updated_at=now() WHERE id=%s",
                (who, job_id),
            )

    async def complete(
        self, conn, job_id: str, actor: Actor, result: dict[str, Any] | None = None
    ) -> None:
        """Complete a human task; unblocks the owning workflow run."""
        async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT status, claimed_by FROM keelflow_jobs WHERE id=%s FOR UPDATE",
                (job_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise KeyError(f"job {job_id} not found")
            who = f"user:{actor.id}"
            if row["claimed_by"] and row["claimed_by"] != who:
                raise PermissionError(
                    f"task claimed by {row['claimed_by']}, cannot complete as {who}"
                )
            await cur.execute(
                "UPDATE keelflow_jobs SET status='done', result=%s,"
                " updated_at=now() WHERE id=%s",
                (json.dumps(result or {}), job_id),
            )
            # link back to the workflow step, if any
            await cur.execute(
                "SELECT id FROM keelflow_step_runs WHERE job_id=%s", (job_id,)
            )
            step = await cur.fetchone()
        if step and self._kernel is not None:
            await on_job_finished(
                conn, self._kernel, self._registry, str(job_id),
                str(step["id"]), JobStatus.DONE, result or {},
            )

    async def escalate_overdue(self, conn, *, older_than_days: int, to_role: str) -> int:
        """Requeue pending human tasks older than N days to a manager role."""
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE keelflow_jobs SET assignee=%s, claimed_by=NULL,
                       status='pending', updated_at=now()
                WHERE type LIKE 'human.%%' AND status IN ('pending','claimed')
                  AND created_at < now() - interval '%s days'
                """,
                (f"role:{to_role}", str(older_than_days)),
            )
            return cur.rowcount
