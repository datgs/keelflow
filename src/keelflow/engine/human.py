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


def _check_human_job(type_name: str) -> None:
    if not type_name.startswith("human."):
        raise ValueError(f"job type {type_name} is not a human task")


def _check_assignee(assignee: str | None, actor: Actor,
                    roles: list[str]) -> None:
    who = f"user:{actor.id}"
    if assignee and assignee.startswith("user:") and assignee != who:
        raise PermissionError(f"task assigned to {assignee}, not {who}")
    if (assignee and assignee.startswith("role:")
            and assignee not in {f"role:{role}" for role in roles}):
        raise PermissionError(
            f"task requires role {assignee}; actor has {roles}")


class HumanTaskService:
    """Inbox queries + claim/complete. All methods take an open conn.

    `complete` handles two task kinds:
    - workflow-step tasks (`human.*` inside a DAG): completing advances
      the run via `workflow.on_job_finished`.
    - SM-created tasks (`creates_job` on a transition): payload carries
      `sm_actions` → `complete_and_transition` completes the task *and*
      applies the mapped SM transition in the same TX.
    """

    def __init__(self, registry, kernel=None, sm=None) -> None:
        from .kernel import as_kernel

        self._registry = registry
        self._kernel = as_kernel(kernel) if kernel is not None else None
        self._sm = sm  # EngineStateMachineService, for complete_and_transition

    async def my_tasks(self, conn, actor: Actor, roles: list[str] | None = None) -> list[dict[str, Any]]:
        """Open tasks assigned to this user or any of their roles."""
        roles = roles or []
        async with conn.cursor(row_factory=dict_row) as cur:
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
                    f"user:{actor.id}",
                    f"user:{actor.id}",
                    [f"role:{r}" for r in roles],
                ),
            )
            return await cur.fetchall()

    async def claim(
        self, conn, job_id: str, actor: Actor,
        roles: list[str] | None = None,
    ) -> None:
        """First claim wins, subject to the task's assignee.

        `assignee=user:X` → only X may claim; `assignee=role:R` → the
        actor must hold role R (pass `roles`); unassigned tasks are
        claimable by anyone. Raises PermissionError on a role mismatch
        or when already claimed by someone else.
        """
        roles = roles or []
        async with conn.transaction(), conn.cursor(
                row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT type, status, claimed_by, assignee FROM keelflow_jobs"
                " WHERE id=%s FOR UPDATE",
                (job_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise KeyError(f"job {job_id} not found")
            _check_human_job(row["type"])
            if row["status"] != "pending" and row["status"] != "claimed":
                raise ValueError(f"task is not open (status={row['status']})")
            who = f"user:{actor.id}"
            if row["claimed_by"] and row["claimed_by"] != who:
                raise PermissionError(
                    f"task already claimed by {row['claimed_by']}")
            _check_assignee(row["assignee"], actor, roles)
            await cur.execute(
                "UPDATE keelflow_jobs SET status='claimed', claimed_by=%s,"
                " updated_at=now() WHERE id=%s",
                (who, job_id),
            )

    async def complete(
        self, conn, job_id: str, actor: Actor,
        result: dict[str, Any] | None = None, *,
        roles: list[str] | None = None,
    ) -> None:
        """Complete a workflow-step human task; unblocks the owning run.

        Caller owns the TX — the job update and the run advance commit
        together.
        """
        async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT type, status, claimed_by, assignee FROM keelflow_jobs"
                " WHERE id=%s FOR UPDATE",
                (job_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise KeyError(f"job {job_id} not found")
            _check_human_job(row["type"])
            if row["status"] not in ("pending", "claimed"):
                raise ValueError(f"task is not open (status={row['status']})")
            who = f"user:{actor.id}"
            if row["claimed_by"] and row["claimed_by"] != who:
                raise PermissionError(
                    f"task claimed by {row['claimed_by']}, cannot complete as {who}"
                )
            _check_assignee(row["assignee"], actor, roles or [])
            await cur.execute(
                "UPDATE keelflow_jobs SET status='done', result=%s,"
                " updated_at=now() WHERE id=%s",
                (json.dumps(result or {}), job_id),
            )
            # link back to the workflow step, if any — inside the TX so
            # task completion + run advance commit together
            await cur.execute(
                "SELECT id FROM keelflow_step_runs WHERE job_id=%s", (job_id,)
            )
            step = await cur.fetchone()
            if step and self._kernel is not None:
                await on_job_finished(
                    conn, self._kernel, self._registry, str(job_id),
                    str(step["id"]), JobStatus.DONE, result or {},
                )

    async def complete_and_transition(
        self,
        conn,
        job_id: str,
        actor: Actor,
        decision: str,
        ctx,
        note: str | None = None,
        *,
        roles: list[str] | None = None,
    ) -> None:
        """Complete an SM-created human task and apply the mapped transition.

        The task's payload `sm_actions` maps decisions to SM actions, e.g.
        `{"approve": "approve", "reject": "reject"}`. The task completes
        and the transition applies in one TX: a rejection can't leave the
        task 'done' while the entity stays in its old state. `roles` must
        come from the application's trusted actor-to-role mapping.
        """
        async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT type, status, claimed_by, assignee, payload FROM keelflow_jobs"
                " WHERE id=%s FOR UPDATE",
                (job_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise KeyError(f"job {job_id} not found")
            _check_human_job(row["type"])
            if row["status"] not in ("pending", "claimed"):
                raise ValueError(f"task is not open (status={row['status']})")
            who = f"user:{actor.id}"
            if row["claimed_by"] and row["claimed_by"] != who:
                raise PermissionError(
                    f"task claimed by {row['claimed_by']}, cannot complete as {who}"
                )
            _check_assignee(row["assignee"], actor, roles or [])
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            sm_actions = (payload or {}).get("sm_actions") or {}
            action = sm_actions.get(decision)
            if action is None:
                raise ValueError(
                    f"decision {decision!r} not allowed; valid: {sorted(sm_actions)}"
                )
            await cur.execute(
                "UPDATE keelflow_jobs SET status='done', result=%s,"
                " updated_at=now() WHERE id=%s",
                (json.dumps({"decision": decision, "note": note}), job_id),
            )
            from ..sdk import EntityRef

            entity = EntityRef(
                type=payload["entity_type"], id=payload["entity_id"])
            if self._sm is None:
                from .statemachine import EngineStateMachineService

                self._sm = EngineStateMachineService(self._registry, self._kernel)
            await self._sm.transition(entity, action, ctx, cur)

    async def release(
        self, conn, job_id: str, actor: Actor,
        roles: list[str] | None = None,
    ) -> None:
        """Release a claimed task back to pending (claimer went away).

        Only the current claimer may release their own claim; admins
        should use escalate_overdue. Raises PermissionError on a
        claim-owner mismatch.
        """
        async with conn.transaction(), conn.cursor(
                row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT type, status, claimed_by FROM keelflow_jobs"
                " WHERE id=%s FOR UPDATE",
                (job_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise KeyError(f"job {job_id} not found")
            _check_human_job(row["type"])
            if row["status"] != "claimed":
                raise ValueError(f"task is not claimed (status={row['status']})")
            who = f"user:{actor.id}"
            if row["claimed_by"] != who:
                raise PermissionError(
                    f"task claimed by {row['claimed_by']}, cannot release as {who}"
                )
            await cur.execute(
                "UPDATE keelflow_jobs SET status='pending', claimed_by=NULL,"
                " updated_at=now() WHERE id=%s",
                (job_id,),
            )

    async def escalate_overdue(self, conn, *, older_than_days: int, to_role: str) -> int:
        """Requeue pending human tasks older than N days to a manager role."""
        async with conn.cursor() as cur:
            await cur.execute(
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
                (f"role:{to_role}", int(older_than_days)),
            )
            return cur.rowcount
