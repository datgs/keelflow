"""Human task layer (T11) — Inbox data API over the jobs store.

Human jobs (`human.*`) live in the same store as machine jobs but are
claimed/completed by people instead of workers. `assignee` is `user:<id>`
or `role:<name>`; `claimed_by` records the first user to open the task
(first claim wins, guarded by the job row lock).

`complete` writes the result and, if the job came from a workflow step,
unblocks the run via `workflow.on_job_finished`.
"""
from __future__ import annotations

from typing import Any

from ..sdk import Actor, JobStatus, Storage
from ..sdk.uow import nested_scope
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
    """Inbox queries + claim/complete.

    `complete` handles two task kinds:
    - workflow-step tasks (`human.*` inside a DAG): completing advances
      the run via `workflow.on_job_finished`.
    - SM-created tasks (`creates_job` on a transition): payload carries
      `sm_actions` → `complete_and_transition` completes the task *and*
      applies the mapped SM transition in the same UoW.
    """

    def __init__(self, registry, kernel=None, sm=None,
                 storage: Storage | None = None) -> None:
        from .kernel import as_kernel

        self._registry = registry
        self._kernel = as_kernel(kernel) if kernel is not None else None
        self._sm = sm  # EngineStateMachineService, for complete_and_transition
        self._storage = storage

    def _store(self) -> Storage:
        if self._storage is None:
            from .worker import _get_storage

            self._storage = _get_storage()
        return self._storage

    async def my_tasks(self, uow, actor: Actor,
                       roles: list[str] | None = None) -> list[dict[str, Any]]:
        """Open tasks assigned to this user or any of their roles."""
        return await self._store().tasks_open_for(
            uow, actor.id, roles or [])

    async def claim(
        self, uow, job_id: str, actor: Actor,
        roles: list[str] | None = None,
    ) -> None:
        """First claim wins, subject to the task's assignee.

        `assignee=user:X` → only X may claim; `assignee=role:R` → the
        actor must hold role R (pass `roles`); unassigned tasks are
        claimable by anyone. Raises PermissionError on a role mismatch
        or when already claimed by someone else. Writes join the
        caller's unit of work.
        """
        roles = roles or []
        storage = self._store()
        async with nested_scope(uow) as nuow:
            row = await storage.job_lock(nuow, job_id)
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
            await storage.task_update(
                nuow, job_id, status="claimed", claimed_by=who)

    async def complete(
        self, uow, job_id: str, actor: Actor,
        result: dict[str, Any] | None = None, *,
        roles: list[str] | None = None,
    ) -> None:
        """Complete a workflow-step human task; unblocks the owning run.

        The job update and the run advance commit together in the
        caller's unit of work.
        """
        storage = self._store()
        async with nested_scope(uow) as nuow:
            row = await storage.job_lock(nuow, job_id)
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
            await storage.task_update(
                nuow, job_id, status="done", result=result or {})
            # link back to the workflow step, if any — inside the UoW so
            # task completion + run advance commit together
            step = await storage.step_run_by_job(nuow, job_id)
            if step and self._kernel is not None:
                await on_job_finished(
                    nuow, storage, self._kernel, self._registry, job_id,
                    step, JobStatus.DONE, result or {},
                )

    async def complete_and_transition(
        self,
        uow,
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
        and the transition applies in one UoW: a rejection can't leave the
        task 'done' while the entity stays in its old state. `roles` must
        come from the application's trusted actor-to-role mapping.
        """
        import json

        storage = self._store()
        async with nested_scope(uow) as nuow:
            row = await storage.job_lock(nuow, job_id)
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
            await storage.task_update(
                nuow, job_id, status="done",
                result={"decision": decision, "note": note})
            from ..sdk import EntityRef
            entity = EntityRef(
                type=payload["entity_type"], id=payload["entity_id"])
            if self._sm is None:
                from .statemachine import EngineStateMachineService

                self._sm = EngineStateMachineService(
                    self._registry, self._kernel, storage=storage)
            await self._sm.transition(entity, action, ctx, nuow)

    async def release(
        self, uow, job_id: str, actor: Actor,
        roles: list[str] | None = None,
    ) -> None:
        """Release a claimed task back to pending (claimer went away).

        Only the current claimer may release their own claim; admins
        should use escalate_overdue. Raises PermissionError on a
        claim-owner mismatch.
        """
        storage = self._store()
        async with nested_scope(uow) as nuow:
            row = await storage.job_lock(nuow, job_id)
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
            await storage.task_update(
                nuow, job_id, status="pending", clear_claim=True)

    async def escalate_overdue(
        self, uow, *, older_than_days: int, to_role: str
    ) -> int:
        """Requeue pending human tasks older than N days to a manager role."""
        return await self._store().tasks_escalate_overdue(
            uow, older_than_days, to_role)
