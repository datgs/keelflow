"""StateMachineService (T12) + workflow-trigger integration.

`transition(entity, action, ctx, tx)` enforces the declared transition
table + permission guard inside the caller's transaction, records the new
state in `keelflow_entity_state`, audits the change, and fires
`creates_job` / `triggers_workflow` side effects in the same TX.

Workflow triggers (`WorkflowTriggerSpec`) persist metadata on the spawned
run (`triggered_by_entity_*`, callback actions, initiating ctx) so the
terminal callback in `workflow._finish_run` can apply `on_succeeded` /
`on_rejected` / `on_failed` transitions to the entity — in the run's own
terminal transaction, not here.

Concurrent-first-transition safety: `_current` inserts the entity row
`ON CONFLICT DO NOTHING` before `FOR UPDATE`, so two simultaneous first
transitions serialize on the row's write lock instead of both seeing
"no row" and proceeding.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from ..sdk import (
    Actor,
    AuditEvent,
    EntityRef,
    JobContext,
    JobInsert,
    StateMachineService,
    Storage,
    WorkflowTriggerSpec,
)
from .context import ctx_to_dict
from .kernel import dispatch_spec


class EngineStateMachineService(StateMachineService):
    def __init__(self, registry, kernel=None, audit=None,
                 storage: Storage | None = None) -> None:
        from .kernel import as_kernel

        self._registry = registry
        self._kernel = as_kernel(kernel) if kernel is not None else None
        self._audit = audit  # optional AuditService impl
        self._storage = storage

    def _store(self, uow=None) -> Storage:
        """Resolve the storage adapter: explicit constructor injection,
        else the dispatch-time storage on the uow's execution ctx, else
        the configured global."""
        if self._storage is not None:
            return self._storage
        from .worker import _get_storage

        return _get_storage()

    async def ensure_schema(self, session) -> None:
        """Apply the core schema (idempotent). Kept as the test/setup hook.

        `session` is a storage Session — DDL runs through its native
        connection (backend-specific setup is the adapter's job)."""
        from ..postgres.schema import apply_async

        await apply_async(getattr(session, "native", session))

    async def transition(
        self, entity: EntityRef, action: str, ctx: JobContext, uow
    ) -> None:
        await self._apply(entity, action, ctx, uow, enforce_permission=True)

    async def _internal_transition(
        self, uow, storage: Storage, entity: EntityRef, action: str,
        ctx: JobContext,
    ) -> None:
        """Engine-authorized transition (workflow callback, resume).

        Skips the *permission* guard — the initiating actor was already
        authorized when the trigger was created. Still validates the
        transition itself, so a bad on_succeeded/on_failed action fails the run
        finalization loudly rather than silently corrupting state.
        """
        await self._apply(entity, action, ctx, uow,
                          enforce_permission=False, storage=storage)

    async def _apply(
        self,
        entity: EntityRef,
        action: str,
        ctx: JobContext,
        uow,
        *,
        enforce_permission: bool,
        storage: Storage | None = None,
    ) -> None:
        sm = self._registry.state_machine(f"{entity.type}@v1")
        storage = storage or self._store(uow)

        current = await storage.entity_ensure_locked(
            uow, entity.type, entity.id, sm.initial)
        t = next(
            (t for t in sm.transitions
             if t.from_state == current and t.action == action),
            None,
        )
        if t is None:
            raise ValueError(
                f"{entity.type}:{entity.id} cannot {action} from {current}"
            )
        perm = (t.guard or {}).get("permission")
        if enforce_permission and perm:
            ctx.require(perm)

        await storage.entity_transition(
            uow, entity.type, entity.id, t.to_state)

        if self._audit is not None:
            await self._audit.record(
                AuditEvent(
                    actor=ctx.actor,
                    action=f"{entity.type}.{action}",
                    entity=entity,
                    changes={"state": (current, t.to_state)},
                    source=ctx.extra.get("job_id") or ctx.request_id,
                    correlation_id=ctx.correlation_id,
                ),
                uow,
            )

        if t.creates_job:
            await self._spawn_job(uow, storage, t.creates_job, entity, ctx, t)
        if t.triggers_workflow:
            await self._trigger_workflow(
                uow, storage, t.triggers_workflow, entity, ctx)

    async def current(self, entity: EntityRef, uow=None) -> str | None:  # type: ignore[override]
        if uow is None:
            return None
        return await self._store(uow).entity_state(
            uow, entity.type, entity.id)

    async def allowed_actions(
        self, entity: EntityRef, actor: Actor, uow,
        *, permissions: frozenset[str] | None = None,
    ) -> list[str]:  # type: ignore[override]
        try:
            sm = self._registry.state_machine(f"{entity.type}@v1")
        except KeyError:
            return []
        current = (await self._store(uow).entity_state(
            uow, entity.type, entity.id)) or sm.initial
        perms = permissions or frozenset()
        out = []
        for t in sm.transitions:
            if t.from_state != current:
                continue
            perm = (t.guard or {}).get("permission")
            if perm and perm not in perms and "*" not in perms:
                continue
            out.append(t.action)
        return out

    # ── helpers ────────────────────────────────────────────────────────

    async def _spawn_job(
        self, uow, storage: Storage, type_name: str, entity, ctx, t
    ) -> None:
        """creates_job → a human task (or machine job) in the same UoW."""
        spec = self._registry.job_type(type_name)
        job_id = uuid.uuid4()
        assignee = t.guard.get("assignee") or t.guard.get("assignee_role")
        assignee = (
            f"role:{t.guard['assignee_role']}" if t.guard.get("assignee_role")
            else assignee
        )
        payload = {
            "entity_type": entity.type,
            "entity_id": entity.id,
            # Linkage for HumanTaskService.complete_and_transition: the
            # task created by a transition knows which SM actions its
            # approve/reject decisions map to.
            "sm_actions": t.guard.get("sm_actions"),
        }
        child_ctx = JobContext(
            actor=Actor.job(str(job_id), via=ctx.actor),
            tenant_id=ctx.tenant_id,
            correlation_id=ctx.correlation_id,
            permissions=ctx.permissions,
            extra={"job_id": str(job_id)},
        )
        await storage.job_insert(uow, JobInsert(
            id=str(job_id), type=type_name, payload=payload,
            context=ctx_to_dict(child_ctx),
            max_attempts=spec.retry.max_attempts if spec.retry else 3,
            run_at=datetime.now(UTC),
            assignee=assignee,
        ))
        if not type_name.startswith("human.") and self._kernel is not None:
            await self._kernel.push_ex(
                uow,
                dispatch_spec(
                    job_id=job_id,
                    max_attempts=spec.retry.max_attempts if spec.retry else 3,
                    meta={"job_id": str(job_id), "type": type_name},
                ),
            )

    async def _trigger_workflow(
        self, uow, storage: Storage,
        trigger: WorkflowTriggerSpec | str, entity, ctx,
    ) -> None:
        trig = (
            trigger
            if isinstance(trigger, WorkflowTriggerSpec)
            else WorkflowTriggerSpec(workflow=trigger)
        )
        run_id = uuid.uuid4()
        await storage.run_insert(
            uow, str(run_id), trig.workflow,
            {"entity_type": entity.type, "entity_id": entity.id},
            triggered_by_entity_type=entity.type,
            triggered_by_entity_id=entity.id,
            trigger_config={
                "on_succeeded": trig.on_succeeded,
                "on_rejected": trig.on_rejected,
                "on_failed": trig.on_failed,
                "failure_job": trig.failure_job,
            },
            trigger_ctx=ctx_to_dict(ctx),
        )
        defn = self._registry.workflow(trig.workflow)
        await storage.step_runs_insert(
            uow, str(run_id),
            [(s.id, s.type) for s in defn.steps])
        from .workflow import advance_run

        await advance_run(
            uow, storage, run_id, self._registry, self._kernel, ctx)
