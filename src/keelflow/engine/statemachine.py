"""StateMachineService (T12) + workflow-trigger integration.

`transition(entity, action, ctx, tx)` enforces the declared transition
table + permission guard inside the caller's transaction, records the new
state in `keelflow_entity_state`, audits the change, and fires
`creates_job` / `triggers_workflow` side effects in the same TX.

Workflow triggers (`WorkflowTriggerSpec`) persist metadata on the spawned
run (`triggered_by_entity_*`, callback actions, initiating ctx) so the
terminal callback in `workflow._finish_run` can apply `on_done` /
`on_rejected` / `on_failed` transitions to the entity — in the run's own
terminal transaction, not here.

Concurrent-first-transition safety: `_current` inserts the entity row
`ON CONFLICT DO NOTHING` before `FOR UPDATE`, so two simultaneous first
transitions serialize on the row's write lock instead of both seeing
"no row" and proceeding.
"""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from ..sdk import (
    Actor,
    AuditEvent,
    EntityRef,
    JobContext,
    StateMachineService,
    Transaction,
    WorkflowTriggerSpec,
)
from .context import ctx_to_dict
from .enqueue import _as_cursor
from .kernel import dispatch_spec


class EngineStateMachineService(StateMachineService):
    def __init__(self, registry, kernel=None, audit=None) -> None:
        from .kernel import as_kernel

        self._registry = registry
        self._kernel = as_kernel(kernel) if kernel is not None else None
        self._audit = audit  # optional PostgresAuditService

    async def ensure_schema(self, conn) -> None:
        """Apply the core schema (idempotent). Kept as the test/setup hook."""
        from ..postgres.schema import apply_async

        await apply_async(conn)

    async def transition(
        self, entity: EntityRef, action: str, ctx: JobContext, tx: Transaction
    ) -> None:
        await self._apply(entity, action, ctx, tx, enforce_permission=True)

    async def _internal_transition(
        self, cursor, entity: EntityRef, action: str, ctx: JobContext
    ) -> None:
        """Engine-authorized transition (workflow callback, resume).

        Skips the *permission* guard — the initiating actor was already
        authorized when the trigger was created. Still validates the
        transition itself, so a bad on_done/on_failed action fails the run
        finalization loudly rather than silently corrupting state.
        """
        await self._apply(entity, action, ctx, cursor, enforce_permission=False)

    async def _apply(
        self,
        entity: EntityRef,
        action: str,
        ctx: JobContext,
        tx: Transaction,
        *,
        enforce_permission: bool,
    ) -> None:
        sm = self._registry.state_machine(f"{entity.type}@v1")
        cursor = _as_cursor(tx)

        current = await self._current(cursor, entity, sm)
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

        await cursor.execute(
            """
            INSERT INTO keelflow_entity_state (entity_type, entity_id, state, version)
            VALUES (%s, %s, %s, 1)
            ON CONFLICT (entity_type, entity_id) DO UPDATE
            SET state = EXCLUDED.state, version = keelflow_entity_state.version + 1,
                updated_at = now()
            """,
            (entity.type, entity.id, t.to_state),
        )

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
                cursor,
            )

        if t.creates_job:
            await self._spawn_job(cursor, t.creates_job, entity, ctx, t)
        if t.triggers_workflow:
            await self._trigger_workflow(cursor, t.triggers_workflow, entity, ctx)

    async def current(self, entity: EntityRef, conn=None) -> str | None:  # type: ignore[override]
        if conn is None:
            return None
        cur = await conn.execute(
            "SELECT state FROM keelflow_entity_state"
            " WHERE entity_type=%s AND entity_id=%s",
            (entity.type, entity.id),
        )
        row = await cur.fetchone()
        return (row["state"] if isinstance(row, dict) else row[0]) if row else None

    async def allowed_actions(
        self, entity: EntityRef, actor: Actor, conn,
        *, permissions: frozenset[str] | None = None,
    ) -> list[str]:  # type: ignore[override]
        try:
            sm = self._registry.state_machine(f"{entity.type}@v1")
        except KeyError:
            return []
        cur = await conn.execute(
            "SELECT state FROM keelflow_entity_state"
            " WHERE entity_type=%s AND entity_id=%s",
            (entity.type, entity.id),
        )
        row = await cur.fetchone()
        current = (row["state"] if isinstance(row, dict) else row[0]) if row else sm.initial
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

    async def _current(self, cursor, entity: EntityRef, sm) -> str:
        """Lock the entity row; create it if absent.

        `INSERT … ON CONFLICT DO NOTHING` ensures the row exists, then
        `SELECT … FOR UPDATE` serializes first transitions on a real row —
        two concurrent first actions can no longer both pass a read that
        returns "no row".
        """
        await cursor.execute(
            "INSERT INTO keelflow_entity_state (entity_type, entity_id, state)"
            " VALUES (%s, %s, %s) ON CONFLICT (entity_type, entity_id) DO NOTHING",
            (entity.type, entity.id, sm.initial),
        )
        await cursor.execute(
            "SELECT state FROM keelflow_entity_state"
            " WHERE entity_type=%s AND entity_id=%s FOR UPDATE",
            (entity.type, entity.id),
        )
        row = await cursor.fetchone()
        return row["state"] if row else sm.initial

    async def _spawn_job(self, cursor, type_name: str, entity, ctx, t) -> None:
        """creates_job → a human task (or machine job) in the same TX."""
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
        await cursor.execute(
            """
            INSERT INTO keelflow_jobs
                (id, type, status, payload, context, max_attempts, run_at,
                 assignee, created_at, updated_at)
            VALUES (%s,%s,'pending',%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                job_id, type_name, json.dumps(payload),
                json.dumps(ctx_to_dict(child_ctx)),
                spec.retry.max_attempts if spec.retry else 3,
                datetime.now(UTC), assignee,
                datetime.now(UTC), datetime.now(UTC),
            ),
        )
        if not type_name.startswith("human.") and self._kernel is not None:
            await self._kernel.push_ex(
                cursor,
                dispatch_spec(
                    job_id=job_id,
                    max_attempts=spec.retry.max_attempts if spec.retry else 3,
                    meta={"job_id": str(job_id), "type": type_name},
                ),
            )

    async def _trigger_workflow(
        self, cursor, trigger: WorkflowTriggerSpec | str, entity, ctx
    ) -> None:
        trig = (
            trigger
            if isinstance(trigger, WorkflowTriggerSpec)
            else WorkflowTriggerSpec(workflow=trigger)
        )
        run_id = uuid.uuid4()
        await cursor.execute(
            """
            INSERT INTO keelflow_workflow_runs
                (id, workflow, status, inputs,
                 triggered_by_entity_type, triggered_by_entity_id,
                 trigger_config, trigger_ctx, callback_state)
            VALUES (%s, %s, 'running', %s, %s, %s, %s, %s, 'pending')
            """,
            (
                run_id, trig.workflow,
                json.dumps({"entity_type": entity.type, "entity_id": entity.id}),
                entity.type, entity.id,
                json.dumps({
                    "on_done": trig.on_done,
                    "on_rejected": trig.on_rejected,
                    "on_failed": trig.on_failed,
                    "failure_job": trig.failure_job,
                }),
                json.dumps(ctx_to_dict(ctx)),
            ),
        )
        defn = self._registry.workflow(trig.workflow)
        for step in defn.steps:
            await cursor.execute(
                "INSERT INTO keelflow_step_runs (id, run_id, step_id, type, status)"
                " VALUES (%s, %s, %s, %s, 'pending')",
                (uuid.uuid4(), run_id, step.id, step.type),
            )
        from .workflow import advance_run

        await advance_run(cursor, run_id, self._registry, self._kernel, ctx)
