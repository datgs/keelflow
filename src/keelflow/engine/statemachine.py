"""StateMachineService (T12).

`transition(entity, action, ctx, tx)` enforces the declared transition
table + permission guard inside the caller's transaction, records the new
state in `keelflow_entity_state` (created lazily — entity statuses live
there for the framework; domain entities keep their own column via
`sync` later), and fires `creates_job` / `triggers_workflow` side effects
in the same TX.
"""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from ..sdk import Actor, EntityRef, JobContext, StateMachineService, Transaction
from .context import ctx_to_dict
from .enqueue import DEFAULT_QUEUE, DISPATCH_FUNC, _as_cursor

ENSURE_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS keelflow_entity_state (
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    state       TEXT NOT NULL,
    version     INT  NOT NULL DEFAULT 1,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_id)
)
"""  # also present in postgres/schema.sql; kept for self-bootstrap


class EngineStateMachineService(StateMachineService):
    def __init__(self, registry, kernel=None) -> None:
        self._registry = registry
        self._kernel = kernel

    async def ensure_schema(self, conn) -> None:
        await conn.execute(ENSURE_STATE_TABLE)
        await conn.commit()

    async def transition(
        self, entity: EntityRef, action: str, ctx: JobContext, tx: Transaction
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
        if perm:
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

    async def allowed_actions(self, entity: EntityRef, actor: Actor, conn) -> list[str]:  # type: ignore[override]
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
        ctx = JobContext(actor=actor)
        out = []
        for t in sm.transitions:
            if t.from_state != current:
                continue
            perm = (t.guard or {}).get("permission")
            if perm and perm not in ctx.permissions and "*" not in ctx.permissions:
                continue
            out.append(t.action)
        return out

    # ── helpers ────────────────────────────────────────────────────────

    async def _current(self, cursor, entity: EntityRef, sm) -> str:
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
        payload = {"entity_type": entity.type, "entity_id": entity.id}
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
            import chancy

            await self._kernel.push_ex(
                cursor,
                chancy.Job(
                    func=DISPATCH_FUNC, queue=DEFAULT_QUEUE,
                    kwargs={"job_id": str(job_id)},
                    max_attempts=spec.retry.max_attempts if spec.retry else 3,
                    meta={"job_id": str(job_id), "type": type_name},
                ),
            )

    async def _trigger_workflow(self, cursor, name: str, entity, ctx) -> None:
        run_id = uuid.uuid4()
        await cursor.execute(
            "INSERT INTO keelflow_workflow_runs (id, workflow, status, inputs)"
            " VALUES (%s, %s, 'running', %s)",
            (
                run_id, name,
                json.dumps({"entity_type": entity.type, "entity_id": entity.id}),
            ),
        )
        defn = self._registry.workflow(name)
        for step in defn.steps:
            await cursor.execute(
                "INSERT INTO keelflow_step_runs (id, run_id, step_id, type, status)"
                " VALUES (%s, %s, %s, %s, 'pending')",
                (uuid.uuid4(), run_id, step.id, step.type),
            )
        from .workflow import advance_run

        await advance_run(cursor, run_id, self._registry, self._kernel, ctx)
