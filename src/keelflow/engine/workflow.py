"""Workflow engine (T7): DAG of steps via `needs`, `if:` guards, outputs.

State model (`keelflow_workflow_runs` / `keelflow_step_runs`):

- `start()` creates the run + a `pending` step_run per definition step, then
  `advance()` enqueues jobs for every step whose `needs` are all done and
  whose `if:` guard holds (steps that fail the guard become `skipped`).
- Each step job carries `meta.step_run_id`; when `worker.dispatch` finishes
  it calls `on_job_finished`, which records outputs and `advance()`s again
  inside the same transaction — the run walks itself.
- `if:` guards are a minimal expression language (decision #6): `${{ }}`
  references plus `== != > < >= <= in and or not` only.
- `inputs` interpolation: `${{ inputs.X }}` / `${{ steps.Y.outputs.Z }}` in
  step `input` values resolve at enqueue time.
- `resume_step()` re-enqueues a failed step's job (same step_run, attempts+1).
- A run is `done` when no step_run is pending/running/blocked; `failed`
  when a step failed and no retry has been requested.
"""
from __future__ import annotations

import ast
import json
import logging
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from ..sdk import (
    Actor,
    EntityRef,
    JobContext,
    JobStatus,
    Registry,
    RunRef,
    StepSpec,
    Transaction,
)
from .context import ctx_to_dict
from .enqueue import DEFAULT_QUEUE, DISPATCH_FUNC, _as_cursor

log = logging.getLogger("keelflow.workflow")

_REF = re.compile(r"^\s*\$\{\{\s*(.+?)\s*\}\}\s*$")
_REF_INLINE = re.compile(r"\$\{\{\s*(.+?)\s*\}\}")


# ── public service ────────────────────────────────────────────────────────


class WorkflowService:
    """SDK-facing workflow service."""

    def __init__(self, registry: Registry, job_service) -> None:
        self._registry = registry
        self._jobs = job_service  # engine.enqueue.JobService (for kernel client)

    async def start(
        self, name: str, inputs: dict, ctx: JobContext, tx: Transaction
    ) -> RunRef:
        defn = self._registry.workflow(name)  # KeyError → unknown
        cursor = _as_cursor(tx)
        run_id = uuid.uuid4()
        await cursor.execute(
            "INSERT INTO keelflow_workflow_runs (id, workflow, status, inputs)"
            " VALUES (%s, %s, %s, %s)",
            (run_id, name, JobStatus.RUNNING.value, json.dumps(inputs)),
        )
        for step in defn.steps:
            await cursor.execute(
                "INSERT INTO keelflow_step_runs (id, run_id, step_id, type, status)"
                " VALUES (%s, %s, %s, %s, %s)",
                (
                    uuid.uuid4(),
                    run_id,
                    step.id,
                    step.type,
                    JobStatus.PENDING.value,
                ),
            )
        await advance_run(cursor, run_id, self._registry, self._jobs.kernel, ctx)
        return RunRef(id=str(run_id))

    async def get_run(self, conn, run_id: str) -> dict[str, Any] | None:
        from psycopg.rows import dict_row

        # Normalize to dict_row regardless of caller's connection factory.
        cur = (conn.connection.cursor(row_factory=dict_row)
               if hasattr(conn, "connection")
               else conn.cursor(row_factory=dict_row))
        row = await (await cur.execute(
            "SELECT id, workflow, status, inputs, terminal_outcome"
            " FROM keelflow_workflow_runs WHERE id=%s",
            (run_id,),
        )).fetchone()
        if row is None:
            return None
        rows = await (await cur.execute(
            "SELECT step_id, type, status, job_id, outputs, attempts"
            " FROM keelflow_step_runs WHERE run_id=%s ORDER BY created_at",
            (run_id,),
        )).fetchall()
        defn = self._registry.workflow(row["workflow"])
        needs_by_id = {s.id: list(s.needs) for s in defn.steps}
        steps = [
            {
                "step_id": r["step_id"], "type": r["type"],
                "status": r["status"],
                "job_id": str(r["job_id"]) if r["job_id"] else None,
                "outputs": r["outputs"], "attempts": r["attempts"],
                "needs": needs_by_id.get(r["step_id"], []),
            }
            for r in rows
        ]
        return {
            "id": str(row["id"]),
            "workflow": row["workflow"],
            "status": row["status"],
            "terminal_outcome": row["terminal_outcome"],
            "inputs": row["inputs"],
            "steps": steps,
        }

    async def resume_step(
        self, conn, run_id: str, step_id: str, ctx: JobContext
    ) -> None:
        """Re-enqueue a failed step's job (same step_run, attempts + 1).

        Caller owns the transaction (writes through conn's open TX).
        """
        cur = await conn.execute(
            "SELECT id, type, status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id=%s",
            (run_id, step_id),
        )
        row = await cur.fetchone()
        if row is None:
            raise KeyError(f"no step_run {step_id} in run {run_id}")
        step_run_id, _type, status = row
        if status not in (JobStatus.FAILED.value, JobStatus.DONE.value):
            raise ValueError(f"step {step_id} is {status}, not resumable")

        from psycopg.rows import dict_row

        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                "UPDATE keelflow_step_runs SET status=%s, job_id=NULL,"
                " attempts=attempts+1, updated_at=now() WHERE id=%s",
                (JobStatus.PENDING.value, step_run_id),
            )
            await cursor.execute(
                "UPDATE keelflow_workflow_runs SET status=%s,"
                " terminal_outcome=NULL, callback_state='pending',"
                " updated_at=now() WHERE id=%s",
                (JobStatus.RUNNING.value, run_id),
            )
            await advance_run(cursor, uuid.UUID(str(run_id)), self._registry,
                              self._jobs.kernel, ctx)

    async def resume_run(
        self, conn, run_id: str, ctx: JobContext
    ) -> None:
        """Resume a failed or rejected run — continue from the broken step.

        Same run ID; successful siblings stay done. Idempotent: calling on
        a non-terminal run is a no-op; calling twice on a terminal run
        fails the second time (run already 'running').
        """
        from psycopg.rows import dict_row

        async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT status, terminal_outcome, triggered_by_entity_type,"
                " triggered_by_entity_id FROM keelflow_workflow_runs"
                " WHERE id=%s FOR UPDATE",
                (run_id,),
            )
            run = await cur.fetchone()
            if run is None:
                raise KeyError(f"run {run_id} not found")
            outcome = run["terminal_outcome"]
            if outcome not in ("rejected", "failed"):
                raise ValueError(
                    f"run {run_id} outcome is {outcome or run['status']!r},"
                    " not resumable"
                )
            # resume the offending step: rejected human step or failed step
            await cur.execute(
                "SELECT id, step_id FROM keelflow_step_runs"
                " WHERE run_id=%s AND (status='failed' OR"
                " (status='done' AND outputs->>'decision'='reject'))",
                (run_id,),
            )
            targets = await cur.fetchall()
            if not targets:
                raise ValueError(f"run {run_id}: no resumable step")
            for t in targets:
                await cur.execute(
                    "UPDATE keelflow_step_runs SET status='pending', job_id=NULL,"
                    " attempts=attempts+1, updated_at=now() WHERE id=%s",
                    (t["id"],),
                )
            await cur.execute(
                "UPDATE keelflow_workflow_runs SET status='running',"
                " terminal_outcome=NULL, callback_state='pending',"
                " updated_at=now() WHERE id=%s",
                (run_id,),
            )
            await advance_run(cur, uuid.UUID(str(run_id)), self._registry,
                              self._jobs.kernel, ctx)
            # entity back to posting/in-progress state via SM (internal)
            et = run["triggered_by_entity_type"]
            ei = run["triggered_by_entity_id"]
            if et and ei:
                sm_svc = self._sm_service()
                cur2 = await cur.execute(
                    "SELECT state FROM keelflow_entity_state"
                    " WHERE entity_type=%s AND entity_id=%s", (et, ei))
                es = await cur2.fetchone()
                back_action = self._resume_action(
                    et, es["state"] if es else None)
                if back_action:
                    await sm_svc._internal_transition(
                        cur, EntityRef(type=et, id=ei), back_action, ctx)


    def _sm_service(self):
        from .statemachine import EngineStateMachineService

        svc = getattr(self, "_sm", None)
        if svc is None:
            svc = EngineStateMachineService(
            self._registry, self._jobs.kernel, audit=_postgres_audit())
            self._sm = svc
        return svc

    def _resume_action(self, entity_type: str, current: str | None) -> str | None:
        """Action that flips the entity from its current failure state back.

        Matches a transition from the entity's *actual* state (e.g.
        posting_failed→posting via retry_post), not just any failure exit.
        """
        if current is None:
            return None
        try:
            sm = self._registry.state_machine(f"{entity_type}@v1")
        except KeyError:
            return None
        for t in sm.transitions:
            if t.from_state == current:
                return t.action
        return None


# ── run advancement ───────────────────────────────────────────────────────


async def advance_run(
    cursor,
    run_id,
    registry: Registry,
    kernel,
    parent_ctx: JobContext,
) -> None:
    """Enqueue jobs for steps whose needs are satisfied; close the run out.

    Must be called inside a transaction on `cursor`. Idempotent — a step_run
    with a job_id already attached is never re-enqueued.
    """
    # Lock the run row first: parallel step completions each call this in
    # their own TX — without the lock the second advance reads a stale
    # snapshot (sees the sibling still 'running'), enqueues nothing, and
    # nobody ever re-advances the run. Serializes single-writer per run.
    run_row = await _fetch_run(cursor, run_id, for_update=True)
    if run_row is None or run_row["status"] != JobStatus.RUNNING.value:
        return
    defn = registry.workflow(run_row["workflow"])
    step_runs = await _fetch_steps(cursor, run_id)

    # terminal state check
    active = [s for s in step_runs
              if s["status"] in ("pending", "claimed", "running", "blocked")]
    failed = [s for s in step_runs if s["status"] == "failed"]
    if failed:
        await _finish_run(cursor, run_id, registry, "failed", step_runs)
        return
    if not active:
        outcome = _terminal_outcome(step_runs)
        await _finish_run(cursor, run_id, registry, outcome, step_runs)
        return
    # human steps park the run without a machine job to wait on — still
    # 'running' while any human step is pending.
    by_id = {s["step_id"]: s for s in step_runs}
    done_ids = {s["step_id"] for s in step_runs if s["status"] in ("done", "skipped")}
    skipped_ids: set[str] = set()

    for spec_step in defn.steps:
        sr = by_id.get(spec_step.id)
        if sr is None or sr["status"] != JobStatus.PENDING.value or sr["job_id"]:
            continue
        if not set(spec_step.needs).issubset(done_ids):
            continue
        # if: guard — false means skip the step entirely.
        if spec_step.if_expr is not None and not eval_guard(
            spec_step.if_expr, run_row["inputs"], step_runs, registry
        ):
            await _set_step_status(cursor, sr["id"], "skipped")
            done_ids.add(spec_step.id)
            skipped_ids.add(sr["id"])
            continue
        spec = registry.job_type(spec_step.type)
        if spec_step.type.startswith("human."):
            await _enqueue_human_step(cursor, sr, spec_step, spec, run_row,
                                      step_runs, parent_ctx, kernel)
        else:
            await _enqueue_step(cursor, sr, spec_step, spec, run_row, step_runs,
                                parent_ctx, kernel)

    # Re-check: a pass that only marked guard-false steps 'skipped' may
    # leave every step terminal while nothing was enqueued — without this,
    # such a run would stay 'running' forever.
    if not any(
        s["status"] in ("pending", "claimed", "running", "blocked")
        for s in step_runs
        if s["id"] not in skipped_ids
    ):
        await _finish_run(cursor, run_id, registry,
                          _terminal_outcome(step_runs), step_runs)


async def on_job_finished(
    conn,
    kernel,
    registry: Registry,
    job_id: str,
    step_run_id: str | None,
    status: JobStatus,
    outputs: dict[str, Any],
) -> None:
    """Hook from worker.dispatch / human.complete: record step outcome and
    advance the run.

    Caller owns the transaction — this function only writes through the
    connection's current TX. Commit is the caller's responsibility
    (worker finish TX or human.complete TX), so step bookkeeping commits
    atomically with the job status.
    """
    if not step_run_id:
        return
    from psycopg.rows import dict_row

    async with conn.cursor(row_factory=dict_row) as cursor:
        sr_status = (
            "done" if status == JobStatus.DONE else
            "failed" if status == JobStatus.FAILED else
            JobStatus.CANCELLED.value
        )
        await cursor.execute(
            "UPDATE keelflow_step_runs SET status=%s, outputs=%s, updated_at=now()"
            " WHERE id=%s",
            (sr_status, json.dumps(outputs), step_run_id),
        )
        row = await (await cursor.execute(
            "SELECT run_id FROM keelflow_step_runs WHERE id=%s", (step_run_id,)
        )).fetchone()
        if row:
            parent_ctx = await _run_ctx(cursor, row["run_id"])
            await advance_run(cursor, row["run_id"], registry, kernel, parent_ctx)


def _postgres_audit():
    from .audit import PostgresAuditService

    return PostgresAuditService()


# ── terminal outcome + trigger callback ───────────────────────────────────


def _terminal_outcome(step_runs: list[dict]) -> str:
    """Run finished: succeeded, or business-rejected if a human step
    completed with decision='reject'.
    """
    for s in step_runs:
        if (s["status"] == "done"
                and isinstance(s.get("outputs"), dict)
                and s["outputs"].get("decision") == "reject"):
            return "rejected"
    return "succeeded"


async def _finish_run(
    cursor, run_id, registry: Registry, outcome: str, step_runs
) -> None:
    """Terminalize the run and fire the workflow trigger callback.

    Inside the caller's TX: set status + terminal_outcome, then apply the
    configured SM transition on the entity that triggered the run.
    Callback failure propagates → the terminal update rolls back → kernel
    or caller retry replays it (idempotent because callback_state stays
    'pending').
    """
    status = (JobStatus.DONE.value if outcome in ("succeeded", "rejected")
              else JobStatus.FAILED.value)
    await cursor.execute(
        "UPDATE keelflow_workflow_runs SET status=%s, terminal_outcome=%s,"
        " updated_at=now() WHERE id=%s",
        (status, outcome, run_id),
    )
    await _run_trigger_callback(cursor, run_id, registry, outcome)


async def _run_trigger_callback(cursor, run_id, registry, outcome: str) -> None:
    """Apply the trigger's SM callback transition for the outcome.

    Exactly-once: guarded by callback_state 'pending' → 'done' in the
    same TX; a rolled-back TX leaves it pending → retried on next advance.
    """
    await cursor.execute(
        "SELECT triggered_by_entity_type, triggered_by_entity_id,"
        " trigger_config, trigger_ctx, callback_state"
        " FROM keelflow_workflow_runs WHERE id=%s FOR UPDATE",
        (run_id,),
    )
    run = await cursor.fetchone()
    if not run or run["callback_state"] != "pending":
        return
    config = run["trigger_config"] or {}
    action = {
        "succeeded": config.get("on_done"),
        "rejected": config.get("on_rejected"),
        "failed": config.get("on_failed"),
    }.get(outcome)

    if action and run["triggered_by_entity_type"]:
        from ..sdk import EntityRef
        from .context import ctx_from_dict
        from .statemachine import EngineStateMachineService

        ctx_d = run["trigger_ctx"] or {}
        ctx = ctx_from_dict(ctx_d) if ctx_d else JobContext(
            actor=Actor.system("keelflow.workflow"))
        entity = EntityRef(
            type=run["triggered_by_entity_type"],
            id=run["triggered_by_entity_id"],
        )
        sm_svc = EngineStateMachineService(registry,
                                           audit=_postgres_audit())
        await sm_svc._internal_transition(cursor, entity, action, ctx)
        # optional failure-side human task
        if outcome == "failed" and config.get("failure_job"):
            await _spawn_failure_task(cursor, registry, config["failure_job"],
                                      entity, ctx)
    await cursor.execute(
        "UPDATE keelflow_workflow_runs SET callback_state='done', updated_at=now()"
        " WHERE id=%s",
        (run_id,),
    )


async def _spawn_failure_task(cursor, registry, type_name, entity, ctx) -> None:
    """Create the configured human fix-up task on run failure."""
    spec = registry.job_type(type_name)
    job_id = uuid.uuid4()
    child_ctx = JobContext(
        actor=Actor.job(str(job_id), via=ctx.actor),
        tenant_id=ctx.tenant_id, correlation_id=ctx.correlation_id,
        permissions=ctx.permissions,
        extra={"job_id": str(job_id)},
    )
    await cursor.execute(
        "INSERT INTO keelflow_jobs (id, type, status, payload, context,"
        " max_attempts, run_at, assignee, created_at, updated_at)"
        " VALUES (%s,%s,'pending',%s,%s,%s,%s,%s,%s,%s)",
        (
            job_id, type_name,
            json.dumps({"entity_type": entity.type, "entity_id": entity.id}),
            json.dumps(ctx_to_dict(child_ctx)),
            spec.retry.max_attempts if spec.retry else 3,
            datetime.now(UTC), spec.ui and f"role:{spec.ui.assignee_role}",
            datetime.now(UTC), datetime.now(UTC),
        ),
    )


# ── step enqueue helpers ──────────────────────────────────────────────────


async def _enqueue_step(
    cursor, sr, spec_step: StepSpec, spec, run_row, step_runs, parent_ctx, kernel
) -> None:
    inputs = resolve_inputs(spec_step.input, run_row["inputs"], step_runs)
    job_id = uuid.uuid4()
    child_ctx = _child_ctx(parent_ctx, job_id)
    now = datetime.now(UTC)

    await cursor.execute(
        """
        INSERT INTO keelflow_jobs
            (id, type, status, payload, context, attempts, max_attempts,
             run_at, claimed_by, lease_until, concurrency_key, assignee,
             parent_job_id, result, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,0,%s,%s,NULL,NULL,NULL,NULL,NULL,NULL,%s,%s)
        """,
        (
            job_id, spec_step.type, JobStatus.PENDING.value,
            json.dumps(inputs), json.dumps(ctx_to_dict(child_ctx)),
            spec.retry.max_attempts if spec.retry else 3, now, now, now,
        ),
    )
    await kernel.push_ex(
        cursor,
        _kernel_job(job_id, spec_step.type, spec, step_run_id=sr["id"]),
    )
    await cursor.execute(
        "UPDATE keelflow_step_runs SET job_id=%s, status=%s, updated_at=now()"
        " WHERE id=%s",
        (job_id, "claimed", sr["id"]),
    )


async def _enqueue_human_step(
    cursor, sr, spec_step, spec, run_row, step_runs, parent_ctx, kernel
) -> None:
    """Human step: durable job row, no kernel claim (people claim via Inbox)."""
    inputs = resolve_inputs(spec_step.input, run_row["inputs"], step_runs)
    job_id = uuid.uuid4()
    child_ctx = _child_ctx(parent_ctx, job_id)
    now = datetime.now(UTC)

    await cursor.execute(
        """
        INSERT INTO keelflow_jobs
            (id, type, status, payload, context, attempts, max_attempts,
             run_at, claimed_by, lease_until, concurrency_key, assignee,
             parent_job_id, result, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,0,%s,%s,NULL,NULL,NULL,%s,NULL,NULL,%s,%s)
        """,
        (
            job_id, spec_step.type, JobStatus.PENDING.value,
            json.dumps(inputs), json.dumps(ctx_to_dict(child_ctx)),
            spec.retry.max_attempts if spec.retry else 3,
            now, _spec_assignee(spec), now, now,
        ),
    )
    await cursor.execute(
        "UPDATE keelflow_step_runs SET job_id=%s, status=%s, updated_at=now()"
        " WHERE id=%s",
        (job_id, JobStatus.BLOCKED.value, sr["id"]),
    )


def _kernel_job(job_id, type_name: str, spec, *, step_run_id) -> object:
    import chancy

    return chancy.Job(
        func=DISPATCH_FUNC,
        queue=DEFAULT_QUEUE,
        kwargs={"job_id": str(job_id), "step_run_id": str(step_run_id)},
        max_attempts=spec.retry.max_attempts if spec.retry else 3,
        meta={"job_id": str(job_id), "type": type_name,
              "step_run_id": str(step_run_id)},
    )


def _spec_assignee(spec) -> str | None:
    ui = getattr(spec, "ui", None)
    if ui and getattr(ui, "assignee_role", None):
        return f"role:{ui.assignee_role}"
    return None


# ── evaluation helpers ────────────────────────────────────────────────────


def resolve_inputs(
    input_map: dict[str, Any], run_inputs: dict, step_runs: list[dict]
) -> dict[str, Any]:
    outputs_by_step = {
        s["step_id"]: s["outputs"] or {} for s in step_runs
    }
    scope = {"inputs": run_inputs, "steps": outputs_by_step}
    return {k: _resolve(v, scope) for k, v in input_map.items()}


def _resolve(value: Any, scope: dict) -> Any:
    if isinstance(value, str):
        m = _REF.match(value)
        if m:
            return _lookup(m.group(1), scope)
        return value
    if isinstance(value, dict):
        return {k: _resolve(v, scope) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_resolve(v, scope) for v in value]
    return value


def _lookup(expr: str, scope: dict) -> Any:
    """Resolve `a.b.c` inside scope (dicts only — no attribute access).

    `steps.X.outputs.Z` is treated as `scope['steps']['X']['Z']` — `outputs`
    is the implicit container for step outputs.
    """
    parts = expr.strip().split(".")
    parts = [p for p in parts if p != "outputs"]
    node: Any = scope.get(parts[0])
    for p in parts[1:]:
        if isinstance(node, dict):
            node = node.get(p)
        else:
            return None
    return node


# minimal if-guard: `${{ steps.X.outputs.Y }}` refs, comparisons, in,
# and/or/not. No calls, no names outside refs/literals.
_ALLOWED_NODES = (
    ast.Expression, ast.BoolOp, ast.BinOp, ast.UnaryOp, ast.Compare,
    ast.Name, ast.Load, ast.Constant, ast.And, ast.Or, ast.Not,
    ast.Eq, ast.NotEq, ast.Gt, ast.Lt, ast.GtE, ast.LtE, ast.In, ast.NotIn,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.USub, ast.UAdd,
    ast.Attribute, ast.Subscript, ast.List, ast.Tuple, ast.Dict,
)
_ALLOWED_NAMES = {"inputs", "steps", "True", "False", "None"}


def validate_guard(expr: str) -> None:
    """Syntax + whitelist check for an `if:` guard (used at load time).

    Raises TypeError on invalid/disallowed expressions. Evaluation is a
    separate step (`eval_guard`) since refs resolve at run time.
    """
    # substitute refs with a placeholder literal — only shape is validated.
    substituted = _REF_INLINE.sub("1", expr)
    try:
        tree = ast.parse(substituted, mode="eval")
    except SyntaxError as exc:
        raise TypeError(f"guard '{expr}' is not a valid expression") from exc
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise TypeError(f"guard '{expr}': disallowed syntax {type(node).__name__}")
        if isinstance(node, ast.Name) and node.id not in _ALLOWED_NAMES:
            raise TypeError(f"guard '{expr}': disallowed name {node.id!r}")
        if isinstance(node, ast.Attribute | ast.Call):
            raise TypeError(f"guard '{expr}': attribute access/calls not allowed")


def eval_guard(expr: str, run_inputs: dict, step_runs: list[dict], _registry=None) -> bool:
    """Evaluate an `if:` guard. `${{ a.b }}` refs substitute before parse."""
    validate_guard(expr)
    substituted = _REF_INLINE.sub(
        lambda m: repr(_lookup(m.group(1), {
            "inputs": run_inputs,
            "steps": {s["step_id"]: s["outputs"] or {} for s in step_runs},
        })),
        expr,
    )
    tree = ast.parse(substituted, mode="eval")
    scope = {
        "inputs": run_inputs,
        "steps": {s["step_id"]: s["outputs"] or {} for s in step_runs},
    }
    return bool(eval(compile(tree, "<guard>", "eval"), {"__builtins__": {}}, scope))


# ── db helpers ────────────────────────────────────────────────────────────


async def _fetch_run(cursor, run_id, *, for_update: bool = False) -> dict[str, Any] | None:
    from psycopg.rows import dict_row

    cur = cursor.connection.cursor(row_factory=dict_row) if hasattr(cursor, "connection") else cursor
    await cur.execute(
        "SELECT id, workflow, status, inputs FROM keelflow_workflow_runs"
        f" WHERE id=%s{' FOR UPDATE' if for_update else ''}",
        (run_id,),
    )
    return await cur.fetchone()


async def _fetch_steps(cursor, run_id) -> list[dict[str, Any]]:
    from psycopg.rows import dict_row

    cur = cursor.connection.cursor(row_factory=dict_row) if hasattr(cursor, "connection") else cursor
    await cur.execute(
        "SELECT id, step_id, type, status, job_id, outputs, attempts"
        " FROM keelflow_step_runs WHERE run_id=%s",
        (run_id,),
    )
    return await cur.fetchall()


async def _set_run_status(cursor, run_id, status: JobStatus) -> None:
    await cursor.execute(
        "UPDATE keelflow_workflow_runs SET status=%s, updated_at=now() WHERE id=%s",
        (status.value, run_id),
    )


async def _set_step_status(cursor, step_run_id, status: str) -> None:
    await cursor.execute(
        "UPDATE keelflow_step_runs SET status=%s, updated_at=now() WHERE id=%s",
        (status, step_run_id),
    )


async def _run_ctx(cursor, run_id) -> JobContext:
    """Rebuild the parent ctx for a run's next-step jobs from its first job."""
    cur = await cursor.execute(
        "SELECT context FROM keelflow_jobs j"
        " JOIN keelflow_step_runs sr ON sr.job_id = j.id"
        " WHERE sr.run_id=%s ORDER BY j.created_at LIMIT 1",
        (run_id,),
    )
    row = await cur.fetchone()
    if row:
        from .context import ctx_from_dict

        ctx_d = row["context"] if "context" in row else row[0]
        if isinstance(ctx_d, str):
            ctx_d = json.loads(ctx_d)
        return ctx_from_dict(ctx_d)
    return JobContext(actor=Actor.system())


def _child_ctx(parent: JobContext, job_id) -> JobContext:
    """Step jobs run as actor=job:<job_id> via the triggering actor."""
    return JobContext(
        actor=Actor.job(str(job_id), via=parent.actor),
        tenant_id=parent.tenant_id,
        correlation_id=parent.correlation_id,
        permissions=parent.permissions,
        request_id=parent.request_id,
        extra=dict(parent.extra),
    )
