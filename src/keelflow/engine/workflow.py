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
import logging
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from ..sdk import (
    Actor,
    EntityRef,
    JobContext,
    JobInsert,
    JobStatus,
    Registry,
    RunRef,
    StepSpec,
    Storage,
)
from ..sdk.uow import nested_scope
from .context import ctx_from_dict, ctx_to_dict

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
        self, name: str, inputs: dict, ctx: JobContext, uow
    ) -> RunRef:
        defn = self._registry.workflow(name)  # KeyError → unknown
        storage = self._jobs.storage
        run_id = uuid.uuid4()
        await storage.run_insert(
            uow, str(run_id), name, inputs)
        await storage.step_runs_insert(
            uow, str(run_id),
            [(s.id, s.type) for s in defn.steps])
        await advance_run(uow, storage, run_id, self._registry,
                          self._jobs.kernel, ctx)
        return RunRef(id=str(run_id))

    async def get_run(self, uow, run_id: str) -> dict[str, Any] | None:
        storage = self._jobs.storage
        row = await storage.run_get(uow, run_id)
        if row is None:
            return None
        rows = await storage.steps_get(uow, run_id)
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
        self, uow, run_id: str, step_id: str, ctx: JobContext
    ) -> None:
        """Re-enqueue a failed step's job (same step_run, attempts + 1).

        Writes participate in the caller's unit of work — the caller
        commits.
        """
        storage = self._jobs.storage
        row = await storage.step_run_for_update(uow, run_id, step_id)
        if row is None:
            raise KeyError(f"no step_run {step_id} in run {run_id}")
        step_run_id = row["id"]
        if row["status"] not in (JobStatus.FAILED.value, JobStatus.DONE.value):
            raise ValueError(
                f"step {step_id} is {row['status']}, not resumable")

        await storage.step_reset(uow, str(step_run_id))
        await storage.run_reopen(uow, run_id)
        await advance_run(uow, storage, uuid.UUID(str(run_id)),
                          self._registry, self._jobs.kernel, ctx)

    async def resume_run(
        self, uow, run_id: str, ctx: JobContext
    ) -> None:
        """Resume a failed or rejected run — continue from the broken step.

        Same run ID; successful siblings stay done. Idempotent: calling on
        a non-terminal run is a no-op; calling twice on a terminal run
        fails the second time (run already 'running'). Writes participate
        in the caller's unit of work — the caller commits.
        """
        storage = self._jobs.storage
        async with nested_scope(uow) as nuow:
            run = await storage.run_get(nuow, run_id, for_update=True)
            if run is None:
                raise KeyError(f"run {run_id} not found")
            outcome = run["terminal_outcome"]
            if outcome not in ("rejected", "failed"):
                raise ValueError(
                    f"run {run_id} outcome is {outcome or run['status']!r},"
                    " not resumable"
                )
            # resume the offending step: rejected human step or failed step
            targets = await storage.steps_resumable(nuow, run_id)
            if not targets:
                raise ValueError(f"run {run_id}: no resumable step")
            for t in targets:
                await storage.step_reset(nuow, str(t["id"]))
            await storage.run_reopen(nuow, run_id)
            await advance_run(nuow, storage, uuid.UUID(str(run_id)),
                              self._registry, self._jobs.kernel, ctx)
            # entity back to posting/in-progress state via SM (internal)
            et = run["triggered_by_entity_type"]
            ei = run["triggered_by_entity_id"]
            if et and ei:
                sm_svc = self._sm_service()
                es = await storage.entity_state(nuow, et, ei)
                back_action = self._resume_action(et, es)
                if back_action:
                    await sm_svc._internal_transition(
                        nuow, storage, EntityRef(type=et, id=ei),
                        back_action, ctx)


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
    uow,
    storage: Storage,
    run_id,
    registry: Registry,
    kernel,
    parent_ctx: JobContext,
) -> None:
    """Enqueue jobs for steps whose needs are satisfied; close the run out.

    Must be called inside an open unit of work. Idempotent — a step_run
    with a job_id already attached is never re-enqueued.
    """
    # Lock the run row first: parallel step completions each call this in
    # their own TX — without the lock the second advance reads a stale
    # snapshot (sees the sibling still 'running'), enqueues nothing, and
    # nobody ever re-advances the run. Serializes single-writer per run.
    run_row = await storage.run_get(uow, run_id, for_update=True)
    if run_row is None or run_row["status"] != JobStatus.RUNNING.value:
        return
    defn = registry.workflow(run_row["workflow"])
    step_runs = await storage.steps_get(uow, run_id)

    # Definition drift: the workflow def was edited without a version
    # bump while this run was in flight (Temporal's classic versioning
    # trap). Fail loudly instead of leaving the run stuck 'running'
    # forever (removed step) or silently short-circuiting (added step).
    defn_ids = {s.id for s in defn.steps}
    if {s["step_id"] for s in step_runs} != defn_ids:
        await _finish_run(uow, storage, run_id, registry, "failed", step_runs)
        return

    # terminal state check — 'cancelled' steps can't satisfy their
    # dependents' needs either, so they terminalize the run like a
    # failure (outcome 'cancelled', not 'succeeded').
    active = [s for s in step_runs
              if s["status"] in ("pending", "claimed", "running", "blocked")]
    failed = [s for s in step_runs if s["status"] == "failed"]
    if failed:
        await _finish_run(uow, storage, run_id, registry, "failed", step_runs)
        return
    if any(s["status"] == "cancelled" for s in step_runs):
        await _finish_run(uow, storage, run_id, registry, "cancelled", step_runs)
        return
    if not active:
        outcome = _terminal_outcome(step_runs)
        await _finish_run(uow, storage, run_id, registry, outcome, step_runs)
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
        # A poisoned step (guard eval or enqueue raising inside a finish
        # TX) would otherwise roll back the whole advance and re-deliver
        # forever — classic stuck 'running' run. Give each step its own
        # savepoint: a deterministic failure marks the step failed, which
        # fails the run but stays resumable via resume_step.
        try:
            async with nested_scope(uow) as nuow:
                # if: guard — false means skip the step entirely.
                if spec_step.if_expr is not None and not eval_guard(
                    spec_step.if_expr, run_row["inputs"], step_runs, registry
                ):
                    await storage.step_set_status(nuow, str(sr["id"]), "skipped")
                    done_ids.add(spec_step.id)
                    skipped_ids.add(sr["id"])
                    continue
                spec = registry.job_type(spec_step.type)
                if spec.is_external_wait:
                    await _enqueue_wait_step(
                        nuow, storage, sr, spec_step, spec, run_row, step_runs,
                        parent_ctx)
                elif spec_step.type.startswith("human."):
                    await _enqueue_human_step(
                        nuow, storage, sr, spec_step, spec, run_row, step_runs,
                        parent_ctx, kernel)
                else:
                    await _enqueue_step(
                        nuow, storage, sr, spec_step, spec, run_row, step_runs,
                        parent_ctx, kernel)
        except Exception as exc:
            log.exception(
                "advance %s: step %s failed to start", run_id, spec_step.id)
            await storage.step_fail(uow, str(sr["id"]), str(exc))
            sr["status"] = "failed"

    # Re-check: a pass that only marked guard-false steps 'skipped' may
    # leave every step terminal while nothing was enqueued — without this,
    # such a run would stay 'running' forever.
    if any(s["status"] == "failed" for s in step_runs):
        await _finish_run(uow, storage, run_id, registry, "failed", step_runs)
    elif not any(
        s["status"] in ("pending", "claimed", "running", "blocked")
        for s in step_runs
        if s["id"] not in skipped_ids
    ):
        await _finish_run(uow, storage, run_id, registry,
                          _terminal_outcome(step_runs), step_runs)


async def on_job_finished(
    uow,
    storage: Storage,
    kernel,
    registry: Registry,
    job_id: str,
    step_run_id: str | None,
    status: JobStatus,
    outputs: dict[str, Any],
) -> None:
    """Hook from worker.dispatch / human.complete: record step outcome and
    advance the run.

    Caller owns the unit of work — this function only writes through it.
    Commit is the caller's responsibility (worker finish UoW or
    human.complete UoW), so step bookkeeping commits atomically with the
    job status.
    """
    if not step_run_id:
        return
    sr_status = (
        "done" if status == JobStatus.DONE else
        "failed" if status == JobStatus.FAILED else
        JobStatus.CANCELLED.value
    )
    await storage.step_settle(uow, str(step_run_id), sr_status, outputs)
    sr = await storage.step_run_get(uow, str(step_run_id))
    if sr:
        ctx_d = await storage.run_parent_ctx(uow, sr["run_id"])
        parent_ctx = ctx_from_dict(ctx_d) if ctx_d else JobContext(
            actor=Actor.system())
        await advance_run(uow, storage, sr["run_id"], registry, kernel,
                          parent_ctx)


def _postgres_audit():
    from .audit import PostgresAuditService

    return PostgresAuditService()


# ── terminal outcome + trigger callback ───────────────────────────────────


def _terminal_outcome(step_runs: list[dict]) -> str:
    """Run finished: cancelled when a step was cancelled (e.g. scheduler
    kill_previous), business-rejected when a human step completed with
    decision='reject', otherwise succeeded.
    """
    for s in step_runs:
        if s["status"] == "cancelled":
            return "cancelled"
    for s in step_runs:
        if (s["status"] == "done"
                and isinstance(s.get("outputs"), dict)
                and s["outputs"].get("decision") == "reject"):
            return "rejected"
    return "succeeded"


async def _finish_run(
    uow, storage: Storage, run_id, registry: Registry, outcome: str,
    step_runs,
) -> None:
    """Terminalize the run and fire the workflow trigger callback.

    Inside the caller's UoW: set status + terminal_outcome, then apply the
    configured SM transition on the entity that triggered the run.
    Callback failure propagates → the terminal update rolls back → kernel
    or caller retry replays it (idempotent because callback_state stays
    'pending').
    """
    status = {
        "succeeded": JobStatus.DONE.value,
        "rejected": JobStatus.DONE.value,
        "cancelled": JobStatus.CANCELLED.value,
    }.get(outcome, JobStatus.FAILED.value)
    await storage.run_finish(uow, run_id, status, outcome)
    await _run_trigger_callback(uow, storage, run_id, registry, outcome)


async def _run_trigger_callback(
    uow, storage: Storage, run_id, registry, outcome: str
) -> None:
    """Apply the trigger's SM callback transition for the outcome.

    Exactly-once: guarded by callback_state 'pending' → 'done' in the
    same TX; a rolled-back TX leaves it pending → retried on next advance.
    """
    run = await storage.trigger_callback_lock(uow, run_id)
    if not run or run["callback_state"] != "pending":
        return
    config = run["trigger_config"] or {}
    action = {
        "succeeded": config.get("on_succeeded"),
        "rejected": config.get("on_rejected"),
        "failed": config.get("on_failed"),
        # a cancelled run is a terminal failure from the entity's
        # perspective — no separate trigger action.
        "cancelled": config.get("on_failed"),
    }.get(outcome)

    if action and run["triggered_by_entity_type"]:
        from ..sdk import EntityRef
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
        await sm_svc._internal_transition(uow, storage, entity, action, ctx)
        # optional failure-side human task
        if outcome == "failed" and config.get("failure_job"):
            await _spawn_failure_task(uow, storage, registry,
                                      config["failure_job"], entity, ctx)
    await storage.run_mark_callback_done(uow, run_id)


async def _spawn_failure_task(
    uow, storage: Storage, registry, type_name, entity, ctx
) -> None:
    """Create the configured human fix-up task on run failure."""
    spec = registry.job_type(type_name)
    job_id = uuid.uuid4()
    child_ctx = JobContext(
        actor=Actor.job(str(job_id), via=ctx.actor),
        tenant_id=ctx.tenant_id, correlation_id=ctx.correlation_id,
        permissions=ctx.permissions,
        extra={"job_id": str(job_id)},
    )
    now = datetime.now(UTC)
    await storage.job_insert(uow, JobInsert(
        id=str(job_id), type=type_name,
        payload={"entity_type": entity.type, "entity_id": entity.id},
        context=ctx_to_dict(child_ctx),
        max_attempts=spec.retry.max_attempts if spec.retry else 3,
        run_at=now,
        assignee=(spec.human_task
            and f"role:{spec.human_task.assignee_role}"),
        due_at=_spec_due_at(spec, now),
    ))


# ── step enqueue helpers ──────────────────────────────────────────────────


async def _enqueue_step(
    uow, storage: Storage, sr, spec_step: StepSpec, spec, run_row,
    step_runs, parent_ctx, kernel,
) -> None:
    inputs = resolve_inputs(spec_step.input, run_row["inputs"], step_runs)
    job_id = uuid.uuid4()
    child_ctx = _child_ctx(parent_ctx, job_id)
    now = datetime.now(UTC)

    await storage.job_insert(uow, JobInsert(
        id=str(job_id), type=spec_step.type, payload=inputs,
        context=ctx_to_dict(child_ctx),
        max_attempts=spec.retry.max_attempts if spec.retry else 3,
        run_at=now,
    ))
    await kernel.push_ex(
        uow,
        _kernel_job(job_id, spec_step.type, spec, step_run_id=sr["id"]),
    )
    await storage.step_attach_job(uow, str(sr["id"]), str(job_id), "claimed")


async def _enqueue_human_step(
    uow, storage: Storage, sr, spec_step, spec, run_row, step_runs,
    parent_ctx, kernel,
) -> None:
    """Human step: durable job row, no kernel claim (people claim via Inbox)."""
    inputs = resolve_inputs(spec_step.input, run_row["inputs"], step_runs)
    job_id = uuid.uuid4()
    child_ctx = _child_ctx(parent_ctx, job_id)
    now = datetime.now(UTC)

    await storage.job_insert(uow, JobInsert(
        id=str(job_id), type=spec_step.type, payload=inputs,
        context=ctx_to_dict(child_ctx),
        max_attempts=spec.retry.max_attempts if spec.retry else 3,
        run_at=now,
        assignee=_spec_assignee(spec),
        due_at=_spec_due_at(spec, now),
    ))
    await storage.step_attach_job(
        uow, str(sr["id"]), str(job_id), JobStatus.BLOCKED.value)


async def _enqueue_wait_step(
    uow, storage: Storage, sr, spec_step, spec, run_row, step_runs,
    parent_ctx,
) -> None:
    """External-wait step: durable job row, no kernel claim — the
    application resolves it via JobService.signal after its own
    provider callback (auth + correlation are app-owned). Emits a
    metadata-only `wait.opened` outbox event in the same TX so an app
    can trigger its external call after commit."""
    inputs = resolve_inputs(spec_step.input, run_row["inputs"], step_runs)
    job_id = uuid.uuid4()
    child_ctx = _child_ctx(parent_ctx, job_id)
    now = datetime.now(UTC)

    await storage.job_insert(uow, JobInsert(
        id=str(job_id), type=spec_step.type, payload=inputs,
        context=ctx_to_dict(child_ctx),
        max_attempts=spec.retry.max_attempts if spec.retry else 3,
        run_at=now,
    ))
    await storage.step_attach_job(
        uow, str(sr["id"]), str(job_id), JobStatus.BLOCKED.value)
    await storage.outbox_insert(uow, "wait.opened", {
        "job_id": str(job_id),
        "type": spec_step.type,
        "step_id": spec_step.id,
        "run_id": str(run_row["id"]),
    })


def _kernel_job(job_id, type_name: str, spec, *, step_run_id):
    from .kernel import dispatch_spec

    return dispatch_spec(
        job_id=job_id,
        max_attempts=spec.retry.max_attempts if spec.retry else 3,
        meta={"job_id": str(job_id), "type": type_name,
              "step_run_id": str(step_run_id)},
        step_run_id=step_run_id,
    )


def _spec_assignee(spec) -> str | None:
    opts = getattr(spec, "human_task", None)
    if opts and getattr(opts, "assignee_role", None):
        return f"role:{opts.assignee_role}"
    return None


def _spec_due_at(spec, now: datetime) -> datetime | None:
    """Human task SLA deadline from human_task.due_in_seconds (None = no SLA)."""
    opts = getattr(spec, "human_task", None)
    if opts and getattr(opts, "due_in_seconds", None):
        return now + timedelta(seconds=opts.due_in_seconds)
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
