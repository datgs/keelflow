# keelflow spec — human tasks, state machines, events

## Human tasks

`human.*` job types reuse `keelflow_jobs`. Semantics:

- `assignee` = `user:<id>` or `role:<name>`; unassigned rows are
  visible to anyone (per `my_tasks` filter).
- `my_tasks(conn, actor, roles)` → open tasks for actor + roles.
- `claim(conn, job_id, actor)` — first claim wins (`FOR UPDATE` +
  null check); second claimant gets `PermissionError`. Claimed-by
  recorded as `user:<id>` in `claimed_by`.
- `complete(conn, job_id, actor, result)` — only claimant (or
  unclaimed) may complete; sets `done` + `result`, then unblocks the
  owning step via `on_job_finished`, **inside the caller's TX**.
- `complete_and_transition(conn, job_id, actor, decision, ctx)` — for
  SM-created tasks whose payload carries `sm_actions`; completes the
  task AND applies the mapped SM action in one TX (a rejected decision
  can't leave the task done while the entity stays put).
- `decision: "reject"` in a workflow-step result makes the run
  `terminal_outcome=rejected` (business rejection, `03-workflow.md`).
- Escalation: `escalate_overdue(conn, older_than_days, to_role)`
  reassigns stale open tasks to `role:<to_role>`.

## Entity state machine

`transition(entity, action, ctx, tx)` MUST, in `tx`:

1. `INSERT … ON CONFLICT DO NOTHING` then `SELECT … FOR UPDATE` — two
   concurrent first transitions serialize on the row lock; default
   state = definition's `initial`.
2. Find transition `(from=current, action)` — missing → `ValueError`.
3. If `guard.permission` — `ctx.require(perm)` → `PermissionError`.
4. Upsert `keelflow_entity_state` (state=to, version+1).
5. `creates_job` → insert job row (+ kernel job unless `human.*`),
   `assignee` from `guard.assignee_role`, payload carries
   `sm_actions` from `guard.sm_actions` when present.
6. `triggers_workflow` → create run + step_runs + `advance_run`,
   persisting trigger metadata for the terminal callback
   (`03-workflow.md`).

An internal variant skips step 3 (permission guard) — used by
workflow-trigger callbacks and `resume_run`, where the initiating
actor was already authorized. Transition validity still applies.

`allowed_actions(entity, actor)` returns actions the actor may take
from current state (guard-filtered).

## Events (transactional outbox)

- `EventBus.publish(tx, event_type, payload)` — INSERT outbox row in
  caller's TX. Rollback → no event (invariant #1).
- `dispatch_events(conn)` polls unpublished rows; each subscriber's
  delivery is recorded in `keelflow_outbox_deliveries (event_id,
  subscriber)` — unique → dedup, at-least-once per subscriber.
