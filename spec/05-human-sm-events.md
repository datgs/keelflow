# keelflow spec — human tasks, state machines, events

## Human tasks

`human.*` job types reuse `keelflow_jobs`. Semantics:

- `assignee` = `user:<id>` or `role:<name>`; unassigned rows are
  visible to anyone (per `my_tasks` filter).
- `my_tasks(conn, actor, roles)` → open tasks for actor + roles.
  Tasks claimed by the actor are always included, even when the
  assignee is a role the actor lacks (a claimer must see what they
  claimed in order to complete it).
- `claim(conn, job_id, actor, roles)` — first claim wins (`FOR
  UPDATE` + null check); second claimant gets `PermissionError`.
  Claimed-by recorded as `user:<id>` in `claimed_by`. The assignee is
  enforced: `user:<id>` tasks only claimable by that user; `role:<name>`
  tasks only by an actor holding that role (via `roles`); unassigned
  tasks by anyone.
- `complete(conn, job_id, actor, result, roles)` — only claimant (or
  unclaimed) may complete; the task assignee is always enforced, so
  `role:<name>` requires a matching trusted role even when unclaimed.
  Only `pending`/`claimed` tasks may be completed. Sets `done` + `result`, then unblocks the
  owning step via `on_job_finished`, **inside the caller's TX**.
- `complete_and_transition(conn, job_id, actor, decision, ctx, roles)` — for
  SM-created tasks whose payload carries `sm_actions`; completes the
  task AND applies the mapped SM action in one TX (a rejected decision
  can't leave the task done while the entity stays put).
- `decision: "reject"` in a workflow-step result makes the run
  `terminal_outcome=rejected` (business rejection, `03-workflow.md`).
- `release(conn, job_id, actor)` — claim owner returns a claimed task
  to `pending`/`claimed_by=NULL` (e.g. the claimer went away).
- `due_at`: `ui.due_in_seconds` on the task spec sets an SLA deadline
  at insert; `escalate_overdue` treats any task past `due_at` as
  overdue regardless of `older_than_days`.
- Escalation: `escalate_overdue(conn, older_than_days, to_role)`
  reassigns stale open tasks to `role:<to_role>` — `due_at`-overdue
  tasks, or tasks older than `older_than_days` when `due_at` is unset.

## Entity state machine

`transition(entity, action, ctx, tx)` MUST, in `tx`:

1. `INSERT … ON CONFLICT DO NOTHING` then `SELECT … FOR UPDATE` — two
   concurrent first transitions serialize on the row lock; default
   state = definition's `initial`.
2. Find transition `(from=current, action)` — missing → `ValueError`.
3. If `guard.permission` — `ctx.require(perm)` → `PermissionError`.
   The string MUST be registered via `Registry.register_permission`
   before `register_state_machine`; an unknown permission fails at
   load time, not as a silent 403 (`06-context.md`).
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
