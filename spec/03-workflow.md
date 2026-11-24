# keelflow spec — workflow DAG

## Definition

```yaml
workflow: name@vN
steps:
  - id: a
    type: type@vN
    needs: [x, y]          # optional
    if: "steps.x.outputs.flag == true"   # optional guard
    input: {k: "${{ steps.x.outputs.v }}"}   # optional refs
    retry: {max_attempts: 3}
```

Load-time validation MUST reject: unknown `needs`, unknown types,
dependency cycles, and guards outside the grammar below.

## Guard grammar (whitelisted, no eval)

`${{ }}` refs · `== != > < >= <= in` · `and or not` · literals only.
Anything else → `DefinitionError` at load.

## Runtime semantics

- `start(name, inputs, ctx, tx)` creates run + all step_runs `pending`,
  in `tx`.
- `advance_run` (called on each step completion): for every `pending`
  step whose `needs` are all `done` and whose `if:` is true, enqueue its
  job — for human and `is_external_wait` types, insert job row only (no kernel
  claim) and mark step `blocked`; wait steps additionally emit a
  metadata-only `wait.opened` outbox event (`job_id`, `type`, `step_id`,
  `run_id`).
- A step whose `if:` is false → `done` with skipped marker.
- Child job context: `actor = job:<job_id> via <parent ctx actor>`;
  `correlation_id` inherited.
- A graph MUST be able to show **multiple active nodes** — `status` per
  step_run is the source of truth; runs are not a single-cursor FSM.

## Terminal outcome

When no step is `pending|claimed|running|blocked` the run terminalizes:

| Condition | run.status | terminal_outcome |
|---|---|---|
| all steps done, none `decision="reject"` | `done` | `succeeded` |
| a human step done with `outputs.decision == "reject"` | `done` | `rejected` |
| any step `failed` | `failed` | `failed` |
| any step `cancelled` (none failed) | `cancelled` | `cancelled` |

`terminal_outcome` is written in the **same transaction** that sets the
run's terminal status, together with the trigger callback below.
`rejected` is business rejection — a deliberate human verdict, not a
technical failure; it does not consume retries. `cancelled` maps to the
trigger's `on_failed` action (it is a terminal failure from the
entity's perspective).

## Advance safety rules

- **Definition drift**: `step_runs` are snapshotted at `start()` while
  the definition is re-read from the registry on every advance. If the
  step sets diverge (the def was edited without a version bump while
  the run was in flight) `advance_run` MUST terminalize the run
  `failed` — never silently drop or add steps.
- **Poisoned steps**: guard evaluation and enqueue inside a finish TX
  run under their own savepoint; an exception marks that step `failed`
  (which fails the run) instead of rolling the advance back into an
  infinite re-delivery loop.

## Workflow triggers (SM → run → SM)

A state-machine transition MAY declare a typed trigger:

```yaml
triggers_workflow:
  workflow: post.wf@v1
  on_succeeded: posted            # SM action on terminal_outcome=succeeded
  on_rejected: posting_rejected
  on_failed: posting_failed  # technical failure (retry exhausted)
  failure_job: human.fix@v1  # optional human task spawned on failed
```

The run MUST record `triggered_by_entity_type/_id`, the trigger config,
and the initiating `JobContext`. On terminalization the engine applies
the matching SM action to that entity **inside the run's terminal
transaction**, then flips `callback_state` `pending → done` — the
callback is exactly-once (a rolled-back TX leaves it `pending` and
replays on next advance). Callback transitions skip the permission
guard (the initiating actor was authorized at trigger time) but still
validate the transition.

`resume_run(conn, run_id, ctx)` reopens a run whose outcome is
`rejected` or `failed`: the offending step resets to `pending`
(successful siblings stay `done`), the entity's failure-exit transition
(`repost`/`retry_post` style) applies, and the run re-executes in place.
Exactly-once: resuming a non-terminal run raises.

## Dispatch transaction

A conforming dispatcher runs `handler.run`, the job finish write, and
`advance_run` in **one transaction**; the handler's domain writes
(`ctx.uow` — never serialized into context) commit or roll back
with the job outcome. An exception → rollback; while kernel retries
remain the job stays `running`; at exhaustion it is terminalized
`failed` and the step fails (→ `on_failed`).

- Retry policy is per job type (`max_attempts`, backoff+jitter); kernel
  owns attempt counter.
- `cancel_requested` is cooperative and pre-start by default: pending
  jobs finish `cancelled`, running jobs drain unless their type opted
  into `cooperative_cancel` checkpoints (`spec/02`). The worker never
  invokes `on_cancel`; kernel must not force-kill mid-transaction.

## Resume-from-failed-step

Re-running a failed step job must not re-enqueue already-done siblings
(idempotent via step status checks).
