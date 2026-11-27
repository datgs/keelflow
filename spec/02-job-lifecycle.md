# keelflow spec — job lifecycle

## Enqueue (in caller TX)

```text
builder(type)                      # registered type "name@vN", else reject
  .context(ctx)                    # REQUIRED — missing → EnqueueError
  .payload(p)                      # schema-validated at enqueue
  [.run_at(t)] [.concurrency_key(k)] [.deduplication_key(k)] [.queue(q)]
  .enqueue(tx)                     # inside caller transaction
```

MUST, atomically in `tx`:

1. Validate `payload` against the type's input schema — invalid → raise
   before any write.
2. INSERT into `keelflow_jobs` (status `pending`, serialized `ctx`).
3. Make the job claimable through the selected kernel in the same TX.
4. INSERT one `keelflow_outbox` row `job.enqueued` (event dispatcher
   handles delivery).

Rollback of `tx` MUST leave no trace in any of the three.

`.deduplication_key(k)` — idempotent enqueue. A partial unique index on
`(type, unique_key)` over non-terminal statuses
(`pending|claimed|running|blocked`) means a duplicate enqueue
**returns the existing job's `JobRef`** — the kernel job and outbox
inserts are skipped (exactly-once intent). Terminal jobs release the
key; NULL keys never conflict.

## Dispatch (worker, new TX)

```text
kernel claims job → dispatch(job_id)
  SELECT * FROM keelflow_jobs WHERE id=$1
  if concurrency_key: acquire named lock on hashtext(key)  # serialized exec
  deserialize context + payload; inject job_id into ctx.extra
  if status in (done | cancelled | failed):                       # re-delivery
      advance owning step idempotently (if any); never re-run handler
  else:
      run handler.run(ctx, payload) with ctx.uow = dispatch unit of work
      handler + job finish + advance_run in ONE TX (see 03 §dispatch TX)
      declared status := done | failed | cancelled; result := outputs
  kernel completion update (same worker TX)
```

- Retry policy is per job type (`RetryPolicy.max_attempts`, `backoff`,
  `base_seconds`, `jitter`); engine owns the attempt counter
  (`keelflow_jobs.attempts` per dispatch claim). **Native claims count an
  attempt at claim time**, including a claim abandoned before the handler
  started (worker death in the claim→dispatch gap). A dispatch that
  observes `attempts > max_attempts`
  settles the job without running the handler — `failed` + DLQ +
  workflow terminalization in one unit. On handler exception:
  attempts remaining → job stays `running` AND a replacement kernel job
  is pushed at `scheduled_at = now + backoff` inside the same TX
  (redelivery re-runs the handler); exhausted → status `failed`, the
  row is copied to `keelflow_dlq`, and the owning step terminalizes
  (`on_failed` path in `03-workflow.md`). Chancy/dispatch increments at
  `job_mark_running` but pre-dispatch fencing or bounded attempt
  accounting is unproven, so claim parity is not guaranteed there.
  Without a kernel client configured, dispatch falls back to kernel-managed
  retries. Kernel delivery budget is `max_attempts + 1` so the kernel always grants
  the final delivery needed for engine-side settlement.
- `timeout_seconds` (per job type) wraps `handler.run` in a deadline;
  `TimeoutError` counts as a failed, retryable attempt.
- Cancellation is **pre-start by default**. `cancel_requested` is
  honored at claim time: a job carrying the flag finishes `cancelled`
  before the handler runs and its owning step becomes `cancelled`.
  The pending→running transition is an atomic conditional UPDATE
  (`cancel_requested=false` required), so a request committed before
  the transition wins — a separate check-then-write would race.
- `JobService.cancel(job_id, ctx)` is the public request entry point
  and requires the `job.cancel` permission. `pending` → `cancelled`;
  `claimed|running|blocked` → flag set, status preserved; terminal
  jobs and repeated requests are no-ops. When a job terminalizes in
  the cancel call itself (`pending` → `cancelled`), the owning step is
  advanced in the same transaction — required for non-kernel
  lifecycles (`wait.*`, `human.*`) that have no later dispatch
  delivery to do it; for machine jobs the worker's terminal skip
  performs the same idempotent write.
- A running job is never force-killed mid-transaction. Default jobs
  drain to `done`/`failed`. Job types opting in with
  `cooperative_cancel=True` may call `await
  ctx.execution.checkpoint()` at safe points: a committed flag
  observed there raises `JobCancelled` (a `BaseException` subclass, so
  `except Exception` cannot swallow it), the handler transaction
  rolls back, and the worker records `cancelled` in a new
  transaction — cancellation bypasses retry and DLQ. A request that
  commits after the last checkpoint can lose to normal completion.
  Cancellation never rolls back external side effects.
- `JobHandler.on_cancel` is an **experimental** placeholder — the
  worker never invokes it. Process liveness remains kernel-owned. Job
  types may opt into `progress_enabled=True` and call
  `await ctx.execution.report_progress(current, total, message=..., metadata=...)`.
  Progress is observational application data, stored independently of the
  handler UoW; it does not prove committed business work. Metadata is
  JSON object data capped at 8 KiB and messages at 256 characters.
- The pending→running transition mints a fresh `execution_token`
  (non-reusable across retries and ops `retry_reset`). Progress,
  finish, and reschedule writes are fenced on that token: a write
  carrying an outdated token is rejected — `report_progress()` raises
  `ExecutionLostError` rather than silently dropping the report, and
  the worker abandons the stale dispatch instead of rescheduling it.
  The report write is fenced on ownership only: deadline enforcement
  lives in the worker's in-process watchdog, so a still-owning claim
  may report even when its DB-side signal is older than the window
  (e.g. after waiting on a `concurrency_key` lock).
- `progress_report_timeout_seconds` measures silence between reports
  (not counter movement) and implies reporting is enabled — it does
  not require `progress_enabled`. Expiry cancels the local handler
  attempt, rolls its UoW back, and the normal retry policy applies. Local cancellation cannot stop a remote
  request already in flight. Configure retry only when stable idempotency keys,
  an outbox with receiver deduplication, or explicit reconciliation makes replay
  safe; ambiguous external outcomes require manual reconciliation, not blind
  retry. Progress reporting is distinct from cooperative cancellation checkpoints.
- Cron `overlap="kill_previous"` follows the same rule: a pending
  previous job is cancelled outright, while a claimed/running one is
  only flagged `cancel_requested` and drains (or checkpoints).
- Size visibility (two-tier): `result_warning_bytes` (soft, default
  32768, never fails — `jlog.warning` + size recorded) and
  `result_max_bytes` (hard, default `None` = opt-in; exceeded → job
  fails with a diagnostic naming the cap and type). Serialized sizes
  are stored in `keelflow_jobs.result_bytes` and
  `keelflow_step_runs.outputs_bytes`; `MonitoringService`
  `results_oversized` ranks the worst job types. Handlers should
  return references, not blobs.
- DLQ: `keelflow_dlq` holds a copy of every terminally-failed job,
  plus `run_id`/`step_run_id` when it was a DAG step.
  `MonitoringService.dlq_list` reads it; `dlq_replay` **resumes the
  owning run** via `resume_step` when the link and a live step_run
  exist (registry must be passed), otherwise it re-enqueues the
  payload as a fresh standalone job.

## Wait jobs (`is_external_wait`, `type` in `wait.*`)

An external-wait type is materialized by workflow steps only — direct
`builder(...).enqueue()` is rejected. The job row is inserted `pending`
(and the step marked `blocked`) but **no kernel job is pushed**: nothing
claims it. The application resolves it later:

```
external system → application callback route → verify/translate
    → JobService.signal(job_id, result, ctx)      → job done, run advances
    → JobService.signal_failure(job_id, err, ctx) → job/step failed
```

Both require the `job.signal` permission, lock the job `FOR UPDATE`,
accept only `pending|claimed` (first signal wins; later signals raise),
write `result`, and run `on_job_finished` in the caller's transaction —
so application domain writes (check rows, correlation tables) commit
atomically with the resolution. Keelflow does not own webhook routes,
provider authentication, or external-key correlation; `job_id` is a
reference, not a credential. Cancelling an open wait terminalizes the
step `cancelled` like any `blocked` job.

## Human jobs (`type LIKE 'human.%'`)

No kernel claim. Row sits `pending`/`claimed` in `keelflow_jobs` until
`HumanTaskService.claim|complete` (see `05-human-sm-events.md`).
`ui.due_in_seconds` sets `due_at` at insert; `release()` returns a
claimed task to `pending` (claim owner only); `escalate_overdue`
reassigns any task past `due_at` (or older than N days when `due_at`
is unset).
