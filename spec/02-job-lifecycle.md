# keelflow spec — job lifecycle

## Enqueue (in caller TX)

```text
builder(type)                      # registered type "name@vN", else reject
  .context(ctx)                    # REQUIRED — missing → EnqueueError
  .payload(p)                      # schema-validated at enqueue
  [.run_at(t)] [.concurrency_key(k)] [.queue(q)]
  .enqueue(tx)                     # inside caller transaction
```

MUST, atomically in `tx`:

1. Validate `payload` against the type's input schema — invalid → raise
   before any write.
2. INSERT into `keelflow_jobs` (status `pending`, serialized `ctx`).
3. INSERT kernel/claim record (same TX — e.g. `chancy.push_ex(cursor)`).
4. INSERT one `keelflow_outbox` row `job.enqueued` (event dispatcher
   handles delivery).

Rollback of `tx` MUST leave no trace in any of the three.

## Dispatch (worker, new TX)

```text
kernel claims job → dispatch(job_id)
  SELECT * FROM keelflow_jobs WHERE id=$1 FOR UPDATE
  if concurrency_key: acquire named lock on hashtext(key)  # serialized exec
  deserialize context + payload; inject job_id into ctx.extra
  if status in (done | cancelled | failed):                       # re-delivery
      advance owning step idempotently (if any); never re-run handler
  else:
      run handler.run(ctx, payload) with ctx.tx = dispatch cursor
      handler + job finish + advance_run in ONE TX (see 03 §dispatch TX)
      declared status := done | failed | cancelled; result := outputs
  kernel completion update (same worker TX)
```

- Retry policy is per job type (`RetryPolicy.max_attempts`, `backoff`,
  `base_seconds`, `jitter`); engine owns the attempt counter
  (`keelflow_jobs.attempts` per dispatch claim). On handler exception:
  attempts remaining → job stays `running` AND a replacement kernel job
  is pushed at `scheduled_at = now + backoff` inside the same TX
  (redelivery re-runs the handler); exhausted → status `failed`, the
  row is copied to `keelflow_dlq`, and the owning step terminalizes
  (`on_failed` path in `03-workflow.md`). Without a kernel client
  configured, dispatch falls back to kernel-managed retries.
- `timeout_seconds` (per job type) wraps `handler.run` in a deadline;
  `TimeoutError` counts as a failed, retryable attempt.
- `cancel_requested` is honored at claim time: a job carrying the flag
  finishes `cancelled` before the handler runs and its owning step
  becomes `cancelled`. An already-running job is not force-killed
  mid-transaction.
- DLQ: `keelflow_dlq` holds a copy of every terminally-failed job;
  `MonitoringService.dlq_list` reads it and `dlq_replay` re-enqueues
  the payload as a fresh job.

## Human jobs (`type LIKE 'human.%'`)

No kernel claim. Row sits `pending`/`claimed` in `keelflow_jobs` until
`HumanTaskService.claim|complete` (see `05-human-sm-events.md`).
`ui.due_in_seconds` sets `due_at` at insert; `release()` returns a
claimed task to `pending` (claim owner only); `escalate_overdue`
reassigns any task past `due_at` (or older than N days when `due_at`
is unset).
