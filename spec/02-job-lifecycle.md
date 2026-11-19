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

- Retry policy is per job type (`max_attempts`, backoff+jitter); kernel
  owns attempt counter. On handler exception: retries remaining → job
  stays `running` (redelivery re-runs the handler); exhausted → status
  `failed` and the owning step terminalizes (`on_failed` path in
  `03-workflow.md`). DLQ row written by a reaper or ops action.
- `cancel_requested` is cooperative: worker checks/sets flag; handler
  `on_cancel` may clean up; kernel must not force-kill mid-transaction.

## Human jobs (`type LIKE 'human.%'`)

No kernel claim. Row sits `pending`/`claimed` in `keelflow_jobs` until
`HumanTaskService.claim|complete` (see `05-human-sm-events.md`).
