# hd-flow spec — job lifecycle

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
2. INSERT into `hd_flow_jobs` (status `pending`, serialized `ctx`).
3. INSERT kernel/claim record (same TX — e.g. `chancy.push_ex(cursor)`).
4. INSERT one `hd_flow_outbox` row `job.enqueued` (event dispatcher
   handles delivery).

Rollback of `tx` MUST leave no trace in any of the three.

## Dispatch (worker, new TX)

```text
kernel claims job → dispatch(job_id)
  SELECT * FROM hd_flow_jobs WHERE id=$1 FOR UPDATE
  if concurrency_key: acquire named lock on hashtext(key)  # serialized exec
  deserialize context + payload; inject job_id into ctx.extra
  run handler.run(ctx, payload)
  status := done | failed | cancelled; result := outputs
  kernel completion update (same worker TX)
```

- Retry policy is per job type (`max_attempts`, backoff+jitter); kernel
  owns attempt counter. Exhausted retries → status `failed`; DLQ row
  written by a reaper or ops action.
- `cancel_requested` is cooperative: worker checks/sets flag; handler
  `on_cancel` may clean up; kernel must not force-kill mid-transaction.

## Human jobs (`type LIKE 'human.%'`)

No kernel claim. Row sits `pending`/`claimed` in `hd_flow_jobs` until
`HumanTaskService.claim|complete` (see `05-human-sm-events.md`).
