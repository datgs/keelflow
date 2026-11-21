# keelflow spec — bulk chunk/row processing

Three levels: parent job → chunks → rows.

## Flow

1. Caller enqueues bulk job + INSERTs staging rows into
   `keelflow_row_status (job_id, row_key, status='pending', data)` —
   same TX (durable intent).
2. Parent handler (`BulkJobHandler.run`) partitions pending+unchunked
   rows into `keelflow_chunks` (idempotent) and enqueues one chunk job
   per chunk — **inside its own dispatch TX** (`ctx.tx`), so rows staged
   by a subclass's `run` (or by the enclosing workflow step) are
   visible and staging + partition + child enqueue commit atomically.
   A conforming implementation MUST NOT partition on a connection that
   cannot see the handler's uncommitted rows. Parent stays `running`
   until fan-in.
3. Chunk handler claims chunk, resets stranded `processing` rows to
   `pending` (kill-resume), processes rows via `process_row(ctx, row, tx)`:
   - `atomic='chunk'` (default): all rows in ONE transaction; a failing
     row rolls back the chunk and is recorded `failed` (with error) in a
     follow-up TX so the error queue remains observable.
   - `atomic='row'`: one transaction per row; failures mark only that
     row.

## Set-based batch callback (`process_batch`)

A `BulkJobHandler` MAY additionally implement
`process_batch(ctx, rows, tx) -> list[BatchRowResult]` — the set-based
fast path for write-heavy imports. Selection: `atomic='chunk'` uses
`process_batch` when the handler overrides it; `atomic='row'` always
uses `process_row`. A handler implementing only `process_batch` MUST
NOT be enqueued with `atomic='row'` — the chunk fails with a config
diagnostic.

MUST, in the one chunk transaction:

1. Receive ALL of the chunk's pending rows (bounded by `chunk_size`),
   each carrying engine `id`, `row_key`, and `data`.
2. Return exactly one `BatchRowResult` per input engine row id
   (order irrelevant; `row_key` is NOT a correlation key — it is
   business identity and is not unique). `error=None` → row `done`;
   non-empty error → row `failed` into the error queue while siblings
   still commit. `outputs` are not persisted.
3. Apply callback domain writes and all row-state flips in that same
   transaction — row outcomes and domain effects commit or roll back
   together.

- A raised exception or a malformed result list (missing, duplicate,
  or unknown row ids; wrong type) rolls back the whole chunk. The
  chunk job retries under its retry policy — attempt budget is copied
  from the PARENT bulk type's registered `RetryPolicy`; only the last
  allowed attempt terminalizes the chunk (`failed`) and seals the
  parent via fan-in. A transient failure of a non-exhausted chunk
  leaves rows `pending` for redelivery; no partial state is visible.
- Cancellation is observed at the checkpoint BEFORE the batch
  transaction starts; a committed batch cannot be interrupted
  mid-flight.
- The batch callback MUST NOT perform external I/O (HTTP, RPC, file
  delivery): the transaction rolls back and retries on transient
  failure, which would repeat the side effect. Stage remote calls as
  durable jobs/outbox events committed in the callback, and reconcile
  outcomes in a later step. Provider auth, correlation, and callback
  handling stay application-owned (see `02-job-lifecycle.md` wait
  jobs).
- Explicit per-row business errors never fail the chunk or the parent;
  they are error-queue data like row-atomic failures.

4. Fan-in: last chunk to settle flips parent to `done` (or `failed` if
   any chunk failed) and merges a summary into `result`
   (`chunks` per-status counts + `rows_total`/`rows_done`/`rows_failed`).
   If the parent job belongs to a workflow step, fan-in MUST call the
   step-finish hook (`on_job_finished`) **in the same transaction** —
   step completion and downstream advance are atomic with the parent's
   final status. Without this, a bulk job can never be a DAG step.
5. Row retention: inside that same fan-in TX, `done` staging rows are
   disposed per `BulkJobPayload.row_disposition`:
   - `archive` (default) — moved to `keelflow_row_archive`
     (job_id, row_id, row_key, chunk_id, data, archived_at), a cold
     table with no FKs to live jobs/chunks so rows survive job cleanup;
   - `keep` — left in `keelflow_row_status` for debugging or strict
     per-row audit;
   - `delete` — removed; use only when the app already has a durable
     source copy.
   `failed` rows are NEVER moved or deleted — they stay in the live
   error queue. The parent `result` records `row_disposition` +
   `archived_rows`/`deleted_rows`/`kept_rows`. The move is idempotent
   (`ON CONFLICT (job_id, row_id) DO NOTHING`) so a retried fan-in
   cannot duplicate archive rows. Once `keelflow_jobs.result` carries
   `rows_total`, `bulk.progress()` MUST read row counters from it —
   the live table may no longer hold `done` rows. Archived rows are
   inspectable via `MonitoringService.rows_list_archived` (keyset
   pagination on (job_id, id)).

## Invariants

- A dead worker's chunk is re-claimed by the kernel; only `pending`
  rows resume — `done`/`failed` rows are never re-processed.
- `process_row` receives the caller's row `data` + `row_key`; engine
  handles status flips around it.
- Chunk-atomic is the default atomicity: one commit per ~1000 rows,
  never all-or-nothing per job.
- Row `data` is the carrier between staging and `process_row` — chunk
  workers' `ctx` has no parent payload; self-contained rows make
  per-row retries deterministic.
- Live `keelflow_row_status` is a hot operational table: after fan-in
  it holds only `failed` rows (default) — apps MUST NOT depend on
  reading `done` rows there unless they opt into `row_disposition='keep'`.
  Inspect completed rows via `keelflow_row_archive` / the summary in
  `keelflow_jobs.result`.
