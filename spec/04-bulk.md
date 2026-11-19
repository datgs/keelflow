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
4. Fan-in: last chunk to settle flips parent to `done` (or `failed` if
   any chunk failed) and merges a summary into `result`
   (`chunks` per-status counts + `rows_total`/`rows_done`/`rows_failed`).
   If the parent job belongs to a workflow step, fan-in MUST call the
   step-finish hook (`on_job_finished`) **in the same transaction** —
   step completion and downstream advance are atomic with the parent's
   final status. Without this, a bulk job can never be a DAG step.

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
