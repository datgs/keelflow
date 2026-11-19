# hd-flow spec — bulk chunk/row processing

Three levels: parent job → chunks → rows.

## Flow

1. Caller enqueues bulk job + INSERTs staging rows into
   `hd_flow_row_status (job_id, row_key, status='pending', data)` —
   same TX (durable intent).
2. Parent handler (`BulkJobHandler.run`) partitions pending+unchunked
   rows into `hd_flow_chunks` (idempotent) and enqueues one chunk job
   per chunk. Parent stays `running` until fan-in.
3. Chunk handler claims chunk, resets stranded `processing` rows to
   `pending` (kill-resume), processes rows via `process_row(ctx, row, tx)`:
   - `atomic='chunk'` (default): all rows in ONE transaction; a failing
     row rolls back the chunk and is recorded `failed` (with error) in a
     follow-up TX so the error queue remains observable.
   - `atomic='row'`: one transaction per row; failures mark only that
     row.
4. Fan-in: last chunk to settle flips parent to `done` (or `failed` if
   any chunk failed) with a summary in `result`.

## Invariants

- A dead worker's chunk is re-claimed by the kernel; only `pending`
  rows resume — `done`/`failed` rows are never re-processed.
- `process_row` receives the caller's row `data` + `row_key`; engine
  handles status flips around it.
- Chunk-atomic is the default atomicity: one commit per ~1000 rows,
  never all-or-nothing per job.
