# Discovery — bulk row retention

## Outcome

Make `keelflow_row_status` safe for large bulk jobs without losing the
ability to inspect completed input rows. Completed rows will be archived by
default, with explicit `keep` and `delete` modes. Failed rows remain in the
live error queue in every mode.

## Verified evidence

- `src/keelflow/engine/bulk.py:55-60` defines `BulkJobPayload` with only
  `chunk_size` and `atomic` today.
- `src/keelflow/engine/bulk.py:307-374` performs fan-in, calculates
  `rows_total/rows_done/rows_failed`, stores the summary in the parent job
  result, and currently leaves all `keelflow_row_status` rows live.
- `src/keelflow/engine/bulk.py:376-425` calculates live progress by querying
  `keelflow_row_status`; this will return zero after cleanup unless terminal
  progress reads the parent summary.
- `src/keelflow/postgres/schema.sql:83-103` stores row payload copies in
  `keelflow_row_status`; `data` is the carrier passed to `process_row`.
- `src/keelflow/postgres/schema.sql:74-81` keeps chunk summaries, including
  processed/failed counts, independently of row status.
- `src/keelflow/engine/monitoring.py:78-90` exposes failed-row inspection;
  error rows must remain available for existing error-queue consumers.
- `spec/04-bulk.md` defines row retry semantics: only pending rows resume;
  done/failed rows must not be reprocessed.
- `keelflow_row_status.job_id` references `keelflow_jobs`; the archive must
  not depend on the live job row because archived data must remain readable
  after job retention/sweep.

## Decisions

1. Add `row_disposition: Literal["archive", "keep", "delete"]` to
   `BulkJobPayload`; default is `"archive"`.
2. The disposition applies only to rows whose status is `done` after the
   parent reaches terminal fan-in.
3. `failed` rows are never archived or deleted by this feature. They remain
   in `keelflow_row_status` for error CSVs and operator inspection.
4. Archive is atomic with parent fan-in: archive insert, live-row cleanup,
   parent result update, and workflow step completion commit together.
5. Archive rows have no foreign key to live jobs/chunks. The archive must
   survive later job/chunk retention.
6. No automatic archive TTL is introduced in this change. A later retention
   policy may purge archive data explicitly; the archive is the durable
   inspection path by default.

## Non-goals

- Partitioning `keelflow_row_status` or the archive table.
- Per-job-type tables.
- Storing arbitrary `process_row()` return values; the current contract does
  not persist them.
- Removing failed-row/error-queue behavior.
- Changing retry, chunk claim, or process-row semantics.
