# Approach — bulk row retention

## Recommendation

Use one archive table, `keelflow_row_archive`, with an explicit per-parent
row disposition:

- `archive` (default): move done rows to the archive at fan-in.
- `keep`: retain done rows in `keelflow_row_status` for live debugging or
  strict audit workflows.
- `delete`: remove done rows after the parent summary is computed, only when
  the application has another durable source of truth.

Do not create one `row_status` table per job type. The engine needs one stable
contract for chunk claims, progress, failed-row inspection, and retention.

## Archive shape

Use a stable archive record that does not reference live jobs:

- archive row id
- parent job id
- source row key
- source chunk id (nullable)
- original row data
- archived timestamp

Add indexes for `(job_id, id)` and `(job_id, row_key)`; do not copy live
status/chunk operational indexes. The archive is cold inspection data, not a
claim table.

The archive should include `job_id` and `row_key` as lookup identity, but no
foreign key to `keelflow_jobs` or `keelflow_chunks`. It must remain queryable
after job retention.

## Fan-in transaction

At the point where `_fan_in` has confirmed that all chunks are terminal:

1. Read row counts and compute the existing summary.
2. Read the parent row disposition from the parent job payload. Old jobs with
   no field use `archive` through the Pydantic default/backward-compatible
   parser.
3. Move only `status='done'` rows according to the disposition:
   - archive: `INSERT ... SELECT` into archive, then delete those live rows;
   - delete: delete those live rows;
   - keep: no row cleanup.
4. Add retention metadata to the parent result, for example:
   `row_disposition`, `archived_rows`, `deleted_rows`, `kept_rows`.
5. Update parent status/result and invoke `on_job_finished` in the same
   transaction as today.

If archive insertion, deletion, parent update, or workflow advancement fails,
rollback must leave the live rows and parent non-terminal state consistent.
Use an idempotent archive insert (`ON CONFLICT` on parent job + source row
identity) so a retry cannot duplicate archive records.

## Progress and observability

`bulk.progress()` must preserve terminal visibility after archive/delete:

- while parent is active: query live `row_status` as today;
- after parent is terminal: use the parent result counters and disposition;
- failed-row count remains queryable from live `row_status`.

Add a monitoring read API for archived rows, or extend failed-row/row lookup
with an explicit archive source. The first version should provide a focused
`rows_list_archived(conn, job_id, limit/cursor)` method rather than hiding
archive reads behind a broad union query.

## Migration and compatibility

- Add an Alembic migration creating the archive table and indexes.
- Update `src/keelflow/postgres/schema.sql` for fresh installs.
- Existing bulk payloads without `row_disposition` default to `archive`.
- Existing live jobs already terminal are not automatically migrated in this
  change; provide a separate operator command or document that cleanup starts
  for new terminal fan-ins. This avoids a risky mass move during migration.
- Update `spec/04-bulk.md`, `docs/guide.md`, and bulk demo README to state the
  retention contract and how to inspect archived rows.

## Key risks

- A bulk fan-in can process millions of rows; archive/delete must be batched
  or use a single set-based statement without loading row data into Python.
- Large archive inserts still create WAL and require monitoring; archive is
  not a replacement for eventual archive retention/partitioning.
- App code must not assume `row_status` contains done rows after terminal
  fan-in. That assumption must be covered by tests and docs.
