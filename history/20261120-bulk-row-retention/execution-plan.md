# Execution plan — bulk row retention and archive

## Goal

Prevent unbounded growth of live `keelflow_row_status` while preserving
post-run inspection. Default completed-row behavior is archive; callers can
explicitly choose keep or delete. Failed rows remain available in the live
error queue in all modes.

## Scope

Engine bulk contract, Postgres schema/migration, monitoring read API, bulk
progress, tests, specs/docs, and the bulk demo's documentation/tests if its
assumptions depend on live done rows.

## Non-goals

No per-job-type tables, table partitioning, archive TTL worker, mass migration
of old terminal jobs, change to chunk retry semantics, or change to failed-row
retention.

## Contracts to preserve

- `BulkJobHandler` still receives each staged row's `data` and `row_key`.
- Only `pending`/`processing` rows are eligible for work; `done`/`failed`
  rows are never reprocessed.
- Parent fan-in still writes `rows_total`, `rows_done`, `rows_failed`, chunk
  counts, and workflow step completion atomically.
- `rows_list_failed` and existing error CSV consumers continue to work.
- Existing payloads without a new field default to `archive`.

## Task graph

### T1 — Schema and migration [P0]

**Goal:** Add durable storage for archived completed rows.

**Context:** `keelflow_row_status` has live operational state and row payload
copies. The archive must survive later deletion of live jobs/chunks.

**Required outcomes:**

- Add `keelflow_row_archive` to `src/keelflow/postgres/schema.sql`.
- Add an Alembic migration after the current head creating the table and
  lookup indexes.
- Archive columns include a generated/identity id, parent `job_id`, source
  `row_key`, optional source `chunk_id`, `data`, and `archived_at`.
- Do not add FKs from archive to live jobs/chunks.
- Add a uniqueness constraint/index that supports idempotent archive moves:
  `(job_id, row_key)` unless the implementation proves row_key is not unique
  for a parent; if not unique, use a stable source-row id in the archive
  identity.
- Migration downgrade removes only the archive objects.

**Constraints:** No destructive migration of existing row data.

**Acceptance evidence:** Fresh schema and `alembic upgrade head` both create
the table; downgrade/re-upgrade works in a disposable database.

**Verification:**

```bash
uv run alembic upgrade head
uv run pytest tests/test_postgres_schema.py -q  # add/adjust focused migration test if needed
uv run ruff check src tests
```

**Ownership:** `src/keelflow/postgres/schema.sql`, `alembic/versions/000X_*.py`,
possibly schema-focused tests.

**Dependencies:** None. T2 depends on this schema contract.

### T2 — Bulk payload and atomic fan-in retention [P0]

**Goal:** Implement archive/keep/delete behavior at terminal fan-in.

**Context:** `BulkJobPayload` currently has `chunk_size` and `atomic`; fan-in
is in `src/keelflow/engine/bulk.py` and already owns the summary/workflow
transaction.

**Required outcomes:**

- Add `row_disposition: Literal["archive", "keep", "delete"] = "archive"`.
- Preserve the default for old payloads.
- At fan-in, count rows before cleanup and operate only on `status='done'`.
- `archive`: set-based insert into archive plus delete from live rows in the
  same transaction. Use conflict-safe insertion.
- `delete`: delete done rows in the same transaction.
- `keep`: retain done rows unchanged.
- Never move/delete failed rows.
- Store disposition and cleanup counts in parent `result`, e.g.
  `row_disposition`, `archived_rows`, `deleted_rows`, `kept_rows`.
- Ensure a retry of fan-in cannot duplicate archive rows or corrupt counts.
- Keep workflow `on_job_finished` atomic with parent status and cleanup.

**Constraints:** Do not load all row data into Python. Do not change chunk
processing/retry behavior. Do not silently default to delete.

**Acceptance evidence:** A 3-row parent reaches terminal state with each mode;
archive contains exactly done rows for archive mode, live done rows remain for
keep, and no done rows remain for delete. Failed rows remain in all modes.

**Verification:**

```bash
uv run pytest tests/test_bulk.py tests/test_bulk_retention.py -q
uv run ruff check src tests
uv run mypy src/keelflow
```

**Ownership:** `src/keelflow/engine/bulk.py`, `src/keelflow/engine/worker.py`
only if required for payload/result handling, and bulk-focused tests.

**Dependencies:** T1.

### T3 — Progress and archive inspection API [P1]

**Goal:** Keep terminal progress useful and expose archived rows to operators.

**Context:** `bulk.progress()` currently counts live `row_status`; archive/delete
will make that query empty after terminal fan-in.

**Required outcomes:**

- Active parent: preserve live row/chunk progress behavior.
- Terminal parent: return counters from parent `keelflow_jobs.result` and
  include the row disposition/cleanup counts.
- Failed-row counts and `rows_list_failed` remain live and unchanged.
- Add a focused monitoring read method such as
  `rows_list_archived(conn, job_id, limit=50, cursor=None)` returning stable
  ordering and archive metadata/data.
- Do not make the hot progress query scan the archive table.

**Constraints:** Read-only API; no broad union query or implicit full archive
scan.

**Acceptance evidence:** After archive mode, `progress()` returns non-zero
terminal totals and the archive API returns the original row keys/data;
after delete mode, progress still returns totals and archive API is empty.

**Verification:**

```bash
uv run pytest tests/test_bulk_retention.py tests/test_monitoring_admin.py -q
uv run ruff check src tests
uv run mypy src/keelflow
```

**Ownership:** `src/keelflow/engine/bulk.py`,
`src/keelflow/engine/monitoring.py`, focused tests.

**Dependencies:** T1 and T2.

### T4 — Contract/spec/docs/demo update [P1]

**Goal:** Make the retention behavior explicit to library users and demo
operators.

**Required outcomes:**

- Update `spec/04-bulk.md` with disposition semantics, atomicity, failed-row
  guarantee, idempotency, and terminal progress behavior.
- Update `docs/guide.md` with a code example showing archive/keep/delete and
  archive inspection.
- Update `examples/bulk_import/README.md` to state that completed staging
  rows are archived by default and failed rows remain downloadable.
- Add/adjust demo tests if the UI or error CSV assumes done rows remain in
  `row_status`.
- Document that this change does not migrate old terminal rows automatically.

**Constraints:** Do not claim archive is an infinite-retention policy; state
that future retention/partitioning can purge the cold table.

**Acceptance evidence:** A new user can identify the default disposition,
inspect archived rows, and understand the difference between archive, keep,
and delete from docs alone.

**Verification:**

```bash
uv run ruff check src examples tests
uv run pytest examples/bulk_import/tests -q
```

**Ownership:** `spec/04-bulk.md`, `docs/guide.md`,
`examples/bulk_import/README.md`, related tests.

**Dependencies:** T2 and T3.

### T5 — Integration and failure-safety verification [P0]

**Goal:** Prove archive cleanup is safe under retries, rollback, and large
row counts.

**Required outcomes:** Add regression coverage for:

1. archive default with done + failed mixed rows;
2. explicit keep;
3. explicit delete;
4. archive idempotency when fan-in is re-entered;
5. archive insert failure rolls back live deletion and parent terminal update;
6. workflow parent: cleanup and downstream advance commit atomically;
7. chunk crash/retry: pending rows resume, done rows are not reprocessed;
8. terminal `progress()` after archive/delete;
9. archived rows remain readable after live job/chunk deletion;
10. set-based operation handles a representative large fixture without
    loading all rows into Python.

**Constraints:** Tests must use synthetic data and the existing Postgres
fixture/skip convention. Do not make tests depend on wall-clock retention.

**Acceptance evidence:** All cases pass, including a forced rollback path;
no orphan live rows or duplicate archive rows are observed.

**Verification:**

```bash
uv run pytest tests/test_bulk.py tests/test_bulk_retention.py \
  examples/bulk_import/tests -q
uv run pytest -q
uv run ruff check src examples tests
uv run mypy src/keelflow
uv run git diff --check
```

**Ownership:** `tests/test_bulk_retention.py`, targeted existing tests,
possibly a small test helper only.

**Dependencies:** T1, T2, T3.

## Recommended execution order

Critical path: **T1 → T2 → T3 → T5**, with T4 after T2/T3.

T1 is the only migration hub and must not run in parallel with other schema
edits. T2 and T3 share bulk contracts and should be implemented sequentially.
T4 can begin after the contracts stabilize. T5 is the integration owner and
runs after T2/T3.

## Rollback

- Before enabling new payloads, old callers continue to work because the
  default is archive.
- If runtime cleanup must be disabled, set `row_disposition='keep'` in
  callers; no schema rollback is required.
- If migration rollback is necessary before any archive data is written,
  downgrade 000X. After archive data exists, preserve the archive table and
  stop cleanup rather than dropping user-inspection data; destructive data
  rollback requires explicit operator approval.

## Final gate

The implementation is ready when:

- all 101+ existing tests plus retention tests pass;
- fresh schema and Alembic migration agree;
- archive/default/keep/delete semantics are tested;
- rollback and retry cannot lose or duplicate done rows;
- terminal progress and archive inspection work after live cleanup;
- docs/specs match the externally observable contract.
