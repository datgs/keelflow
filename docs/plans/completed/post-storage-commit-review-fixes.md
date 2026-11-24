# Plan: Fix storage-port review findings

Date: 2026-11-24

## Status

Completed 2026-11-24.

## Result

All five findings fixed, no aliases added:

1. `PgSession.begin()` now raises `RuntimeError` on any pending
   transaction (`transaction_status != IDLE`) instead of silently
   rolling back. `key_lock()` commits its session-scoped advisory
   lock/unlock statements so the connection returns to IDLE, and
   refuses to run inside a pending transaction. Proofs:
   `tests/test_session_begin.py` (fake-conn + live cases).
2. Legacy `on_done` data: preflight on the configured database
   (`keelflow` @5432) found **0 workflow runs** — no migration needed;
   runtime stays alias-free. Schema comment and remaining test
   docstrings updated to `on_succeeded`.
3. Docs synced: `docs/guide.md` (UoW construction, `ctx.uow`,
   `list_jobs`/`get_run`/`purge_expired_records`, `PostgresStorage`
   wiring), `README.md` (`is_external_wait`, `on_succeeded`, `ctx.uow`),
   example READMEs, `spec/01,02,03,04,06` — zero `ctx.tx`/`is_wait`/
   `on_done`/`sweep` references remain.
4. `mcp/server.py` and `admin/app.py` now build `PostgresStorage(dsn)`
   and inject it into `MonitoringService`/`PostgresAuditService`; tools
   and views open `session.begin()` units — no `KEELFLOW_DSN`
   dependency. Proofs: `tests/test_ops_wiring.py` (fake storage with
   `KEELFLOW_DSN` unset).
5. `deduplication_key()` added to the public `JobBuilder` protocol;
   surface-parity test asserts every engine builder setter exists on
   the SDK protocol.

Whitespace issues from the review (trailing space in the completed
plan header, extra EOF blank line in `services.py`) corrected.

Validation: `tests/` 216 passed + 3 skipped; `examples/` 39 passed;
`uv run ruff check src examples tests` clean; `uv run mypy
src/keelflow` clean; `git diff --check` clean.
One flake observed: `test_workflow.py::test_resume_from_failed_step`
failed once in a full-suite run (timing-sensitive worker dispatch),
passed on isolated rerun and in the subsequent full-suite run.

## Outcome

Resolve the five review findings against the storage-port commits, without
adding old API aliases. Keep transaction ownership, persisted workflow
callbacks, admin/MCP access, public SDK typing, and user-facing examples
consistent with the accepted Storage and UnitOfWork contracts.

## Evidence and current findings

Verified by reading the current source:

1. `src/keelflow/postgres/storage.py:82-102`: `PgSession.begin()` calls
   `conn.rollback()` before opening a transaction. This contradicts the
   `Session.begin()` contract in `src/keelflow/sdk/uow.py:71-78`, which
   requires an idle session and says pending work must raise.
2. `src/keelflow/engine/workflow.py:393-424` reads only
   `trigger_config.on_succeeded`. The PostgreSQL schema comment still names
   `on_done` at `src/keelflow/postgres/schema.sql:60`. Existing run data has
   not been inspected; the reviewer's report about stored `on_done` data is
   not yet independently verified.
3. Current docs are stale: `docs/guide.md` still teaches `ctx.tx`, old
   MonitoringService names, and incomplete service construction; `README.md`
   still lists `is_wait`, `on_done`, and `ctx.tx`.
4. `src/keelflow/mcp/server.py:15-23` and
   `src/keelflow/admin/app.py:19-30` accept a DSN but construct services
   without `PostgresStorage`. Those services lazily call worker-global
   `_get_storage()`, which needs `KEELFLOW_DSN`.
5. `src/keelflow/engine/enqueue.py:80-85` implements
   `deduplication_key()`, but `src/keelflow/sdk/services.py:130-138` omits
   it from the public `JobBuilder` protocol.

The review reports an in-memory repro for items 1 and 4, but those repros
were not rerun in this read-only planning pass. `git show --check` reports
whitespace issues in the two storage-port commits: trailing whitespace in
the plan header and an extra blank line at EOF in `sdk/services.py`.

## Scope

In scope:

- Fix all five findings and update affected docs and schema comments.
- Add focused regression tests and type-check evidence.
- Inspect legacy trigger data safely before deciding if data migration is
  needed; keep the public API rename alias-free.
- Correct the reported commit whitespace issues in the fix commit.

Out of scope:

- Reintroducing `on_done`, `ctx.tx`, old monitoring names, or old builder
  method aliases.
- Running tests that truncate a shared database before its test DSN is
  confirmed dedicated and safe.
- Unrelated storage-port refactoring or release actions.

## Approach

### 1. Make `Session.begin()` preserve pending work

Check psycopg transaction status before opening a scope. Raise a clear
`RuntimeError` when the connection is not idle; do not call `rollback()`
to make it appear idle. Keep the existing nested-`begin()` rejection.
Add deterministic fake-connection tests for idle, pending implicit work,
and already-open UoW states. Confirm clean scopes still commit and raised
scopes still roll back using the existing isolated Postgres contract tests
when a safe test database is available.

### 2. Resolve persisted `on_done` trigger data

Before choosing compatibility or migration behavior, run a read-only count
against the configured database for workflow rows whose `trigger_config`
contains `on_done`. Do not use fixtures that run `TRUNCATE ... CASCADE` for
this preflight. Record the database identity and count, but do not expose
row payloads.

- If the count is zero, keep runtime behavior alias-free and update the
  schema comment and stale test descriptions.
- If rows exist, add a one-time, transactional data migration that renames
  the JSON key `on_done` to `on_succeeded` only when the new key is absent.
  Preserve an explicitly present `on_succeeded`; report affected-row
  count. Test migration idempotency and both-key conflict handling. Do not
  silently mark a callback complete or add a public API alias.

Do not implement the data-dependent branch until the preflight result is
known. If the target database cannot be safely identified, stop and ask
which database to inspect.

### 3. Pass explicit storage and UoW in admin and MCP

Construct `PostgresStorage(dsn)` and inject it into `MonitoringService`
and `PostgresAuditService`. Use storage-owned sessions and
`session.begin()` units for monitoring and audit operations, including the
admin retry action. Remove dependency on worker-global DSN state. Add tests
that call tools/routes with an explicit DSN while `KEELFLOW_DSN` is unset;
use mocked storage/session objects where possible, plus a safe integration
case if an isolated PostgreSQL test database is available.

### 4. Restore public SDK typing and repair docs

Add `deduplication_key(key: str) -> JobBuilder` to the SDK protocol. Add a
static typing test that uses the builder through `JobService`'s public
protocol.

Update `docs/guide.md` and `README.md` to show current construction with
`PostgresStorage`, `Session.begin()` and `UnitOfWork`, `ctx.uow` or
`ctx.execution.uow` as appropriate, current monitoring names, and renamed
spec fields. Rework direct SQL handler examples as Postgres-specific
repository examples using the documented `uow.native` adapter edge; do not
present SQL as the engine's portable transaction contract. Search for old
names across the current guide and README after edits.

### 5. Clean commit whitespace and validate

Remove the trailing whitespace and extra EOF blank line reported by
`git show --check`. Run focused unit and typing tests first. Run PostgreSQL
integration tests only after confirming `KEELFLOW_TEST_DSN` targets a
throwaway/dedicated database: current fixtures truncate `keelflow_*` tables.
Then run the safe full suite, examples, ruff, mypy, and diff checks, and
report any skipped PostgreSQL proof clearly.

## Risks and recovery

- **Data migration risk: high until preflight.** A callback key rewrite
  changes persisted records. Stop before migration if the database target,
  row count, or both-key conflict policy is unclear. Keep the migration
  transactional and idempotent; capture a pre-migration backup or an
  equivalent recoverable snapshot before applying it.
- **Transaction boundary risk: medium.** Rejecting pending implicit work
  may expose callers that relied on `begin()` to discard it. That is the
  accepted contract; update callers rather than restoring rollback behavior.
- **Test data risk: high.** Existing Postgres fixtures truncate tables.
  Never run them against an unverified shared or production database.
- **Recovery:** revert the focused fix commit if code behavior regresses.
  If a persisted-data migration ran, restore from the approved snapshot or
  run a separately reviewed reverse migration; do not infer recovery from
  the public API alias.

## Progress

- [x] Read repository workflow and inspect each cited implementation,
  contract, and documentation area.
- [x] Confirm the five code/documentation mismatches from current source.
- [x] Preflight on configured DB (`keelflow` @5432): 0 workflow runs,
  0 `on_done` keys → alias-free path taken, no data migration.
- [x] Implement focused fixes and regression proofs.
- [x] Full validation on the dedicated test database.

## Validation

- Fake-connection regression tests for session idle-state behavior.
- Mocked explicit-DSN admin/MCP tests with `KEELFLOW_DSN` removed.
- SDK static typing proof for `deduplication_key()`.
- Legacy trigger migration tests if preflight finds rows.
- Documentation search for obsolete names in `README.md` and `docs/guide.md`.
- With a confirmed dedicated Postgres test database: focused storage/MCP/
  admin tests, then repository test suites and demo tests.
- `uv run ruff check src examples tests`, `uv run mypy src/keelflow`,
  `git diff --check`, and `git show --check` on the resulting commit.

## Approval

User approved the plan ("implement plan"). Implementation completed
2026-11-24; see Result above.
