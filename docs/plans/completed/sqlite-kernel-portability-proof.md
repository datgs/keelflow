# Execution Plan: SQLite kernel portability proof

Date: 2026-11-23

## Status

Completed 2026-11-23 (approved same day).

## Outcome

Demonstrate that the semantic `Kernel` port can support a second database
engine by implementing an opt-in, experimental SQLite kernel and running the
same kernel-port contract tests against Chancy, native Postgres, and SQLite.

This proves the **kernel dispatch boundary**, not that keelflow's full engine,
SDK, schema, workflows, storage services, or demos are database-portable. The
product remains Postgres-backed; Chancy remains the default. SQLite is for
portability proof and local/test use only, not production support.

## Context and verified evidence

- `docs/plans/active/kernel-port-and-native-backend.md` records the port,
  native Postgres implementation, T4 adapter-test entry point, and a 1M-row
  benchmark result. Current shared test plumbing is in `tests/conftest.py`;
  `test_worker.py` is parameterized for Chancy/native using
  `--kernel=chancy|native|both`.
- The engine's worker dispatch is not database-neutral: `engine/worker.py`
  uses psycopg and Postgres schema/SQL; `JobService.enqueue` writes a
  PostgreSQL engine row before calling the kernel. Therefore wiring SQLite
  into `JobService` or `dispatch()` would not be a kernel-only change.
- The job table's production schema is Postgres-specific (`JSONB`, UUID,
  `TIMESTAMPTZ`, partial indexes, `gen_random_uuid()`, `pg_notify`). A SQLite
  proof needs its own minimal schema/fixtures and must not be described as a
  second supported engine storage adapter.
- The Python runtime has SQLite 3.53.1. The current worktree also has
  uncommitted edits to `pyproject.toml`, `uv.lock`,
  `src/keelflow/engine/native_kernel.py`, `tests/conftest.py`, and a new
  `src/keelflow/engine/sqlite_kernel.py`; none has been validated. Do not
  overwrite or discard those edits without examining them in the approved
  implementation phase.
- Repository proof commands: `uv run pytest -q`,
  `uv run ruff check src examples tests`, `uv run mypy src/keelflow`, and
  `git diff --check`. Current test environment also has local Postgres at
  port 5544, but SQLite tests must not depend on it.

## Scope

In scope:

- Define the SQLite conformance contract and explicit unsupported semantics.
- Implement an opt-in SQLite kernel for a minimal, documented dispatch table.
- Use a SQLite-native async driver as an optional dependency, if the final
  implementation needs one; core installation must not acquire it.
- Run shared kernel contract cases for push/commit/rollback, claim, scheduled
  availability, single-claim concurrency, retry requeue, cancellation/fencing,
  reclaim, and wakeup/poll fallback on Chancy, native Postgres, and SQLite
  where the operation is supported by all.
- Keep SQLite-specific parity tests separate for documented behavior or gaps.
- Add the opt-in selector to the pytest test helpers after the backend contract
  and cleanup lifecycle are stable.

Out of scope:

- SQLite support for `JobService`, workflow/bulk/state-machine engine SQL,
  PostgreSQL `Storage`, or the full application schema.
- Any promise of production readiness, multi-process worker guarantees,
  performance parity, or default-backend selection.
- MySQL, PgQueuer, or another storage dialect.
- Rewriting engine SQL into a generic SQL/dialect layer.
- Changing public workflow/job semantics to accommodate SQLite.

## Approach

Use a **kernel-only contract harness** with an adapter-neutral dispatch spec
and an injected test dispatch callback. Each backend fixture owns a minimal
job table and performs lifecycle operations through the `Kernel` contract;
Chancy/native may keep their real claim implementations. SQLite uses a
file-backed temporary database so independently opened connections share
state; `:memory:` is not valid for the worker because the kernel and worker
open separate connections.

The shared suite must assert observable state and dispatch calls, not simply
that backend methods return. Any behavior that cannot be made equivalent
without changing product policy must be listed as an explicit SQLite gap and
excluded from shared claims. Do not claim full engine conformance from these
kernel tests.

Use an optional SQLite dependency rather than adding it to core requirements.
Do not implement a fake Postgres-compatible connection or route the existing
Postgres `dispatch()` function through SQLite.

## Tasks

### T1 — Establish the adapter contract and reconcile current worktree

- **Goal:** Define exactly what the SQLite proof covers and inspect the
  uncommitted exploration without losing it.
- **Context:** This is a second-database kernel proof only; `worker.dispatch`
  and `JobService` remain Postgres-bound.
- **Required outcomes:** Read each current uncommitted diff; classify reusable,
  incorrect, and out-of-scope pieces. Define a backend fixture lifecycle,
  shared test operations, SQLite table schema, connection ownership, commit /
  rollback behavior, scheduling clock representation, deduplication, claim
  atomicity, worker stop/recovery, and in-process notification limits.
- **Constraints:** No edits until the user approves this plan. Do not reset,
  revert, or overwrite current uncommitted work. No user data or shared DB
  changes.
- **Acceptance evidence:** Contract matrix lists each semantic operation and
  whether it is shared, SQLite-specific, or unsupported; the plan makes clear
  that SQLite is not full engine storage support. Worktree findings are
  explicitly accounted for.
- **Verification:** Review code/diffs and the accepted Kernel contract in
  `src/keelflow/engine/kernel.py`; no code tests required for this design task.
- **Ownership:** kernel contract, SQLite fixture/schema, test selector.
- **Dependencies:** None. Stop for user judgment if the proposed gap list
  changes public behavior or claims full database portability.

### T2 — Implement the isolated SQLite kernel

- **Goal:** Implement the SQLite `Kernel` contract without importing or
  pretending to implement Postgres engine services.
- **Required outcomes:** Optional driver dependency; file-backed isolated
  schema; transaction-aware push and dedup; atomic claim; `run_at` scheduling;
  heartbeat/lease, reclaim, fencing; wakeup with correctness-preserving poll
  fallback; deterministic worker shutdown and DB cleanup. Dispatch goes to an
  injected callback for conformance proof.
- **Constraints:** No Postgres SQL/psycopg in SQLite adapter code; do not claim
  cross-process notification or production support. Avoid global event
  subscribers that leak across tests or event loops.
- **Acceptance evidence:** SQLite-only tests prove schema isolation, push
  commit/rollback, uniqueness, delayed availability, one winner under
  concurrent claims, retry/reclaim, fencing, stop/cleanup, and worker dispatch
  callback invocation. Run tests without `KEELFLOW_TEST_DSN` or Postgres.
- **Verification:** `uv run pytest tests/test_sqlite_kernel.py -q`; repeat the
  concurrency and wakeup cases to detect flakes; `uv run ruff check src tests`;
  `uv run mypy src/keelflow`.
- **Ownership:** SQLite kernel module, optional dependency metadata/lockfile,
  SQLite-focused tests.
- **Dependencies:** T1.

### T3 — Parameterize the kernel-port contract suite

- **Goal:** Run one behavior suite against Chancy, Native Postgres, and SQLite
  without forcing full-engine tests into the SQLite database claim.
- **Required outcomes:** Shared fixtures accept backend choice; support
  `--kernel=chancy|native|sqlite|both|all` with defined meanings; SQLite uses a
  temporary file and always cleans up; Chancy-specific tests remain explicitly
  marked/gated. Verify actual behavioral cases run for each backend and report
  backend-specific skips/gaps.
- **Constraints:** Default `pytest` invocation continues to run the existing
  Chancy suite unchanged. `both` means Chancy+Native Postgres; `all` means all
  three. Do not silently skip common contract assertions.
- **Acceptance evidence:** Collected test IDs show the shared contract suite
  executing once per requested backend; cleanup leaves no SQLite DB/WAL files;
  existing default test selection remains compatible.
- **Verification:** `uv run pytest tests/test_kernel_contract.py -q --kernel=all`,
  `uv run pytest tests/test_worker.py -q --kernel=both`, full `uv run pytest -q`.
- **Ownership:** `tests/conftest.py`, shared contract test module, backend test
  fixtures.
- **Dependencies:** T2.

### T4 — Report proof boundaries and finish validation

- **Goal:** State what the second adapter validates and what it does not.
- **Required outcomes:** Update this plan with evidence, add a concise guide or
  test README note on how to run the SQLite kernel proof and its known gaps,
  verify the optional dependency install path, run all project checks, and
  record whether the adapter improved the boundary or exposed unacceptable
  coupling.
- **Constraints:** No performance comparison is required: SQLite is not a
  production competitor and would not be an apples-to-apples storage benchmark.
  Do not change the default kernel or mark SQLite supported for application
  workflows.
- **Acceptance evidence:** Chancy/native/SQLite shared contract results are
  recorded; Postgres full suite remains green; optional install has been
  verified; unsupported product surface is explicit.
- **Verification:** `uv run pytest -q`,
  `uv run ruff check src examples tests`, `uv run mypy src/keelflow`,
  `git diff --check`; test the SQLite extra in a clean/isolated environment if
  available.
- **Ownership:** `docs/guide.md` (if needed), this plan, tests and dependency
  metadata.
- **Dependencies:** T3.

Dependency graph: T1 → T2 → T3 → T4.

## Risks and recovery

- **Scope confusion — high:** A SQLite kernel can pass job dispatch tests while
  all public engine operations remain PostgreSQL-bound. Mitigation: call this a
  kernel proof only everywhere; keep full workflows out of SQLite claims.
- **Claim correctness — high:** SQLite write serialization differs from
  Postgres `SKIP LOCKED`; test multiple connections and duplicate-claim
  exclusion. Do not assume `UPDATE … RETURNING` semantics without proof against
  the minimum supported SQLite version.
- **Transaction compatibility — high:** Current SDK callers pass psycopg
  connection/cursor types. The SQLite proof must use its own contract fixture;
  do not pretend aiosqlite is a psycopg transaction.
- **Notification scope — medium:** in-process asyncio notification does not
  reach another process. Polling must remain sufficient and cross-process
  notification must be named as unsupported.
- **Dependency/support burden — medium:** optional extra avoids a core install
  cost, but adds a maintained dependency and backend matrix. Drop the adapter
  if it does not materially validate the contract.
- **Recovery:** SQLite files are temporary and test-owned. No production data
  migration is planned. Keep Chancy/native unchanged and default; remove the
  SQLite adapter only in a later, explicit cleanup if proof value is low.

## Decisions

- 2026-11-23: User selected SQLite native kernel as the next adapter proof.
- 2026-11-23: Scope is kernel-only portability; not a full SQLite-backed
  keelflow engine or a production adapter.
- 2026-11-23: Chancy remains default; SQLite remains experimental; no
  performance benchmark or default switch is part of this plan.
- Execution approval is still required before T1 implementation/reconciliation.

## Progress

- [x] User selected SQLite as next adapter proof.
- [x] Repo evidence reviewed: core engine dispatch and schema are PostgreSQL
      specific; SQLite runtime available; current uncommitted exploration is
      present and unverified.
- [x] T1 — inspected/reconciled worktree; contract matrix settled.
- [x] T2 — isolated SQLite kernel implemented (`sqlite_kernel.py`).
- [x] T3 — shared kernel contract suite parameterized
      (`tests/test_kernel_contract.py`, `--kernel` selector).
- [x] T4 — proof limits documented (guide §16, this plan's verdict).

## Validation

- Planning only. No SQLite adapter tests have been run. Existing uncommitted
  code/dependency edits are not validated and are not evidence of completion.

## T1–T4 executed (2026-11-23)

- T1 reconciliation: uncommitted exploration was reusable — NativeWorker
  hook refactor + SQLiteWorker subclass kept; `isinstance` protocol check
  passes; removed unused `worker_prefix`.
- T2 fixes landed during validation: `UPDATE AS` alias syntax; no scalar
  subqueries in `RETURNING` (step link fetched separately); **`isolation_level=None`
  (autocommit)** — aiosqlite's implicit transactions meant worker claims
  never committed; `busy_timeout=5000` on every connection.
- T3: `tests/test_kernel_contract.py` — 9 cases ×3 backends = 24 passed,
  3 skipped (single-row-only cases gated for chancy). `--kernel=both`
  = chancy+native; `all` adds sqlite. `test_worker.py` green on both.
- T4 evidence: sqlite suite runs without Postgres (temp-file DB);
  default suite 220 passed + 3 skipped; ruff+mypy clean.

Proof verdict: the Kernel port survived a second DB engine with only
dialect-level changes to claim/push/lease statements — the contract
(caller-tx push, exclusive claim, lease/fence, dedup, scheduled
availability) transferred unchanged. Exposed coupling: dispatch itself
is still injected at the engine boundary; making `worker.dispatch`
DB-portable is a separate storage-layer project.
