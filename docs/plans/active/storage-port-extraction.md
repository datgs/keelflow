# Backend-Neutral Storage And Transaction Ports

**Status**: proposed — waiting approval to execute  
**Date**: 2026-11-23

## Outcome

Replace the engine's PostgreSQL-specific persistence and `ctx.tx` SQL contract
with semantic storage operations and a backend-neutral unit-of-work boundary.
PostgreSQL remains the only implementation in this project phase. The resulting
contract must be a credible foundation for a future MongoDB storage + kernel
adapter, without claiming MongoDB support or cross-database atomicity now.

The required atomic unit is: application-owned domain writes, Keelflow state
changes, and dispatch enqueue either commit together or roll back together when
they use one compatible backend transaction. For a MongoDB adopter, this means
the domain repository, Keelflow storage, and kernel must join the same MongoDB
session/transaction (and MongoDB's transaction prerequisites must be met).
There is no distributed transaction guarantee when domain data and Keelflow
state live in different databases.

## Context And Authority

- `docs/architecture-design-brief.md:54,99` makes transactional durability a
  core rule: durable work intent commits in the same transaction as the data
  change that created it. Sections 3.3 and 5 apply the same rule to events,
  state transitions, and audit records.
- `src/keelflow/sdk/context.py:26-61` exposes a handler transaction cursor and
  runs cancellation checkpoint SQL through it.
- `src/keelflow/sdk/services.py:128-138` defines `Transaction` as
  `execute(query)` + `cursor()`, so it is a SQL-shaped contract, not a portable
  transaction abstraction. `Storage` at `:215-222` is a separate incomplete
  protocol.
- `src/keelflow/engine/kernel.py:63-67` passes a cursor to push operations.
  Enqueue paths in `engine/enqueue.py`, `workflow.py`, `bulk.py`,
  `statemachine.py`, and `scheduler.py` rely on that same PostgreSQL transaction.
- PostgreSQL persistence is not limited to the ten engine modules previously
  counted: SDK checkpoint code, example handlers, audit, cancellation, wait
  signals, monitoring callers, and kernel push participation are also relevant.
  The previous 144-call-site count is a partial inventory, not an acceptance
  target.
- Existing PostgreSQL implementation: `src/keelflow/postgres/storage.py` has
  one working method (`defer`) and four incomplete methods. It must be unified
  with the new port, not left as a second contract.
- Existing accepted decision `docs/decisions/0001-kernel-chancy.md` chooses
  Chancy, while the completed kernel-port work added Native and SQLite kernel
  implementations. The storage project must preserve current behavior for all
  supported kernel selections.

## Scope

### In scope

- One semantic persistence port for all Keelflow-owned durable state: jobs,
  workflow runs/steps, chunks/row status/archive, human tasks, events/outbox,
  audit, scheduler state, and operational reads/actions.
- One backend-neutral unit-of-work / transaction contract shared by storage,
  kernel dispatch enqueue, SDK services, and handlers.
- Remove SQL execution from engine and SDK core. Database-specific queries live
  in the PostgreSQL adapter.
- Replace direct `ctx.tx.execute()` / cursor use in shipped examples with the
  new application repository/service + unit-of-work pattern. Keep PostgreSQL
  examples backed by PostgreSQL repositories; do not promise that arbitrary
  application SQL is portable.
- Postgres behavior and atomicity regression tests, plus operation-level
  storage contract tests.
- Document adapter composition and the conditions under which atomicity holds.

### Out of scope

- Implement MongoDB or claim compatibility with a MongoDB deployment.
- Distributed transactions across separate databases, or automatic bridging
  between a PostgreSQL domain store and MongoDB Keelflow state.
- Translate SQL strings/SQLAlchemy-like generic query builders into Mongo
  operations. The port abstracts behavior, not query syntax.
- Replace every application's domain repository. Only shipped examples are
  migrated to demonstrate the new contract.

## Architecture Direction

Use two related but distinct contracts:

1. **Storage operations** — semantic methods for Keelflow-owned records and
   invariants. PostgreSQL-specific SQL is private to `PostgresStorage`.
2. **Unit of work** — backend-owned transaction context that can be passed to
   Storage and Kernel operations and to application repositories. It owns
   commit/rollback; nested work follows explicit savepoint/session semantics.
   Core code does not require SQL `execute()` or `cursor()`.

A future backend may expose its native transaction/session only through an
adapter-owned repository integration point; that capability must not become a
mandatory SQL-shaped method on the common unit-of-work protocol. The exact
capability handoff, transaction ownership, nesting behavior, and how an
application repository participates are T1 design outputs and must be validated
against current Postgres and kernel behavior before implementation. Do not
invent a universal database query API.

The public handler contract is intentionally in scope for change: handlers use
application services/repositories that accept the unit of work rather than
issuing SQL through `ctx.tx`. `ctx.execution.checkpoint()` becomes a semantic
operation, not a query. Kernel `push_ex`/`push_many_ex` receive the transaction
contract rather than a psycopg cursor, with each backend adapter ensuring its
queue record participates in the same transaction as storage writes.

## Work Graph

### T0 — Complete persistence and transaction-boundary inventory

**Goal**: make the full extraction boundary evidence-based before locking the
port design.

**Ownership**: read-only discovery and plan update across `src/keelflow/`,
`examples/`, tests, and docs.

**Required outcomes**:
- Inventory every raw query/driver access and every transaction open,
  commit, rollback, savepoint, or caller-owned transaction path in core SDK,
  engine, adapters, kernel adapters, and shipped examples.
- Classify each operation by behavior and atomicity participants (domain,
  Keelflow records, dispatch/kernel rows), including external connection
  ownership and lifecycle.
- Identify behavior-level test coverage and current rollback/failure semantics.
- Produce a compact matrix in this plan (or linked discovery note if it becomes
  too large); correct the prior counts and list explicit exclusions.

**Acceptance evidence**: every direct DB use found by repository search maps to
an inventory category or a named out-of-scope use; no assumed API list is
accepted as complete without the sweep.

**Verification**: static searches plus review of all matches; no runtime changes.

### T1 — Define and prove the backend-neutral contracts

**Goal**: settle one Storage port, one UnitOfWork contract, and the composition
boundary among Storage, Kernel, and application repositories.

**Ownership**: SDK contracts, engine kernel/storage ports, PostgreSQL adapter
interface. No broad SQL move yet.

**Required outcomes**:
- One canonical Storage protocol and one implementation owner; remove the
  duplicate/stub contract rather than expanding both `sdk.services.Storage`
  and a second engine protocol.
- Define transaction lifecycle/ownership, commit/rollback, nesting/savepoint
  behavior, cancellation, and how errors unwind.
- Define how Storage and Kernel participate in the same UoW without exposing
  psycopg cursors in shared interfaces.
- Define the application repository participation pattern and where a
  Postgres-specific native connection/session is allowed (adapter/application
  edge only).
- Define semantic operation contracts from the T0 inventory, including
  results, idempotency, locking/serialization, pagination, and error behavior.
  Avoid speculative methods and generic CRUD.
- Document a backend capability boundary: which semantics must be provided by
  every adapter, and which optional features (e.g. advisory-key serialization,
  change-stream wakeup) may be backend-specific.
- Preserve the accepted same-transaction durability rule. State explicitly
  that atomicity only spans resources joined to the same backend transaction.

**Acceptance evidence**:
- Contract review against at least one complete dispatch path, one workflow
  advancement path, one bulk fan-in path, one caller-owned enqueue path, and
  handler domain write + finish path.
- Positive and rollback tests on PostgreSQL prove all participants commit and
  roll back together, including kernel enqueue.
- No core contract method accepts SQL text, cursor, or a PostgreSQL connection.

**Verification**: focused tests for transaction lifecycle and rollback; static
search verifies the agreed shared protocol surface has no SQL/driver types.

**Gate**: do not proceed to T2 until the contract works for all three existing
kernel adapters (Chancy, Native, SQLite where supported) or the plan records a
specific compatibility boundary and user approval.

### T2 — Extract PostgreSQL persistence behind the contracts

**Goal**: move Keelflow-owned SQL behind `PostgresStorage` without behavior
change.

**Ownership**: PostgreSQL adapter + engine/SDK persistence call sites; do not
change product semantics.

**Required outcomes**:
- Move SQL by behavior groups discovered in T0, not by guessed method names or
  file counts. Keep transaction boundaries and locks equivalent.
- Cover job lifecycle, retry/cancel/signal, workflow/step advancement, bulk
  partition/fan-in/archive, state machine/human tasks, scheduler/outbox/events,
  audit, and monitoring operations found in scope.
- Migrate cancellation checkpoints to a storage semantic operation.
- Ensure SQL is confined to the PostgreSQL adapter/schema/migrations and
  explicitly allowed Postgres-specific operational code; no SQL in SDK/engine
  business logic.
- Preserve Chancy, Native, and SQLite kernel behavior through the shared UoW
  contract without leaking their transaction-specific APIs into core.
- Update shipped example handlers to use repository/service abstractions with
  the UoW, replacing direct `ctx.tx` SQL and cursor access.
- Add an architecture boundary check only after the exact allowed scope is
  accepted. It must scan SDK, engine, examples, and adapters precisely, allow
  SQL inside the PostgreSQL adapter, and prove both an allowed and forbidden
  example.

**Acceptance evidence**:
- Existing test suites and demo suites behave as before.
- Targeted atomicity tests verify application write + engine finish/workflow
  update + kernel enqueue roll back/commit together.
- Search-based boundary check proves no SQL-shaped transaction contract or
  direct SQL in core and shipped handlers.

**Verification**: focused group tests after each cohesive group; then
`uv run pytest -q`, demo adapter suites, `uv run ruff check src examples tests`,
`uv run mypy src/keelflow`, `git diff --check`, and all relevant kernel
contract tests.

### T3 — Storage contract and PostgreSQL conformance suite

**Goal**: codify behavior-level semantics independently of PostgreSQL query
shape.

**Ownership**: storage contract tests and PostgreSQL implementation.

**Required outcomes**:
- Tests assert accepted behavior, not SQL implementation details: atomicity,
  terminal transitions, idempotency, fan-in, row lifecycle, workflow
  serialization, event/audit writes, pagination, retry/cancel, and rollback.
- Separate pure semantic conformance cases from PostgreSQL-specific integration
  cases (e.g. advisory lock implementation details).
- Exercise positive and negative cases only where expected behavior is defined
  by current accepted rules/tests. Do not invent “block or fail politely”
  alternatives in a conformance contract.
- Register PostgreSQL as the sole current Storage implementation. Do not call
  single-adapter tests proof of cross-backend portability.
- Document how a future MongoDB implementation would register and what it must
  prove, without marking skipped Mongo tests as portability evidence.

**Acceptance evidence**: each T0 operation group maps to semantic conformance
or an explicit adapter-specific test/exclusion; the suite passes on PostgreSQL.

## Risks And Recovery

| Risk | Level | Evidence / mitigation |
|---|---|---|
| Public handler and transaction contract change breaks app code | High | User approved a coordinated breaking change (no compatibility/deprecation phase). Migrate all in-repository handlers/examples in scope; publish a migration note and make the new contract explicit. Do not keep a SQL-shaped `ctx.tx` alias. External consumers must migrate when upgrading this contract. |
| Atomicity cannot span unrelated databases | High | Accepted architecture rule is same-transaction durability; require same-backend UoW, explicitly reject distributed atomicity claims. |
| Kernel adapters cannot share a portable transaction handle | High | T1 gate checks all current kernels before broad extraction; stop and revise contract if cursor coupling cannot be removed without behavior loss. |
| SQL extraction changes lock order or rollback behavior | High | T0 transaction map, per-group regression tests, rollback/failure tests, same PostgreSQL integration flows. |
| Broad adapter boundary check over-blocks SQL at app edge | Medium | Define allowed surface first; positive and negative proof required; no bare grep by `execute(` alone. |
| Mongo semantics differ (transactions require deployment prerequisites; document model, locking, change streams differ) | High | No Mongo support claim in this phase; future adapter has separate design and integration plan. |

**Recovery**: keep each behavior group independently reviewable. If a group
regresses, stop the extraction at that boundary and restore the last verified
adapter/core pair; do not leave a half-migrated shared transaction contract.
Because public handler contract migration is involved, rollback includes the
matching SDK and example contract together, not only individual SQL files.

## Progress

- [x] Re-reviewed initial proposal; found SQL-shaped `Transaction`, duplicate
  `Storage` contracts, incomplete transaction ownership, and a partial SQL
  inventory.
- [x] Confirmed user intent: include the `ctx.tx` contract change and maximize
  backend abstraction to enable a future MongoDB implementation.
- [x] T0 complete inventory and transaction map — `storage-port-inventory.md`
  (6 participation modes P1-P6, 8 operation groups B1-B8, per-path commit
  boundaries, T2 guard scope, open questions for T1). Key findings: prior 144
  count was literal-sites-only; real surface ~200 calls including SDK context,
  monitoring, DLQ. `conn.transaction()` is used as both fresh TX and savepoint
  — UoW nesting must be explicit. `pg_stat_activity` is a PG-only monitoring
  metric (capability boundary). Read-path API shape (uow vs conn vs
  `storage.reader()`) is a T1 open item.
- [x] T1 contract decision and proofs — `sdk/uow.py` (`Tx`/`Session`/
  `UnitOfWork`: `native` adapter edge + `nested()` savepoint scope),
  `sdk/storage.py` (semantic `Storage` protocol — jobs, runs/steps, bulk,
  sm/audit, outbox, ops, dlq; supersedes `sdk.services.Storage` which was
  deleted), `postgres/storage.py` (`PgSession`/`PgUnitOfWork` + first ops),
  `tests/test_uow_contract.py` 6 proofs: commit visibility, full rollback
  of storage+outbox+repo write, savepoint child-only rollback, non-idle
  begin() rejection, chancy push_ex rollback inside uow, native
  dispatch_queued push inside uow. Signature flip `tx → uow` lands with
  T2 (mypy stays green until callers migrate). Kernel-push compatibility
  gate: psycopg conn via `uow.native` + dict_row cursor works for chancy;
  native flag push works; sqlite consumes `uow.native` aiosqlite conn
  (verified at T2 signature flip, trivially satisfied).
- [ ] T2 PostgreSQL extraction and example migration.
- [ ] T3 behavior-level conformance suite.

## Decisions

- 2026-11-23: Include changing the public handler transaction contract; user
  confirmed the phase should abstract `ctx.tx` as much as practical for a future
  MongoDB adapter.
- 2026-11-23: Use a coordinated breaking change (option A): replace the
  SQL-shaped `ctx.tx` contract without a compatibility/deprecation phase. Migrate
  in-repository examples and document the migration; do not retain a SQL alias.
- 2026-11-23: No MongoDB implementation in this phase. Future atomicity requires
  domain repository, Keelflow storage, and kernel enqueue to share one Mongo
  transaction/session; no cross-database atomicity is promised.
- 2026-11-23: Keep semantic operations, not SQL/dialect translation, as the
  persistence abstraction. Exact interface remains a T1 output, not a guessed
  method list.
- 2026-11-23 (T1): Contract shape decided — `Session` owns a connection
  (`begin()` → `UnitOfWork`, `key_lock(key)`, `close()`); `UnitOfWork` is
  one atomic boundary with `nested()` savepoint scopes; every storage op
  takes `uow` (reads included — Mongo session reads need it). `uow.native`
  is the opaque adapter edge (psycopg conn / aiosqlite conn / future Motor
  session): kernel `push_ex` and app repositories narrow it, engine/SDK
  core never touches it. Ops never commit — `begin()` owns the boundary.
  `begin()` raises on a non-idle session (no silent commit of stray
  implicit-TX work). Monitoring reads migrate to `uow` params too.
  `pg_stat_activity` stays a PgStorage-internal optional metric.
- 2026-11-23 (T1): Signature flip `Transaction → UnitOfWork` deferred to
  T2's mechanical sweep (keeping T1 mypy-green); `push_ex(uow, spec)`
  target recorded — adapters narrow `uow.native` (chancy needs a
  `dict_row` cursor, sqlite uses `.execute`).

## Validation Strategy

- T0 static inventory and source review.
- T1 contract tests for commit, rollback, nested scope/savepoint behavior,
  cancellation, and atomic participation by Storage + Kernel + app repository.
- T2 behavior regressions for each operation group and all current kernel
  selections where supported.
- Final repository proof: `uv run pytest -q`, `uv run ruff check src examples tests`,
  `uv run mypy src/keelflow`, `git diff --check`, demo adapter tests, and kernel
  contract tests. Report actual counts/results at execution time; historical
  counts are not fixed acceptance values.
- Any architecture guard requires a known allowed positive case and a
  deliberately forbidden negative case that fails for the intended diagnostic.

## Approval

User approved the scope direction (include `ctx.tx` contract change for future
MongoDB adaptability) and selected a coordinated breaking change with no
compatibility/deprecation phase. This approves the public-contract policy only;
implementation still awaits explicit execution approval after review of the
revised plan.
