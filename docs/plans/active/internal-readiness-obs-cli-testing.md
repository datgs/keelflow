# Plan: Internal readiness — observability, CLI, testing toolkit

Date: 2026-11-24

## Status

Proposed — waiting for approval to implement.

## Outcome

Prepare Keelflow for safe internal adoption by HD projects. The sequence
starts with correctness, recovery, upgrade safety, and security boundaries;
only then adds operator and developer convenience.

1. **Readiness gate (T0):** state the actual durability/retry guarantees,
   prove crash/restart/recovery paths on isolated Postgres, test upgrades
   from a previous schema version, reject or enforce declared-but-unsupported
   settings, and document the minimum data/security boundaries for internal
   deployments.
2. **Postgres-first testing (T1):** provide isolated, guarded Postgres
   fixtures and recovery tests. An in-memory fake may later speed up pure
   unit tests, but never stands in for transaction, lock, isolation, or
   crash-recovery evidence.
3. **Observability (A1):** actionable signals for pending age, execution
   time, failures/DLQ, worker and scheduler health; structured fields;
   optional trace-context propagation; an app-mounted metrics exporter.
4. **CLI (A4):** a small `keelflow` command for worker, scheduler, admin,
   MCP, and explicit schema initialization/upgrade commands.

Positioning: internal first, public framework later. Do not add shared-table
multi-tenant isolation now. State clearly that `tenant_id` is context only,
not an isolation boundary; separate deployments/databases are the current
safe option where isolation is required. Admin auth may remain deferred only
for trusted-network use or a proxy with authentication, authorization, and
write protection. Public-beta packaging/matrix work remains a later gate.

## Context and evidence

- Logging today: `worker.py:42,176,522` uses `logging` +
  `LoggerAdapter` with `job_id`/`type`/`correlation_id` on the job
  logger only; metrics are a DB snapshot (`MonitoringService.metrics_snapshot`).
  There is no worker/scheduler heartbeat metric or execution-duration
  histogram yet.
- Entry points: `python -m keelflow.engine.worker`
  (`worker.py:528` → `run_worker`), `python -m keelflow.admin.app`,
  `python -m keelflow.mcp`; no console script. Schema has both
  `apply_async()` (`postgres/schema.py`) and Alembic revisions
  (`alembic/versions/`), so CLI must separate fresh initialization from
  versioned upgrade and must not report `CREATE IF NOT EXISTS` as upgrade.
- Kernel capability: `NativeKernel` documents missing rate limits,
  queue pause/resume, worker tags, priority and plugin metrics
  (`native_kernel.py:20-23`). SQLite is explicitly local/proof-level,
  not a production durability target (`sqlite_kernel.py:17-18`). Do not
  market either as feature-equivalent production backends.
- Storage port: `sdk/storage.py` + `tests/test_storage_contract.py`
  cover Postgres behavior. The current contract fixture truncates tables
  (`test_storage_contract.py:58-72`); it needs an explicit safety guard
  and isolated database before more destructive integration tests.
- `ConcurrencySpec.max_workers` is in the public spec but is not enforced
  (`README.md:162-163`). Make the current behavior explicit and fail fast
  or enforce before internal adoption.
- `spec/06-context.md:21-27` says permissions are snapshotted at enqueue;
  revoking permissions later does not change queued work. Tenant context
  is present but not an isolation boundary. External side effects (email,
  payment, webhook) cannot roll back with the UoW and need application-level
  idempotency.
- Inbox UI exists in `examples/invoice_flow/src/invoice_flow/app.py:271-285`;
  it is an example, not a reusable package.

## Scope

In scope:

- T0 readiness: recovery/upgrade evidence; accurate guarantees and limits;
  safe Postgres integration-test isolation; reject or enforce
  `ConcurrencySpec.max_workers`; security/data-handling guidance for admin,
  MCP, payload/context/audit, logs/traces, and permission snapshot behavior.
- T1 Postgres testing toolkit: disposable database fixture with a guard that
  refuses the configured default/shared DSN; transaction, locking, worker
  crash, database disconnect, concurrent retry, scheduler restart, outbox
  redelivery, migration-forward and backup/restore rehearsal proofs.
- A1 observability: actionable pending age, run duration, failures/DLQ,
  worker heartbeat and scheduler heartbeat/status; distinguish legitimate
  blocked human/external-wait jobs from stuck work. Structured log fields
  and trace-context propagation (W3C traceparent when available) across
  enqueue → worker → retry. Optional OTel instrumentation; app-mounted
  Prometheus exposition, no built-in server.
- A4 CLI: `worker`, `scheduler`, `admin`, `mcp`, plus separate `init-db`
  (fresh schema only) and `upgrade-db` (Alembic versioned migrations).
  `upgrade-db` checks the revision table and refuses to claim an upgrade
  from unversioned `CREATE IF NOT EXISTS` state without an explicit
  baseline/stamp procedure.
- Optional in-memory kernel/fake storage only for pure unit tests after
  T1. Keep it internal test support until teams show a need for a public
  `keelflow.testing` API. It is unit-test-only and cannot satisfy Postgres
  conformance, reliability, or recovery acceptance.

Out of scope:

- Implementing admin authentication, tenant isolation, or permission
  revocation for already queued jobs. Document deployment boundaries and
  known behavior instead.
- Prometheus/OTel server, dashboard, injectable clock, and time travel.
- Multi-queue/priority/rate limiting, saga compensation, shared inbox,
  webhook ingress helper, and MongoDB adapter.
- Public-beta package matrix, compatibility policy, and API reference;
  track as a later public-beta gate, not as implied internal readiness.

## Approach

### T0 — Reliability and security readiness gate (first)

1. Write a guarantee matrix: exactly-once applies only to participating
   state changes committed in one UoW; job execution, kernel delivery,
   and outbox delivery are at-least-once. Email, payment, webhook, and
   other external effects are not rolled back and require application
   idempotency keys/deduplication. State recovery automation and operator
   actions explicitly; update README examples and the runbook so no
   example implies exactly-once external effects.
2. Add deterministic Postgres integration proofs for worker death during
   handler work, DB disconnect/reconnect, concurrent retry, scheduler
   process restart, and outbox redelivery. Assert no lost intent, no
   permanently claimed job, correct rollback/claim recovery, and bounded
   duplicate effects only under idempotent handler behavior.
3. Test Alembic upgrade from the previous shipped revision with persisted
   job/run data. State workflow/handler versioning rule: old runs must
   keep resolving their recorded workflow/type version or upgrade must
   pause until the app deploys compatible registrations. Document backup,
   upgrade, and rollback limits; schema downgrade is not a substitute for
   restoring data.
4. Protect Postgres tests: require an explicit disposable test DB marker
   plus verify the target database identity before destructive setup.
   Fail closed on the default development DSN unless the caller explicitly
   confirms it is disposable. Prefer a CI-owned service database or
   per-test schema/database isolation; never truncate a shared database
   silently. Include a negative test proving the guard blocks an unsafe
   target before any `TRUNCATE` runs.
5. Resolve `ConcurrencySpec.max_workers`: until consistent enforcement
   exists, reject non-None values at registration or startup with a clear
   unsupported-capability error. Add tests for rejection under Chancy,
   Native, and SQLite; do not silently accept a no-op setting.
6. Add minimum security/data notes: admin is unauthenticated; only use
   on loopback or behind a trusted proxy that enforces authentication,
   authorization, and write protection. MCP is read-only but payload,
   context, and audit are sensitive; document who may access them and
   retention/deletion expectations. Logs/traces must not include payloads,
   secrets, or raw context by default. Permissions are enqueue-time
   snapshots. `tenant_id` is not isolation; use separate deployments or
   databases where tenant isolation is required.

### A1 — Observability (actionable signals first)

1. Define structured log fields: `job_id`, `job_type`, `run_id`,
   `step_id`, `correlation_id`, `tenant_id`, `worker_id`, `attempt`.
   Ensure error and terminal logs in worker/workflow/scheduler/events
   carry available fields, without logging payload/context.
2. Add metrics with clear units and labels: pending age, execution
   duration, retries/failures/DLQ count, worker heartbeat, scheduler
   last-success/heartbeat. Keep legitimate `blocked` human/wait steps
   separate from pending/stuck jobs. Add trace-context propagation across
   enqueue/worker/retry via W3C trace context; do not equate business
   `correlation_id` with trace context.
3. Optional OTel integration and Prometheus text output. Apps mount the
   exporter; no built-in listener. Test that sensitive values are absent
   from logs and metric labels.

### A4 — CLI

1. `src/keelflow/cli.py` with `argparse` (no new dependency):
   `worker`, `scheduler`, `admin`, `mcp`, `init-db`, and `upgrade-db`.
   Worker options include registry, kernel, DSN, queue, and prefix.
2. `init-db` is only for a fresh database. `upgrade-db` invokes Alembic,
   verifies known revision state, reports current/target revision, and
   refuses unknown/unversioned databases until an explicit baseline
   procedure is supplied. No command calls `apply_async()` and labels it
   a versioned upgrade.
3. `--registry module:attr` resolves a `Registry` instance or zero-arg
   factory with clear import/attribute/factory diagnostics.
4. Add `[project.scripts] keelflow = "keelflow.cli:main"`; do not expose
   SQLite as production-supported. Show its proof/local-only status in
   `--help` and docs.

### T1 — Postgres-first test toolkit

1. Provide a test-owned disposable Postgres database/schema fixture with
   identity checks and explicit destructive-operation opt-in. Add
   transaction/recovery tests from T0 and reuse the existing conformance
   suite against real Postgres.
2. Provide `keelflow.testing.postgres()` to wire registry, storage,
   kernel and services against that isolated database. It must not point
   at or truncate the normal developer database by default.
3. Only after Postgres proofs are safe and stable, consider a small
   `InMemoryKernel`/fake storage for pure unit tests. Label it
   non-conformant and unit-only; do not run Postgres conformance claims
   against a fake. `drain()` is optional and needs demonstrated test
   value before it enters the public package.

## Risks and recovery

- **Recovery proof may expose design defects (high):** keep T0 as a hard
  gate. Do not label the framework internally ready until observed
  recovery behavior matches the guarantee matrix.
- **Migration and old workflow versions (high):** run upgrades on copied
  prior-version data; preserve handler/workflow registrations until old
  runs terminate or define an explicit migration. Do not downgrade schema
  blindly after data writes.
- **Destructive tests (high):** dedicated database identity plus a
  fail-closed guard; no implicit `TRUNCATE` against shared DBs.
- **Security/privacy (high):** logs, traces, MCP and admin can reveal
  payload, context and audit data. Allowlist telemetry fields and document
  network/auth boundaries before internal deployment.
- **Unsupported configuration (medium):** `max_workers` must be enforced
  or rejected, not silently ignored. Kernel feature gaps must be shown as
  capabilities, not hidden behind a common interface.
- **Testing fake drift (medium):** in-memory tools remain optional unit
  helpers, never the proof for storage, lock, isolation or recovery.

Recovery: commits are separate and revertible. Database migrations and
recovery rehearsals require a disposable copy and backup before changes.

## Work graph

### T0 — Reliability, upgrade, and security gate

- **Goal:** define accurate guarantees and prove safe recovery and upgrade
  behavior before operator convenience work.
- **Required outcomes:** guarantee matrix; isolated Postgres recovery tests
  (worker death during handler, disconnect/reconnect, concurrent retry,
  scheduler restart, outbox redelivery); Alembic upgrade from previous
  revision with persisted jobs/runs; fail-closed destructive-test guard;
  reject unsupported `max_workers`; security/data handling policy for
  admin, MCP, tenant context, permissions, and telemetry.
- **Constraints:** run migrations/recovery only on test-owned copies;
  don't call `apply_async()` a versioned upgrade; don't claim exactly-once
  for external side effects.
- **Acceptance:** every guarantee links to executable proof or an explicit
  limitation/operator action; migration preserves representative live data;
  unsafe test DSN is blocked before writes; unsupported config errors on all
  kernels.
- **Verification:** focused fault-injection and migration tests, then full
  Postgres suite; inspect test output for safety guard proof.
- **Ownership:** worker/retry/scheduler/outbox recovery tests, Alembic test
  harness, `ConcurrencySpec` validation, README/runbook/security docs.
- **Dependencies:** none. Output contract: approved guarantee matrix,
  migration policy, destructive-test safety rule, and safe telemetry fields.

### T1 — Postgres-first test toolkit

- **Goal:** make Postgres integration and recovery tests safe and easy to
  run for HD project teams.
- **Required outcomes:** database identity check and explicit disposable
  opt-in; test-owned schema/database lifecycle; helper wiring registry,
  Postgres storage, kernel, and services; existing storage conformance tests
  run only against isolated Postgres.
- **Constraints:** never replace Postgres lock/isolation/recovery evidence
  with an in-memory fake; don't truncate a shared developer database.
- **Acceptance:** a deliberately unsafe DSN fails before any DDL/DML;
  isolated tests can create, use, and clean only test-owned state.
- **Verification:** run the negative safety test, isolated contract tests,
  recovery tests, and examples that use the fixture.
- **Ownership:** `keelflow.testing.postgres` and test fixtures; no engine
  behavior changes.
- **Dependencies:** T0's safety rule. Output contract: documented fixture
  API and isolated test database lifecycle.

### T-A1 — Actionable observability

- **Goal:** operators can distinguish stuck work from valid waits and see
  failures and process health without reading payload data.
- **Required outcomes:** pending age; execution duration; retry/failure/DLQ
  measures; worker heartbeat; scheduler last-success/heartbeat; blocked
  human/external-wait counts separate from pending; safe structured fields;
  trace-context propagation; optional OTel and app-mounted Prometheus output.
- **Constraints:** no payload, secrets, or raw context in logs/traces; no
  built-in metrics server; no hard OTel dependency.
- **Acceptance:** metrics correctly distinguish waiting from stuck; trace
  context survives enqueue→worker→retry; captured logs/traces contain only
  approved fields.
- **Verification:** metric golden tests, log capture/privacy tests, trace
  propagation tests with and without optional OTel installed.
- **Ownership:** observability module, worker/workflow/scheduler/outbox
  instrumentation, metrics storage queries, docs.
- **Dependencies:** T0 guarantee matrix and telemetry allowlist.

### T-A4 — Safe CLI and migration commands

- **Goal:** provide simple operational commands without hiding schema state.
- **Required outcomes:** worker/scheduler/admin/MCP commands; fresh-only
  `init-db`; versioned `upgrade-db` with current/target revision reporting;
  registry loader with clear errors; support status shown for each kernel.
- **Constraints:** no upgrade-success claim from `CREATE IF NOT EXISTS`;
  unknown/unversioned DB state fails closed until explicit baseline
  procedure; SQLite remains local/proof-only.
- **Acceptance:** init rejects a non-empty DB; upgrade advances a prior
  Alembic revision while preserving representative rows; unknown revision
  state is refused; CLI help accurately lists kernel capabilities.
- **Verification:** parser/loader unit tests and CLI subprocess tests against
  disposable Postgres copies.
- **Ownership:** `keelflow.cli`, console script, Alembic invocation, docs.
- **Dependencies:** T0's migration/baseline policy.

### Optional T2 — In-memory unit helpers

- **Goal:** speed pure unit tests only if internal teams demonstrate a need.
- **Constraints:** remain private/internal by default; cannot claim storage
  conformance, portability, transaction isolation, or recovery.
- **Acceptance:** helpers simplify a real handler/service unit test and are
  clearly separate from Postgres integration proofs.
- **Dependencies:** T1 complete; implementation may be dropped if no proven
  use case.

Critical path: T0 → T1; T-A1 and T-A4 can start after T0 and run in
parallel if ownership stays separate; optional T2 follows T1. Each work
item is committed separately. Public-beta readiness is a later plan:
wheel install, Python/Postgres support matrix, dependency compatibility,
API/deprecation policy, API reference, and specialist security review.

## Validation

- T0: recovery matrix with executable Postgres tests; Alembic upgrade
  from prior revision preserves representative jobs/runs; destructive
  fixture refuses an unsafe target before writes; `max_workers` fails
  clearly on every kernel until consistently supported; docs distinguish
  same-UoW exactly-once state from at-least-once delivery and external
  idempotency; permissions are enqueue-time snapshots; tenant isolation
  and admin/MCP data access boundaries are explicit.
- T1: disposable Postgres fixture proves commit/rollback, concurrent
  claims/retries, worker crash recovery, scheduler restart, outbox
  redelivery and backup/restore procedure. Fake tests do not replace
  these proofs.
- A1: capture log records and assert required safe fields; metrics
  distinguish blocked human/wait work from stuck pending work; trace
  context crosses enqueue/worker/retry; assert payload/context/secrets
  never appear in logs, traces, or metric labels.
- A4: installed `keelflow --help`; registry loader errors; `init-db`
  only on empty disposable DB; `upgrade-db` advances Alembic revision
  from prior-version fixture and refuses unknown/unversioned state.
- Optional fake: pure unit examples only; report separately from Postgres
  conformance and recovery results.
- Repo proof: `uv run pytest -q`, `uv run ruff check src examples tests`,
  `uv run mypy src/keelflow`, `git diff --check`. Report CI support
  matrix limits honestly.

## Decisions and review notes

- 2026-11-24: Internal adoption comes before public beta. Internal
  readiness is a correctness, recovery, upgrade, and security gate —
  A1/A4 convenience work cannot substitute for it.
- 2026-11-24: Postgres integration and recovery tests are the source of
  durability evidence. In-memory helpers are optional unit tools only.
- 2026-11-24: `tenant_id` is not tenant isolation; separate deployments
  or databases are the safe current choice where isolation is required.
  No shared-table tenancy implementation in this plan.
- 2026-11-24: Admin auth, shared inbox, saga, webhook helper, and MongoDB
  remain use-case driven. A reverse proxy alone is not an auth control;
  the deployment runbook must require authentication, authorization,
  and write protection if admin is exposed beyond loopback.
- 2026-11-24: Schema initialization and versioned migration are distinct
  CLI operations. `CREATE IF NOT EXISTS` is not an upgrade proof.
- 2026-11-24: `ConcurrencySpec.max_workers` must be rejected until
  supported consistently; SQLite stays local/proof-level, Native stays
  experimental with documented capability gaps.
- 2026-11-24: Public beta is a separate future gate: wheel installation,
  supported Python/Postgres matrix, dependency compatibility, API and
  deprecation policy, API reference, and security review.

## Approval

Plan updated from the supplied review; awaiting approval to implement.
