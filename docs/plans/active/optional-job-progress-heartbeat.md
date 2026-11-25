# Plan: Optional progress heartbeat and stall handling

Date: 2026-11-25

## Status

Implemented for Postgres-backed dispatch (Chancy and Native kernel paths).
SQLite progress persistence/watchdog remains unsupported because SQLiteKernel
maintains a separate job table from the shared PostgresStorage contract.

## Outcome

Allow selected long-running job types to report durable progress and expose
when that progress is stale. Progress reporting is optional. A configured
no-progress timeout fails the current attempt and uses the existing retry
policy, with application-owned retry safety as an explicit requirement.

This feature reports application progress; it does not claim that a heartbeat
proves useful work is happening or that an external request has stopped.

## Context and verified evidence

- `JobTypeSpec` already has opt-in per-job execution controls such as
  `timeout_seconds` and `cooperative_cancel` (`src/keelflow/sdk/specs.py:110-119`).
- `ExecutionContext` is injected per dispatch and currently exposes
  `checkpoint()` for cooperative cancellation (`src/keelflow/sdk/context.py:22-59`).
- The handler executes within a UoW and `timeout_seconds` wraps its coroutine;
  a timeout rolls back that handler UoW and follows the failure/retry path
  (`src/keelflow/engine/worker.py:211-239,301-320`). This cannot undo an
  already-issued external effect.
- Process liveness is separate from job progress: workers heartbeat every 10s
  and are counted alive for 30s freshness (`src/keelflow/engine/worker.py:547-590`,
  `src/keelflow/postgres/storage.py:1231-1243`).
- Reliability policy says external effects are not rolled back and need
  application idempotency (`docs/reliability.md:20-27`).
- `spec/02-job-lifecycle.md:83-87` says handler-level heartbeat was removed;
  worker liveness is kernel-owned and progress/cancellation responsiveness uses
  explicit checkpoints. This feature must define a distinct opt-in progress
  contract and update that spec if accepted.
- Chancy is the production kernel; Native and SQLite have their own lease and
  recovery paths. Any timeout contract must not be mistaken for process/kernel
  liveness or silently differ by backend.

## Scope

In scope:

- Optional progress reporting selected per job type.
- Structured progress data: `current`, `total`, optional short message, and
  optional JSON metadata, following the user's confirmed preference.
- Durable latest-progress and last-reported timestamp visible in monitoring.
- Optional no-progress timeout. When configured, expiry fails that attempt and
  invokes the existing `RetryPolicy`; configuring it asserts that retries are
  safe under the job's application-owned idempotency/reconciliation contract.
- Explicit guidance for safe checkpoints, transactional outbox, stable external
  idempotency keys, ambiguous external outcomes, and manual reconciliation when
  the provider cannot deduplicate or report the result.
- Postgres behavior proof, and backend-specific proof where the shared contract
  applies.

Out of scope:

- Heartbeating every job by default.
- Treating a reported heartbeat as proof of liveness, correctness, or committed
  business progress.
- Guaranteeing an external provider stopped processing after local timeout.
- Engine-managed compensation or exactly-once external effects.
- Retrofitting progress reporting into all current examples/job types.

## Decisions

- Progress reporting is opt-in per job type; jobs without progress configuration
  retain current behavior and incur no per-job progress writes.
- Progress payload uses structured counters (`current`, `total`), optional
  message, and optional JSON metadata.
- No-progress handling is opt-in. When configured, a stale timeout fails the
  current attempt and relies on the existing retry policy.
- Enabling automatic retry after a no-progress timeout is an application
  assertion that replay is safe. Side effects must use a stable idempotency key,
  transactional outbox plus receiver/provider deduplication, or an explicit
  reconciliation strategy. If an external outcome is ambiguous and cannot be
  deduplicated or reconciled, the job must not be configured for blind automatic
  retry; alert/manual handling is required.
- Progress is reported only at meaningful application checkpoints. For work
  split across commits, progress advances only after a committed or otherwise
  verifiable unit. Progress itself is observational and does not commit the
  handler's business transaction.
- A local timeout cannot guarantee remote cancellation. The timeout records a
  failed attempt; it does not assert that an in-flight external request stopped.

## Approach

1. **Set the shared contract.** Define the per-job opt-in settings, handler API,
   progress data validation/size limits, persistence ownership, and monitoring
   fields. Inspect Chancy, Native, and SQLite execution boundaries so the
   behavior is consistent or backend limitations are explicit. Keep progress
   disabled by default.
2. **Persist progress independently.** Progress must be visible while a long
   handler is still running, so it cannot depend only on the handler's final
   UoW commit. Define attempt/claim fencing so an old or timed-out worker cannot
   overwrite progress from a newer attempt. Avoid storing secrets, payloads, or
   unbounded metadata.
3. **Add opt-in stale-progress failure.** For configured jobs only, detect a
   missing progress update beyond the configured interval, cancel/stop the
   local attempt safely, roll back its active UoW, and send it through the
   existing failure/retry policy. No watchdog is created for jobs without the
   setting. Align timeout handling with kernel lease/recovery and late-result
   fencing across supported backends.
4. **Expose operational evidence.** Make latest progress, report timestamp/age,
   attempt identity, and configured timeout available through the existing
   monitoring surface. Distinguish process heartbeat, job progress, and job
   terminal status. Document that application-reported progress is not an
   independent proof of committed work.
5. **Prove and document retry safety.** Add tests for progress visibility before
   completion, opt-out compatibility, a progressing job, stale-progress
   failure, retry/DLQ behavior, cancellation and UoW rollback, and stale-attempt
   fencing. Add documentation for stable external idempotency keys, outbox and
   receiver deduplication, safe checkpoint placement, and manual reconciliation
   for ambiguous external effects.

## Work graph

### P1 — Define and persist the progress contract

- **Goal:** expose a small optional handler API and durable latest-progress
  record for an executing job.
- **Required outcomes:** default-off per-job configuration; validated structured
  data (`current`, `total`, optional message and JSON metadata); progress and
  report time visible before handler completion; attempt/claim fencing; bounded
  writes and metadata size; safe treatment of messages/metadata as untrusted
  application data.
- **Constraints:** do not log or export arbitrary metadata by default; no
  progress writes for unconfigured jobs; progress updates do not join or commit
  the handler business UoW.
- **Acceptance evidence:** Postgres integration test observes a progress update
  while the handler is still blocked; an old attempt cannot overwrite a newer
  attempt; a job with progress disabled behaves as before.
- **Verification:** focused context/worker/storage/monitoring tests and schema
  migration tests; ruff and mypy.
- **Ownership:** `src/keelflow/sdk/specs.py`, `src/keelflow/sdk/context.py`,
  storage/schema/migration, worker, and focused tests.
- **Dependencies:** none. Output contract: accepted progress API and storage
  record semantics for P2/P3.

### P2 — Implement opt-in no-progress timeout and retry routing

- **Goal:** fail only configured attempts that exceed their no-progress window,
  then use the existing retry policy.
- **Required outcomes:** timeout is disabled by default; configured timeout
  detects lack of progress; timed-out handler UoW rolls back; retry attempt is
  fenced against late progress/results from the old attempt; retry exhaustion
  reaches the normal failed/DLQ path; Chancy, Native, and SQLite behavior is
  consistent or the unsupported scope is explicit.
- **Constraints:** do not claim remote request cancellation; do not retry
  external effects safely by assumption; enabling timeout is the application's
  assertion that idempotency/deduplication/reconciliation makes replay safe.
- **Acceptance evidence:** deterministic tests for progress within timeout,
  timeout without progress, UoW rollback, retry and exhaustion, late completion
  fencing, and no watchdog for default/unconfigured jobs.
- **Verification:** focused worker/recovery tests for each applicable backend,
  then full test suite.
- **Ownership:** dispatcher/watchdog, kernel integration/fencing, retry path,
  tests, and backend-specific tests.
- **Dependencies:** P1 contract.

### P3 — Expose progress and publish the safety contract

- **Goal:** operators can distinguish a live process, a running job, and a job
  that has stopped reporting progress.
- **Required outcomes:** monitoring includes latest progress, report age, and
  job/attempt identity; docs explain semantics and safe retry requirements;
  lifecycle spec no longer ambiguously says handler-level heartbeat was removed
  without distinguishing progress from process liveness.
- **Constraints:** no high-cardinality metrics labels or arbitrary metadata
  export; no sensitive payload/context in logs or metrics.
- **Acceptance evidence:** monitoring tests assert current and stale progress
  independently of process heartbeat; docs include an example of a safely
  idempotent retry and an ambiguous external outcome that requires manual
  reconciliation.
- **Verification:** monitoring/observability tests, documentation checks, full
  repository validation.
- **Ownership:** monitoring/observability, `docs/reliability.md`,
  `spec/02-job-lifecycle.md`, guide and tests.
- **Dependencies:** P1 and P2 output contracts.

Critical path: P1 → P2 → P3. The contract spans dispatch, persistence, retry,
and monitoring, so implementation should be integrated rather than parallelized
across shared schema and worker code.

## Risks and recovery

- **Duplicate external effects (high):** a timed-out coroutine may have sent a
  remote request even if local UoW rolls back. Mitigate with stable idempotency
  keys, outbox/receiver deduplication, or reconciliation; otherwise do not opt
  into automatic retry.
- **Stale worker overwrites (high):** old attempts may report after a retry
  starts. Fence progress writes and terminal updates by current attempt/claim.
- **False stall detection (medium):** a valid long computation may not emit
  progress frequently. Timeout is opt-in and is an application-selected
  contract; progress cadence and timeout must be chosen together.
- **Database write overhead (medium):** updates can add load. Keep disabled by
  default and avoid excessively frequent writes; establish observable limits in
  implementation tests/measurements rather than promising a rate now.
- **Migration (medium):** progress persistence may require schema changes. Add a
  forward migration, preserve existing rows, and keep old jobs valid with
  progress disabled.
- **Recovery:** revertible code changes; schema rollback must follow repository
  migration policy. Never delete job/progress records as part of recovery.

## Validation

- Focused positive proof: configured handler reports progress and monitoring
  sees it before handler completion; updating job completes successfully.
- Focused negative proof: no report past configured timeout causes one failed
  attempt and existing retry behavior; stale attempt updates are rejected; an
  unconfigured job has no watchdog and retains current behavior.
- Side-effect proof: a test handler uses a stable idempotency key across retry
  and produces one domain/external-effect record; document that this does not
  prove every external provider's semantics.
- Backend proof: Postgres is required for persistence/transaction behavior;
  Chancy/Native/SQLite lifecycle tests cover the contract where implemented.
- Repository checks: `uv run pytest tests/ -q`, `uv run pytest examples -q`,
  `uv run ruff check src examples tests`, `uv run mypy src/keelflow`, and
  `git diff --check`.

## Implementation record

- Added default-off `progress_enabled` and optional
  `no_progress_timeout_seconds` settings. Timeout requires progress opt-in.
- Added bounded structured progress reporting through
  `ctx.execution.report_progress(current, total, message=..., metadata=...)`;
  metadata is capped at 8 KiB and messages at 256 characters.
- Persisted latest progress, report time, and configured timeout in Postgres,
  with writes on a separate connection and attempt-number fencing. Monitoring
  exposes progress fields on job reads and a stale-progress query.
- No-progress expiry cancels the local handler task, rolls back its UoW, and
  follows the existing retry/DLQ path. It cannot stop remote work already in
  flight.
- Updated lifecycle and reliability documentation. Added progress visibility,
  retry, UoW rollback, SDK validation, and migration proof.
- SQLite uses an independent job table, so shared Postgres storage cannot
  persist the API or enforce the timeout there yet; Chancy and Native rely on
  the common Postgres job row and worker dispatcher.
- Validation: 34 focused tests passed; `uv run ruff check src examples tests`,
  `uv run mypy src/keelflow`, and `git diff --check` passed. Full tests and
  examples exceeded the 120-second command windows and were terminated. One
  initial full-suite attempt failed in Native kernel tests amid cross-test DB
  state/cleanup failures. Full-suite status remains unresolved.

## Approval

- User approved the mitigation direction: fail-and-retry only when explicitly
  configured, with application-owned idempotency/reconciliation; use stable
  idempotency keys, transactional outbox plus receiver deduplication, and
  meaningful committed/verifiable progress checkpoints; ambiguous effects need
  manual reconciliation rather than blind retry.
- Implementation was explicitly requested on 2026-11-25. User selected all-backend intent, 8 KiB metadata, 256-character messages, required integer counters, and `0 <= current <= total`.
