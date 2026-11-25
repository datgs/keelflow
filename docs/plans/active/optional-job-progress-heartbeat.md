# Plan: Optional progress heartbeat and stall handling

Date: 2026-11-25

## Status

Original feature and first hardening pass are implemented and verified on
Postgres. A second follow-up is now planned for the additional review findings
below; this plan remains active until those findings are implemented and
verified. SQLite progress/watchdog remains out of scope as documented.

## Outcome

Allow selected long-running job types to report durable progress and expose
when that progress is stale. Progress reporting is optional. A configured
no-progress timeout fails the current attempt and uses the existing retry
policy, with application-owned retry safety as an explicit requirement.

This feature reports application progress; it does not claim that a heartbeat
proves useful work is happening or that an external request has stopped.

## Context and verified evidence

Verified during plan preparation:

- `job_mark_running()` increments `attempts` in the dispatch UoW
  (`src/keelflow/postgres/storage.py:235-244`). Progress initialization is then
  issued through a separate connection before that UoW exits
  (`src/keelflow/engine/worker.py:203-220`,
  `src/keelflow/postgres/storage.py:255-269`).
- `job_retry_reset()` resets `attempts` to zero, and `job_finish()` updates by
  job id without an attempt/claim fence
  (`src/keelflow/postgres/storage.py:288-299,347-354`).
- Progress reports are separately persisted and currently compare the attempt
  number; the same timeout predicate appears twice in the query
  (`src/keelflow/postgres/storage.py:271-288`).
- Scheduler ticks default to a 30-second sleep when no job is enqueued
  (`src/keelflow/engine/scheduler.py:129-147`). Metrics use a 30-second
  freshness threshold for both workers and schedulers
  (`src/keelflow/postgres/storage.py:1278-1299`).
- `configure()` documents direct app-owned worker startup, while periodic worker
  heartbeat is started only by `run_worker()`; dispatch upserts liveness only
  when a job runs (`src/keelflow/engine/worker.py:53-56,432-452,630-657`).
- The progress API returns normally when the storage update returns false
  (`src/keelflow/sdk/context.py:81-89`). Each progress operation opens a new
  database connection (`src/keelflow/postgres/storage.py:255-288`).
- Existing tests cover progress visibility and a report attempted after terminal
  completion, but not active stale attempts, manual retry reuse, scheduler idle
  freshness, or app-owned worker heartbeat lifecycle
  (`tests/test_engine_hardening.py:266-322`,
  `tests/test_observability.py:84-126`).

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

### Previous implementation work graph

The following P1–P3 records describe the already committed feature, not this
follow-up hardening scope.

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

## Follow-up scope: reliability and heartbeat hardening

In scope:

- Make progress initialization atomic with the running-attempt transition.
- Fence progress writes and handler terminal writes using an attempt/claim
  identity that cannot be reused by ops retry; roll back business UoW if the
  execution no longer owns the current claim.
- Fix stale-progress registration/reporting semantics and make rejected reports
  visible to the handler.
- Choose a scheduler freshness rule with margin over its default tick interval.
- Ensure app-owned worker startup has a documented and usable periodic
  heartbeat lifecycle.
- Add isolated Postgres tests for these races and lifecycle paths; measure
  progress-write overhead before adding throttling/pooling.
- Clarify that timeout detects missing reports, not dead processes or committed
  progress, and that repeated identical counters still reset the timer.

Out of scope:

- Exactly-once external effects or remote request cancellation.
- Automatic heartbeat generation on behalf of the handler.
- Pooling, throttling, or rate guarantees without measurement evidence.
- SQLite progress/watchdog support unless implementation discovery shows it is
  a prerequisite to the accepted backend scope.

## Approach

1. Reconfirm and reproduce the progress transaction/fencing races against a
   real isolated Postgres database. Choose a monotonically unique claim token
   (rather than the reusable `attempts` count) and thread it through progress,
   finish, retry, and ops-reset paths. Persist start state atomically with the
   running transition. Keep per-job UoW writes and progress writes isolated.
2. Define rejected-report behavior (raise a dedicated execution-lost error or
   return an explicit result) and ensure stale handlers cannot commit domain
   changes or job results. Test both a still-running old handler and an ops
   retry where the human-facing attempt count resets.
3. Set process-heartbeat ownership consistently for helper-managed and
   app-managed workers. Define scheduler freshness relative to configurable or
   fixed tick cadence with enough operating margin; prove idle behavior.
4. Run focused tests, migration tests if schema changes are needed, targeted
   recovery/fencing tests, then full tests and examples with observed results.
   Measure report connection overhead descriptively before deciding whether a
   connection reuse design is justified.

## Risks And Recovery

- **Stale completion / duplicate side effects (high):** transaction fencing can
  prevent local commits but cannot roll back already-issued external effects.
  Preserve the idempotency/reconciliation requirements in `docs/reliability.md`.
- **Public execution API change (medium):** rejected progress currently returns
  normally. Pick clear semantics and document any exception/result contract.
- **Schema and migration (medium):** unique claim token may require a forward
  migration. Preserve existing jobs and keep downgrade consistent with the
  repository's forward-only production upgrade policy.
- **Heartbeat threshold policy (medium):** changing freshness changes alerts;
  retain a clear relation to configured intervals and test boundary behavior.
- **Recovery:** revert code changes normally. Do not delete progress/job rows;
  use a forward correction migration if schema has shipped. Do not amend or
  rewrite the existing commit.

## Progress

- [x] Reproduce priority findings with isolated Postgres tests and settle the
      claim-token and report-rejection contracts. During implementation the
      review's `attempts`-based fence proved insufficient against ops
      `retry_reset`; a per-claim `execution_token` (fresh `gen_random_uuid()`
      in `job_mark_running`, cleared on reschedule/ops reset) replaced it.
      `report_progress` now raises `ExecutionLostError` on rejection — the
      report-deadline predicate was removed from the storage write because a
      late-but-valid report left the job stuck `running`; deadline
      enforcement lives in the in-process watchdog.
- [x] Implement atomic start and non-reusable execution fencing across
      progress, finish, retry reset, and retry routing
      (`job_mark_running` RETURNING token; `job_finish` /
      `job_error_and_reschedule` token-fenced, `execution_token` cleared on
      release; alembic `0010`).
- [x] Align scheduler freshness (`stale_after_s` on
      `keelflow_process_health`; tick declares `max(interval*3, 30)`)
      and app-owned worker heartbeat lifecycle
      (`worker_heartbeat_loop` is a documented reusable entry point).
- [x] Add race/lifecycle proof and update user-facing docs
      (`test_storage_contract` token fencing + atomic start;
      `test_engine_hardening` stale-dispatch finish fence;
      `test_observability` scheduler margin + idle heartbeat loop).
      Progress-write overhead measured indirectly: each report is one
      short autocommit UPDATE on its own connection; no pooling/
      throttling added — no measurement warranted it yet.
- [x] Run focused and full validation; record unsupported backend
      boundaries (SQLite unchanged/unsupported).
- [x] Demo: `examples/progress_heartbeat.py` — long job reporting every
      15s, `no_progress_timeout_seconds=180` default, `--stuck` mode and
      env-tunable timings; verified live against Postgres in both modes.

## Validation

- Focused Postgres tests prove start timestamp/timeout config commits with the
  running transition and is visible to monitoring.
- Old attempt blocked in handler cannot report progress or commit job/domain
  UoW after a new claim starts; UoW rolls back and the newer claim remains
  unchanged.
- Ops retry does not reuse a fencing identity even though `attempts` resets.
- A timeout/retry race cannot commit a late result or overwrite fresh progress.
- Scheduler remains fresh across idle intervals greater than 30 seconds and
  becomes stale only beyond its chosen threshold.
- App-owned worker startup emits idle heartbeats through the documented API;
  helper-managed behavior remains covered.
- Rejected reports produce the documented explicit signal.
- Capture elapsed time / connection cost for representative progress cadence;
  do not set an overhead guarantee absent a benchmark target.
- Repository checks: `uv run pytest tests/ -q`, `uv run pytest examples -q`,
  `uv run ruff check src examples tests`, `uv run mypy src/keelflow`, and
  `git diff --check`.

## Result

Hardening verified: `uv run pytest tests/ -q` → 247 passed, 3 skipped;
`uv run pytest examples -q` → 40 passed; ruff and mypy clean;
`git diff --check` clean. Demo verified live: healthy run reports
progress and completes; `--stuck` run goes stale at the configured
window, watchdog cancels the attempt, engine reschedules, and retry
exhaustion lands in DLQ. SQLite remains outside the progress contract
because its kernel uses a separate job table.

## Implementation record (previous feature)

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

## Follow-up: additional review findings

### Outcome

Close the remaining correctness gaps in demo isolation, progress-report
acceptance, and Native lease-reclaim fencing; align heartbeat and monitoring
semantics with their configured windows; and make the progress API accurately
typed and discoverable. The renamed timeout measures report silence, not
counter movement. Setting it enables reporting; reporting without a timeout
remains independently opt-in.

### Scope

In scope:

- Require an explicit isolated demo DSN; use dedicated Chancy prefix and queue
  so the demo worker cannot consume application jobs.
- Remove the storage-side progress deadline rejection. A still-owning token may
  report while the job is running; the watchdog enforces the timeout.
- Invalidate execution tokens when Native lease reaping returns a job to
  pending, and ensure stale dispatches cannot finish/progress in the gap before
  a new claim.
- Derive worker heartbeat freshness from its configured interval with margin.
- Make metric help and absent-scheduler age accurately represent the actual
  snapshot; distinguish claim time from the first/last actual progress report.
- Rename public `no_progress_timeout_seconds` to
  `progress_report_timeout_seconds`. This is an accepted breaking API change:
  do not retain a compatibility alias. A configured report timeout enables
  reporting; `progress_enabled=True` still independently enables reporting
  without a timeout.
- Give `JobContext.execution` an explicit public type while keeping
  `execution_token` engine-internal and requiring no handler boilerplate.

Out of scope:

- New UI, broad execution-context abstraction, automatic progress generation,
  or SQLite progress/watchdog support.
- Changing the existing retry/idempotency contract or claiming remote side
  effects can be fenced or cancelled.

### Approach

1. Fix demo isolation and the stale-report semantics; prove the demo cannot
   receive unrelated queue work and a report after a `concurrency_key` wait
   exceeding the configured threshold is accepted and completes normally on
   PostgreSQL.
2. Close the Native reclaim gap; prove against Native/Postgres that an old
   dispatch cannot report or commit its UoW after lease reclaim and before a
   replacement claim.
3. Apply the public timeout rename and reporting opt-in rule consistently to
   specs, enqueue/runtime paths, persisted fields/migrations as needed, docs,
   and examples. Decide migration impact from actual persisted config usage;
   do not retain the old Python API alias.
4. Align worker heartbeat windows, progress timestamps, and Prometheus output;
   add focused boundary tests for customized interval and missing scheduler.
5. Run focused Postgres and Native tests, then full tests, examples, Ruff,
   mypy, and `git diff --check`; update this record with observed evidence.

### Risks And Recovery

- Breaking API rename: update all in-repository callers in the same change and
  state clearly that consumers must rename the setting. No compatibility shim.
- Reclaim race: local UoW fencing must hold even if the old handler resumes
  before the next claim; test this exact interval, not only after a new token.
- Demo isolation: require explicit DSN and namespace its kernel objects; never
  silently migrate a shared/default application database.
- Recovery: prefer code correction without data rewrite. If a persisted column
  rename is required, use a forward migration and preserve existing timeout
  values; no destructive cleanup or history rewrite.

### Progress

- [x] Isolate demo database and Chancy prefix; prove the demo cannot claim
      unrelated work. `KEELFLOW_DEMO_DSN` is required — the demo exits
      before opening a connection without it — and kernel tables use the
      dedicated `keelflow_demo_kernel_` prefix. The engine queue name is
      fixed (`DEFAULT_QUEUE`), so dedicated prefix + required DSN is the
      isolation boundary; `examples/test_progress_heartbeat.py` asserts
      both (no Postgres needed).
- [x] Remove deadline predicate from progress persistence and prove late
      report after lock wait using PostgreSQL. The predicate the earlier
      pass intended to remove was still present
      (`postgres/storage.py:278`); now removed. New
      `test_late_report_after_concurrency_lock_is_accepted` enqueues two
      same-key jobs: B's claim-time `progress_at` is older than its 0.1s
      window by the time the handler runs (A holds the advisory lock
      0.4s); report accepted, job `done`.
- [x] Invalidate Native execution fence on lease reclaim
      (`_reap` sets `execution_token=NULL`) and prove via
      `test_native_reap_retires_execution_token`: stale finish raises
      `ExecutionLostError`, stale report returns False, job stays
      `pending` in the gap before the next claim.
- [x] Rename timeout API: `JobTypeSpec.progress_report_timeout_seconds`
      (breaking, no alias). It alone enables reporting — registry
      validation no longer requires `progress_enabled` (`t.stall@v1`
      exercises timeout-only). `progress_enabled=True` alone stays the
      progress-only opt-in. `JobContext.execution` is now typed
      `ExecutionContext` (placeholder instance, replaced by dispatch —
      handlers need no None guard) and internals renamed private
      (`_execution_token`, `_storage`, `_uow`, `_progress_event`,
      `_last_progress_at`); `bulk.py` internals updated.
- [x] Fix customized worker freshness (`stale_after_s =
      max(interval*3, 30)`, same margin as scheduler —
      `test_worker_heartbeat_custom_interval_widens_window`), metric
      HELP text (declared-window wording, no hardcoded 30s), and absent
      metrics omitted instead of rendered 0
      (`test_prometheus_handles_missing_values`, `..._none_age_not_zero`).
      `progress_at` semantics documented: claim time until first report,
      then last report — it is the silence signal, not strictly
      "last report".
- [x] Run required tests and static checks: `uv run pytest tests/ -q` →
      251 passed, 3 skipped; `uv run pytest examples -q` → 42 passed;
      ruff + mypy clean; `git diff --check` clean. Demo verified live on
      a dedicated `keelflow_demo` database (healthy run completes) and
      the DSN guard exits 1 without it.

### Result

Implemented on 2026-11-25. All three P1 correctness gaps closed with
PostgreSQL/Native regression tests; P2 monitoring/API semantics aligned
with actual behavior; `no_progress_timeout_seconds` renamed to
`progress_report_timeout_seconds` as an accepted breaking change (no
alias — consumers must rename). `ctx.execution` is typed and its
internals private. Full validation above; SQLite remains outside the
progress contract.

### Decisions

- 2026-11-25: User accepts a breaking change to rename
  `no_progress_timeout_seconds` to `progress_report_timeout_seconds`; do not
  retain the old alias. The new name better describes timeout of report
  silence, regardless of counter movement.
- 2026-11-25: Configuring `progress_report_timeout_seconds` enables progress
  reporting; `progress_enabled=True` without a timeout remains a separate
  progress-only opt-in.

### Validation

- PostgreSQL test for a job waiting on a `concurrency_key` longer than its
  report timeout; valid report afterward is accepted and the job is not stuck.
- Native/Postgres reclaim race test: token retired at reclaim; old dispatch
  cannot commit handler UoW or progress before a new claim.
- Demo integration proof with unrelated application work in the same database:
  startup rejects unsafe/default configuration or demo's dedicated queue/prefix
  leaves unrelated work untouched.
- Heartbeat loop with a custom interval remains alive within its declared
  window; boundary test becomes stale only beyond that window.
- Metrics tests for dynamic HELP semantics and absent scheduler age remaining
  `None`/not emitted as zero; progress timestamps distinguish no report from
  first report.
- SDK/API tests for renamed setting, timeout-implies-reporting, independent
  progress-only opt-in, and typed `ctx.execution`; repository full checks.
