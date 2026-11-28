# Plan: Optional progress heartbeat and stall handling

Date: 2026-11-25

## Status

Progress reporting and the two prior hardening passes are implemented and
verified. The Native + Chancy reliability pass was implemented in commit
`a3aca0c`, but review found unresolved correctness gaps. The reliability work
and a gated switch to NativeKernel as the default are planned below; this plan
remains active until the reviewed guarantees have executable evidence and the
backend migration behavior is documented. SQLite progress/watchdog remains out
of scope as documented.

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

### Result (progress API follow-up)

Implemented on 2026-11-25. All three P1 correctness gaps closed with
PostgreSQL/Native regression tests; P2 monitoring/API semantics aligned
with actual behavior; `no_progress_timeout_seconds` renamed to
`progress_report_timeout_seconds` as an accepted breaking change (no
alias — consumers must rename). `ctx.execution` is typed and its
internals private. Full validation above; SQLite remains outside the
progress contract.

## Follow-up: Native + Chancy reliability pass

### Outcome

Provide behavior-level recovery evidence for both production-relevant
PostgreSQL worker paths: NativeKernel and the default Chancy adapter. Prove
claim ownership, recovery after transient and prolonged database connection
loss, orderly shutdown and lease retention, retry exhaustion after abrupt
worker death, deduplication during active execution, and idle queue behavior.
Expose whether workers are genuinely consuming eligible work, not only whether
a process heartbeat is fresh. Report recovery time as observed components and
measurements, not a fixed SLA without evidence.

### Scope

In scope:

- Fence Native work across the entire claim→dispatch boundary, before
  `job_mark_running()` can mint an execution token. A stale dispatch must not
  steal a newer claim whether it arrives before or after the replacement claim.
- Count a successful claim against `max_attempts`, as approved on
  2026-11-25, including a worker dying after claim but before handler start.
  Repeated crash/reclaim must end in a durable failed state, DLQ record, and
  workflow terminalization where applicable; those writes must be atomic.
  Apply equivalent externally visible semantics to Chancy despite its own
  broker-attempt counter and claim lifecycle.
- Keep extending leases for in-flight Native jobs while graceful shutdown has
  stopped new claims and drains handlers. At drain timeout, cancel and await
  actual task completion before closing maintenance/DB resources; then make
  unfinished jobs safely reclaimable. Verify ordinary and over-lease drains.
- Make both Native and Chancy worker DB-loss recovery explicit. For each,
  exercise a short outage, outage longer than one job lease, and one-way
  connection loss. After connectivity returns, workers must reconnect and
  resume claims; existing jobs must have consistent state and new jobs must
  not remain stranded.
- Fix Native idle claim polling if runtime measurement confirms continuous
  empty claim queries. Retain prompt wakeup and bounded poll fallback.
- Make `recover(conn=...)` and `_reap()` invalidate the same claim fence.
- Prove concurrent duplicate enqueue while the original job is running remains
  one live job/one execution across enqueue→kernel push for Chancy and Native.
- Test shutdown, claim/recovery, workflow/DLQ consistency, and one idempotent
  business effect together, not only isolated status updates.
- Add/clarify a metric or monitoring signal for age of eligible pending work
  (queue starvation); heartbeat alone is not evidence of consumption.

Out of scope:

- SQLite production reliability guarantees, exact-once external effects, or
  claiming that database fencing prevents already-issued remote side effects.
- A fixed recovery-time SLA before measurements define a service objective.
- Unbounded retries or changing the approved claim-counts-as-attempt policy.
- Automatically patching or forking Chancy without first proving the exact
  failure in the pinned 0.26.0 dependency and identifying a supported recovery
  seam.

### Evidence and current assessment

User reviewed code at commit `2fd3339`; the following are code findings or
reported upstream cases, not yet reproduced as Keelflow failures:

- **VERIFIED code gap — Native claim/dispatch ownership:**
  `NativeWorker._claim_once()` sets `claimed_by` and `lease_until`, then
  `_spawn()` schedules `dispatch(job_id, ...)` without a claim generation
  (`src/keelflow/engine/native_kernel.py:225-257`). Later,
  `PostgresStorage.job_mark_running()` accepts `pending`, `claimed`, or
  `running` by id and does not validate `claimed_by`/claim generation
  (`src/keelflow/postgres/storage.py:236-257`). This leaves a pre-token
  dispatch race not covered by current post-start fencing tests.
- **VERIFIED code gap — Native shutdown:** `stop()` cancels both claim and
  maintenance tasks before waiting for in-flight dispatches; maintenance is
  what renews their leases (`native_kernel.py:274-287,334-348`). A drain longer
  than a lease may allow a second worker to reclaim while the old handler is
  still executing. Timeout path sends `task.cancel()` but must prove completion
  before releasing resources.
- **VERIFIED code gap — Native idle polling:** `_claim_loop()` continues
  unconditionally whenever capacity exists, regardless of whether
  `_claim_once()` found any rows (`native_kernel.py:200-206,225-250`). Runtime
  query rate and impact have not yet been measured.
- **VERIFIED code gap — recovery hook:** `_reap()` clears `execution_token`,
  but `recover(conn=...)` does not (`native_kernel.py:319-370`).
- **VERIFIED code gap — crash retry budget:** Native `attempts` increments in
  `job_mark_running()`, not in the Native claim update. A process death after
  claim and before dispatch therefore consumes no engine attempt; `_reap()`
  returns the row to pending without checking exhaustion. Handler exceptions
  do run exhaustion/DLQ in the dispatcher
  (`postgres/storage.py:243-255`, `native_kernel.py:319-332`,
  `engine/worker.py:333-369`). The same durable contract must be examined for
  Chancy, where the kernel also has its own attempt count.
- **VERIFIED existing coverage gap:** recovery test currently terminates the
  handler's UoW connection (`tests/test_recovery.py:46-66,112-143`); it does
  not disconnect Native claim/maintenance or Chancy Leadership/listener
  connections. Existing contract tests explicitly skip Chancy's lease/reclaim
  internals (`tests/test_kernel_contract.py:300-306`).
- **VERIFIED positive signal:** engine storage enqueue uses partial-index
  `ON CONFLICT DO NOTHING` for active `(type, unique_key)` and returns the
  existing id (`postgres/storage.py:180-234`); kernel contract already tests
  duplicate keys (`tests/test_kernel_contract.py:280-297`). The missing proof is
  concurrent application enqueue while the first job is actually running, for
  each adapter.
- **UPSTREAM report, not Keelflow proof:** Chancy issue #104 reports the worker
  exiting after a short PostgreSQL interruption in 0.26.0; #88 reports a
  queue-maintenance task dying on transient DB failure; #93 covers shutdown
  drain behavior; #78 covers a LISTEN connection leak; #51 reports duplicate
  execution from unique-key conflict resetting a running row. These are
  hypotheses to test against Keelflow's pinned adapter/runtime, not evidence
  that the same failures occur here.
- **UPSTREAM analogy:** Celery #10205 reports a live worker stopping consumption
  after broker reconnect. This supports checking queue age/consumption separate
  from process heartbeat; it does not establish a Keelflow defect.
- **User-approved decisions (2026-11-25):** DB outage behavior should be
  automatic reconnect (not fail-fast); Native claims count toward
  `max_attempts` at claim time, including claims abandoned before handler start.
  Chancy pre-dispatch attempt accounting is not proven and is an accepted
  documented limitation, not a release gate.

### Approach and task graph

Use one isolated disposable PostgreSQL database, real worker loops, and at
least two workers for race/reclaim tests. Keep Chancy and Native kernel tables
namespaced separately. Inject connection failures only through test-owned
connections/proxy resources; never interrupt shared services. Capture job row,
lease/claim identity, DLQ, workflow state, effect count, worker task/process
state, and PostgreSQL query/connection evidence together.

**Task A — Claim identity and bounded crash recovery (foundation)**

- Goal: Native claim generation owns each dispatch; each Native claim consumes
  one attempt and repeated abandoned claims terminalize safely. Chancy is not
  required to provide equivalent pre-dispatch accounting for release.
- Context: Native has a pre-dispatch gap and attempt count currently starts at
  handler start; user approved count-at-claim semantics. Chancy is the default
  backend and must meet the same engine-visible outcome.
- Required outcomes: carry/validate claim ownership at dispatch entry; stale A
  cannot start or mint over B whether A resumes before or after B claims. On
  attempt exhaustion without handler exception, atomically persist failed,
  DLQ, and workflow settlement. Keep kernel and engine attempt counts
  consistent and document their relationship.
- Constraints: do not use reusable attempts as the fencing identity; no
  unbounded retry; external effects remain at-least-once.
- Acceptance evidence: deterministic two-worker PostgreSQL race for both
  backends; crash/reclaim loop stopped at configured max attempts; assert one
  terminal row, one DLQ row, correct workflow terminal status, and effect
  count consistent with exactly which handler starts occurred.
- Verification: focused Native and Chancy integration tests; then existing
  engine hardening, recovery, and kernel contract suites.
- Ownership: `native_kernel.py`, storage claim contract, `worker.py`, and
  tests. Chancy internals are a discovery dependency: first inspect pinned
  0.26.0 claim/recovery behavior and a supported adapter seam before changing
  wrapper policy.
- Dependencies: none; output contract is a documented claim-generation and
  claim-counted attempt invariant consumed by Tasks B/C.

**Task B — Lease-safe drain and idle claim behavior**

- Goal: stop new claims without abandoning ownership of in-flight work; avoid
  empty-queue database spin.
- Context: Native stop cancels renewal before draining; empty `_claim_once`
  currently loops without waiting.
- Required outcomes: drain continues lease renewals, shutdown timeout stops
  renewal only after handler tasks have actually ended/cancelled and been
  awaited; unfinished rows become reclaimable and no handler overlaps after
  expiry. Empty polls wait for notification or bounded poll interval; newly
  enqueued work remains promptly claimable.
- Constraints: no fixed recovery SLA; preserve LISTEN/NOTIFY plus poll
  correctness; do not close a connection while its maintenance task uses it.
- Acceptance evidence: short drain completes once without redelivery; drain
  longer than lease never overlaps; timed-out handler is awaited and then
  reclaimable; count idle claim queries over a measured window and verify
  bounded query rate; enqueue while idle is claimed within measured
  notification/poll bound.
- Verification: Native integration tests with two real workers and controlled
  lease/handler durations; query-count instrumentation or PostgreSQL statement
  stats isolated to the test.
- Ownership: `native_kernel.py`, Native tests, reliability docs.
- Dependencies: Task A claim identity informs what is safe to cancel/reclaim.

**Task C — Database recovery on Native and Chancy (high-risk integration)**

- Goal: both worker backends survive recoverable connection loss and resume
  processing without manual intervention.
- Context: current recovery test kills only the handler UoW; Chancy 0.26.0 has
  upstream connection-loss reports, not yet reproduced here.
- Required outcomes: reconnect failed claim/maintenance/listener connections,
  restore notification subscription or retain polling fallback, continue
  heartbeat/lease management, and resume both old eligible work and new work.
  Cover short outage, longer-than-lease outage, and one-way connection failure.
  If Chancy's pinned plugin exits irrecoverably and offers no supported
  reconnect seam, stop and present the smallest architecture decision rather
  than monkey-patching private upstream internals.
- Constraints: isolate by fresh database/prefix and test-owned proxy/fault
  injector; cleanup only resources created by the test. Chancy and Native may
  have different mechanics but must satisfy the same observable contract.
- Acceptance evidence: real worker integration tests for each backend; after
  DB restoration, worker remains alive and claims a new job, old job state is
  consistent with its lease/attempt, no stale execution commits, and shutdown
  cleans all listener/worker connections. Measure recovery elapsed time and
  decompose it into injected outage, connection retry/backoff, remaining lease,
  reap cadence, and poll/dispatch delay.
- Verification: focused reconnect/fault-injection tests plus `test_recovery.py`
  and `test_native_kernel.py`; confirm PostgreSQL `pg_stat_activity` has no
  test-owned leaked worker/listener connections after stop.
- Ownership: Chancy adapter/worker bootstrap and Native connection lifecycle;
  tests and runbook/reliability documentation.
- Dependencies: Task A/B contracts; outputs are per-backend recovery behavior,
  measured recovery components, and connection cleanup proof.

**Task D — Active dedup and consumption observability**

- Goal: protect live-job dedup and detect workers that heartbeat but do not
  consume eligible work.
- Context: existing dedup test is sequential and kernel-level; heartbeat only
  proves a process reported liveness.
- Required outcomes: concurrent duplicate enqueue while original handler is
  blocked in running produces one live engine job and one handler execution
  for both adapters. Monitoring distinguishes fresh process heartbeat from
  oldest eligible pending job/queue age (or another accepted measurable
  consumption signal).
- Constraints: deduplication is not external-effect idempotency; avoid a new UI
  or broad metrics system.
- Acceptance evidence: two producers + real worker, deterministic barrier,
  assert one result/handler start; idle consumer failure simulation yields
  fresh heartbeat plus increasing eligible queue age; normal consumer drains
  the age signal.
- Verification: PostgreSQL integration tests and focused monitoring/metrics
  tests; docs explain signal limitations.
- Ownership: enqueue/storage, monitoring/observability, kernel contract tests.
- Dependencies: Task A claim/retry semantics for interpreting queue age; can
  develop concurrently after its contract is agreed.

### Risk and recovery

- **Overall risk: High.** This changes backend claim ownership, attempts,
  retry exhaustion, shutdown, and reconnect semantics; Chancy 0.26.0 has
  upstream reports that may require a dependency-level decision.
- **Public/operational contract:** claim-counts-as-attempt means a crash before
  handler start consumes retry budget. This is explicitly accepted; update
  docs and surface attempt/DLQ evidence accordingly.
- **Chancy integration risk:** do not assume an adapter-level retry loop can
  revive a Chancy worker task that has already exited. Prove lifecycle and
  supported extension points first; if upstream fix/version constraint is
  necessary, stop and ask before changing dependencies or forking.
- **Fault-test risk:** network proxy, `pg_terminate_backend`, or real worker
  processes can affect shared resources. Only use disposable DBs, fresh table
  prefixes, and resources owned by the test; no shared DB interruption.
- **Recovery:** code changes are reversible; preserve job rows and inspect
  attempts/leases before any manual cleanup. If a schema migration is needed
  for claim identity or attempts, forward-migrate and preserve active jobs.
  Do not commit/push without explicit request.

### Validation

- Task A: Native + Chancy two-worker pre-dispatch fencing race; both arrival
  orderings; repeated claim crashes through attempt exhaustion; durable DLQ and
  workflow proof; assert effect counts.
- Task B: lease renewal during drain longer than TTL; timeout cancellation is
  awaited; no duplicate overlap; idle SQL query count bounded; wakeup and poll
  fallback work.
- Task C: short, long, and one-way DB outages for both backends; worker
  reconnect/resume; old/new jobs converge; all test-owned connections close;
  record measured recovery components, no unsupported fixed SLA.
- Task D: concurrent active dedup for each backend; queue-age signal detects
  fresh-heartbeat/no-consumption state and clears after drain.
- Integration: `uv run pytest tests/test_native_kernel.py tests/test_recovery.py
  tests/test_kernel_contract.py tests/test_engine_hardening.py -q`, then full
  tests and examples, Ruff, mypy, and `git diff --check`.
- Runtime checks only on disposable Postgres DBs; record backend versions,
  Chancy version, injected fault duration, lease settings, observations, and
  cleanup evidence.

### Prior implementation record — not completion

Commit `a3aca0c` implemented the first version of these tasks. The claims below
are historical implementation/test records, not a completion declaration; the
review findings and corrective plan in “Follow-up: reviewed reliability fixes
and Native default” supersede them. Do not move this plan to completed until
that follow-up passes its acceptance gates.

The prior implementation added `tests/test_reliability.py` (10 tests — run under
both `--kernel=native` and `--kernel=chancy`).

**Task A — claim identity + bounded crash recovery (prior implementation)**

- Native claim now mints the `execution_token` and consumes the attempt inside
  `_claim_once`'s single UPDATE (`native_kernel.py`); `_run` passes the token
  to `dispatch(job_id, step_run_id, *, execution_token=…)`, and
  `job_mark_running(claim_token=…)` validates `status='claimed' AND
  execution_token=…` — a dispatch whose claim was reclaimed in the
  claim→dispatch gap can never mint its own token over the newer claim
  (`test_stale_dispatch_cannot_steal_reclaimed_claim`: two-worker race,
  second claim wins, stale dispatch skips, job done once, attempts=2).
- The tokenless path (`claim_token=None`, chancy/direct dispatch) still mints
  a fresh token and counts the attempt at `mark_running`, but now refuses to
  hijack a `claimed` row that already carries a token.
- Claim exhaustion is settled by the dispatch itself via `_settle_exhausted`
  in `worker.py` — `failed` + DLQ + workflow step settle in one unit — shared
  with the handler-exception exhausted path
  (`test_repeated_crash_abandoned_claims_exhaust_to_dlq`,
  `test_chancy_redelivery_settles_exhausted_claim`; second dispatch owns the
  row, settles it, and the straggler's finish stays fenced).
- Kernel `max_attempts` is engine `max_attempts + 1` in `dispatch_spec` so the
  kernel always grants one final delivery for the engine to settle an
  exhausted job instead of dead-ending the row kernel-side.
- `recover(conn=…)` now clears `execution_token`, matching `_reap`.
- `_run`'s inflight cleanup pops by task identity so an older dispatch
  finishing last cannot clear a newer dispatch's slot.

**Task B — lease-safe drain + paced idle polling (prior implementation)**

- `stop()` splits `_stop_claim` (claim loop only) from `_stop` (maintenance):
  claiming stops first, the maintenance loop keeps renewing in-flight leases
  through the drain window, pending dispatches are cancelled **and awaited**
  for real completion before connections close
  (`test_graceful_stop_renews_lease_through_drain`: sibling `_reap` returns 0
  for a drain held 1.6s against a 1s lease; job then completes `done`).
- The claim loop only short-loops when `_claim_once` actually claimed rows;
  idle iterations wait for LISTEN wakeup or `poll_interval`
  (`test_idle_claim_loop_paces_polling`: ≤12 claim queries in 0.65s idle at
  100ms poll, previously unbounded).

**Task C — DB recovery both backends (prior implementation)**

- Native claim/maintenance loops call `_ensure_conn` each cycle; any failure
  drops the suspect connection and the next iteration reconnects, preserving
  LISTEN wakeup (`test_native_worker_recovers_after_connection_drop`:
  `pg_terminate_backend` on the claim conn → reconnect + LISTEN restored +
  new job completes). The claim/notify connection is tagged
  `application_name='keelflow-worker-claim'` so ops/tests can find it in
  `pg_stat_activity` (LISTEN is not retained as `query`).
- `run_worker` now runs both backends under `_run_supervised`: a worker whose
  `wait_for_shutdown` raises is stopped and replaced by a fresh
  `worker_factory()` after `restart_delay`, forever; the heartbeat task keeps
  declaring liveness through the gap
  (`test_chancy_pipeline_recovers_after_connection_drop`: every backend
  `pg_terminate_backend`ed at once → pipeline still completes new work —
  chancy 0.26.0 either healed itself or was replaced by supervision; in the
  observed run the pipeline recovered without worker death, so the reported
  upstream issue #104 worker-exit did **not** reproduce here).
- `test_no_listener_connection_leak_after_stop` proves the LISTEN backend
  disappears from `pg_stat_activity` after `stop()`.

**Task D — dedup + consumption observability (prior implementation)**

- `test_concurrent_duplicate_enqueue_single_execution`: two concurrent
  same-key enqueues while the handler is blocked mid-run → same `JobRef`, one
  job row, one handler execution (both kernels).
- `ops_metrics_snapshot` gained `pending_due` (`pending AND run_at<=now()`),
  exported as `keelflow_pending_due`; `oldest_pending_s` HELP now explains
  the alive-but-not-consuming pattern (heartbeat fresh + `pending_due`
  rising). `test_pending_due_tracks_claimable_backlog` proves only due rows
  count.

**Known Chancy limitation (not fixed):** a Chancy kernel job abandoned
*before* the engine `mark_running` does not consume an engine attempt. Chancy
`Recovery` requeues abandoned jobs and increases the kernel job's own
`max_attempts`; the `engine max_attempts + 1` setting therefore does **not**
prove a bounded number of pre-dispatch crashes. The engine row may remain
`pending` if Chancy stops delivering it. Chancy claim fencing and bounded
pre-dispatch accounting remain unsupported unless a supported seam and real
worker tests prove otherwise. Native is the intended default for the stronger
claim/recovery contract.

**Validation evidence (2026-11-26):** `uv run pytest tests/ -q` → 255 passed,
9 skipped (chancy); `--kernel=native` → 263 passed, 1 skipped; `ruff check`/
`mypy` clean; `git diff --check` clean. The `--kernel=sqlite` suite mode has
one pre-existing fixture error (`test_worker.py` passes the Postgres DSN as an
SQLite path — reproduces on the unmodified baseline; unrelated to this pass).

### Follow-up: reviewed reliability fixes and Native default

#### Outcome

Close the review findings against commit `a3aca0c`, establish and test NativeKernel
as the default backend, keep Chancy explicitly selectable with honest best-effort
limits, and document the support boundaries of Chancy, Native, and SQLite in
`README.md`. Do not claim a reliability guarantee unless the corresponding
backend-level behavior is proven by tests.

The user accepts NativeKernel as the current code default, while allowing a
release with documented Chancy limitations. This is not a requirement for
Native/Chancy reliability parity. Requests for a capability a selected backend
cannot serve must fail clearly rather than being silently ignored. Chancy remains
opt-in; warn when selected about pre-dispatch fencing and attempt-accounting
limits, without presenting them as guarantees shared by both backends.

#### Verified review findings and repository evidence

- **VERIFIED — destructive test scope:**
  `tests/test_reliability.py` terminates every other backend in the active DB
  (`:458-462`). The database-name guard in `conftest.py` does not make that DB
  exclusive. Replace this with test-owned PIDs or a per-test disposable database
  via `keelflow.testing.temp_test_db()`.
- **VERIFIED — Native dispatch writes before fence:**
  cancel, unregistered-type, and invalid-payload branches can write before
  `job_mark_running(claim_token=...)` (`engine/worker.py:162-210`). Fenced
  `job_finish` only applies a token predicate when supplied
  (`postgres/storage.py:311-335`). Validate ownership before every mutating
  dispatch path, including workflow and bulk settlement.
- **VERIFIED — Chancy pre-dispatch ownership is not fenced:**
  Chancy creates `claim_id` at claim time (`chancy/worker.py:930-934`), but the
  adapter invokes engine `dispatch` without that identity; the tokenless storage
  path can replace a running token (`postgres/storage.py:263-280`). The current
  Chancy test invokes `dispatch()` twice directly rather than exercising two
  Chancy claims (`tests/test_reliability.py:271-301`). Do not depend on private
  Chancy internals to force a guarantee it does not support.
- **VERIFIED — exhausted bulk chunk lacks bookkeeping:**
  `_settle_exhausted` marks the job/DLQ/workflow, but does not mark the chunk or
  invoke bulk fan-in (`engine/worker.py:646-676`); bulk handler bookkeeping lives
  in `engine/bulk.py:305-345,449-538`. A crash-exhausted chunk may strand parent
  and workflow state.
- **VERIFIED — Chancy bounded crash claim is overstated:**
  Chancy Recovery requeues abandoned jobs and increases kernel `max_attempts`
  (`.venv/lib/python3.12/site-packages/chancy/plugins/recovery.py:105-111`),
  while engine attempts increment at `job_mark_running`. The existing
  `engine max_attempts + 1` does not prove bounded pre-dispatch recovery.
  Remove the shared guarantee unless new evidence supports it.
- **VERIFIED — Native reconnect race and shutdown timeout gap:**
  two loops call `_ensure_conn` without serialization
  (`engine/native_kernel.py:209-248`); after the first `stop(timeout)` wait,
  pending tasks are awaited without a second bound (`:379-411`).
- **VERIFIED — backlog metric includes non-worker work:**
  `pending_due` counts every due pending row (`postgres/storage.py:1304-1314`),
  while state-machine human jobs are not dispatched to a worker
  (`engine/statemachine.py:191-205`).
- **VERIFIED — backend defaults and capabilities:**
  CLI worker and scheduler share `--kernel=chancy` default
  (`cli.py:188-210`); `run_worker(kernel=None)` owns Chancy
  (`engine/worker.py:425-449`). The guide labels Chancy default and Native
  experimental, with gaps for rate limits, pause/resume, tags, priority, and
  plugin metrics (`docs/guide.md:774-807`). README currently repeats the old
  default (`README.md:77-87,146-149`) and has a stale Chancy disconnect warning
  (`:106-108`).

#### Task graph

**Status: partially complete.** Tasks 1, 2, 3, 5, and 7 have implementation
work and focused proof. Tasks 4 and 6 remain incomplete; Task 8 release validation
is not complete. No cutover rehearsal, full-suite run, or complete bulk-exhaustion
integration proof has been recorded. Keep this plan active.

**Task 1 — Make destructive fault tests test-owned (P1, prerequisite)**

- Goal: fault injection cannot terminate or roll back work owned by another
  test, developer, or application.
- Required outcomes: run Chancy reconnect/recovery tests on a uniquely named
  disposable Postgres database or target only connection PIDs created by that
  test; cleanup only test-owned resources. Prefer the existing
  `temp_test_db()` helper when Chancy prefixes and migrations fit its lifecycle.
- Constraints: no cluster-wide kill, no shared-DB outage, no broad
  `pg_terminate_backend` predicate.
- Acceptance evidence: test asserts each terminated PID was captured from its
  own worker/client; test cleanup removes only its own database/prefix/state.
- Verification: focused fault test repeated; inspect emitted SQL/target list.
- Ownership: `tests/test_reliability.py`, test fixture/helper only if needed.
- Dependencies: none.
- Progress: changed the reliability fixture to a fresh disposable DB per case;
  Chancy recovery fault injection is excluded from the shared suite until it can
  be proven against test-owned resources. Native reliability suite passes.

**Task 2 — Fence Native dispatch before every mutation and settle bulk exhaustion (P1)**

- Goal: an obsolete Native claim cannot alter a newer execution through any
  early-error, cancellation, bulk, workflow, or terminal path.
- Required outcomes: validate claim identity before cancel/type/payload branches
  mutate state; require the fence in all status-changing storage operations for
  a claimed dispatch. Exhausted bulk chunks must atomically mark the chunk
  failed, run fan-in, terminalize the parent/workflow as appropriate, and write
  one DLQ record. Keep settlement idempotent.
- Constraints: cancellation of a running job keeps the accepted drain semantics;
  no stale dispatch may terminalize a newer claim. Preserve business UoW
  rollback and external-effects caveats.
- Acceptance evidence: real Postgres races where A is reclaimed, B is running,
  then A encounters (a) cancel request, (b) unregistered type, and (c) invalid
  payload; B's job/workflow remains unchanged by A. Crash a bulk chunk through
  exhaustion; assert job, chunk, parent, workflow, DLQ, and effect counts.
- Verification: focused storage/worker tests plus `tests/test_bulk.py`, workflow,
  cancellation, and reliability tests on Native.
- Ownership: `engine/worker.py`, storage contract/implementation, bulk settlement,
  and focused tests.
- Dependencies: Task 1 for safe integration fault tests.
- Progress: Native dispatch rejects stale tokens before mutation, establishes
  the running state atomically before cancel/type/payload failures, and bulk
  attempt exhaustion now settles chunk fan-in in the same UoW. Claim-race and
  existing Native tests pass. Dedicated cancel/type/payload stale-race and
  exhausted-bulk parent/workflow proofs remain outstanding.

**Task 3 — Make Native reconnect and shutdown truthful and leak-safe (P2)**

- Goal: one owned DB connection survives reconnect races; each `stop(timeout)`
  call reports within a documented bound without falsely claiming a
  cancellation-resistant handler has stopped.
- Required outcomes: serialize reconnect/open/close across claim and maintenance
  loops; ensure stop does not close a connection while maintenance uses it. On
  timeout, request cancellation and wait a defined cancellation grace. If a
  handler still runs, return `False` (incomplete drain) while keeping the worker,
  maintenance loop, connection, and lease renewals alive. A later `stop()` may
  finish cleanup after all dispatch tasks end. The caller must keep the worker
  object alive and must not treat `False` as stopped. A hard process kill remains
  an at-least-once crash; fencing must prevent a later stale DB commit.
- Constraints: user decision is **keep ownership** rather than teardown after
  grace. Never close lease/fence resources while a handler may still commit.
  Do not equate cancellation request with task termination.
- Acceptance evidence: concurrent `_ensure_conn` after repeated connection
  termination leaves exactly one live claim connection and zero after completed
  stop; a handler that catches/delays cancellation makes `stop()` return `False`
  within `timeout + grace`, keeps lease renewal active, cannot be reclaimed by a
  sibling, and can later be stopped/cleaned up after release.
- Verification: deterministic asyncio tests plus real Postgres reconnect,
  lease-reclaim, and repeated-stop lifecycle tests.
- Ownership: `engine/native_kernel.py`, Native lifecycle tests, reliability docs.
- Dependencies: Task 2's ownership contract.
- Progress: reconnect/open/close use a connection lock; start/stop are
  synchronized; timeout plus cancellation grace is bounded; incomplete stop
  retains resources and reports `False`. Native lifecycle tests pass. A
  concurrent reconnect stress/leak test remains outstanding.

**Task 4 — Define honest Chancy compatibility and reliability policy (P1)**

- Goal: keep Chancy usable with an explicit warning about unproven pre-dispatch
  fencing and bounded attempt accounting; these limitations do not block release
  when documented and accepted.
- Required outcomes: emit one clear warning per bootstrap invocation while
  Chancy's pre-dispatch fencing and bounded engine attempt accounting remain
  unproven. The warning names those limits without labeling Chancy generally
  unreliable. Investigate supported ways to improve these guarantees, but do not
  block release solely on parity. Correct `docs/reliability.md` and guide claims.
- Constraints: no private Chancy monkey-patching/fork. Do not infer a supported
  claim-id injection seam without proof. If a request/guarantee is not currently
  expressible at an API boundary, document that limitation rather than creating
  broad capability machinery.
- Acceptance evidence: test the warning once per bootstrap invocation on each
  Chancy selection path; tests show ordinary explicit Chancy remains available
  and any explicit unsupported request fails before worker startup. Docs make no
  shared bounded-recovery/fencing claim. If a supported claim-id seam is
  discovered, add a real two-worker/recovery race; otherwise explicitly mark
  claim fencing/bounded pre-dispatch accounting unsupported.
- Verification: Chancy bootstrap tests and reliability tests on isolated DB.
- Ownership: Chancy adapter/bootstrap, docs and tests.
- Dependencies: Tasks 1-2; decision gate before attempting dependency-specific
  behavior.
- Progress: removed the shared Chancy bounded-recovery claim and documented
  Chancy as best-effort. CLI worker/scheduler and Python bootstrap warning
  coverage is incomplete; Chancy reliability tests are skipped because no
  supported fencing seam exists and the existing disconnect test has unsafe
  broad session termination.

**Task 5 — Current Native default and explicit backend capabilities**

- Goal: distinguish the implemented Native code default from release status;
  keep Chancy usable with documented reliability limitations, and reject
  unsupported backend-specific options.
- Required outcomes: change defaults for CLI worker, CLI scheduler, and
  `run_worker(kernel=None)` together. When `run_worker` creates Native itself,
  it owns migrate/start/stop/close on success, startup failure, and cancellation;
  an explicitly injected kernel remains caller-owned. Explicit `--kernel chancy`
  continues to work. Reject Chancy-only `--queue` / `queues=` when Native is
  selected. Build a concrete request matrix for CLI worker, CLI scheduler,
  `run_worker`, and public job/kernel APIs: for each queue/prefix/priority/limits/
  rate/pause/tags/metrics option record support, how explicit request differs
  from default/omission, validation boundary, and actionable error. Validate
  only options users can actually request; do not infer explicit `--prefix`
  from argparse's default value. Keep SQLite clearly proof/local-only. Define
  Native's implicit queue/concurrency behavior.
- Constraints: do not change job data schema solely to switch defaults. Keep
  SQLite out of production recommendations. Do not silently reinterpret a
  Chancy-only option.
- Acceptance evidence: CLI/parser and bootstrap tests show the implemented
  default selects Native without silently applying Chancy-only options. Chancy
  remains explicitly selectable; its unproven pre-dispatch guarantees are
  documented and warned about. Capability errors are actionable.
- Verification: CLI/bootstrap tests, both kernel contract suites, worker and
  scheduler integration tests.
- Ownership: `cli.py`, `worker.py`, kernel selection tests, API docs.
- Dependencies: Tasks 2-4; Native reliability gate must pass before claiming
  Native guarantees, but Chancy parity is not a release prerequisite.
- Progress: CLI and Python worker defaults now select Native; Chancy is explicit
  opt-in; `--queue`/`queues=` is rejected outside Chancy. CLI/bootstrap tests
  pass. Native owned-kernel cleanup on startup failure/cancellation and the
  full explicit capability request matrix remain unproven.

**Task 6 — Backend cutover policy and README support matrix**

- Goal: operators can safely move existing deployments to the new default and
  understand backend limits before choosing one.
- Required outcomes: test whether Chancy and Native dispatch rows can coexist or
  be consumed by mismatched workers/schedulers. If safe coexistence is not
  proven, prescribe a drain-first, stop-scheduler/workers, inspect/reconcile
  outstanding backend jobs, then switch all components together; do not run a
  mixed fleet. State how to handle pending Chancy deliveries and in-flight jobs.
  Update README with a comparison for Chancy, Native, SQLite: support level,
  claim/retry behavior, operational features, reconnect/recovery limits, and
  production suitability. Update stale README defaults and Chancy disconnect
  warning. Provide explicit commands/configuration for retaining Chancy.
- Constraints: do not assert mixed-backend safety based only on separate kernel
  tests. No unverified migration/cleanup command; describe only rehearsed steps.
- Acceptance evidence: isolated integration tests in both directions (Chancy→
  Native and Native→Chancy) for ready pending jobs, scheduled-future jobs,
  in-flight claims, scheduler/worker mismatch, and mixed worker fleets. The
  result must establish one of: (a) safe convergence, (b) mismatch is detected
  and rejected before consumption, or (c) mixed operation is unsupported and a
  tested drain-first all-components-together cutover prevents it. If neither
  safe convergence nor detection exists, characterize the stranded/duplicate
  states and prescribe a tested operational ban on mixed fleets. Do not delete
  kernel rows or claim rollback safety without a rehearsal. README accurately
  matches test results, lists Chancy/Native/SQLite limitations, gives explicit
  Chancy opt-in instructions, and includes the tested cutover/rollback sequence.
- Verification: isolated disposable DB cutover tests; README link/command
  checks; full backend test matrix.
- Ownership: README and cutover procedure; Task 4 owns reliability docs, Task 5
  owns guide/API/CLI docs, avoiding shared-file edits.
- Dependencies: Task 5; choose the cutover policy from this task's mismatch-test results.
- Progress: README now documents the backends and a conservative drain-first
  policy, explicitly noting no end-to-end rehearsal. Mixed-backend/pending-work
  integration tests and a verified cutover procedure remain outstanding.

**Task 7 — Worker-eligible backlog metric (P2)**

- Goal: the consumption metric counts due machine work a worker can actually
  claim, not human tasks or external waits.
- Context: current `pending_due` counts every due pending row; state-machine
  human jobs can remain pending without a kernel dispatch.
- Required outcomes: define a reliable eligibility predicate shared with the
  Native claim contract, or use a separately named metric if backend eligibility
  differs. Exclude human and external-wait work that is not worker-dispatchable.
  Update metric help/docs to state its precise scope.
- Constraints: no broad monitoring redesign and no false claim that queue age
  proves business progress.
- Acceptance evidence: with due machine job, future-scheduled machine job, due
  human task, and external wait present together, only claimable machine jobs
  contribute; prove the metric rises when a worker is stopped and drains when
  consumption resumes.
- Verification: Postgres monitoring tests on the default Native backend and
  explicit Chancy; test exact eligibility against each backend's claim query.
- Ownership: storage metrics, monitoring/observability tests, metric docs.
- Dependencies: Task 2's ownership/eligibility contract.
- Progress: `pending_due` now counts due rows with `dispatch_queued`; Native test
  covers future and non-dispatchable rows. A due human/external-wait integration
  case and explicit Chancy metric validation remain outstanding.

**Task 8 — Backend capability validation and closeout**

- Goal: complete fresh behavior-level evidence for the current Native default
  and accurately describe the supported backend matrix and its limitations.
- Acceptance evidence: Native reliability, cancellation, bulk/workflow,
  reconnect, stop, dedup, metric eligibility, CLI/API defaults, and cutover
  behavior have evidence appropriate to their claims. Chancy's tested behavior
  and known pre-dispatch limitations are explicit; no Native-equivalent fencing
  or bounded-attempt guarantee is claimed. SQLite contract tests pass and its
  limitations are explicit. README, guide, reliability docs, CLI help, and
  module support notes agree on defaults, capability gaps, warnings, and
  cutover status.
- Verification: focused suites per task; then full tests with Native default,
  explicit Chancy tests, SQLite contract tests, examples, `ruff check`, `mypy`,
  and `git diff --check`. Fault injection only on isolated/test-owned DBs.
- Ownership: integration owner; no unrelated cleanup.
- Dependencies: Tasks 1-7.
- Progress: Native-focused reliability suite (34 passed, 1 skipped), Native +
  Chancy kernel contract suite (15 passed, 3 skipped), bootstrap/CLI suite
  (14 passed), `ruff`, `mypy`, and `git diff --check` pass. Full tests/examples,
  Chancy lifecycle proof, cutover rehearsal, and complete acceptance criteria
  remain outstanding.

#### Independent plan review

Oracle reviewed the plan after the user's direction to default to Native and
accept backend limitations. The review found five material gaps; this revision
adds the missing worker-eligible metric task, selects the shutdown behavior,
requires owned-Native cleanup tests, specifies request-vs-default capability
validation, and makes the cutover acceptance alternatives consistent.

#### Dependency order and decision gates

`1 → 2 → 3`; Task 4 follows Task 1 and can proceed alongside Tasks 2-3 where
file ownership is separate. Task 5 records current defaults and backend
capability validation; Chancy parity is not a release gate. Task 6 tests cutover
after the default selection is implemented. Task 7 follows Task 2's eligibility definition and
can proceed independently of CLI cutover; Task 8 integrates all outputs.
Chancy claim-identity work is an improvement investigation inside Task 4: if no
supported seam exists, document the limitation; do not patch private dependency
code or present Chancy as reliability-equivalent to Native.

#### Risks and recovery

- **High:** claim fencing and terminalization cross storage, workflow and bulk
  state. Use deterministic Postgres fault tests before broad suite runs.
- **High:** changing defaults affects worker and scheduler and may alter active
  deployments. Require a tested drain-first cutover if mixed backend operation
  is not proven.
- **Medium:** warning/error semantics and unsupported-option validation are
  public behavior; errors must identify selected backend, unsupported request,
  and supported alternative.
- **Medium:** shutdown cancellation policy must not create false claims that a
  non-cooperative coroutine stopped. After timeout+grace, `stop()` returns
  incomplete and retains worker/maintenance/lease ownership; callers must keep
  the worker alive and retry cleanup after the handler exits.
- Recovery: do not delete old Chancy kernel rows as part of cutover. Stop new
  scheduling, preserve rows, inspect status/claim/attempts, and use only a
  rehearsed migration procedure. Rollback by explicitly selecting Chancy only
  after deciding how Native-created work is handled; never mix workers by
  assumption.

### Decisions

- 2026-11-28: NativeKernel is the current code default. A release may proceed
  with Chancy's documented pre-dispatch fencing and bounded attempt-accounting
  limitations; parity is not required. Do not claim the backends are
  reliability-equivalent. Keep Chancy selectable with an explicit warning.
  SQLite remains proof/local-only.
- 2026-11-26: Do not depend on or patch unsupported private Chancy internals for
  claim fencing. If no supported seam proves the claim contract, keep Chancy
  best-effort and state the limitation; Native owns the stronger guarantee.
- 2026-11-26: On Native `stop(timeout)`, if a handler remains after the timeout
  and cancellation grace, return `False` and keep the worker, maintenance loop,
  connection, and lease renewals alive. The caller retains ownership and may
  retry stop after the handler exits; do not tear down resources underneath it.

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
