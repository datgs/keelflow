# Plan: Stress evidence and cancellation correctness

Date: 2026-11-24

## Status

Proposed — review and approval required before implementation or another
stress run.

## Outcome

Correct the conclusions from the multi-demo soak, close the cancellation
correctness gaps, make health signals usable for alerting, and provide a
repeatable stress run with run-scoped, independently checked evidence.
The result is a bounded reliability assessment for the tested environment,
not a general production-readiness certificate.

## Context and verified evidence

The 2026-11-24 review is supported by the committed stress driver and its
local report/artifacts:

- `.spikes/stress_demo/stress.py` starts one Chancy `Worker` with a queue
  concurrency setting; it does not start 12 worker processes
  (`stress.py:653-670`).
- The chaos query selects only `pending` or `claimed` jobs and increments
  its count after calling cancel, without checking the result or terminal
  state (`stress.py:522-541`). It does not prove cancellation during a
  handler.
- Metric samples use unscoped counts over the whole database and directly
  query SQL rather than `MonitoringService.metrics_snapshot`
  (`stress.py:546-585`). Existing rows from earlier attempts contaminate
  these figures.
- `workers_alive` counts every worker heartbeat row without a freshness
  bound (`src/keelflow/postgres/storage.py:1228-1234`). Worker heartbeat is
  written only at dispatch (`src/keelflow/engine/worker.py:109-115`), so
  an idle live worker does not refresh it; old process rows remain.
- Bulk fan-in waits for pending/claimed/running chunks, then selects
  `cancelled` only from the parent cancel flag and `failed` only from
  failed chunks (`src/keelflow/engine/bulk.py:464-487`). A cancelled child
  can therefore leave rows pending while the parent completes.
  `tests/test_bulk.py:484-488` explicitly expects the unrequested parent
  to finish `done`.
- `resume_run()` accepts only `rejected` or `failed`
  (`src/keelflow/engine/workflow.py:122-146`). The trigger maps a cancelled
  run to `on_failed`, but only `failed` outcomes spawn the configured
  failure task (`src/keelflow/engine/workflow.py:397-424`). A cancelled
  invoice workflow can leave its entity without a recovery task.
- Previous report claims “zero lost jobs”, “every case correct”,
  “at-least-once proven benign”, “28–55 jobs/s sustained”, and “no pool
  starvation” exceed the collected evidence. Aggregate database counts,
  one duplicate-chunk retry, and absence of a logged pool error do not prove
  those statements.
- The stress driver deletes imports without first requiring a successful
  terminal outcome (`stress.py:204-224`) and returns success even when its
  anomaly list is non-empty (`stress.py:744-761`).
- The first 60-minute run and later run used an already populated test DB;
  one-worker concurrency, non-terminal waits, and artifacts were not
  isolated/scoped well enough to support per-run throughput or completeness
  claims. Existing records must not be truncated or deleted as part of this
  plan.

## Scope

In scope:

- Correct the report language and preserve its history as an exploratory
  soak, not a readiness certification.
- Decide and encode explicit workflow and bulk cancellation outcomes.
- Correct worker/scheduler liveness semantics and stale-row handling in
  `ops_metrics_snapshot`.
- Build stress-run identity, baselines, drain, outcome oracles, latency
  measurements, resource observations, and safe artifact handling.
- Re-run only on a new, isolated, test-owned database after approval.

Out of scope:

- Declaring the framework production-ready based on a single soak.
- General capacity claims beyond the measured host, database, kernel,
  configuration, and workload.
- Changing business semantics for cancellation without an explicit
  decision and permission/audit contract.
- Cleaning or truncating the existing `keelflow_test` database.
- Adding a new metrics server or mandatory telemetry dependency.

## Recommended approach and decisions needed

### D1 — Bulk partial completion (clarify status contract)

There are two different cases, and the plan must not conflate them:

1. **All rows are accounted for, but some row-level operations fail** — for
   example, 50 rows complete and 50 are recorded in the error queue. The
   orchestrator finished its pass; the application needs the counts and a
   clear partial/business outcome. This is not the same as silently
   unprocessed rows.
2. **A chunk is cancelled before it reaches the remaining rows** — some
   rows remain pending/unprocessed. The parent must not report ordinary
   full success or allow a normal-success workflow continuation.

**Current repository contract:** `spec/04-bulk.md:75-76` says explicit
per-row business errors do not fail the chunk or parent; `spec/04-bulk.md:78-84`
says fan-in completes the parent `done` unless a chunk failed. So a fully
accounted 50-done/50-row-error pass currently means parent job `done`, with
`rows_done=50`, `rows_failed=50`, and error rows retained; as a workflow step,
it is currently marked done and downstream steps may continue. `done` means
the pass terminated, not that every business row succeeded.

**User direction:** if every input row reached a terminal disposition, the
bulk pass is **completed** even when some rows failed and were placed in the
error queue. Keep parent status `done`; report exact `rows_done` and
`rows_failed`; retain error rows; do not retry them automatically. Whether an
application wants to call that business outcome `partial`/non-success can be
checked later from the summary. Do not add a workflow opt-in or new engine
status for row-level errors in this work.

Separately, if cancellation leaves rows pending/unprocessed, the work is not
completed. The parent must not report ordinary `done` or allow normal-success
workflow progression. Current behavior can finish `done` when only a child
was cancelled (`tests/test_bulk.py:484-529`); change this incomplete-work case
to a non-success/cancelled result while preserving the distinction from
fully accounted row-level failures.

Implementation must distinguish parent-requested cancellation from child-only
cancellation, and row-level terminal failures from pending/unprocessed rows.
Do not collapse all cases into a single generic failed count.

### D2 — Cancelled workflow recovery (blocking product decision)

**Recommendation:** do not automatically resume a cancelled run. Cancellation
may be an intentional business decision, and prior side effects may already
have committed. Define an explicit operator/application recovery action,
with authorization, audit, and a rule for whether recovery resumes a run or
starts a new run. Define how prior successful steps and external effects are
handled. Until chosen, report this as a known recovery gap, not a supported
path.

### D3 — Liveness contract

**Recommendation:** heartbeat independently of job dispatch so idle live
workers remain observable. Define a heartbeat cadence and stale threshold;
count only fresh worker processes as alive, expose stale processes separately
or exclude them, and make scheduler freshness use the same explicit policy.
Tests must include old rows, fresh idle workers, dispatching workers, and a
dead process. The cadence/threshold must be stated in code/docs, not inferred
from whichever rows happen to exist.

## Work graph

### P0 — Correct the evidence record

- **Goal:** make all existing statements match what was actually measured.
- **Required outcomes:** replace aggregate totals with explicitly labeled
  database snapshots/deltas; retract unproven job-loss, benign-redelivery,
  sustained-throughput, cancellation, heartbeat, and pool-health claims;
  identify run3/run4/run5 as exploratory attempts, including driver defects
  and contaminated initial state. Keep raw artifacts local or in an approved
  artifact location; do not commit unredacted logs/JSONL by default.
- **Constraints:** do not rewrite history as if prior runs had stronger
  oracles than they did.
- **Acceptance evidence:** report distinguishes observed facts, inferences,
  unknowns, and known gaps; every numeric claim links to a run-scoped source
  or is labeled whole-database/non-run-scoped.
- **Verification:** compare report text against the existing JSONL/JSON files
  and `stress.py`; no database access is needed for this correction.
- **Ownership:** `.spikes/stress_demo/REPORT.md` and this plan.
- **Dependencies:** none.

### P1 — Decide cancellation contracts

- **Goal:** agree externally observable behavior before changing runtime code.
- **Required outcomes:** record D1's exact bulk parent status/result contract
  and downstream workflow behavior; record D2's authorized recovery action,
  audit event, idempotency/side-effect policy, and actor permissions.
- **Constraints:** no implicit auto-resume; no “partial success” hidden in a
  normal success result; do not assume committed external effects can roll
  back.
- **Acceptance evidence:** owner-approved decisions recorded in the plan or
  decision docs before implementation starts.
- **Verification:** behavior tables cover parent cancel, child cancel,
  failed child, incomplete row set, cancelled triggered workflow, and
  explicit recovery attempt.
- **Ownership:** product/API contract and workflow/bulk boundary.
- **Dependencies:** user review of D1/D2. P2 and P3 are blocked until these
  decisions are confirmed.

### P2 — Make bulk cancellation fail closed

- **Goal:** distinguish a completed pass with row-level errors from an
  incomplete pass caused by cancelled/unprocessed chunks.
- **Required outcomes:** fan-in checks chunk terminal states and row
  accounting; fully accounted rows (done + failed/error-queue rows) retain
  parent `done` and exact counts; cancelled chunks or pending/unprocessed
  rows yield non-success/cancelled and cannot advance normal-success
  downstream workflow; terminal summaries account for total, done, failed,
  cancelled, pending, and any retained rows.
- **Constraints:** preserve row-level transaction and retry semantics; do not
  mark valid human/external waits as failures; no rows silently disappear.
- **Acceptance evidence:** deterministic Postgres tests for child-only cancel,
  parent cancel, mixed done/cancelled chunks, failed chunk, row-level errors,
  and exact input-row accounting; positive proof that a fully accounted pass
  with row-level failures completes `done` with counts/error rows retained;
  negative proof that a cancelled/incomplete pass does not satisfy the
  normal-success contract.
- **Verification:** focused `tests/test_bulk.py`, storage tests as needed,
  workflow integration tests, then full suite.
- **Ownership:** `src/keelflow/engine/bulk.py`, bulk storage summary if needed,
  `tests/test_bulk.py`, relevant spec/docs.
- **Dependencies:** P1 D1.

### P3 — Define an explicit cancelled-run recovery path

- **Goal:** prevent an entity from being stranded after a cancelled triggered
  workflow, without resuming business work automatically.
- **Required outcomes:** implement the D2-approved action, authorization,
  audit record, and idempotent treatment of already completed steps/effects;
  expose a clear unsupported/rejected response when no authorized recovery is
  selected.
- **Constraints:** cancellation remains a distinct terminal outcome;
  `resume_run` must not silently broaden its accepted states; no claim that
  external effects are rolled back.
- **Acceptance evidence:** Postgres integration tests for a cancelled run,
  unauthorized recovery, authorized recovery, duplicate recovery request,
  audit trace, and no duplicate domain effect; explicit proof that ordinary
  cancellation does not auto-resume.
- **Verification:** focused workflow/state-machine tests, recovery tests,
  and full suite.
- **Ownership:** `workflow.py`, state-machine/recovery API boundary, invoice
  example tests/docs.
- **Dependencies:** P1 D2.

### P4 — Repair process-health metrics

- **Goal:** heartbeat metrics reflect live processes rather than historical
  rows or recent dispatch volume.
- **Required outcomes:** worker heartbeat refreshes while idle; scheduler
  heartbeat refreshes per documented lifecycle; `workers_alive` counts only
  fresh workers; stale process rows do not count alive; expose scheduler age
  and stale-worker count consistently; monitoring API returns the same data
  tested by the exporter.
- **Constraints:** no payload or high-cardinality labels; avoid a background
  task that outlives worker shutdown; process identity must distinguish
  concurrently running processes and tolerate restarts.
- **Acceptance evidence:** Postgres tests with a fresh idle worker heartbeat,
  stale prior-process row, current worker row, and stale scheduler row; tests
  assert threshold boundaries and no false alive count. Prometheus formatter
  golden tests use the same `MonitoringService.metrics_snapshot` result.
- **Verification:** focused storage/monitoring/observability tests, lifecycle
  start/stop proof, full suite, `ruff`, `mypy`.
- **Ownership:** worker lifecycle, scheduler, process-health storage/query,
  monitoring and observability tests/docs.
- **Dependencies:** D3 cadence and stale threshold approved/recorded.

### P5 — Rebuild the stress harness around run-scoped oracles

- **Goal:** every stress run can be independently reconciled and fails when
  its declared correctness oracle fails.
- **Required outcomes:** unique `run_id` stored on every generated app record
  or an equivalent manifest mapping; baseline and end snapshots; stop-load
  timestamp and separate drain phase; per-run job/run/row/business-effect
  queries; injected-failure labels; verify create/update/delete outcomes;
  refuse to delete imports unless they reached an allowed terminal state and
  all expected rows/effects reconcile; non-empty unexpected anomalies yield a
  non-zero exit. Record seed, git commit, package versions, DSN identity (not
  credentials), exact worker-process count/concurrency, and parameters.
- **Constraints:** never query or report global table totals as this run's
  totals; keep test data and artifacts free of secrets; do not truncate the
  existing database; preserve failed/timed-out records for diagnosis.
- **Acceptance evidence:** unit tests for clean and failing oracles; a smoke
  run proves run scoping against pre-existing unrelated rows; duplicate,
  missing, extra, non-terminal, and injected expected-failure cases are
  distinguished; failed run leaves diagnostic records intact.
- **Verification:** static/unit tests plus a short isolated Postgres smoke;
  inspect manifest, reconciliation output, and non-zero exit on a deliberate
  oracle failure.
- **Ownership:** `.spikes/stress_demo/stress.py`, stress tests/helpers, report
  schema and artifact policy.
- **Dependencies:** P2/P3/P4 contracts for final end-to-end oracle.

### P6 — Controlled fault and load validation

- **Goal:** collect bounded, reproducible evidence after P2–P5 pass.
- **Required outcomes:** use a newly created test-owned disposable database;
  record setup/baseline, load start, stop-load, drain start/end, final
  reconciliation; exercise all three apps; ramp concurrency; compare one
  process with higher concurrency against multiple independent worker
  processes; chaos targets a verified `running` handler and records request,
  cancel return, handler checkpoint, terminal state, and business effect;
  separately rehearse worker kill/restart, DB disconnect/reconnect,
  redelivery, and retry collision with exact affected IDs. Measure
  enqueue-to-start and end-to-end p50/p95/p99, backlog by type, DB connection
  count, and host CPU/memory/I/O when available. Record pool-acquisition wait
  only if the active Chancy pool exposes a reliable measurement; otherwise
  mark it unmeasured rather than inferring from absence of errors.
- **Constraints:** no production/shared database; no claim beyond measured
  environment and workload; do not intentionally induce destructive DB
  faults outside the disposable instance; use a fixed drain budget declared
  before each run.
- **Acceptance evidence:** exact reconciliation has zero unexplained missing,
  extra, duplicate, stranded, or non-terminal items after drain; all injected
  failures are classified; metrics are scoped to this run; expected and
  unexpected failures are distinguished; all configured resource and latency
  measurements include units and sample counts. Any unmet oracle is a failed
  run, not a warning-only pass.
- **Verification:** increasing stages (short smoke, medium soak, then approved
  1–2 hour run); retain per-run manifest, sanitized raw samples, final report,
  and exact commands. Run repository tests after code changes.
- **Ownership:** isolated stress execution and report; operator owns the
  disposable DB and run window.
- **Dependencies:** P2–P5 complete and reviewed; user separately approves the
  longer soak, target DB, duration, and resource impact.

## Risks and recovery

- **Product contract risk (high):** cancellation and partial-success semantics
  affect downstream business decisions. Stop before P2/P3 until D1/D2 are
  explicitly approved.
- **Data risk (high):** old test DB contains prior soak state. Do not clean it;
  create and verify a new disposable DB owned by the next run.
- **Evidence risk (high):** a passing long test can still miss workload or
  failure modes. Report measured bounds and uncovered cases, never a broad
  readiness claim.
- **Operational risk (medium):** kill/restart and DB disconnect perturb the
  test system. Scope them to the isolated run and verify ownership before
  applying faults.
- **Recovery:** code changes are revertible; test data cleanup is limited to
  the database created by the run after artifacts and reconciliation are
  safely recorded. No cleanup of pre-existing data.

## Approval gates

1. Review and approve/revise D1, D2, and D3 before product/metric behavior
   changes.
2. Review the corrected report and P0–P5 plan before implementation.
3. Review P2–P5 results before authorizing any 1–2 hour run; separately
   approve the isolated database, exact configuration, and resource window.
4. Completion report must separate verified behavior, measured capacity
   signals, unresolved gaps, and untested scenarios. It must not label the
   framework production-ready unless a separate readiness authority approves
   that conclusion.

## Progress

- [x] Review supplied critique against driver, report, bulk fan-in, workflow
      recovery, and metrics implementation.
- [ ] Correct previous report's unsupported conclusions (P0).
- [x] D1a: user confirmed fully accounted pass with row-level errors is
      completed (`done`) with exact counts/error queue retained; partial
      business outcome can be reviewed later, no new opt-in/status now.
- [x] D1b: user confirmed pending/unprocessed rows are not completed; a
      cancelled chunk must make parent non-success/cancelled and block
      normal-success downstream.
- [x] D2: user selected a new run for cancelled-workflow recovery; define
      authorization/audit/side-effect reconciliation in P3.
- [x] D3: user selected 10s heartbeat / 30s stale threshold.
- [ ] Implement and prove bulk cancellation contract (P2).
- [ ] Implement and prove explicit cancelled-run recovery (P3).
- [ ] Repair heartbeat freshness and monitoring metrics (P4).
- [ ] Rebuild run-scoped stress oracles and artifacts (P5).
- [ ] Run staged isolated validation and approved long soak (P6).

## Decisions

- 2026-11-24: Treat prior results as an exploratory soak, not production
  readiness certification; retract claims that lack run-scoped or independent
  proof.
- 2026-11-24: Do not modify or truncate the already populated test database.
- 2026-11-24: Recommend no automatic resume for cancelled workflows; user
  selected a new run after an authorized application/operator decision, with
  audit and side-effect reconciliation still to define.
- 2026-11-24: Worker heartbeat cadence/freshness selected as 10s / 30s;
  heartbeat must refresh while idle and stale rows must not count alive.
- 2026-11-24: User confirmed D1: when every row has a terminal disposition,
  the pass is completed even if some rows are in the error queue; parent stays
  `done` with exact row counts and no automatic retry. Application/business
  partial classification can be assessed later. If a cancelled chunk leaves
  rows unprocessed, the pass is incomplete and must be non-success/cancelled;
  no normal-success downstream progression.
- 2026-11-24: Long stress runs require isolated, run-scoped reconciliation and
  separate approval after short validation.

## Validation

- Planning review only; no code changes, database access, or stress execution
  are authorized by this plan.
- Implementation and stress proof commands will be finalized against the
  selected test-owned Postgres setup when the plan is approved.
