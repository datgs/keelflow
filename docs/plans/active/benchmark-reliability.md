---
status: completed
last_reviewed: 2026-11-21
superseded_by: []
related_code:
  - examples/bulk_import/src/bulk_import/bench.py
  - examples/bulk_import/tests/test_bench.py
  - .spikes/erp_bulk_bench/soak.py
  - .spikes/erp_bulk_bench/RESULTS.md
---

# Execution Plan: Make ERP bulk benchmarks trustworthy

Date: 2026-11-21
Path: Standard

## Outcome and Approach

Make benchmark results valid under large data and concurrent workflows. Every measured run must own a unique ID, query and clean only its own rows, use an explicitly isolated benchmark database, and pass exact per-run result validation before its timing is included. Benchmark execution must not overlap with soak execution on the same database. Add phase measurements that distinguish queue delay from actual execution and record enough environment/configuration data to reproduce the result.

The 1M run performed alongside the 60-minute soak is **invalid for comparative conclusions**: the correctness gate observed unrelated/stale `bench_products` rows because the benchmark queried/deleted shared output tables globally. The soak and benchmark also contended for the same DB. Preserve the raw files as invalid diagnostic evidence; mark them as contaminated in `RESULTS.md`, do not report their timings as valid measurements, and rerun only after the isolation changes.

## Context and Verified Findings

- `examples/bulk_import/src/bulk_import/bench.py:302-313` `_wipe_output()` currently deletes all `bench_products` and `bench_errors`, and `_snapshot()` counts those entire tables without a `batch_id` filter. Concurrent soak workloads can be deleted or counted as benchmark output.
- `bench.py:349-355` gates SQL on global output counts and Keelflow variants on global product count plus per-job errors. This permitted false failures/contamination when unrelated output rows existed.
- `examples/bulk_import/tests/test_bench.py:37-43` truncates shared engine and benchmark tables. This is safe only under the current serial test fixture assumptions; benchmark/soak must never use that cleanup pattern.
- `.spikes/erp_bulk_bench/soak.py:279-301` checks products by batch id, but it shares `bench_products`, `bench_staging`, engine tables and DB with the benchmark. Its logs recorded 13 product-count anomalies while the 1M benchmark ran concurrently; these cannot establish engine defects because the benchmark's global deletes/counts race with soak rows.
- The reported 1M file `.spikes/erp_bulk_bench/result_1m.json` has correctness failures on several reps and is contaminated. Do not use its timings as results.
- Benchmark currently records total processing wall time but does not separate queue wait, handler staging/partition, chunk callback, row-state update, fan-in/retention, and end-to-end time. Chunk `created_at`→`updated_at` includes queue wait; it is not callback execution time.
- Valid 100k/500k v2 measurements were previously run without the soak and passed current gates, but should be labelled as earlier measurements pending rerun under the stricter per-run checks.

## Scope

In scope:

- Fail closed unless a dedicated benchmark DSN is explicitly supplied; remove implicit default to the test/development DSN and do not auto-create/drop databases.
- Prevent benchmark and soak from running at the same time against one DB using a PostgreSQL advisory lock keyed to the benchmark suite; fail immediately with a clear message if already held.
- Give each benchmark invocation, staged dataset, and variant repetition a unique run/batch ID.
- Filter every output read/write/delete by the owning `batch_id` and/or job ID; SQL errors by batch ID; engine row/chunk state by job ID. No global `DELETE`/`TRUNCATE` in runners.
- Make correctness an exact per-run gate: expected invalid count, exact product rows for that run (SKU/name/price/stock), no missing/extra/duplicate SKU, error row identities and error semantics, terminal job/parent status and row summaries. If any check fails, persist the diagnostic, mark run invalid, return non-zero, and exclude timing from summary.
- Instrument stage/load, enqueue/dispatch start, parent staging+partition, chunk queue latency, callback execution, row-state persistence, fan-in/archive/delete, and total elapsed separately when observable. Label approximations and do not mislabel chunk age as execution time.
- Record host/architecture, Python, Postgres version and relevant settings, package/source revision, DB identity (not credentials), worker process count, queue concurrency, chunk size, atomic mode, row disposition, dataset seed/failure ratio, run order, warm-ups, and repetitions.
- Add positive/negative tests for contamination and isolation; update `RESULTS.md` to invalidate the contaminated 1M results and document the corrected protocol.
- Rerun an isolated 1M comparison and report only runs passing the exact gate. Do not run soak concurrently; if a soak is needed, use a different dedicated DB instance.

Out of scope:

- Engine optimization or new public API changes.
- Timing thresholds in CI.
- External product comparison.
- Running benchmark and soak concurrently against a shared database.
- Broad performance claims beyond the documented machine, Postgres setup, workload, and worker profile.

## Proposed Benchmark Safety Contract

1. Runner requires `KEELFLOW_BENCH_DSN`; no fallback to `KEELFLOW_DSN` or test DSNs. `--allow-db` remains explicit acknowledgement, not a substitute for DB isolation.
2. Runner obtains a session-level advisory lock for the dedicated benchmark database before applying schema or writing data. Soak runner obtains the same lock. A second process must exit before writes if lock acquisition fails.
3. Use only benchmark-owned table names plus existing Keelflow engine tables in the dedicated benchmark DB. Do not truncate/drop tables. Every output row is scoped by invocation/run ID; every cleanup uses a fully parameterized scoped predicate.
4. Record a stable invocation UUID in each output record and in a benchmark run ledger. Do not store DSN/passwords in JSON or logs.
5. Correctness compares one variant's run to an independent deterministic oracle from the staged dataset, not to counts of all rows in shared tables.
6. If any invariant or result comparison fails, that variant/rep is invalid; do not aggregate it or print a successful summary. Keep its raw diagnostics for investigation.

## Task Graph

### Task 1 — Isolate every benchmark data operation

- **Goal:** Eliminate cross-run and soak/benchmark contamination.
- **Context:** Global `_wipe_output` and `_snapshot` caused contaminated 1M correctness counts; soak and benchmark shared DB/table state.
- **Required outcomes:** Explicit dedicated benchmark DSN; invocation and batch IDs; every output and cleanup scoped; no table-wide truncate/delete; lock shared by benchmark and soak; both runners acquire it before migrations/writes and release it on normal/error exit.
- **Constraints:** Do not auto-create/drop databases; do not include credentials in reports; use the same explicit lock/resource owner in the two runners.
- **Acceptance evidence:** Test seeds unrelated products/errors and proves a benchmark run neither deletes nor counts them; concurrent second-run attempt fails before writing; subprocess/exception path releases the advisory lock.
- **Verification:** Focused benchmark safety tests; run one benchmark and one soak concurrently against same DSN and prove exactly one obtains the lock; `git diff --check`.
- **Ownership:** `examples/bulk_import/src/bulk_import/bench.py`, `.spikes/erp_bulk_bench/soak.py`, `examples/bulk_import/tests/test_bench.py`.
- **Dependencies:** None.

### Task 2 — Exact correctness oracle and failure reporting

- **Goal:** A timing is accepted only if that specific run's complete output matches expected row-level outcomes.
- **Context:** Current count-only checks miss identity/value errors and global contamination; some 1M variants were falsely marked NO.
- **Required outcomes:** Deterministic expected outcome set by SKU/row; exact per-run product set/value comparison; error identity/count validation; job, chunk, row summary and terminal-state validation; raw diagnostic output; invalid runs excluded and command exits non-zero if any requested run is invalid.
- **Constraints:** Keep SQL and both Keelflow variants semantically identical; preserve row error vs chunk exception distinction; no global output query.
- **Acceptance evidence:** Positive tests for all three variants; negative tests deliberately insert an unrelated product, omit one output, duplicate/misclassify a row, and assert the intended diagnostic and non-zero outcome.
- **Verification:** `uv run pytest examples/bulk_import/tests/test_bench.py -q`; sample 20k smoke with each variant; inspect JSON output.
- **Ownership:** Benchmark oracle/report path and tests.
- **Dependencies:** Task 1.

### Task 3 — Phase-level timing and environment metadata

- **Goal:** Identify the real dominant phase rather than infer it from total wall time or chunk age.
- **Context:** Parent total time and chunk `created_at`→`updated_at` do not separate queue wait from execution.
- **Required outcomes:** Record distinct metrics for input generation/staging; enqueue to parent start; parent staging/partition; chunk enqueue-to-start; handler callback wall duration; row-state persistence (or clearly document when only callback-plus-status can be observed); chunk finish/fan-in and retention; total elapsed. Record queue concurrency/process count and complete environment metadata. Store raw per-run/per-chunk samples plus summary median, range/percentiles.
- **Constraints:** Instrument benchmark/example code only; no new engine telemetry API; do not call approximate values exact; instrumentation overhead must be identified and either excluded from primary timing or measured consistently for every path.
- **Acceptance evidence:** A controlled smoke run shows phase durations are non-negative, scoped to invocation, and their measured/defined relation to total time is documented; chunk queue delay is distinct from callback duration.
- **Verification:** Unit/integration tests for telemetry aggregation; one smoke run; inspect raw JSON and report.
- **Ownership:** Benchmark handler, timing tables/results writer, README/RESULTS docs.
- **Dependencies:** Task 1.

### Task 4 — Rerun 1M benchmark under isolated conditions

- **Goal:** Replace the contaminated 1M file with reliable measurements and state the slowest measured phase.
- **Context:** Existing 1M run overlapped the 60-minute soak and failed correctness counts; timings are invalid.
- **Required outcomes:** Dedicated isolated DB; benchmark holds lock; soak stopped or uses a separate DB; warm-up and at least three valid reps per selected variant/profile; run 1M row, batch, and SQL variants with exact gates; report phase timing and per-run variability. If full 1/4/8 worker matrix is prohibitively long, run the previously selected concurrency profile(s) and state the reduced matrix before execution; do not silently omit cases.
- **Constraints:** Do not overwrite contaminated JSON; label old file `invalid_contaminated` in docs and retain for forensic trace; only new valid output is used for conclusions.
- **Acceptance evidence:** All included reps pass exact output checks; result records DB identity, no concurrent benchmark/soak owner, run UUID, configuration, phase timings, and correctness status.
- **Verification:** Execute documented isolated 1M commands; inspect each rep and summary; full repository test/lint/type/diff checks.
- **Ownership:** `.spikes/erp_bulk_bench/` results and interpretation.
- **Dependencies:** Tasks 1–3.

### Task 5 — Update benchmark documentation and recovery record

- **Goal:** Keep reports honest and make safe reproduction repeatable.
- **Context:** `RESULTS.md` currently presents 1M contaminated values as valid-looking metrics and still contains obsolete interpretations.
- **Required outcomes:** Mark current 1M file/results as invalid due to shared-table/global-count race and concurrent soak; publish new results only if gates pass; document explicit DSN/lock/run commands, phase definitions, environment and interpretation limits; distinguish application callback time from engine bookkeeping and DB baseline.
- **Constraints:** No general parity claims; no deleting raw contaminated evidence; README run instructions must not suggest sharing a test DB.
- **Acceptance evidence:** Fresh reader can reproduce a small smoke and understand why old result is invalid and what every reported metric includes.
- **Verification:** Run commands from docs in smoke mode; check links/paths; `git diff --check`.
- **Ownership:** `.spikes/erp_bulk_bench/RESULTS.md`, `examples/bulk_import/README.md`.
- **Dependencies:** Task 4.

Dependency graph: Task 1 → Tasks 2 and 3 → Task 4 → Task 5.

## Risks and Recovery

- **Accidental shared DB writes (high):** require explicit benchmark DSN, `--allow-db`, and exclusive advisory lock; no destructive schema/table cleanup. Stop before writes if any check fails.
- **Long 1M run / resource use (medium):** use isolated local DB, capture start/config, allow graceful cancellation; retain scoped run ledger and artifacts for diagnosis; do not claim completion if interrupted.
- **Instrumentation distortion (medium):** keep primary throughput measurement with minimal instrumentation and report instrumentation/phase method; compare variants consistently.
- **Failed correctness (high evidence impact):** exclude failed run from aggregate, return non-zero, preserve raw evidence and report exact mismatching key/count.
- **Recovery:** only delete rows with a recorded invocation UUID after the worker is stopped and jobs are terminal; no global cleanup. Preserve invalid result files under their current names and annotate them rather than overwriting.

## Validation Strategy

- Safety positive: runner with dedicated DSN/lock creates its invocation ledger and processes a small workload.
- Safety negative: missing explicit DSN, unrelated DB marker, or already-held lock refuses before any writes.
- Isolation: unrelated bench product/error rows remain unchanged and do not affect expected counts.
- Correctness negative: injected missing/extra/incorrect product or error causes failed run, useful diagnostic, and non-zero exit.
- Phase timing: scoped queue delay and callback execution differ in a deliberate test with a gated worker; total and phase fields are present and consistently defined.
- Scale: 1M matrix runs only after all smoke/integration gates; no simultaneous soak on same DB.
- Repository checks: `uv run pytest`, `uv run ruff check src examples tests`, `uv run mypy src/keelflow`, `git diff --check`.

## Result (2026-11-21)

All tasks implemented and validated:

- **Task 1** — `bench.py` requires `KEELFLOW_BENCH_DSN` (no fallback),
  acquires shared `pg_try_advisory_lock(BENCH_LOCK_KEY)` before any
  write; `soak.py` acquires the same lock on its main session. All
  output access scoped by `batch_id` (`_wipe_output`, `_products`,
  `bench_errors` queries); engine rows cleaned by `job_id`
  (`_cleanup_engine_rows`). Invocation ledger:
  `bench_invocations`/`bench_run_batches`.
- **Task 2** — deterministic `_oracle(n)` (full product set +
  row_key→error map); `verify_sql`/`verify_keelflow` do exact set
  comparisons incl. archive accounting, stranded-row and status
  checks. Failed runs get diagnostics, are excluded from summary,
  and `main` exits 1.
- **Task 3** — in-process hooks: `parent_queue_s`, `parent_stage_s`,
  `parent_partition_s`, `callback_s`/`callback_calls` recorded per
  run; env metadata (PG version/settings, git rev+dirty, package
  version, full config) in the JSON report.
- **Task 4** — v3 runs executed on the dedicated DSN with no
  concurrent soak: 100k+500k full matrix (reps=3, workers 1/4/8) and
  1M reduced matrix (reps=3, workers 4/8, stated in RESULTS.md).
  Zero invalid runs; all exact gates pass.
- **Task 5** — `RESULTS.md` rewritten: v3 protocol documented, old 1M
  marked INVALID/contaminated, v2 numbers labelled pre-v3. README
  benchmark section updated.

Validation: 8 bench tests pass (incl. lock contention, lock release,
scoping, exact-oracle positive/negative, DSN/`--allow-db` refusals);
full suite 199 passed; ruff/mypy/`git diff --check` clean.

Key measured finding (1M): in the batch path the application callback
is ~15s of ~69s total — framework staging (~17s executemany) +
partition (~20s) dominate; in the row path the per-row `process_row`
TX dominates (callback sum ≈ saturating all 8 workers).
