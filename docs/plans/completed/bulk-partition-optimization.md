---
status: completed
last_reviewed: 2026-11-22
superseded_by: []
related_code:
  - src/keelflow/engine/bulk.py
  - examples/bulk_import/src/bulk_import/bench.py
  - examples/bulk_import/src/bulk_import/registry.py
  - spec/04-bulk.md
---

# Execution Plan: Bulk partition and staging throughput

Date: 2026-11-22
Path: Standard

## Outcome and Approach

Improve 1M-row bulk throughput without changing the atomicity or
recovery contract. This is mainly a **framework** change to the
partition path in `BulkJobHandler.run`; faster staging is a separate,
optional **application/demo** change because handlers own staging rows
inside `ctx.tx`.

First correct phase measurement. Current benchmark reports
`parent_partition_s` as the complete `super().run()` duration, not only
partitioning, and its `parent_queue_s` is measured from a timer started
after the worker starts rather than from enqueue time. Callback timing
is a sum across concurrent workers and can exceed elapsed wall time.
Therefore current phase readings do not support subtracting phases to
infer a residual or attributing all wall-time cost to a component.

After measurement is trustworthy, make small framework changes first:
hoist loop-invariant database reads; then evaluate set-based row-to-chunk
assignment while preserving `FOR UPDATE SKIP LOCKED` semantics and the
single transaction boundary. Do not assume that `push_many_ex` batches
SQL: Chancy 0.26.0's implementation still executes one kernel-job
INSERT per job. Any change to direct kernel table writes is excluded
unless separately designed and approved against Chancy's locking,
unique-key, notification, and transaction semantics.

Do not split the transaction. The accepted contract is that staging,
partitioning, chunk enqueue, and parent finish commit atomically. Set-
based statements can reduce round-trips and transaction duration, but
they do not reduce the number of rows written, row locks held, or WAL
necessarily generated. Measure those separately where possible; make
no fixed speedup promise before results exist.

## Context and Verified Findings

- `src/keelflow/engine/bulk.py:117-209`: bulk parent handler runs on
  `ctx.tx`; it selects pending, unassigned row IDs with
  `FOR UPDATE SKIP LOCKED`, fetches them to Python, loops over chunks,
  inserts `keelflow_chunks`, updates row `chunk_id`, creates child job
  records, pushes kernel jobs, and returns the parent-hold result.
- `bulk.py:170-181`: each chunk currently re-reads the parent's
  `cancel_requested` flag and resolves the parent's job type/retry
  policy, though these values are invariant during this transaction.
- Chancy 0.26.0 `push_many_ex(cursor, jobs)` loops through `_in_lock_order`
  and executes `_push_job_sql()` once per job. It preserves a caller
  cursor/transaction and notifications, but does **not** reduce insert
  statement count. Do not describe it as a batch SQL optimization.
- `examples/bulk_import/src/bulk_import/bench.py`: staging is performed
  in `_BenchH.run`; phase hooks currently measure staging around the
  staging INSERT, `parent_partition_s` around all of `super().run()`,
  and callback durations summed across workers. Queue timing origin is
  currently after worker startup, not enqueue.
- `bench.py`, `examples/bulk_import/src/bulk_import/registry.py`, and
  `.spikes/erp_bulk_bench/soak.py` currently stage with `executemany`.
  A set-based `INSERT … SELECT` or COPY may improve this app-owned work;
  measure it independently from framework partitioning.
- `spec/04-bulk.md` is authority for keeping staging + partition + chunk
  enqueue atomic in one transaction. This plan does not change that
  contract.

## Scope

### Framework work

1. **Correct phase measurements before setting targets.**
   - Record enqueue timestamp before enqueue and handler-start timestamp
     in the same process clock domain; compute queue delay from those
     points (including explicit statement of whether worker startup is
     inside or outside the interval).
   - Rename the current broad timer to `parent_super_run_s` unless
     instrumentation can separate its internal phases.
   - Capture stage and partition separately only at actual boundaries.
     If fine-grained partition instrumentation requires engine code,
     keep it internal/non-public and ensure it is disabled or near-zero
     overhead outside the benchmark; do not alter `JobResult` outputs.
   - Report callback **sum**, **max**, **count**, and parent wall time
     separately. Callback sum is aggregate worker-time, not a portion
     that can be subtracted from wall time.
   - Measure dispatch transaction duration. If practical in the local
     benchmark, capture transaction lock/WAL indicators with documented
     PostgreSQL instrumentation; do not claim transaction size shrank
     based on fewer SQL statements alone.

2. **Hoist invariant reads.** Fetch parent cancellation state and
   parent retry policy once, outside the chunk loop, preserving the
   current fallback for missing registry type and cancellation
   inheritance.

3. **Evaluate set-based partitioning.** Replace client-side 1M-ID
   materialization and per-chunk row assignment only if a SQL design can
   preserve the current claim contract:
   - pending/unassigned rows are selected under `FOR UPDATE SKIP LOCKED`
     in deterministic `id` order;
   - each selected row maps to exactly one chunk, each chunk has at
     most `chunk_size` rows, indices remain deterministic, and no rows
     are omitted or duplicated;
   - `keelflow_chunks`, engine child jobs, and kernel jobs remain in the
     same transaction as row assignment and parent completion.

   Compare a CTE/window-function approach with a bounded Python-ID
   approach. Prefer the simplest design that is both correct and
   measurably faster; do not force one SQL statement if it worsens row
   locks, memory, or query plan. Avoid assuming `gen_random_uuid()` is
   installed; Python-generated UUIDs passed as arrays are an option.

4. **Kernel queue inserts are not a presumed optimization.** Keep
   `kernel.push_ex` until a separately verified method can preserve
   Chancy's unique-key lock ordering, conflict behavior, return refs,
   notifications, and caller transaction. `push_many_ex` may be used
   for call-site clarity only; it is not evidence of fewer DB inserts.

### Application/demo work (optional, independent)

- In the benchmark handler, compare `executemany` staging with
  `INSERT … SELECT` that constructs identical JSONB data from
  `bench_staging`; COPY is an alternative only if it stays clear and
  correct.
- Keep staging inside `ctx.tx` and compare it as a separate app/demo
  optimization. Do not require every application to use this strategy.
- If it proves useful and readable in `bulk_import`, document it as an
  example pattern in its README. Update `spec/04-bulk.md` only if needed
  to clarify existing staging transaction ownership; do not turn an
  optional implementation technique into a framework requirement.

### Out of scope

- Splitting the dispatch transaction or changing atomicity.
- Changing `chunk_size`, retry/cancel behavior, error queue, or row
  disposition defaults.
- Direct writes to Chancy-owned kernel tables.
- Claiming transaction row count/WAL/lock footprint is smaller without
  measurements.
- Optimizing per-row `process_row` (large imports should use
  `process_batch`).
- Fixed throughput targets or extrapolated speedups before the new
  measurements.

## Proposed Invariants to Preserve

These are existing behavior/contracts, not new public policies:

- One transaction boundary: app staging + bulk partition + chunk job
  insertion/enqueue + parent finish commit together or roll back.
- The same pending, unassigned row set is claimed with
  `FOR UPDATE SKIP LOCKED` and ordered by row ID.
- For N claimed rows and chunk size S, exactly `ceil(N/S)` chunks are
  created, indexed `0..n-1`; each gets 1..S rows, except possibly the
  final chunk.
- Every claimed row belongs to exactly one chunk. Rows not returned
  by `SKIP LOCKED` stay eligible for a later parent dispatch, matching
  current semantics.
- Chunk job payload/context, parent ID, retry budget, cancel flag,
  kernel queue, and parent-hold result remain behaviorally equivalent.

## Task Graph

### Task 1 — Fix benchmark timing semantics

- **Goal:** Know which durations are wall-clock phases and which are
  aggregate worker time before comparing optimizations.
- **Change:** Fix enqueue-to-handler-start origin; rename the broad
  `super().run()` timer; report callback sum/max/count separately;
  instrument transaction duration. Capture separate partition
  subphases only where safe and accurately observable.
- **Acceptance evidence:** A controlled test delays worker start and
  proves measured queue delay includes the delay; a concurrent callback
  test demonstrates callback sum may exceed wall time without being
  labelled as wall time; phase fields are non-negative and scoped to
  invocation.
- **Verification:** focused benchmark tests + 20k smoke; inspect JSON
  and Markdown definitions.
- **Ownership:** benchmark harness and tests.
- **Dependencies:** none.

### Task 2 — Hoist loop-invariant lookups

- **Goal:** Remove redundant SELECTs without changing chunk behavior.
- **Change:** Fetch cancellation flag and parent retry policy once before
  the loop. Preserve current `KeyError` fallback and max-attempts value.
- **Acceptance evidence:** tests cover retry override, unknown registry
  type fallback, and cancel inheritance; SQL/phase evidence shows fewer
  lookup statements.
- **Verification:** bulk tests + randomized bulk matrix; small benchmark
  before/after.
- **Ownership:** `src/keelflow/engine/bulk.py`, tests.
- **Dependencies:** Task 1 for performance attribution; can implement
  independently if keeping the change isolated.

### Task 3 — Compare partition implementations

- **Goal:** Reduce partition overhead only if exact assignment and
  concurrency behavior are preserved.
- **Change:** Prototype the simplest set-based assignment against the
  current loop using large and boundary cases. Inspect query plan and
  transaction duration; choose set-based only if it wins and retains
  the claim semantics. If no safe improvement is demonstrated, keep
  current algorithm and record why.
- **Acceptance evidence:** exact row→chunk membership oracle for zero,
  one, exact multiple, non-multiple, and large row counts; competing
  dispatches with `SKIP LOCKED`; rollback leaves no partial chunks/jobs;
  randomized lifecycle and cancellation tests pass.
- **Verification:** focused tests; 100k benchmark comparison; full bulk
  suite before scaling to 1M.
- **Ownership:** `src/keelflow/engine/bulk.py`, `tests/test_bulk.py`.
- **Dependencies:** Task 1; Task 2 recommended.

### Task 4 — Optional app-owned staging optimization

- **Goal:** Measure whether a set-based staging query improves the demo
  without changing row data or the transaction contract.
- **Change:** Compare current `executemany` and `INSERT … SELECT` in
  `bench.py` using identical staged values. Only apply the winning,
  readable form to the demo handler; update README guidance if useful.
- **Acceptance evidence:** JSONB data equality and the exact v3 product
  and error oracle both pass; staging time is measured independently;
  transaction still contains staging + partition.
- **Verification:** bench correctness tests and demo import tests; 20k
  smoke and 100k comparison.
- **Ownership:** bulk import demo and its tests/docs.
- **Dependencies:** Task 1; independent of framework tasks.

### Task 5 — Re-measure and report

- **Goal:** Publish defensible before/after evidence, without
  pre-committing to a target.
- **Change:** Run isolated v3 protocol at 100k/500k and 1M (same
  profiles as baseline where feasible); report wall time, corrected
  phase fields, callback aggregate, transaction duration, and
  correctness. State what changed, what did not, and variance.
- **Acceptance evidence:** every included run passes exact oracle; no
  concurrent soak; artifacts include environment, config, invocation
  ID, and all timing definitions.
- **Dependencies:** Tasks 1–4 for whichever changes are accepted.

Dependency graph: Task 1 → Tasks 2 and 3; Task 1 → Task 4; all accepted
changes → Task 5.

## Risks and Recovery

- **Row assignment errors (high):** exact membership tests, boundary
  cases, `SKIP LOCKED` concurrency and rollback tests before large runs.
  Keep a clean isolated commit so the partition algorithm can be
  reverted without undoing benchmark instrumentation.
- **Lock/transaction behavior changes (high):** set-based update can
  lock many rows for longer even if it uses fewer statements. Inspect
  actual transaction time and PostgreSQL lock/WAL signals where
  available; retain the old algorithm if the footprint or contention
  worsens materially.
- **Chancy semantics (high):** do not bypass Chancy push APIs or write
  its tables directly within this plan.
- **Instrumentation overhead (medium):** report its method and keep it
  consistent across variants; do not claim precise phases if hooks
  cannot observe them without material distortion.
- **No validated win (acceptable):** retain safe existing behavior and
  document measurements rather than ship complexity for a theoretical
  gain.

## Validation Strategy

- `uv run pytest tests/test_bulk.py tests/test_bulk_retention.py -q`
- `uv run pytest tests/test_random_mixed.py -q`
- `uv run pytest examples/bulk_import/tests/ -q`
- `uv run pytest -q`
- `uv run ruff check src examples tests && uv run mypy src/keelflow`
- `git diff --check`
- Benchmark 20k smoke for instrumentation, then 100k before/after; only
  proceed to 500k/1M if correctness and lower-scale evidence are sound.
- No numeric speedup gate in advance. Report wall-clock speed,
  transaction duration, row-lock/WAL evidence if available, and
  run-to-run spread; state explicitly if a metric was not collected.

## Result (2026-11-22)

Approved ("Ok, implement và test lại") and implemented:

- **Task 1** — instrumentation fixed: `parent_queue_s` now measures
  enqueue-commit → handler start; the broad `super().run()` timer is
  `parent_super_run_s`; `parent_tx_s` added; callback reported as
  `callback_sum_s`/`callback_max_s`/`callback_calls` (aggregate
  worker-time, no longer subtracted from wall time).
- **Tasks 2+3 merged** — `BulkJobHandler.run` rewritten set-based:
  one pending count → Python chunk uuids → a single CTE
  (`claimed FOR UPDATE SKIP LOCKED` + `row_number()` + `unnest`
  chunk ids → `INSERT keelflow_chunks` + `UPDATE … FROM` hash-joined
  row assignment) + one multi-row `INSERT keelflow_jobs` via unnest +
  `push_many_ex`. Loop-invariant `cancel_requested`/`_own_type`
  lookups are gone by construction. First attempt joining through a
  data-modifying CTE was ~4× SLOWER (nested loop, ~250M comparisons —
  EXPLAIN evidence); `unnest` restored a hash join. `n_rows !=
  n_pending` logged loudly instead of silently dropping rows.
- **Task 4** — `INSERT … SELECT` server-side JSONB staging in
  `bench.py` `_BenchH`, `registry.py` `_ImportBulk`, and soak
  `_SoakBatchH`; guidance added to `spec/04-bulk.md` + README.
- **Task 5** — v3-protocol re-measure at 1M (3 reps, w4+w8):

  | phase | before (v3) | after (v4) |
  |---|---|---|
  | stage | ~17s | ~9s |
  | super().run | ~20s | ~17s |
  | parent TX | ~37s | ~26s |
  | batch w8 total | ~69s | ~60s (~16.5k r/s) |

  Row path unchanged (~3.4k r/s). All runs pass the exact oracle
  gate; kernel-row inserts (one per chunk — Chancy `push_many_ex` is
  still per-job INSERTs) are the remaining ~17s of `super_run`.

Validation: new `test_partition_exact_membership_and_shape`
(0/1/exact/non-multiple chunk boundaries × exact row→chunk
membership), full suite 200 passed, ruff/mypy/diff-check clean.
