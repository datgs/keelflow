# Execution Plan: ERP bulk import benchmark

Date: 2026-11-21

## Status

Proposed — awaiting approval to implement.

## Outcome

Add a reproducible, opt-in benchmark to the existing `bulk_import` example. At 100k and 500k synthetic catalog rows, compare Keelflow's row-atomic durable bulk execution with a direct PostgreSQL set-based processing baseline. Both must enforce the same validation, upsert valid products, record the same invalid-row outcomes, and pass correctness checks before timings are accepted. Publish environment-qualified results without claiming general parity with other workflow products.

## Context

- Product-fit discovery: `history/20261121-product-fit/discovery.md`.
- Proposed design: `history/20261121-product-fit/approach.md`.
- Existing use case: `examples/bulk_import/README.md` and `examples/bulk_import/src/bulk_import/registry.py`.
- Existing noop benchmark and its limitations: `.spikes/bulk_bench.py`, `.spikes/bulk_bench.md`.
- Bulk contract: `spec/04-bulk.md`.

## Scope

In scope:

- Reuse the supplier catalog import workload from `bulk_import`.
- Add deterministic valid/invalid input generation for 100k and 500k records.
- Measure Keelflow's actual bulk path and a direct PostgreSQL set-based baseline.
- Separate generation/staging time from processing time; report processing time, rows/s, result counts, and environment details.
- Verify equivalent data outcomes before publishing measurements.
- Add concise usage and interpretation documentation, including benchmark limitations.

Out of scope:

- Engine optimizations or new public engine APIs.
- n8n, Flowise, Jira, or commercial ERP benchmarking.
- Timed human approval/wait periods; compare machine execution only.
- UI changes, editor/configuration features, and performance thresholds in CI.

## Approach

1. Build an isolated, benchmark-owned schema/data path. Require explicit opt-in and refuse unsafe/shared database targets; use benchmark-scoped IDs and cleanup only. Never truncate shared Keelflow engine tables.
2. Generate one deterministic dataset and run it through both variants. The Keelflow variant uses the existing `BulkJobHandler` path with row atomicity and SKU upserts. The baseline uses PostgreSQL set-based validation/upsert/error persistence, with `COPY` only if useful for loading.
3. Check equivalent valid/invalid outcomes and product state before accepting timings. Measure load/stage separately from processing.
4. Run one warm-up where practical and at least three measured repetitions for both sizes. Alternate variant order. Test worker concurrency 1, 4, and 8 if runtime/resources permit; use a fixed chunk size for primary results and treat any chunk-size sweep as secondary.
5. Record hardware/database/runtime configuration and write machine-readable results plus a Markdown interpretation. Do not convert local timings into CI gates or broad vendor claims.

## Risks And Recovery

- **Shared DB/data risk (high impact, avoidable):** use dedicated benchmark-owned tables or isolated database/schema, explicit opt-in, scoped cleanup; test safety refusal. Do not use existing benchmark cleanup that truncates shared `keelflow_*` tables.
- **Non-equivalent workload:** enforce identical validation rules and compare output/error counts and product rows before comparing times.
- **Unfair baseline:** the SQL baseline omits workflow/job/chunk bookkeeping. Label it as a database-path lower bound, not an apples-to-apples workflow-engine comparison.
- **Local result overclaim:** record environment and limitations; no parity claim beyond the measured workload.
- **Long runtime/resource use:** provide small smoke mode, explicitly selected sizes, and cleanup; run 500k only when requested by benchmark invocation.
- **Recovery:** benchmark state is isolated and keyed by unique run ID; remove only that run's rows/schema after the process stops. Do not alter shared engine or demo state.

## Progress

- [x] Inspect existing bulk demo, benchmark, spec, and project validation rules.
- [x] Record product-fit discovery and proposed approach.
- [x] Implement isolated deterministic benchmark runner and data variants.
- [x] Add correctness/safety proof and small smoke verification.
- [x] Run 100k/500k repetitions in the documented local environment; record results and limits.
- [x] Update bulk demo documentation with reproduction instructions and interpretation.
- [ ] Run focused tests, full project validation, and `git diff --check`.

## Task Graph

### Task 1 — Isolated benchmark dataset and safety boundary

- **Goal:** Create deterministic benchmark input and isolated staging/product/error storage for the catalog-import workload.
- **Context:** The existing `bulk_import` example already represents this business task, but its integration tests use small data and `.spikes/bulk_bench.py` truncates shared engine tables.
- **Required outcomes:** Explicit opt-in; refuse unsafe database targets; uniquely scope each run; deterministic 100k/500k inputs with documented invalid cases; scoped cleanup only; shared engine tables are never truncated.
- **Constraints:** Reuse existing product semantics; no real/customer data; do not change engine contracts.
- **Acceptance evidence:** Safety guard rejects an unsafe target; generated dataset is repeatable and has expected counts; cleanup removes only benchmark-owned rows; inspection proves no shared-table truncate path.
- **Verification:** Focused benchmark tests and `uv run pytest examples/bulk_import/tests -q`.
- **Ownership:** Benchmark runner/data fixture under `examples/bulk_import/` or `.spikes/`; tests for the safety and data contract.
- **Dependencies:** None.

### Task 2 — Equivalent Keelflow and PostgreSQL execution variants

- **Goal:** Run the same staged workload through Keelflow bulk processing and a set-based PostgreSQL baseline.
- **Context:** Existing `ProcessRowsH` provides the Keelflow operation; the baseline is deliberately a database-processing lower bound.
- **Required outcomes:** Both paths apply identical validation, SKU-keyed upsert, and error recording; both report success/error counts and verify final product/error correctness before timing output is accepted.
- **Constraints:** Keep human steps outside timed intervals; include Keelflow's durable row/chunk bookkeeping in its measured processing time; baseline must not imitate Keelflow orchestration overhead.
- **Acceptance evidence:** Same expected accepted/rejected counts and equivalent final product/error state at smoke scale, 100k, and 500k; failed correctness suppresses/invalidates timings.
- **Verification:** Benchmark smoke run plus `uv run pytest examples/bulk_import/tests -q`.
- **Ownership:** Benchmark-specific handlers/SQL and runner integration; avoid changing reusable engine internals.
- **Dependencies:** Task 1's input/storage contract.

### Task 3 — Repeatable measurement and report

- **Goal:** Capture reproducible measurements and explain what they do and do not show.
- **Context:** Current benchmark only measures a noop handler; no representative ERP-write comparison exists.
- **Required outcomes:** Measure 100k and 500k rows; separate staging and processing; report rows/s, repetitions, concurrency, chunk size, environment, correctness counts; warm-up and alternate variant order; write raw CSV/JSON and Markdown summary; include local-run commands and explicit limitations.
- **Constraints:** No CI timing thresholds; no n8n/Flowise/Jira equivalence claim; run sizes and concurrency are explicit; avoid creating unbounded benchmark load.
- **Acceptance evidence:** At least three measured repetitions per selected size/concurrency after correctness checks; raw data is reproducible; report states hardware, Postgres version/settings if available, worker configuration and source revision.
- **Verification:** Run at 100k and 500k; review output/raw records; `uv run ruff check src examples tests`, `uv run mypy src/keelflow`, `uv run pytest`, `git diff --check`.
- **Ownership:** Benchmark invocation/docs and result artifacts; depends on the two validated execution variants.
- **Dependencies:** Task 2.

Dependency graph: Task 1 → Task 2 → Task 3.

## Decisions

- 2026-11-21: Reuse `bulk_import` rather than create a separate ERP demo, because it already models staged catalog rows, validation, upsert, and row errors.
- 2026-11-21: Compare execution only; exclude editors, human waiting time, and external product connectors.
- 2026-11-21: Compare Keelflow with a direct PostgreSQL set-based baseline; label the baseline as a data-path lower bound, not equivalent workflow orchestration.
- 2026-11-21: Benchmark sizes are 100k and 500k rows.
- 2026-11-21: No engine optimization, CI timing gate, or external vendor performance claim before evidence.

## Validation

- Focused proof: benchmark safety/data-contract tests; small smoke run for both variants; output correctness equality.
- Integration or end-to-end proof: real Postgres runs at 100k and 500k, ≥3 measured repetitions per selected worker setting.
- Repository-required checks: `uv run pytest`, `uv run ruff check src examples tests`, `uv run mypy src/keelflow`, `git diff --check`.

## Result

Pending implementation and benchmark execution.
