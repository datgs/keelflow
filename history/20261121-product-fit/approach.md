# Approach: ERP bulk runtime benchmark

Date: 2026-11-21
Status: proposed; awaiting implementation approval.

## Recommendation

Extend the existing `examples/bulk_import` scenario with a dedicated, opt-in benchmark runner and deterministic workload fixtures. Keep benchmark execution separate from normal integration tests and the web demo. Measure Keelflow's real row-atomic bulk workflow against direct set-based PostgreSQL processing, using isolated benchmark-owned tables/database state.

## Workload contract

- Simulated ERP task: import/update supplier product catalog by SKU.
- Record fields: SKU, name, positive unit price, nonnegative stock.
- Deterministic, documented proportion of malformed rows (for example, missing SKU/name, invalid/nonpositive price, negative/invalid stock).
- Both variants must apply identical validation rules, valid-row upserts, and per-row error outcomes.
- Use identical pre-generated and staged input data for each variant.
- Run at 100,000 and 500,000 rows.

## Variants

### Keelflow

Use the real `BulkJobHandler` path, row-level atomicity, durable row/chunk statuses, retries/resume semantics, row errors, and SKU upsert. Do not include human approval gates in timed execution: those measure business wait time, not worker throughput. State clearly that this isolates the machine bulk step within the existing import scenario.

### PostgreSQL baseline

Use set-based SQL (and `COPY` if useful for loading) to validate rows, upsert valid products, and persist errors with equivalent outcomes. Do not emulate Keelflow chunk/status bookkeeping in the baseline; the baseline answers database-processing capacity, not orchestration overhead. State this difference explicitly.

## Measurement design

- Report input generation and staging separately from processing.
- Primary metric: processing wall time after inputs are staged; also report rows/s, valid/error counts, and final product/error correctness.
- Secondary metrics: staging duration, total end-to-end duration, chunk count, and Keelflow status/error bookkeeping counts.
- Test 1, 4, and 8 workers/concurrency if feasible; keep chunk size fixed (default 1,000) for the primary comparison and optionally sweep chunk size as a secondary experiment.
- For each size/concurrency pair: perform one unreported warm-up where feasible, then at least three measured repetitions. Alternate variant order across repetitions to reduce cache/order bias.
- Capture environment details (CPU/architecture, RAM if known, PostgreSQL version/settings, connection locality, Python version, package revision, worker count, chunk size, atomicity).
- Produce machine-readable CSV/JSON plus a concise Markdown summary; do not turn measurements into CI timing thresholds.

## Correctness and safety

- Isolate all data in benchmark-owned tables or a dedicated database/schema; benchmark must refuse to operate on a database lacking an explicit benchmark safety marker/name.
- Never truncate `keelflow_jobs` or other shared engine tables. Use unique benchmark IDs and scoped cleanup only.
- Confirm both variants produce the same expected valid, invalid, inserted, and updated counts and equivalent product state before reporting timings.
- Keep benchmark opt-in and document that synthetic/local results are not a vendor parity claim.

## Risks and mitigations

- Direct SQL does less orchestration work: label as lower-bound/data-path baseline, not an apples-to-apples feature comparison.
- A single local database may be the bottleneck: capture environment and observe DB load; avoid overclaiming.
- Repetition at 500k may take time and use disk: keep data synthetic, isolate resources, expose explicit sizes and a small smoke option, and ensure cleanup is scoped.
- Current workflow imports raw rows into per-job `keelflow_row_status`; this adds intentional durability/status cost. Include it in Keelflow processing time and report it, rather than excluding the engine's real responsibilities.

## Alternatives rejected

- New unrelated ERP demo: repeats the product scenario already present in `bulk_import`.
- Compare full n8n/Flowise/Jira products immediately: no shared deployment/integration contract, and this would conflate connector/platform overhead with database execution. Revisit only after workload and metric definitions stabilize.
- Optimize engine before measuring: no current representative result proves the engine is the bottleneck.
