# Discovery: ERP bulk execution benchmark

Date: 2026-11-21
Status: planning evidence; no implementation approved.

## Desired outcome

Measure Keelflow's execution performance on a concrete ERP-like task at 100,000 and 500,000 records, against a PostgreSQL set-based baseline that produces equivalent business outcomes. This compares operation/runtime, not workflow editors or visual configuration tools.

## Verified repository facts

- `examples/bulk_import` already models catalog import: staged CSV rows, validation, human gates, row-level bulk processing, SKU upsert, and failed-row export (`examples/bulk_import/README.md:3-20`).
- Bulk handlers process rows through `process_row`; row atomicity isolates failures (`spec/04-bulk.md:18-24,55-65`).
- `ProcessRowsH` validates and upserts one product per callback (`examples/bulk_import/src/bulk_import/registry.py:161-186`).
- Existing bulk benchmark uses a noop handler; its 100k/10k rows/s observation is not representative of ERP writes (`.spikes/bulk_bench.py:31-33`, `.spikes/bulk_bench.md:3-15`).
- Existing benchmark truncates shared engine tables (`.spikes/bulk_bench.py:37-42`); the new benchmark must use an isolated database/schema and must not truncate a general development/test DB.
- Validation expects Postgres integration tests and project commands are `uv run pytest`, `uv run ruff check src examples tests`, `uv run mypy src/keelflow`, and `git diff --check` (repository instructions).

## Scope

Reuse the bulk import product scenario, adding a reproducible benchmark path rather than creating an unrelated example. Compare:

1. Keelflow's actual workflow/bulk path with row validation, per-row error recording, and product upsert.
2. A direct PostgreSQL set-based implementation over the same staged rows, applying identical validation/upsert/error outcomes.

Measure processing wall time and throughput separately from input generation/staging. Report counts and correctness along with performance. Clearly label direct SQL as a data-processing baseline, not a comparison of workflow orchestration, human gates, or external connectors.

## Non-goals

- No claim of general performance parity with n8n, Flowise, Jira, or ERP products.
- No workflow editor, AI configuration authoring, connector catalog, or new engine feature.
- No engine optimization before benchmark evidence identifies a bottleneck.
- No benchmark against external products or services.

## Constraints and risks

- Benchmark data is synthetic and contains deterministic valid/invalid rows.
- Product writes must be isolated by benchmark/import ID or use a dedicated isolated database; benchmark cleanup must never truncate shared project tables.
- The comparison must produce equivalent accepted/rejected row counts and product state.
- Report hardware, PostgreSQL version/configuration, worker count/concurrency, chunk size, atomicity, repetitions, and whether staging is included.
- Database cache/warm-up and execution order can bias results; use warm-up and alternate or randomize variant order across repetitions.

## Assumptions / open items

- Assumption: the meaningful ERP task is supplier catalog import/update with validation, SKU-keyed upsert, and a persistent row error queue.
- Assumption: measure execution from already-staged inputs; report staging separately so transfer/CSV parsing does not obscure processing runtime.
- Still unknown: which worker concurrency levels best represent target installations. Plan uses a bounded 1/4/8 worker-concurrency matrix unless the first pilot makes this impractical.
- Still unknown: performance on the user's production-like hardware/database. Initial results describe only the documented local benchmark environment.

## Evidence quality

High confidence for current example behavior and benchmark limitations (direct source inspection). No performance parity claim is currently supported by measured comparable workloads.
