---
status: completed
last_reviewed: 2026-11-21
superseded_by: []
related_code:
  - src/keelflow/engine/bulk.py
  - spec/04-bulk.md
  - examples/bulk_import/src/bulk_import/bench.py
  - examples/bulk_import/tests/test_bench.py
---

# Execution Plan: Chunk-level batch callback for bulk jobs

Date: 2026-11-21
Path: Standard

## Outcome and Approach

Add an optional `process_batch` callback to `BulkJobHandler` so applications can perform set-based database work for one engine chunk while Keelflow retains durable chunk/row state, retries, cancellation checkpoints between chunks, fan-in, and error reporting. Preserve `process_row` as the compatibility path.

The callback receives all rows in one engine chunk and the transaction cursor. It returns exactly one typed outcome for each engine-owned row ID. The whole chunk callback and all corresponding row-state updates commit or roll back together. Batch mode requires `atomic='chunk'`; `atomic='row'` remains supported by the existing per-row callback and is rejected for batch-only handlers. Callback exceptions or malformed result sets roll back and retry the chunk under the existing chunk-job retry policy. Explicit per-row error outcomes are committed to the failed-row queue without discarding successful outcomes in that batch.

This is also an external-integration boundary: batch callbacks must not perform remote side effects inside a retryable DB transaction. Applications should commit durable intent/outbox work and state before external calls, then reconcile results via idempotent callbacks; the batch callback itself should perform transaction-bound DB work only.

Before using benchmark data to evaluate the feature, correct the existing benchmark's worker-concurrency wiring and scoping/correctness gates, then measure the old per-row path and new batch path on 100k/500k catalog workloads against the PostgreSQL baseline.

## Accepted Decisions

- `process_batch` is optional; `process_row` continues to work.
- The batch boundary is exactly one `keelflow_chunks` unit, bounded by existing `chunk_size`. Cancellation is observed between chunks, not inside the transaction.
- Batch callbacks are chunk-atomic only. `atomic='row'` is incompatible with the batch path; do not silently ignore the flag.
- Return a typed list of row outcomes, each containing the engine-owned row ID, optional output dict, and optional error string. Require exactly one unique result per input row; missing, duplicate, or unknown IDs are malformed batch results.
- Row ID, not `row_key`, correlates callback outcomes. `row_key` remains application identity/error context and is not made unique.
- Callback exception or malformed result: roll back all callback effects and retry the chunk under the current retry policy. After retry exhaustion, use the existing chunk failure/DLQ/fan-in behavior; do not mark every row as a business-invalid row.
- Explicit outcome error: that row becomes `failed` with its error, while successful rows become `done`; all row state and application DB effects commit in the same transaction.
- Applications own external authentication, request/callback security, provider correlation, and reconciliation. Keelflow exposes durable job/outbox/wait/signal mechanisms; no external network request is made by the engine batch machinery.
- No automatic fallback from a failing/malformed `process_batch` to `process_row`; that would hide bugs and create non-obvious runtime behavior.

## Context and Evidence

- Bulk implementation currently loops per row and performs processing/status SQL in `_process_one_row` (`src/keelflow/engine/bulk.py:313-334`). Chunk-atomic path likewise invokes `process_row` for each row (`src/keelflow/engine/bulk.py:279-311`).
- Chunk dispatch fetches a full chunk, then branches on `atomic` (`src/keelflow/engine/bulk.py:205-230`); cancellation checkpoint currently occurs between rows in row-atomic mode (`src/keelflow/engine/bulk.py:215-224`). Batch mode must put its checkpoint before entering the one-transaction batch callback.
- Chunk job retry is currently registered with `RetryPolicy(max_attempts=3)` (`src/keelflow/engine/bulk.py:542-556`).
- `row_key` has no uniqueness constraint (`src/keelflow/postgres/schema.sql:99-112`), so it is not a safe outcome key.
- Spec currently defines chunk/row atomicity and per-row processing (`spec/04-bulk.md:18-24,51-65`); this is an externally visible contract and must be updated with the implementation.
- The local ERP benchmark reports ~3.5k rows/s for per-row handling and ~150k rows/s for a set-based PostgreSQL baseline (`.spikes/erp_bulk_bench/RESULTS.md`). The baseline omits workflow/job/chunk state, so it is a data-path floor, not equal-cost comparison.
- Benchmark caveat discovered during planning: `bench.py` declares the queue using `max(args.workers)` once, while the `n_workers` argument is unused (`examples/bulk_import/src/bulk_import/bench.py:184-193,353-357`). Thus the current 1/4/8 results are not valid separate concurrency measurements. The runner also needs stronger run scoping and correctness gating before publishing comparative results.
- External integrations are app-owned. Transactional intent and same-connection rules are defined in `spec/00-overview.md:15-24`; external wait/callback ownership and atomic signal resolution are defined in `spec/02-job-lifecycle.md:106-126`.

## Scope

In scope:

- Public SDK result type and handler hook contract for batch processing.
- Batch handler dispatch, result validation, row-state updates, and chunk-atomic transaction behavior.
- Atomic-mode validation for batch-only handlers; preserve existing `process_row` behavior.
- Retry, exhaustion, cancellation-at-chunk-boundary, fan-in, row error, duplicate row_key, and duplicate/unknown/missing outcome coverage.
- Update authoritative bulk spec and API documentation/example.
- Use the ERP import demo to exercise batch SQL validation/upsert/error queue.
- Correct benchmark concurrency configuration, isolate all per-run output by `batch_id`, verify correctness before reporting timings, and record valid 100k/500k results.

Out of scope:

- New database adapter or SQL dialect abstraction; callbacks receive the existing transaction/cursor contract.
- Automatic SQL generation, inferred set-based transformations, or implicit fallback to per-row callbacks.
- Atomicity spanning multiple chunks or the entire bulk job.
- Holding DB transactions open across HTTP/API calls; provider calls belong in separate durable jobs/outbox handling.
- New external connector framework, workflow editor, or UI.
- Performance threshold in CI or general parity claim against n8n, Flowise, Jira, or ERPs.

## Proposed Contract

```python
@dataclass(frozen=True)
class BatchRowResult:
    row_id: int
    outputs: dict[str, Any] | None = None
    error: str | None = None

class BulkJobHandler(JobHandler):
    async def process_batch(
        self, ctx: JobContext, rows: list[dict[str, Any]], tx
    ) -> list[BatchRowResult]: ...
```

Contract details to finalize in implementation against SDK style:

- A handler may override `process_row`, `process_batch`, or both. Batch-only handlers are valid only with `atomic='chunk'`; row-only handlers retain current behavior. If both exist, the selected implementation must be deterministic and documented (recommended: `process_batch` is selected for `atomic='chunk'`; `process_row` selected for `atomic='row'`).
- `rows` include at least engine-owned `id`, `row_key`, and `data`, ordered by engine row ID.
- Outcome list must contain exactly the input row IDs once each; result order may differ because matching is by ID.
- A success has `error is None`; an explicit business error has a non-empty error string and may include outputs only if product semantics approve them. Do not treat exception as row-level business failure in batch mode.
- Malformed outcome list raises a dedicated/value error before any row status is committed; the surrounding transaction rolls back.
- For valid outcomes, update row states and errors in a set-based statement keyed by row ID in the same transaction as callback writes.
- Batch callback raises on infrastructure/transaction failures. Existing retry policy retries the entire chunk. Business-invalid rows are returned as explicit error outcomes and do not fail the chunk.

## Execution Tasks

### Task 1 — SDK contract and spec semantics

- **Goal:** Define and expose a stable typed row outcome and optional batch callback contract.
- **Context:** Accepted decisions above; existing `process_row` is public and remains supported.
- **Required outcomes:** Export `BatchRowResult` (or equivalent chosen naming); document required fields and one-result-per-engine-row invariant; define handler selection when both hooks exist; define batch-only atomic-mode validation; specify exception vs explicit row-error behavior and retry exhaustion.
- **Constraints:** No uniqueness requirement on `row_key`; no new status enum; no automatic fallback; keep API minimal.
- **Acceptance evidence:** SDK unit tests for result semantics and registration/configuration validation; spec describes allowed and forbidden uses.
- **Verification:** `uv run pytest tests/test_sdk.py tests/test_bulk.py -q`; `uv run ruff check src examples tests`; `uv run mypy src/keelflow`.
- **Ownership:** `src/keelflow/sdk/`, `src/keelflow/engine/registry.py` if registration validation is chosen, `spec/04-bulk.md`, SDK exports.
- **Dependencies:** None.

### Task 2 — Chunk-atomic batch engine path

- **Goal:** Execute one batch callback per chunk in a single transaction and atomically persist all per-row outcomes.
- **Context:** Current chunk worker branches into per-row or chunk-atomic loops (`src/keelflow/engine/bulk.py:205-334`).
- **Required outcomes:** Batch path selected according to Task 1 contract; pass a full bounded chunk; run cancellation checkpoint before batch TX; run callback plus set-based status/error updates in the same transaction; malformed result/exception rolls all of it back; retry entire chunk; after exhaustion existing chunk failure/fan-in applies; retry does not duplicate committed effects; explicit row failures do not fail sibling rows or chunk; duplicated `row_key` works because engine row ID is the outcome key.
- **Constraints:** No network calls in engine; do not change per-row semantics; no cross-chunk transaction; no mid-TX cancellation.
- **Acceptance evidence:** Integration tests prove transaction rollback on callback exception/malformed result, retry then success, exact outcomes, status/domain-write atomicity, chunk failure on exhausted retry, cancellation between chunks, and duplicate row_key support.
- **Verification:** `uv run pytest tests/test_bulk.py -q`; repeat focused concurrency/retry tests; `git diff --check`.
- **Ownership:** `src/keelflow/engine/bulk.py`, focused `tests/test_bulk.py`.
- **Dependencies:** Task 1.

### Task 3 — Application example and external-side-effect guidance

- **Goal:** Demonstrate a real set-based ERP catalog import using the new callback, while retaining the row callback path as a documented alternative.
- **Context:** `examples/bulk_import` already has validation, SKU upsert, row error queue, and human gates; timed machine benchmark excludes human waiting.
- **Required outcomes:** Batch handler performs validation/classification, valid-row upsert, and explicit per-row error result; results correlated by engine row ID; test compares final domain/error state against the existing per-row path on identical data. Document that callback runs in a retryable transaction and must not call external APIs; show durable intent/outbox → remote call outside transaction → callback/reconciliation sequence in guidance if an external side effect is relevant.
- **Constraints:** Keep provider credentials/callback routes app-owned; no new UI; do not put remote API effects inside batch callback.
- **Acceptance evidence:** Example integration test proves identical valid/invalid outcome counts and product state across row and batch implementations; failure/retry does not duplicate domain writes.
- **Verification:** `uv run pytest examples/bulk_import/tests -q`; ruff and mypy repository commands.
- **Ownership:** `examples/bulk_import/src/bulk_import/registry.py`, benchmark code/tests, example README; avoid engine internals.
- **Dependencies:** Task 2.

### Task 4 — Correct benchmark and measure actual concurrency/performance

- **Goal:** Produce valid comparisons for per-row, batch callback, and SQL baseline at 100k/500k rows.
- **Context:** Current benchmark's worker concurrency input is unused; old measurements cannot substantiate concurrency behavior.
- **Required outcomes:** Configure a separate queue/worker per concurrency level or otherwise ensure each run actually uses the requested concurrency; capture effective worker config; unique output scoping per benchmark run/variant; verify expected result counts and product/error state before accepting timing; warm-up and ≥3 repetitions at 100k, and ≥3 at 500k if runtime permits; record raw JSON/CSV and Markdown summary; separate staging from processing; summarize median/spread and rows/s.
- **Constraints:** Isolated benchmark DB/schema/tables and explicit opt-in; never truncate shared engine tables; SQL baseline labeled data-path floor; include engine row/chunk bookkeeping in engine timings; no CI timing gate.
- **Acceptance evidence:** Test proves requested concurrency is what worker queue executes; no cross-run result contamination; each timed run passes correctness validation; complete environment/configuration recorded; reproducible 100k/500k results.
- **Verification:** benchmark smoke test; run documented 100k and 500k commands; inspect raw results; `uv run pytest`, `uv run ruff check src examples tests`, `uv run mypy src/keelflow`, `git diff --check`.
- **Ownership:** `examples/bulk_import/src/bulk_import/bench.py`, `examples/bulk_import/tests/test_bench.py`, `.spikes/erp_bulk_bench/` results/docs.
- **Dependencies:** Task 3.

### Task 5 — Integrated contract and operational review

- **Goal:** Verify public contract, failure handling, external-side-effect guidance, and full repository stability together.
- **Context:** Batch processing affects retries, transaction boundaries, cancellation, bulk reporting, and error semantics.
- **Required outcomes:** Review spec/code/tests alignment; run focused tests repeatedly; run full validation; check no migration is needed (expected: existing row IDs/status fields suffice); update plan with benchmark findings and unresolved risks.
- **Constraints:** No release, tag, push, CI policy change, or engine optimization beyond approved callback scope.
- **Acceptance evidence:** Full test/ruff/mypy/diff checks pass; no uncommitted shared-data artifacts; behavior matrix covers legacy row path and new batch path.
- **Verification:** `uv run pytest`; `uv run ruff check src examples tests`; `uv run mypy src/keelflow`; `git diff --check`.
- **Ownership:** Integration owner across `spec/04-bulk.md`, source/tests, example docs, plan.
- **Dependencies:** Tasks 1–4.

Dependency graph: Task 1 → Task 2 → Task 3 → Task 4 → Task 5.

## Execution Guidance

- Critical path: typed contract → engine transaction semantics → example → corrected benchmark → integrated validation.
- Do not parallelize Tasks 1–4: shared bulk API/spec/registry/benchmark assumptions require a single evolving contract. Independent test review can be added only if ownership does not overlap.
- Update this plan if benchmark wiring or retry behavior shows assumptions are wrong; stop for user approval if semantics must change.

## Risk and Recovery

| Area | Risk | Evidence / mitigation |
|---|---|---|
| Transaction / retry | Callback could commit external side effects then retry | Contract and docs forbid external I/O in callback; use app-owned outbox/job and idempotency/reconciliation |
| Atomicity | `atomic='row'` conflicts with whole-batch commit | Require batch path to use `atomic='chunk'`; reject incompatible batch-only configuration |
| Result integrity | Missing/duplicate/unknown IDs could silently strand rows | Validate exact set of engine row IDs before writes; malformed result aborts transaction and retries |
| Memory / lock time | Huge chunk means large input/result lists and long transaction | Reuse configured `chunk_size` as the upper bound; cancellation only at chunk boundaries |
| Compatibility | Existing handlers rely on `process_row` behavior | Preserve current branch and tests; opt-in batch hook only |
| Performance claims | SQL baseline omits durable orchestration; prior concurrency readings invalid | Label baseline honestly, fix benchmark setup, verify correctness per run, report environment and spread |
| Recovery | Worker dies during batch | Transaction rollback leaves rows pending after reclaim reset; retry whole chunk; durable application writes roll back with row state |

## Verification Strategy

- Unit: typed outcomes, malformed result validation, handler selection/configuration.
- Postgres integration: batch success + business errors; callback exception rollback; malformed list rollback/retry; retry exhaustion; duplicate row keys; cancellation boundary; fan-in/parent status; legacy `process_row` regression.
- Example: same staged deterministic catalog through both paths; compare valid products and error counts.
- Benchmark: 100k and 500k × per-row, batch, SQL baseline; requested concurrency truly enforced; ≥3 measured reps where feasible; correctness gate per run.
- Repository: `uv run pytest`, `uv run ruff check src examples tests`, `uv run mypy src/keelflow`, `git diff --check`.

## Risks and Approval

- Accepted decisions: callback/result semantics, chunk-only atomicity, row ID correlation, retry whole batch for raised/malformed outcomes, `chunk_size` bound, cancellation between chunks.
- Open risk: local PostgreSQL benchmark results may not predict customer hardware or DB settings; report only measured environment.
- No schema migration expected; verify by implementation.
- External API integration remains application-owned and outside the callback transaction.
- User approval to implement batch feature: discussion decisions accepted; **explicit approval to execute this detailed plan is still required**.

## Live Plan Rule

If implementation evidence changes the callback, retry, atomicity, benchmark safety, or external-side-effect contract, pause, write an evidence-backed plan-change proposal here, and request user approval for material behavior changes.
