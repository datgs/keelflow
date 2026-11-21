# Product-fit discussion: ERP batch processing and adjacent workflow tools

Status: discussion draft — no feature decisions approved.

## Current focus: ERP bulk processing

Keelflow is an embedded, Postgres-backed execution library for applications that own their database and domain logic. The strongest product fit is a durable engine inside an ERP or business application, not a drop-in replacement for a full workflow platform.

Current bulk contract already supports parent → chunks → rows, chunk- or row-level atomicity, retry/resume of unfinished rows, a failed-row queue, and retention (`spec/04-bulk.md`). Local benchmark evidence is limited to a noop handler: 100,000 rows staged at ~163k rows/s and processed at ~10k rows/s with one worker and chunk size 1,000 (`.spikes/bulk_bench.md`). This is not evidence for real ERP writes or a performance guarantee.

Open product questions:
- Which slow ERP task is the target: importing new records, mass updates, validation, recalculation, posting, or bulk approval?
- What dataset sizes and completion-time target matter (e.g. 100k, 1M rows; minutes vs hours)?
- Must every row be independently committed, or can a chunk/whole set be atomic?
- Is the bottleneck DB writes, Python row handlers, lock contention, workflow/job scheduling, or external services?
- Should Keelflow own row orchestration only, while applications provide set-based SQL/COPY handlers where appropriate?
- What operational behavior is required: progress, pause/cancel, retry failed rows, audit, preview/dry-run, and resumable import?

Possible gap to investigate, not an approved feature: current bulk execution calls `process_row` per row and updates row status individually; the existing benchmark uses a noop handler. A fair next benchmark should include representative Postgres validation and writes, realistic indexes and constraints, multiple worker counts, and both row and chunk atomicity before proposing optimization. A set-based/COPY path may be more valuable than adding generic workflow features, but evidence is still missing.

## Parked comparison topics — revisit later

### n8n
Explore Keelflow as an execution backend for selected business-critical steps called by an n8n workflow, not as a replacement for n8n's visual editor, integration/node catalog, and broad automation surface. Validate the integration boundary and failure/retry ownership before proposing features.

### Jira workflows
Compare Keelflow's entity state machines and human tasks with Jira statuses/transitions, transition conditions, validators, post-functions, screens, and plugin rules. Likely fit is application-owned ERP lifecycle, not Jira project administration or issue tracking.

### Flowise
Compare only if AI/agent orchestration becomes a target. Flowise has chat/agent-oriented flows, tool integrations, and conversational state; Keelflow's likely role would be durable business execution, external waits, and human approval around selected calls, not LLM graph authoring or chat UX.

These are parking-lot topics only. No compatibility, replacement, or feature commitment is made.

## Batch callback decisions accepted in discussion

- Add an optional batch callback alongside `process_row`; the existing per-row API remains supported.
- The callback returns exactly one outcome for every input row. Outcomes are correlated by the engine-owned row ID, not `row_key`; `row_key` remains the application/business identifier and error-queue context. This avoids imposing uniqueness on application row keys, which the current schema does not guarantee (`src/keelflow/postgres/schema.sql`, `keelflow_row_status`).
- The whole batch runs in one transaction. If the callback raises or its result set is malformed (missing, duplicate, or unknown row IDs), rollback and retry the batch under the job retry policy. Explicit per-row business errors are returned as row outcomes and stored in the error queue.
- `process_row` remains the compatibility path.

## External integration / transaction sequencing principle

A database transaction cannot atomically commit an external HTTP/API side effect. Applications must not hold a database transaction open while calling a remote service or assume a rollback can undo the remote call. Prefer: commit durable intent and a visible state transition plus an outbox message; a worker sends the request after commit, using an idempotency/correlation key; then the application verifies/translates the callback and resolves the Keelflow wait plus its domain updates in one caller-owned transaction. Unknown remote outcomes require idempotent retry or reconciliation. Keelflow supplies durable jobs, transactional outbox, and wait/signal primitives; provider credentials, request/callback handling, and business correlation remain application-owned (`spec/00-overview.md`, `spec/02-job-lifecycle.md`).

These decisions are accepted discussion inputs for a future implementation plan; implementation has not been approved.
