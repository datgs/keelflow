# keelflow spec — schema contract

Reference DDL: `src/keelflow/postgres/schema.sql`. Column names are
normative; types adapt per DB (`jsonb`→`json`/`nvarchar(max)`…).

## Tables

| Table | Purpose | Key invariants |
|---|---|---|
| `keelflow_jobs` | One row per job (durable intent + mirror of kernel job) | `status` enum; `context` JSONB snapshot at enqueue; `concurrency_key` serializes execution; `parent_job_id` for bulk children; `assignee`/`claimed_by`/`due_at` for human jobs; `cancel_requested` cooperative cancel flag |
| `keelflow_workflow_runs` | One run of a workflow definition | `inputs` JSONB; `status` incl. `blocked` while a human step waits; trigger columns `triggered_by_entity_type/_id`, `trigger_config`, `trigger_ctx`, `terminal_outcome` (`succeeded|rejected|failed|cancelled`), `callback_state` (`none|pending|done|failed`) — see `03-workflow.md` |
| `keelflow_step_runs` | Per-step state within a run | `job_id` links to the executing job; `outputs` JSONB feeds `${{ }}` refs |
| `keelflow_chunks` | Bulk scheduling unit | `parent_job_id`; `status`; lease fields for reclaim |
| `keelflow_row_status` | Per-row outcome for bulk jobs | `(job_id, row_key)` unique-ish; `status` pending/processing/done/failed; `chunk_id` links row→chunk; `error` for error queue |
| `keelflow_outbox` | Transactional event intents | written in caller TX; `published_at` set by dispatcher |
| `keelflow_outbox_deliveries` | Per-subscriber delivery dedup | `(event_id, subscriber)` unique; enables at-least-once + dedup |
| `keelflow_entity_state` | State-machine current state | PK `(entity_type, entity_id)`; `version` for optimistic checks |
| `keelflow_audit` | Append-only audit | actor JSONB incl. `via` chain; `changes` JSONB `{k:{before,after}}`; `correlation_id` |
| `keelflow_dlq` | Execution failures after retry exhaustion | requeue with edited payload |

## Status enums (normative)

- jobs / step_runs / workflow_runs / chunks:
  `pending | claimed | running | blocked | done | failed | cancelled`
- workflow_runs.terminal_outcome: `succeeded | rejected | failed`
- workflow_runs.callback_state: `none | pending | done | failed`
- row_status: `pending | processing | done | failed`
- outbox: presence in table = pending; `published_at NOT NULL` = delivered
