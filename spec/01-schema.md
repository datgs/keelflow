# hd-flow spec — schema contract

Reference DDL: `src/keelflow/postgres/schema.sql`. Column names are
normative; types adapt per DB (`jsonb`→`json`/`nvarchar(max)`…).

## Tables

| Table | Purpose | Key invariants |
|---|---|---|
| `hd_flow_jobs` | One row per job (durable intent + mirror of kernel job) | `status` enum; `context` JSONB snapshot at enqueue; `concurrency_key` serializes execution; `parent_job_id` for bulk children; `assignee`/`claimed_by` for human jobs; `cancel_requested` cooperative cancel flag |
| `hd_flow_workflow_runs` | One run of a workflow definition | `inputs` JSONB; `status` incl. `blocked` while a human step waits |
| `hd_flow_step_runs` | Per-step state within a run | `job_id` links to the executing job; `outputs` JSONB feeds `${{ }}` refs |
| `hd_flow_chunks` | Bulk scheduling unit | `parent_job_id`; `status`; lease fields for reclaim |
| `hd_flow_row_status` | Per-row outcome for bulk jobs | `(job_id, row_key)` unique-ish; `status` pending/processing/done/failed; `chunk_id` links row→chunk; `error` for error queue |
| `hd_flow_outbox` | Transactional event intents | written in caller TX; `published_at` set by dispatcher |
| `hd_flow_outbox_deliveries` | Per-subscriber delivery dedup | `(event_id, subscriber)` unique; enables at-least-once + dedup |
| `hd_flow_entity_state` | State-machine current state | PK `(entity_type, entity_id)`; `version` for optimistic checks |
| `hd_flow_audit` | Append-only audit | actor JSONB incl. `via` chain; `changes` JSONB `{k:{before,after}}`; `correlation_id` |
| `hd_flow_dlq` | Execution failures after retry exhaustion | requeue with edited payload |

## Status enums (normative)

- jobs / step_runs / workflow_runs / chunks:
  `pending | claimed | running | blocked | done | failed | cancelled`
- row_status: `pending | processing | done | failed`
- outbox: presence in table = pending; `published_at NOT NULL` = delivered
