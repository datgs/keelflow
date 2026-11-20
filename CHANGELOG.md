# Changelog

All notable changes to this project will be documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added (next release)

- Engine hardening: `cancelled` as a distinct terminal outcome, a
  definition-drift guard in `advance_run`, a per-step savepoint, claim-time
  `cancel_requested`, real `RetryPolicy` backoff scheduling,
  `JobTypeSpec.timeout_seconds`, `TaskUiSpec.due_in_seconds` → `due_at`,
  `HumanTaskService.release`, and a non-blocking outbox subscriber path.
- `keelflow_dlq` + `MonitoringService.dlq_list`/`dlq_replay`, with
  `run_id`/`step_run_id` links so replay of a dead DAG step **resumes the
  owning run** rather than orphaning it.
- `MonitoringService.sweep` — caller-invoked, batched retention for
  terminal jobs/runs, published outbox, `row_archive`, and DLQ rows.
- Bulk `row_disposition` (`archive` default / `keep` / `delete`) — `done`
  staging rows move to `keelflow_row_archive` at fan-in; a `FOR UPDATE`
  parent lock removes a fan-in race.
- Size visibility — `result_max_bytes` (opt-in hard cap),
  `result_warning_bytes` (soft warning), `result_bytes`/`outputs_bytes`
  columns, and `results_oversized`.
- `JobBuilder.unique_key` — idempotent enqueue dedup on
  `(type, unique_key)` for non-terminal jobs.
- `examples/rag_demo` — employee-helpdesk RAG demo; i18n (5 locales)
  across the demo UIs; Mermaid workflow diagrams + roles in demo READMEs.
- Alembic 0003–0007.

## [0.1.0a1] — 2026-11-19

### Added — engine (0.1.x alpha)

- Durable jobs with transactional enqueue (`keelflow_jobs` + kernel row
  in one TX), pydantic payload validation at enqueue and dispatch,
  `JobContext` (actor `via` chain, permissions, correlation).
- DAG workflows: `needs`/`if:` guards/`${{ outputs }}` refs, parallel
  ready steps, `human.*` steps park a run (`blocked`), `resume_step` /
  `resume_run`.
- **Workflow triggers**: `WorkflowTriggerSpec` on SM transitions —
  `on_done`/`on_rejected`/`on_failed` callbacks applied exactly-once in
  the run's terminal transaction; `failure_job` fix-up task on technical
  failure; `terminal_outcome` (`succeeded|rejected|failed`) and
  `callback_state` columns.
- **Single-TX dispatch**: handler run + job finish + `advance_run` in
  one transaction; domain writes via `ctx.tx` (never serialized).
- Entity state machines: guarded transitions, `creates_job` /
  `triggers_workflow`, `allowed_actions`, first-transition locking,
  `complete_and_transition` linking SM tasks to mapped actions.
- Human tasks: `my_tasks` inbox, first-claim-wins, overdue escalation;
  `decision: reject` → business-rejection outcome.
- Bulk processing: `BulkJobHandler` + `bulk.chunk@v1` fan-out/fan-in,
  per-row status, `atomic='chunk'|'row'`, kill-resume, error queue.
- Transactional outbox events with per-subscriber delivery dedup.
- Cron scheduling (`CronSpec`: window, overlap policies) and
  key-serialized execution (`concurrency_key` advisory locks).
- Monitoring data APIs, minimal SuperAdmin UI (no auth), read-only MCP
  tools, audit service with `via` actor chains.
- `spec/` — language-agnostic contract (8 files) with conformance
  checklist.
- `examples/invoice_flow` — full demo app (SM + HITL + DAG + triggers);
  `examples/order_flow.py` minimal example.
- CI: GitHub Actions (Postgres service; ruff + mypy + pytest).

### Fixed

- `concurrency_key` no longer maps to the kernel `unique_key` (jobs were
  silently deduplicated away); serialization uses advisory locks.
- Kernel table prefix defaults to `keelflow_kernel_` — avoids collision
  with engine `keelflow_jobs`/`keelflow_queues`.
- `blocked` steps count as active for run completion.
- Dispatch skips re-running handlers for terminal jobs (double domain
  effects on kernel redelivery) while still advancing the run.
- Handler exceptions leave the job `running` while kernel retries
  remain — only exhaustion marks it `failed` and fires `on_failed`.

## [0.1.0a1] — 2026-11-19

First public alpha release on PyPI.
