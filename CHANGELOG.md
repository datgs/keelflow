# Changelog

All notable changes to this project will be documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Kernel port (`keelflow.engine.kernel`): engine dispatch now goes
  through a backend-neutral `Kernel` protocol (`KernelJobSpec`,
  `dispatch_spec`, `as_kernel`). `ChancyKernel` adapts the existing
  chancy client with no behavior change; `configure` still accepts a
  raw `chancy.Chancy` for compatibility.
- Experimental `NativeKernel` (`keelflow.engine.native_kernel`): a
  second dispatch backend that claims straight from `keelflow_jobs`
  (no kernel table) — `FOR UPDATE SKIP LOCKED` claim, per-job
  `lease_until` renewal, reaper for expired claims, heartbeat fencing,
  LISTEN/NOTIFY wakeup with poll fallback. Opt-in only; Chancy remains
  the default. Known gaps: rate limits, queue pause, tags, priority,
  metrics plugins. Conformance: `tests/test_native_kernel.py`.
- Proof-only `SQLiteKernel` (`keelflow.engine.sqlite_kernel`, optional
  `keelflow[sqlite]` extra): same single-record model in SQLite —
  file-backed, `UPDATE … RETURNING` claim under write serialization,
  in-process wakeup. Exercises the kernel port outside Postgres; the
  engine and `JobService` remain Postgres-bound.
- Adapter-parameterized kernel contract suite
  `tests/test_kernel_contract.py` with injected stub dispatch —
  `--kernel=chancy|native|sqlite|both|all` proves push/claim/lease/
  fencing/dedup semantics per backend.

## [0.1.0a3] — 2026-11-23

### Added

- `examples/financial_approval` — the largest demo (~23-step payment-run
  DAG): intake → parallel risk fan-out (machine + `wait.fx_quote` /
  `wait.bank_preverify`) → risk fan-in → a sequential multi-level human
  approval chain gated by `if_expr` (manager → controller → CFO →
  compliance) → GL posting → a set-based `process_batch` payment-line
  step → `wait.bank_execute` external settle → reconcile/remit/audit.
  Exercises parallel `needs`, wait/signal, conditional human steps,
  bulk, cooperative cancel, retry, and audit in one run.
- Permission catalog: `Registry.register_permission` +
  `Registry.permissions`. `InMemoryRegistry.register_state_machine`
  rejects a `guard.permission` not previously registered (engine
  reserves `job.cancel`/`job.signal`) — unknown permissions now fail
  at load time per the Registry contract, not as a silent 403.

## [0.1.0a2] — 2026-11-23

### Added

- Chunk-level batch callback `BulkJobHandler.process_batch(ctx, rows, tx)`
  returning `list[BatchRowResult]` — an optional set-based fast path for
  write-heavy bulk imports: one callback per engine chunk (bounded by
  `chunk_size`), one outcome per ENGINE row id (`row_key` stays business
  identity, not a correlation key), whole callback + row-state flips in
  one transaction. Requires `atomic='chunk'`; `process_row` unchanged and
  still the row-atomic path. Callback exception or malformed result
  (missing/duplicate/unknown row ids) rolls back and retries the chunk —
  attempt budget inherited from the parent bulk type's `RetryPolicy`; only
  the last attempt terminalizes the chunk/parent. Per-row business errors
  commit siblings to `done` while failed rows enter the error queue.
  Batch callbacks must not perform external I/O (retry would repeat the
  side effect) — stage remote calls via durable jobs/outbox. ERP benchmark
  (`examples/bulk_import` bench, v5): batch ≈ 5× the per-row path at equal
  worker counts (≈15–16.7k vs ≈3.4k rows/s at 8 workers on 1M rows);
  ~60s total for 1M rows at workers 8–16, `chunk_size` 1000–2500.
- External-wait steps (`wait.*` + `JobTypeSpec.is_wait`): a DAG step
  that parks the run `blocked` with a durable job row and **no kernel
  claim** — zero worker capacity while waiting. The application
  resolves it via `JobService.signal(job_id, result, ctx)` /
  `signal_failure` after its own external callback (provider auth,
  correlation and translation stay app-owned); resolution + step update
  + run advance commit in the caller's transaction. First signal wins
  (`FOR UPDATE`); cancelled/terminal waits reject later signals; direct
  enqueue of wait types is rejected. A `wait.opened` outbox event marks
  the committed point where the app may start its external call.
  `supplier_onboarding` demos it: `wait.bank_verify@v1` resolved by an
  app-owned mock provider callback route.
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
- Cooperative checkpoint cancellation — `JobService.cancel(job_id, ctx)`
  (requires `job.cancel`) records a durable request: pending jobs
  terminalize `cancelled`; running jobs keep status and drain unless
  the type opted into `cooperative_cancel=True`, in which case
  `await ctx.execution.checkpoint()` observes the flag, raises
  `JobCancelled` (BaseException — cannot be swallowed by `except
  Exception`), rolls back the handler transaction, and terminalizes
  `cancelled` without retry or DLQ. The pending→running transition is
  an atomic conditional update so a cancel committed before start
  always wins.
- `kill_previous` cron overlap now drains running jobs (flag only)
  instead of flipping them to `cancelled` mid-run.
- `JobService.cancel` on a `pending` job now advances its owning
  workflow step in the same transaction — previously a cancelled
  `wait.*`/`human.*` step stayed `blocked` and hung the run (found by
  randomized event-sequence tests).
- Bulk-aware cancellation: `JobService.cancel` cascades the request to
  non-terminal children; `bulk.chunk@v1` checkpoints between rows
  (`cooperative_cancel=True`); a cancelled chunk settles its chunk row
  and runs fan-in — including when cancelled before dispatch — so the
  parent terminalizes `cancelled` instead of hanging `running`;
  children spawned after a request inherit the flag. `bulk_import`
  demo gained a **Cancel job** button wired to `jobs.cancel`.
- `examples/rag_demo` — employee-helpdesk RAG demo; i18n (5 locales)
  across the demo UIs; Mermaid workflow diagrams + roles in demo READMEs.
- Alembic 0003–0007.

### Performance

- Set-based bulk partition — one statement does claim + row numbering +
  chunk-row insert + `unnest`-joined row→chunk assignment + multi-row
  `keelflow_jobs` insert (hash join, no nested loop). Combined with
  `INSERT … SELECT` staging (`jsonb_build_object` server-side), this cut
  1M batch total ~69s → ~60s; stage ~17s → ~9s; parent dispatch TX
  ~37s → ~26s.

### Testing

- Benchmark reliability gate — dedicated `KEELFLOW_BENCH_DSN` (no dev
  fallback), shared advisory lock with soak (mutual exclusion), all
  output scoped by `batch_id`, engine cleanup by `job_id`, exact
  per-row product+error oracle; invalid runs excluded + non-zero exit.
- `--variants` flag on the bench runner to sweep a subset
  (`keelflow-batch` only) — used for the v5 worker-scaling + chunk-size
  sweep (chunk 250–5000 × workers 1–16, 50/50 runs pass the gate).

### Removed

- `JobHandler.heartbeat()` — was never invoked; worker liveness is
  kernel-owned (chancy heartbeat + Recovery). `JobHandler.on_cancel`
  remains an experimental placeholder the worker does not invoke.

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
