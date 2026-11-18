---
status: completed
last_reviewed: 2026-11-18
superseded_by: []
related_code: [src/pyerp]
---

# Execution Plan: pyerp reusable workflow engine

Team: pyerp
Path: Standard

## Outcome and Approach
Build `pyerp`, a reusable Python library providing a durable job/workflow engine on Postgres: SDK contracts, storage adapter, DAG workflow runtime, bulk chunk/row processing, human tasks, entity state machines, JobContext, audit hooks, monitoring data APIs, and a minimal SuperAdmin. Kernel lifecycle is adopted (Chancy or PgQueuer per spike); all differentiating layers are built in-house. See `discovery.md` and `approach.md`; design source of truth is `docs/architecture-design-brief.md`.

## Tasks

| # | Task | Blocked by | Ownership | Output contract |
|---|---|---|---|---|
| T0 | Repo/package scaffold (`pyerp`), pyproject, CI, Alembic env | — | repo root, `flow/` skeleton | `pip install -e .` works; `pytest` runs |
| T1 | SDK contracts module (`pyerp/sdk`): JobContext, Actor, JobTypeSpec, JobHandler, JobResult, Registry, JobService, WorkflowService, EventBus, StateMachineService, PermissionService, AuditService, Storage — Protocol/ABC + type stubs | T0 | `pyerp/sdk/` | importable contract package; mypy-clean |
| T2 | Kernel benchmark spike: Chancy vs PgQueuer — enqueue/claim throughput, kill-resume, duplicate-exec, LISTEN/NOTIFY behavior | T0 | `.spikes/kernel/` | written decision memo + benchmark numbers |
| T3 | Postgres storage adapter (`pyerp/postgres`): migrations for jobs/chunks/row_status/step_runs/workflow_runs/outbox_events/audit_events; implement `Storage` protocol | T1 | `pyerp/postgres/` | `Storage` impl + `alembic upgrade head` creates schema |
| T4 | Transactional enqueue pipeline: `JobBuilder` with mandatory `.context()`, `enqueue(tx)` writing intent inside caller TX + outbox record | T1, T3 | `pyerp/engine/enqueue.py` | commit→job visible; rollback→no job/outbox row |
| T5 | Worker runtime: claim loop (kernel-backed), heartbeat/lease renew, reaper for expired leases, structured logging with job_id/correlation_id | T2, T3 | `pyerp/engine/worker.py` | `python -m flow worker` claims and completes a job end-to-end |
| T6 | Handler registry + schema validation: register `type@vN` with input/output Pydantic schemas; validate at enqueue and at execution | T1, T4 | `pyerp/engine/registry.py` | unknown/badly-typed payload rejected before enqueue |
| T7 | Workflow engine: DAG resolver (`needs`, `if:` guards, outputs), `workflow_runs`/`step_runs` state, resume-from-failed-step | T4, T5, T6 | `pyerp/engine/workflow.py` | run a multi-step DAG; kill mid-run; resume from failed step only |
| T8 | Bulk chunking runtime: parent job partitions staging rows into chunks, workers claim chunks, per-row status, error queue, chunk-atomic commit | T5, T6 | `pyerp/engine/bulk.py` | 500k synthetic rows → chunks; kill worker mid-chunk → resume pending rows only |
| T9 | YAML compiler: `workflows/*.yaml` + `state_machines/*.yaml` → internal DAG/SM models with load-time validation (cycles, needs, schema refs) | T1 | `pyerp/compiler/` | invalid definitions rejected at load with clear errors |
| T10 | Concurrency + scheduling: per-type `max_workers`, `concurrency_key` serialization, cron definitions with timezone/window/overlap | T5 | `pyerp/engine/concurrency.py`, `pyerp/engine/scheduler.py` | same key never parallel; scheduled jobs enqueue on time |
| T11 | Human task layer: `human.*` job types, Inbox data API (`my_tasks`, claim, complete), assignee by user/role, escalation timeout | T5, T6 | `pyerp/engine/human.py`, `pyerp/sdk/human.py` | human job pending → claim → complete unblocks workflow |
| T12 | State machine service: `sm.transition()` enforcing transitions+guards in TX, `allowed_actions`, `creates_job`/`triggers_workflow` hooks | T1, T7, T11 | `pyerp/engine/statemachine.py` | invalid transition rejected; approve transition triggers workflow/human job |
| T13 | EventBus + outbox dispatcher: publish events in caller TX, dispatcher delivers to subscribers with at-least-once + dedup | T3, T4 | `pyerp/engine/events.py` | rollback → no event; dispatch survives restart |
| T14 | Audit + RBAC hooks: `AuditService.record(tx)` append-only writer, `PermissionService.require` + `scope_filter` helpers, actor model enforcement | T1, T3 | `pyerp/engine/audit.py`, `pyerp/engine/permissions.py` | every mutation API records audit; scope filter applied in queries |
| T15 | Monitoring data APIs + SuperAdmin: `jobs.list`, `runs.get`, `rows.list_failed`, `metrics.snapshot`; minimal server-rendered read-only admin | T3, T5, T8 | `pyerp/admin/` + `pyerp/sdk/monitoring.py` | ops can see queue depth, job DAG, failed rows, retry a job |
| T16 | MCP tools adapter: read-only tools (`jobs.list`, `runs.get`, `rows.list_failed`, `audit.trace`) over service interfaces; actor-scoped | T14, T15 | `pyerp/mcp/` | MCP server exposes tools; permission checks enforced |
| T17 | Integration/acceptance suite: end-to-end demo app (order→review→approve→fulfill) exercising DAG, human task, bulk, SM, outbox, audit; benchmark report vs targets | All | `tests/e2e/` + `examples/` | all 8 must-prove items from design brief pass |

## Execution Guidance
- Status: T0, T1, T3 done; **T2 done 2026-11-18 — Chancy adopted** (`docs/decisions/0001-kernel-chancy.md`, benchmarks in `.spikes/kernel/`). Next: T4 → T5 → T7 → T8.
- Critical path: T0 → T1 → T3 → T4 → T5 → T7 → T8 → T17 (and T13 for outbox completeness).
- Safe parallel work: T2 (spike) with T1/T3; T9 (compiler) with T7; T14/T15/T16 with T10–T12; T11 with T8/T10.
- Shared hubs: `pyerp/sdk/` types (T1) — everything depends on it; DB schema (T3) — T4/T13/T15 build on it; `jobs` table + worker (T5) — human/bulk/scheduler all reuse.

## Verification Strategy
- Task checks: each task's acceptance list; `pytest` at package level.
- Integration checks: e2e demo (T17) running real Postgres in Docker; kill/resume drills; 500k-row import; actor-scoped queries.
- Final acceptance: the 8 must-prove items in `../../architecture-design-brief.md` §9 all demonstrated and recorded.

## Risks and Approval
- Accepted assumptions: see `approach.md`.
- Open risks: kernel bugs (mitigated by spike + adapter boundary); performance under 1M events/day (benchmark in T17).
- User approval: pending.

## Live Plan Rule
Workers may submit evidence-backed plan-change proposals. Material scope or risk changes require review before execution continues.
