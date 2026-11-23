# T0 — Persistence And Transaction-Boundary Inventory

> Companion to `storage-port-extraction.md`. Classifies every raw DB access,
> transaction boundary, and transaction participant in core, adapters, and
> shipped examples. Dated 2026-11-23.

## A. Transaction participation modes found

| Mode | Description | Sites |
|---|---|---|
| **P1 caller-owned TX join** | Service writes inside a TX the caller opened; caller commits | `enqueue`, `WorkflowService.start`, `sm.transition`, `human.complete`, `EventBus.publish`, `audit.record`, `BulkJobHandler.run` (via `ctx.tx`), `resume_step`, `dlq_replay` |
| **P2 engine-owned conn+TX** | Engine opens its own conn, runs one or more commit boundaries | `worker.dispatch`, `ChunkJobHandler.run`, `JobService.cancel`, `scheduler.due`, `events.dispatch_once/_deliver_row`, `monitoring.retry_job/sweep/dlq_replay`, `bulk.progress` (read) |
| **P3 savepoint sub-scope** | `conn.transaction()` nested inside an open TX — isolated rollback | `worker.dispatch` handler TX, `advance_run` per-step, `_process_batch_atomic`, `_process_one_row`, `_process_chunk_atomic`, `_signal`, `human.complete*`, `_finish/_write_dlq`, `_run_trigger_callback` paths, `resume_run`, `_cancel_terminalize` |
| **P4 session lock** | `pg_advisory_lock` — session-scoped, outside any TX | `_KeyLock` in `worker.py:613-626` |
| **P5 wakeup/notify** | `pg_notify` inside push TX (native), `LISTEN` conn (worker), aiosqlite in-process event | `native_kernel:104,164`, `sqlite_kernel` |
| **P6 DDL/migration** | `ensure_schema`, `kernel.migrate()` — own conn, stays backend-specific | `statemachine.ensure_schema`, kernels |

## B. Operation groups (semantic operations T1 must cover)

### B1 job lifecycle — `keelflow_jobs` (jobs.*)
| Op | Site | Participants | Semantics |
|---|---|---|---|
| job_insert | enqueue:126, statemachine:209, scheduler:120, workflow `_enqueue_*` ×3, bulk:237 (batch unnest) | +kernel push +outbox | dedup via `(type, unique_key)` partial index → returns live id (no push/outbox on dup) |
| job_load_for_dispatch | worker:483 | — | type/status/payload/context/concurrency_key/cancel_requested |
| job_transition_running | worker:505 | — | atomic pending/claimed/running→running, attempts+1, conditioned `cancel_requested=false`, `RETURNING`/rowcount |
| job_finish | worker:575 (`_write_finish` in-TX), 597 (own TX) | ±step advance in same TX | status/result/result_bytes/error |
| job_cancel_flag | enqueue:251-274 (own TX) | +cascade to children +step advance if terminalized | pending→cancelled immediately; live→flag only |
| job_attempts_read | worker:259 | — | attempts vs max_attempts → retry decision |
| job_retry_reschedule | worker:308 | +kernel re-push same TX | update error+run_at + `push_ex` |
| wait_signal | enqueue:326-347 | +step advance | FOR UPDATE, type is_wait guard, status gate |
| bulk children insert | bulk:237 | +kernel push_many +chunks | unnest batch, inherit parent cancel flag + retry budget |
| escalate / release / claim / my_tasks (human) | human.py | +step advance on complete | FOR UPDATE first-claim-wins; `human.*` type guard |
| checkpoint read | sdk/context.py:56 | — | `cancel_requested` flag read inside handler TX |
| progress read | bulk:667 | — | row/chunk counts + parent result summary |

### B2 workflow — `keelflow_workflow_runs`, `keelflow_step_runs` (runs.*, steps.*)
| Op | Site | Notes |
|---|---|---|
| run_insert + step_runs insert | workflow:64-80, statemachine:243-270 | trigger metadata fields on SM-triggered runs |
| run_lock (`FOR UPDATE`) | workflow:274 (`_fetch_run`), 465, 171 | single-writer advance; run terminal |
| steps_fetch | workflow:278 | all step_runs for run |
| step_status_set | workflow:803, 349 | pending/skipped/failed/claimed/blocked |
| step_attach_job | workflow:565,596,631 | job_id + status after enqueue |
| step_settle | workflow:396 | status+outputs on job finish |
| run_finish | workflow:435 | status+terminal_outcome |
| trigger_callback lock+mark | workflow:459-508 | `callback_state pending→done` exactly-once; SM transition + optional failure task inside |
| run_lookup_by_job | worker:105,273, bulk:640, human:154, enqueue:347 | step_run id by job_id |
| resume_step/run | workflow:124-225 | step→pending attempts+1, run→running; rejected-run SM restore |
| run_ctx read | workflow:810 | ctx from run row for advance |

### B3 bulk — `keelflow_chunks`, `keelflow_row_status`, `keelflow_row_archive` (bulk.*)
| Op | Site | Notes |
|---|---|---|
| pending_count | bulk:140 | partition input |
| partition_claim | bulk:157 | SKIP LOCKED + row_number + unnest chunk ids + insert chunks + stamp rows — ONE statement; heaviest CTE |
| parent meta | bulk:211-218 | cancel flag + type lookup |
| chunk_claim | bulk:280 | status running + lease |
| stranded_rows_reset | bulk:287 | processing→pending |
| rows_fetch_pending | bulk:409 | ordered by id |
| rows_mark processing/done/failed | bulk:479-470 / 460-470 / 489-520 | inside per-row or per-batch savepoints |
| chunk_mark done/failed/cancelled | bulk:345,356,531,656 | +result json |
| fan_in counts + terminalize | bulk:548-651 | chunk counts → parent lock FOR UPDATE → row counts → disposition (archive CTE/delete/keep) → parent result merge → step advance same TX |
| archive rows | bulk:601-617 | insert archive ON CONFLICT DO NOTHING + delete live — CTE |
| attempts_read | bulk:380 | retry exhaustion |
| bulk.progress | bulk:667 | read-only |

### B4 events/outbox — `keelflow_outbox`, `keelflow_outbox_deliveries` (events.*)
| Op | Site | Notes |
|---|---|---|
| outbox_insert | enqueue:184, events:37, workflow wait.opened:636, scheduler:138 | in caller TX |
| outbox_claim | events:75 | FOR UPDATE SKIP LOCKED one row per TX |
| delivery_seen / delivery_mark | events:97,112 | dedup per (outbox_id, subscriber) |
| outbox_settle | events:118,124 | published / failed+attempts |

### B5 state machine — `keelflow_entity_state` (sm.*)
| Op | Site | Notes |
|---|---|---|
| entity_ensure+lock | statemachine:164-183 | insert-if-absent + FOR UPDATE — first-transition race safety |
| entity_set | statemachine:96 | ON CONFLICT version+1 |
| entity_read | statemachine:128,144 | `current`/`allowed_actions` reads |
| audit_insert | audit:19 | same caller TX |
| audit_trace | audit:38 | read |

### B6 scheduler (sched.*)
| Op | Site | Notes |
|---|---|---|
| due_triggers | scheduler:91 | cron defs due |
| live_check + kill_previous | scheduler:95-111 | live (type) job lookup + cancel flag |
| insert + kernel push + outbox | scheduler:120-141 | one TX per fired job |

### B7 monitoring/ops (ops.*)
| Op | Site | Notes |
|---|---|---|
| jobs_list / job_get | monitoring:20-52 | filters + keyset cursor |
| runs_get / runs_list | monitoring:54-77 | |
| rows_failed / rows_archived | monitoring:79-111 | keyset |
| results_oversized / metrics_snapshot | monitoring:113-148 | aggregate + `pg_stat_activity` (PG-only metric — capability boundary) |
| retry_job | monitoring:152 | failed→pending reset |
| sweep retention | monitoring:159 | 5 bounded DELETEs; caller-invoked |
| dlq_list / dlq_replay | monitoring:252-302 | ±resume_step or fresh enqueue |
| dlq_insert | worker:455 | ON CONFLICT upsert + run/step link |

### B8 kernel-owned (stays in kernel adapters; TX participation via UoW)
- `push_ex`/`push_many_ex` — chancy: INSERT `{prefix}jobs`; native: UPDATE `keelflow_jobs.dispatch_queued` + `pg_notify`; sqlite: same on sqlite conn.
- `dispatch_queued` flag + notify are native-kernel internals — the push TX must be the same UoW.

## C. Transaction-boundary inventory (who commits)

| Path | Commits today | Notes for UoW |
|---|---|---|
| `worker.dispatch` | `_transition_to_running` commit (pre-handler), handler-TX commit at 245, cancel/retry paths each commit | Multiple commit boundaries on one conn — UoW needs `commit()` callable mid-dispatch or separate units; KEEP semantics: P2 model = `storage.begin()` per boundary |
| `ChunkJobHandler.run` | claim commit(292), processing commits per row/batch, mark_chunk(536), fan_in(651), safe_mark_failed(661) | same — multiple sequential commit boundaries |
| caller TX joins | caller commits | `uow` wraps caller conn; ops never commit |
| `_KeyLock` | session lock, no TX | outside UoW, on same conn |
| `JobService.cancel` | own conn, one TX | `storage.begin()` |
| `events.dispatch_once` | one TX per row claim + per delivery | `storage.begin()` per item |
| `scheduler` | own conn, commit per job | `storage.begin()` |
| `monitoring` reads | caller conn (tests) / own commits for writes | reads may accept uow or native conn — capability decision |
| `bulk.progress` | caller conn | read-only |

## D. Boundary candidates for SQL confinement (T2 guard scope)

- **Allowed SQL**: `keelflow/postgres/*` (storage impl, schema), kernel adapters
  (`native_kernel.py`, `sqlite_kernel.py`, `chancy_kernel.py` — the psycopg
  `push_ex` adapter boundary), `examples/*/repositories.py` (new PG repos —
  domain writes), `tests/` and `.spikes/` (test fixtures may SQL directly).
- **Forbidden SQL**: `keelflow/sdk/*` (incl. context checkpoint), `keelflow/engine/*`
  except the three kernel adapter files, example handlers/registries (must go
  through repos).
- **Open**: `postgres/schema.py` migrations — explicitly allowed (DDL).

## E. Facts, assumptions, open questions

**Verified**
- All SQL sites listed above; ~200 `execute`-level sites when counting per-call
  (previous "144" counted only engine `execute(`/`cursor(` literals).
- `conn.transaction()` is used as BOTH "start TX" (P2) and "savepoint" (P3)
  depending on whether an implicit TX is already open — UoW must make nesting
  explicit.
- `push_ex` callers always pass a cursor bound to the atomic TX (enqueue,
  retry-reschedule, sm spawn, workflow step enqueue, bulk children, scheduler).
- `JobContext.tx` / `ExecutionContext.tx` are the only handler-facing TX leaks;
  `_as_cursor` is the only normalization shim.
- Monitoring `metrics_snapshot` uses `pg_stat_activity` — PG-only.
- `bulk.progress`, `monitoring` methods accept caller conns — read-path API
  shape decision needed (uow vs native conn vs storage-owned read conn).

**Assumptions**
- Mongo adapter is future work; no adapter must be implemented now.
- `pg_advisory_lock` key-serialization stays a Storage capability; a Mongo
  backend substitutes its own (document + fencing).

**Open (resolved in T1)**
- UoW naming/shape: `uow.native` opaque handle for adapters.
- Whether `atomic_scope` (savepoint semantics) is a required capability or a
  declared-optional capability with defined fallback behavior.
- Read-path signature: keep `conn` param vs `uow` vs `storage.reader()`.
