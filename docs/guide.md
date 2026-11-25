# keelflow — User Guide

How to build an application on keelflow: jobs, DAG workflows, human
tasks, state machines, workflow triggers, bulk imports, outbox events,
scheduling, and ops tooling. Everything below reflects the real API —
the `examples/invoice_flow` demo is a complete working app using all of it.

Requirements: Python ≥3.11, Postgres ≥14.

```bash
pip install keelflow
```

---

## 1. Mental model

```
┌────────────────────────── your application ──────────────────────────┐
│ handlers (business logic)   domain tables   FastAPI/UI                │
└──────────────┬───────────────────────────────────────────────────────┘
               │  same DB transaction
┌──────────────▼──────────── keelflow engine ──────────────────────────┐
│ keelflow_jobs · keelflow_workflow_runs · keelflow_step_runs          │
│ keelflow_chunks · keelflow_row_status · keelflow_outbox              │
│ keelflow_entity_state · keelflow_audit · keelflow_dlq                │
└──────────────┬───────────────────────────────────────────────────────┘
               │  claim / retry / schedule
┌──────────────▼────────────── Chancy kernel ──────────────────────────┐
│ keelflow_kernel_* tables — leases, queues, workers                    │
└──────────────────────────────────────────────────────────────────────┘
```

Two invariants to remember:

1. **Transactional intent** — enqueue a job, publish an event, or record
   a state transition inside *your* transaction. Commit → the job is
   guaranteed to exist. Rollback → nothing leaks.
2. **Handlers must be idempotent** — the kernel may redeliver a job after
   a worker crash. Write domain effects with upserts/natural keys.

---

## 2. Setup

### 2.1 Database

```bash
docker run -d --name kf-pg -e POSTGRES_PASSWORD=postgres \
  -p 5544:5432 postgres:16-alpine
docker exec kf-pg createdb -U postgres keelflow
export KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow
```

Apply the engine schema once (idempotent):

```python
import psycopg
from keelflow.postgres.schema import apply_async

async with await psycopg.AsyncConnection.connect(DSN) as conn:
    await apply_async(conn)          # creates keelflow_* tables
```

Or via alembic (`alembic upgrade head`) in real apps.

### 2.2 The two processes

keelflow separates **app-side calls** (in your transaction) from
**worker-side execution**:

| Process | Runs | Env needs |
|---|---|---|
| Your app (API, web, CLI) | `JobService.enqueue`, `sm.transition`, `human.complete` | `KEELFLOW_DSN` |
| Worker | `run_worker(registry)` — claims jobs, dispatches handlers | `KEELFLOW_DSN` |

```python
# worker process — blocks until SIGINT/SIGTERM
from keelflow.engine.worker import run_worker
run_worker(registry=build_registry())
```

`run_worker` migrates the kernel, declares the `default` queue, and
claims jobs. Run it under a supervisor (systemd/docker restart policy):
upstream Chancy ≤0.26 can crash the worker loop after a transient DB
disconnect — a restart policy recovers automatically.

### 2.3 App-side services

```python
import chancy
from keelflow.engine.enqueue import JobService
from keelflow.engine.workflow import WorkflowService
from keelflow.engine.statemachine import EngineStateMachineService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.audit import PostgresAuditService
from keelflow.postgres.storage import PostgresStorage

kernel = chancy.Chancy(DSN, prefix="keelflow_kernel_")
await kernel.migrate()
await kernel.declare(chancy.Queue(
    "default", concurrency=8, executor=chancy.Chancy.Executor.Async),
    upsert=True)

storage = PostgresStorage(DSN)
audit  = PostgresAuditService(storage)
jobs   = JobService(registry, kernel, storage)
wf     = WorkflowService(registry, jobs)
sm     = EngineStateMachineService(registry, kernel, audit=audit)
human  = HumanTaskService(registry, kernel, sm=sm)
```

The kernel prefix **must not** be `keelflow_` (engine prefix) — kernel
tables would collide with `keelflow_jobs`.

---

## 3. Defining job types

```python
from pydantic import BaseModel
from keelflow.sdk import JobHandler, JobResult, JobTypeSpec, RetryPolicy

class SendEmail(BaseModel):
    to: str
    subject: str

class SendEmailH(JobHandler):
    async def run(self, ctx, payload: SendEmail) -> JobResult:
        # domain work — optionally in the dispatch transaction (§4)
        return JobResult(status="done", outputs={"sent": True})

registry.register_job_type(JobTypeSpec(
    type="mail.send@v1",              # "domain.action@vN" — versioned
    input_schema=SendEmail,
    handler=SendEmailH,
    retry=RetryPolicy(max_attempts=5),
    concurrency=ConcurrencySpec(key="payload.to"),  # serialize per recipient
))
```

`JobResult(status="failed")` is a **declared failure** — the job ends
immediately, no retry burn. Raising an exception (or hitting
`timeout_seconds`) is a **technical failure** — the engine schedules a
redelivery after `RetryPolicy.backoff`/`base_seconds` (+`jitter`) until
`max_attempts`, then the job (and any workflow step it belongs to)
fails permanently and is copied to `keelflow_dlq`.

For a long-running handler that should remain active while it makes progress,
opt into application-reported progress. For example, report every 10 completed
items in a 1,000-item job:

```python
class ImportItemsH(JobHandler):
    async def run(self, ctx, payload):
        items = await load_items(ctx, payload)
        for completed, item in enumerate(items, start=1):
            await process_item(ctx, item)
            if completed % 10 == 0 or completed == len(items):
                await ctx.execution.report_progress(completed, len(items))
        return JobResult(status="done")

registry.register_job_type(JobTypeSpec(
    type="items.import@v1",
    input_schema=ImportItems,
    handler=ImportItemsH,
    progress_report_timeout_seconds=120,
    retry=RetryPolicy(max_attempts=3),
))
```

Choose the report cadence and the silence window together: here, the
120-second window must exceed the longest expected interval between
reports. Setting `progress_report_timeout_seconds` implies reporting
is enabled — no `progress_enabled=True` needed; set `progress_enabled`
alone when you want the progress snapshot with no stall detection. A
report-silence timeout cancels the local handler attempt, rolls back
its current UoW, and uses the normal retry
policy. It cannot guarantee that an already-issued external request stopped, so
retries need stable idempotency keys, outbox/receiver deduplication, or a
reconciliation strategy. Report after a committed or otherwise verifiable unit
when possible; reported progress itself does not prove business work committed.

`report_progress` takes integer `current` and `total` with
`0 <= current <= total`, an optional message (up to 256 characters), and
optional JSON object metadata (up to 8 KiB). Progress is persisted outside the
handler UoW and is fenced to the owning claim — a report from a claim
that lost ownership (a retry, reschedule, or ops reset replaced it) is
rejected with `ExecutionLostError` and that stale dispatch is abandoned
rather than retried. The in-process watchdog owns the deadline, so a
still-owning claim may report even when its claim-time signal is older
than the window (e.g. after waiting on a `concurrency_key` lock). The
visible `progress_at` is the claim time until the first report lands,
then the last report time — the stale-silence signal `list_stale_progress`
and the watchdog both measure against. It is application progress, not
an automatic worker/process-liveness heartbeat. SQLite does not
currently support persisted progress or the report-silence timeout.

```python
retry=RetryPolicy(max_attempts=5, backoff="exponential",
                  base_seconds=30, jitter=True),
timeout_seconds=60,    # absolute cap: kill the handler coroutine after 60s
result_warning_bytes=32768,   # soft: warn + record size, never fails
result_max_bytes=None,        # hard cap, opt-in; exceeded -> job fails
```

Handlers should return **references, not blobs** — outputs land in
`keelflow_jobs.result` and `keelflow_step_runs.outputs`, and every
`advance_run` loads step outputs into the interpolation scope. Both
tiers are measured at finish: the soft threshold logs a warning and
records `result_bytes`/`outputs_bytes`; find hot job types with
`monitoring.results_oversized(conn, threshold=32768)` (type, oversized
count, worst bytes).

Ops replay: `MonitoringService(jobs, registry=reg)` exposes
`dlq_list`/`dlq_replay`. A dead DAG-step job replays **into its run**
(`resume_step` — the run advances when the re-enqueued step finishes);
a non-step job re-enqueues standalone.

`ctx` (`JobContext`): `actor`, `tenant_id`, `correlation_id`,
`permissions`, `request_id`, `extra` (read-only in handlers — downstream
data goes through `outputs`). `ctx.require("perm")` raises
`PermissionError` for service-layer checks.

---

## 4. The handler unit of work (`ctx.uow`)

While your handler runs, `ctx.uow` is a `UnitOfWork` — **one transaction
that also finishes the job and advances the workflow**. On the Postgres
adapter, `ctx.uow` exposes `execute`/`cursor`/`executemany` at the
adapter edge (`uow.native` is the psycopg connection); a domain
repository may use them for app-owned SQL. Engine and SDK code use the
semantic `Storage` port instead, so the contract stays backend-neutral.

```python
async def run(self, ctx, payload):
    await ctx.uow.execute(
        "INSERT INTO orders (id, status) VALUES (%s, 'placed')"
        " ON CONFLICT (id) DO NOTHING",
        (payload.order_id,),
    )
    return JobResult(status="done")
```

If the handler raises, the unit rolls back — no partial domain writes,
no job marked done. This is how you get "job effect commits with domain
effect" without extra plumbing. `ctx.uow` is never serialized into the
job context; it is valid only inside `handler.run`.

---

## 5. Enqueueing

```python
session = await storage.connect()
async with session.begin() as uow:
    await uow.execute("UPDATE orders SET status='shipped' WHERE id=%s",
                      (order_id,))
    ref = await jobs.builder("mail.send@v1") \
        .payload({"to": user, "subject": "Shipped"}) \
        .context(ctx) \
        .concurrency_key(user) \
        .enqueue(uow)
    # commit → job + domain write visible together
```

`session.begin()` requires an idle session — it raises when a
transaction is already pending rather than silently discarding it.

`.payload` takes a dict or `BaseModel` (validated at enqueue AND at
dispatch). `.run_at(ts)` delays. `.concurrency_key` serializes execution
per key via a Postgres advisory lock — same-key jobs never run in
parallel.

`.deduplication_key(k)` — **idempotent enqueue**: one non-terminal job per
`(type, unique_key)`. A duplicate (UI double-click, retry, replay)
returns the *existing* job's `JobRef` — no second kernel job or outbox
row, so intent stays exactly-once. The key frees once the job is
terminal; unrelated to `concurrency_key`.

## 6. DAG workflows

```python
registry.register_workflow(WorkflowDefinition(
    name="post.wf@v1",
    steps=(
        StepSpec(id="ledger", type="inv.post_ledger@v1",
                 input={"invoice_no": "${{ inputs.entity_id }}"}),
        StepSpec(id="record", type="inv.record_posting@v1",
                 needs=("ledger",)),
        StepSpec(id="gl_check", type="human.gl_check@v1",
                 input={"entity_id": "${{ inputs.entity_id }}"},
                 needs=("record",)),
    ),
))
```

- `needs` — dependencies; ready steps run in parallel.
- `input` — `${{ inputs.x }}` reads run inputs; `${{ steps.Y.outputs.z }}`
  reads a completed step's outputs.
- `if_expr` — optional guard: `if: "${{ steps.check.outputs.risk }} == 'low'"`.

```python
ref = await wf.start("post.wf@v1",
                     {"entity_type": "invoice", "entity_id": "INV-1"},
                     ctx, cur)
await wf.get_run(conn, ref.id)          # status + step details
await wf.resume_run(conn, ref.id, ctx)  # reopen a failed/rejected run
await wf.resume_step(conn, ref.id, "ledger", ctx)  # re-enqueue one step
```

`resume_run` is the recovery primitive: it resets the failed (or
business-rejected) step to `pending`, keeps successful siblings `done`,
and re-executes — same run id, no duplicate success effects.

### Human steps in a DAG

A step whose type starts with `human.` parks the run (`blocked`) until a
person completes the task — no kernel claim involved:

```python
JobTypeSpec(type="human.gl_check@v1", input_schema=ReviewPayload,
            handler=_NoopH, is_human=True,
            human_task=HumanTaskOptions(assignee_role="gl"))
```

`human.complete` writes `result` into the step outputs →
`{"decision": "reject"}` makes the run's terminal outcome `rejected`
(business rejection, see §8); anything else counts as success.

### External-wait steps in a DAG

A step whose type is registered with `is_external_wait=True` (`wait.*` namespace)
parks the run `blocked` the same way — but it is resolved by your
application, not by a person and not by a worker. There is no kernel
job: a wait step consumes zero worker capacity for as long as it waits.

```python
JobTypeSpec(type="wait.bank_verify@v1", input_schema=Sup,
            is_external_wait=True)                      # no handler
StepSpec("bank", "wait.bank_verify@v1", input=i, needs=("validate",))
```

The boundary: the external system calls **your** application — your
route verifies it, correlates the provider reference to the wait
`job_id` (your table, your rules), translates the payload, and calls
`signal` inside a transaction you own. Domain writes (e.g. a check
result row) and the resolution commit together:

```python
await conn.execute("INSERT INTO check_results …", (…))
await jobs.signal(conn, wait_job_id,
                  {"verified": True}, svc_ctx)   # job.signal perm
await conn.commit()                              # all-or-nothing
```

- `signal` → job/step `done`, run advances. `signal_failure` → step
  `failed` → run `failed` (`on_failed` path). A domain "declined" is
  data in `result`, driven by `if:` guards — the engine never
  interprets it.
- First signal wins (row locked `FOR UPDATE`); duplicate or late
  callbacks on a terminal wait raise — safe for at-least-once
  webhooks.
- No engine buffer: a signal for a wait that doesn't exist yet fails
  fast. Call the provider *after* the wait commits (the `wait.opened`
  outbox event marks that point) — providers retry, or your adapter
  does.
- `jobs.cancel` on an open wait terminalizes the step `cancelled` —
  which cancels the run. Long waits never burn a worker.
- Direct `builder().enqueue()` of a `wait.*` type is rejected; waits
  only exist as DAG steps in v1.

---

## 7. Entity state machines

```python
registry.register_state_machine(StateMachineDefinition(
    name="invoice@v1", entity="invoice",
    states=("draft", "in_review", "posting", "posted", "needs_fix"),
    initial="draft",
    transitions=(
        TransitionSpec("draft", "in_review", "submit",
                       guard={"permission": "inv.submit",
                              "assignee_role": "finance",
                              "sm_actions": {"approve": "approve",
                                             "reject": "reject"}},
                       creates_job="human.review@v1"),
        TransitionSpec("in_review", "posting", "approve",
                       guard={"permission": "inv.approve"},
                       triggers_workflow=WorkflowTriggerSpec(
                           workflow="post.wf@v1",
                           on_succeeded="posted",
                           on_rejected="posting_rejected",
                           on_failed="posting_failed",
                           failure_job="human.posting_fix@v1")),
        TransitionSpec("in_review", "needs_fix", "reject"),
        TransitionSpec("posting", "posted", "posted"),
        ...
    ),
))
```

```python
async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
    await sm.transition(EntityRef("invoice", "INV-1"), "submit", ctx, cur)
    # entity row created-or-locked FOR UPDATE; guard checked; audit written;
    # creates_job + triggers_workflow side effects land in the same TX

state   = await sm.current(EntityRef("invoice", "INV-1"), conn)
actions = await sm.allowed_actions(EntityRef("invoice", "INV-1"),
                                   actor, conn,
                                   permissions=ctx.permissions)
```

Guard keys in `TransitionSpec.guard`:
- `permission` — `ctx.permissions` must contain it (`"*"` wildcard ok).
  The string must also be declared via
  `Registry.register_permission("inv.submit")` **before** the state
  machine is registered — an unknown permission raises `ValueError`
  at load time rather than failing as a 403 at runtime.
  (`InMemoryRegistry` pre-registers the engine's own `job.cancel` and
  `job.signal`.)
- `assignee_role` — the `creates_job` task goes to `role:<name>`.
- `sm_actions` — maps task decisions to SM actions (see §8).

Concurrent first transitions serialize on the entity row lock — two
simultaneous `submit`s can't both pass.

---

## 8. Human tasks

```python
# Resolve this list from your authenticated actor-to-role mapping.
roles = ["finance"]
tasks = await human.my_tasks(conn, actor, roles=roles)
await human.claim(conn, task_id, actor, roles=roles)  # assignment + first-claim-wins
await human.complete(conn, task_id, actor,
                     {"decision": "approve", "note": "ok"}, roles=roles)
await human.complete_and_transition(
    conn, task_id, actor, "approve", ctx, note="ok", roles=roles)  # SM tasks
```

Two task kinds:

1. **SM-created** (`creates_job`): payload carries `sm_actions` → use
   `complete_and_transition`. The task completes *and* the mapped SM
   transition applies in one transaction — a rejected decision can't
   leave the task done while the entity stays put.
2. **Workflow-step** (`human.*` in a DAG): use `complete` — unblocks the
   run inside the same TX.

`escalate_overdue(conn, older_than_days=N, to_role="lead")` reassigns
stale pending tasks.

---

## 9. Workflow triggers (SM → run → SM)

`WorkflowTriggerSpec` closes the loop between a state machine and a
workflow run:

- The transition that spawns the run records the entity + trigger
  config + initiating ctx on the run row.
- When the run terminalizes, the engine applies the configured SM action
  **in the run's own terminal transaction** — atomically, exactly once
  (`callback_state` guards replay).
- `on_rejected` fires when a human step ends with `decision="reject"`
  (business rejection — no retry burn, run status `done`, outcome
  `rejected`).
- `on_failed` fires on technical failure (retry exhaustion or declared
  fail — run status `failed`, outcome `failed`) and optionally spawns
  `failure_job`, a human fix-up task.
- `resume_run` from `posting_failed`/`posting_rejected` applies the
  matching exit transition (`retry_post`/`repost`) and re-executes the
  run in place.

This is how "approve invoice → post to ledger → GL signs off → posted"
becomes a single atomic story instead of polling + compensating code.

---

## 10. Bulk imports

```python
class ImportRows(BulkJobHandler):
    async def process_row(self, ctx, row, tx):
        await tx.execute(
            "INSERT INTO products (sku, name) VALUES (%s,%s)"
            " ON CONFLICT (sku) DO NOTHING",
            (row["data"]["sku"], row["data"]["name"]))
        return {"sku": row["data"]["sku"]}

bulk.register(registry)  # internal bulk.chunk@v1 type
registry.register_job_type(JobTypeSpec(
    type="import.products@v1", input_schema=BulkJobPayload,
    handler=ImportRows))
```

Stage rows and enqueue in **one** transaction:

```python
async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
    ref = await jobs.builder("import.products@v1") \
        .payload({"chunk_size": 1000, "atomic": "chunk"}) \
        .context(ctx).enqueue(cur)
    for i, row in enumerate(csv_rows):
        await cur.execute(
            "INSERT INTO keelflow_row_status (job_id, row_key, status, data)"
            " VALUES (%s, %s, 'pending', %s)",
            (ref.id, f"row-{i}", json.dumps(row)))
```

- `atomic="chunk"` — a chunk's rows commit together; one bad row fails the
  chunk. `atomic="row"` — per-row commits, bad rows land in the error
  queue without blocking siblings.
- The parent job partitions pending rows into chunks → `bulk.chunk@v1`
  children → fan-in marks the parent done when all chunks settle.
- Kill-resume re-executes only still-`pending` rows.
- Failed rows: `monitoring.rows_list_failed(conn, job_id)`.

### Row retention — `row_disposition`

Staging rows are per-row copies of the input. After fan-in the parent
result already holds the totals, so `done` rows are disposed per job:

```python
await jobs.builder("imp.rows@v1").payload({
    "chunk_size": 1000,
    "atomic": "row",
    # "archive" (default) | "keep" | "delete"
    "row_disposition": "archive",
}).enqueue(cur)
```

- `archive` (default) — done rows move to `keelflow_row_archive` in the
  same TX as the fan-in. The archive has no FK to live jobs, so rows
  stay inspectable after job cleanup. Failed rows always stay live.
- `keep` — done rows stay in `keelflow_row_status` (debug / strict
  per-row audit).
- `delete` — done rows are dropped; choose only when your app already
  has a durable source copy.

Read archived rows with `monitoring.rows_list_archived(conn, job_id,
limit=50, after_id=last_id)` (keyset pagination). `bulk.progress()`
keeps reporting correct totals after cleanup — once a job is terminal
it reads the fan-in summary, not the live table.

Measured: ~100k rows partition+complete in ~10s locally
(`.spikes/bulk_bench.py`).

---

## 11. Outbox events

```python
bus = OutboxEventBus()
bus.subscribe("order.shipped", my_handler)  # async fn(event, ctx)

async with conn.transaction(), conn.cursor() as cur:
    await bus.publish(DomainEvent(type="order.shipped",
                                  payload={"id": "O-1"}), cur)
    # event row commits with the domain write — never a phantom event

# delivery: inside the worker process
run_worker(registry=reg, event_bus=bus)   # dispatches pending rows in a loop
```

Delivery is per-subscriber deduplicated (`keelflow_outbox_deliveries`) —
handlers still must be idempotent (at-least-once delivery).

## 12. Scheduling

```python
JobTypeSpec(type="nightly.recon@v1", ..., 
            schedule=CronSpec(cron="0 2 * * *", timezone="UTC",
                              window="02:00-06:00", overlap="skip"))
```

```python
from keelflow.engine.scheduler import run_scheduler
await run_scheduler(dsn, registry, kernel, interval=30)
```

`overlap`: `skip` (default), `queue`, `kill_previous`. Each slot enqueues
at most one job (`unique_key` dedup) — run the scheduler in exactly one
process.

## 13. YAML definitions

`compiler.load_definitions(dir, registry)` loads `dir/workflows/*.yaml`
and `dir/state_machines/*.yaml` — validated at load (unknown types,
cycles, bad guards fail immediately):

```yaml
# definitions/workflows/order.yaml
workflow: order.fulfill@v1
steps:
  - id: pick
    type: wms.pick@v1
  - id: pack
    type: wms.pack@v1
    needs: [pick]
  - id: inspect
    type: human.qa@v1
    needs: [pack]
    if: "${{ steps.pack.outputs.fragile }} == True"
```

```yaml
# definitions/state_machines/invoice.yaml
name: invoice@v1
entity: invoice
states: [draft, in_review, posting, posted, needs_fix]
initial: draft
transitions:
  - from: draft
    to: in_review
    action: submit
    creates_job: human.review@v1
    guard: {permission: inv.submit, assignee_role: finance,
            sm_actions: {approve: approve, reject: reject}}
  - from: in_review
    to: posting
    action: approve
    triggers_workflow:
      workflow: post.wf@v1
      on_succeeded: posted
      on_rejected: posting_rejected
      on_failed: posting_failed
      failure_job: human.posting_fix@v1
```

## 14. Ops: monitoring, admin, MCP

```python
mon = MonitoringService(storage=PostgresStorage(DSN))
session = await storage.connect()
async with session.begin() as uow:
    await mon.metrics_snapshot(uow)      # queue depth, running, failed_24h
    await mon.list_jobs(uow, statuses=["failed"])
    await mon.get_run(uow, run_id)       # run + step details
    await mon.rows_list_failed(uow, job_id)
    await mon.retry_job(uow, job_id)     # ops retry

    # retention — caller-invoked, batched, idempotent; repeat until 0
    await mon.purge_expired_records(uow, jobs_days=30, outbox_days=7,
                                    archive_days=90, dlq_days=90, batch=500)
```

`purge_expired_records` deletes, oldest-first and `batch` per table per
call: terminal
`workflow_runs` + `jobs` (their `chunks`/`row_status`/`step_runs`
cascade — a job still linked to a live run or child job is skipped),
`published` outbox rows, `row_archive`, and `keelflow_dlq`. In-flight
work, pending/failed outbox, `keelflow_audit`, and `entity_state` are
never touched; `None` disables a category.

- **SuperAdmin** (no auth — loopback or an authenticating proxy with
  write protection only): `keelflow admin` → http://127.0.0.1:8410
- **MCP read-only tools** for agents: `keelflow mcp`

## 15. CLI

```bash
keelflow init-db --dsn $KEELFLOW_DSN        # fresh DB only (tables
                                          # exist → use upgrade-db)
keelflow upgrade-db --dsn $KEELFLOW_DSN     # Alembic versioned upgrade
keelflow worker --registry myapp.registry:build_registry \
                --kernel chancy --dsn $KEELFLOW_DSN
keelflow scheduler --registry myapp.registry:build_registry
keelflow admin    --dsn $KEELFLOW_DSN      # no auth — see security.md
keelflow mcp      --dsn $KEELFLOW_DSN
```

`init-db` and `upgrade-db` are deliberately separate: `init-db` refuses
a database that already has `keelflow_jobs`; `upgrade-db` refuses a
database with tables but no `alembic_version` until you baseline it
(`alembic stamp …`) — a plain schema apply is never reported as a
versioned upgrade. `--kernel sqlite` is local/proof-only.

## 16. Testing

Integration tests use a dedicated database — fixtures truncate
`keelflow_*` tables. The suite defaults to
`postgresql://…/keelflow_test` (auto-created); `KEELFLOW_TEST_DSN`
overrides and must point at a test-named database or carry
`KEELFLOW_TEST_ALLOW_DESTRUCTIVE=1`. `keelflow.testing.postgres_env()`
wires registry + storage + kernel + services for app tests; migration
rehearsals use `temp_test_db()` for throwaway databases. See
`docs/reliability.md` for what each recovery test proves.

## 17. Observability

Engine logs carry structured `extra` fields (`job_id`, `job_type`,
`run_id`, `step_id`, `correlation_id`, `tenant_id`, `worker_id`,
`attempt`) — never payload/context. `traceparent` propagates through
`JobContext.extra` across enqueue → dispatch → retry; spans activate
when `keelflow[otel]` is installed (`tracer()` is a no-op otherwise).

`format_metrics_prometheus(await mon.metrics_snapshot(uow))` renders
queue depth, claimed, running, waiting (blocked human/wait steps —
separate from stuck work), failed_24h, oldest pending, done duration,
DLQ depth, worker liveness and scheduler heartbeat age as Prometheus
text. Workers and schedulers upsert `keelflow_process_health` rows and
monitoring treats a row as alive while `last_seen` is fresher than its
`stale_after_s` window. `run_worker` starts a worker heartbeat every
10s (`stale_after_s=30`); a scheduler's heartbeat is its tick, declared
at `max(interval*3, 30)` so a healthy idle scheduler is not reported
stale. Mount the text on your own /metrics endpoint — no built-in
listener.

Running a caller-owned kernel worker instead of `run_worker`?
`configure()` only wires dispatch — start the heartbeat yourself so an
idle worker still shows alive:

```python
from keelflow.engine.worker import configure, worker_heartbeat_loop

configure(registry=reg, dsn=DSN, kernel=kernel, storage=storage)
heartbeat = asyncio.create_task(
    worker_heartbeat_loop(storage))          # 10s cadence, 30s window
# ... start your kernel worker; cancel heartbeat on shutdown.
```

- **Audit**: pass `PostgresAuditService` to `EngineStateMachineService`;
  every transition (including workflow-trigger callbacks) lands in
  `keelflow_audit`. `audit.trace(conn, entity)` returns the chain.

## 15. Cooperative cancellation

Cancellation is **pre-start by default**. `JobService.cancel(job_id, ctx)`
(requires the `job.cancel` permission) records a durable request:

- `pending` → `cancelled` immediately (the worker's terminal skip advances
  any owning workflow step when the kernel job is delivered).
- `claimed`/`running`/`blocked` → `cancel_requested=true`, status kept —
  the job drains to `done`/`failed` unless it opted into checkpoints.
- Terminal jobs and repeated requests are idempotent no-ops.

Long-running job types may opt in to **cooperative checkpoints**:

```python
JobTypeSpec(type="etl.load@v1", input_schema=In, handler=LoadH,
            cooperative_cancel=True)

class LoadH(JobHandler):
    async def run(self, ctx, payload):
        for batch in batches:
            await ctx.execution.checkpoint()   # raises JobCancelled
            await ctx.uow.execute("INSERT ... ", (batch,))
```

- `checkpoint()` reads the durable flag inside the handler transaction
  (READ COMMITTED) — a request committed after the last checkpoint can
  still lose to normal completion; place checkpoints between units of
  work.
- `JobCancelled` is a `BaseException` subclass: `except Exception`
  cannot swallow it; if you catch `BaseException`, re-raise it.
- When observed, the handler transaction rolls back and the job is
  terminalized `cancelled` in a new transaction — never retried, never
  DLQ'd.
- External side effects (HTTP, email, filesystem) are **not** rolled
  back — make them idempotent or checkpoint before issuing them.
- `JobHandler.on_cancel` is an unused experimental placeholder. Worker
  liveness/recovery is kernel-owned (chancy), not heartbeat-per-job.

## 16. Dispatch backends (experimental)

Engine dispatch goes through a kernel port (`keelflow.engine.kernel`):

| Backend | Class | Status | Notes |
|---|---|---|---|
| chancy | `ChancyKernel` (wraps `chancy.Chancy`) | default | full ADR feature set |
| native | `NativeKernel` | experimental | claims `keelflow_jobs` directly; no rate limits/pause/tags/priority/metrics plugins |
| sqlite | `SQLiteKernel` (`pip install keelflow[sqlite]`) | proof only | kernel-portability proof; in-process wakeup, single-writer; engine services remain Postgres-bound |

Kernel contract tests run per backend:

```bash
uv run pytest tests/test_kernel_contract.py --kernel=all   # all three
uv run pytest tests/test_kernel_contract.py --kernel=sqlite # no Postgres needed
```

`--kernel=both` = chancy+native; `all` adds sqlite. `test_worker.py` is
parameterized for chancy/native; files touching chancy internals gate on
`require_chancy`.

Standalone worker bootstrap accepts the backend explicitly:

```python
run_worker(registry=reg)                      # Chancy default (owns the client)
run_worker(registry=reg, kernel=NativeKernel(dsn))  # caller-owned adapter
run_worker(registry=reg, kernel=ChancyKernel(client))  # client ctx owned by caller
```

A supplied kernel is caller-owned — `run_worker` migrates + starts its
worker through the port and stops the worker on exit, but never closes
the kernel/client. `queues=` is Chancy-specific and raises `ValueError`
on other backends; engine `dispatch` itself remains Postgres-bound
(SQLite kernel is a portability proof, not an engine backend).

## 17. Environment variables

| Var | Default | Purpose |
|---|---|---|
| `KEELFLOW_DSN` | — | Postgres DSN (worker + handlers) |
| `KEELFLOW_REGISTRY` | — | dotted path `pkg.mod:registry` for dispatch |
| `KEELFLOW_TEST_DSN` | `...:5544/keelflow_test` | test DB (auto-created; fixtures truncate) |

## 18. Production checklist

- [ ] Handlers idempotent (upserts / natural keys / dedupe).
- [ ] Workers under a restart supervisor.
- [ ] One scheduler process per deployment.
- [ ] Kernel prefix ≠ `keelflow_`.
- [ ] `payload_max_bytes` reviewed for large payloads.
- [ ] SuperAdmin bound to localhost or behind a proxy — it has **no auth**.
- [ ] Permissions populated in `JobContext.permissions`; guards rely on it.
