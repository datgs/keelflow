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

kernel = chancy.Chancy(DSN, prefix="keelflow_kernel_")
await kernel.migrate()
await kernel.declare(chancy.Queue(
    "default", concurrency=8, executor=chancy.Chancy.Executor.Async),
    upsert=True)

audit  = PostgresAuditService()
jobs   = JobService(registry, kernel)
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

```python
retry=RetryPolicy(max_attempts=5, backoff="exponential",
                  base_seconds=30, jitter=True),
timeout_seconds=60,    # kill the handler coroutine after 60s
```

Ops replay: `MonitoringService(jobs)` exposes `dlq_list`/`dlq_replay` —
re-enqueues a DLQ row as a fresh job.

`ctx` (`JobContext`): `actor`, `tenant_id`, `correlation_id`,
`permissions`, `request_id`, `extra` (read-only in handlers — downstream
data goes through `outputs`). `ctx.require("perm")` raises
`PermissionError` for service-layer checks.

---

## 4. The handler transaction (`ctx.tx`)

While your handler runs, `ctx.tx` is a psycopg cursor inside **one
transaction that also finishes the job and advances the workflow**:

```python
async def run(self, ctx, payload):
    await ctx.tx.execute(
        "INSERT INTO orders (id, status) VALUES (%s, 'placed')"
        " ON CONFLICT (id) DO NOTHING",
        (payload.order_id,),
    )
    return JobResult(status="done")
```

If the handler raises, the TX rolls back — no partial domain writes, no
job marked done. This is how you get "job effect commits with domain
effect" without extra plumbing. `ctx.tx` is never serialized into the
job context.

---

## 5. Enqueueing

```python
async with conn.transaction(), conn.cursor() as cur:
    await cur.execute("UPDATE orders SET status='shipped' WHERE id=%s",
                      (order_id,))
    ref = await jobs.builder("mail.send@v1") \
        .payload({"to": user, "subject": "Shipped"}) \
        .context(ctx) \
        .concurrency_key(user) \
        .enqueue(cur)
    # commit → job + domain write visible together
```

`.payload` takes a dict or `BaseModel` (validated at enqueue AND at
dispatch). `.run_at(ts)` delays. `.concurrency_key` serializes execution
per key via a Postgres advisory lock — same-key jobs never run in
parallel.

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
            ui=TaskUiSpec(assignee_role="gl"))
```

`human.complete` writes `result` into the step outputs →
`{"decision": "reject"}` makes the run's terminal outcome `rejected`
(business rejection, see §8); anything else counts as success.

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
                           on_done="posted",
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
      on_done: posted
      on_rejected: posting_rejected
      on_failed: posting_failed
      failure_job: human.posting_fix@v1
```

## 14. Ops: monitoring, admin, MCP

```python
mon = MonitoringService()
await mon.metrics_snapshot(conn)      # queue depth, running, failed_24h
await mon.jobs_list(conn, statuses=["failed"])
await mon.runs_get(conn, run_id)      # run + step details
await mon.rows_list_failed(conn, job_id)
await mon.retry_job(conn, job_id)     # ops retry
```

- **SuperAdmin** (no auth — bind localhost): `python -m keelflow.admin.app`
  → http://127.0.0.1:8410
- **MCP read-only tools** for agents: `python -m keelflow.mcp`
- **Audit**: pass `PostgresAuditService` to `EngineStateMachineService`;
  every transition (including workflow-trigger callbacks) lands in
  `keelflow_audit`. `audit.trace(conn, entity)` returns the chain.

## 15. Environment variables

| Var | Default | Purpose |
|---|---|---|
| `KEELFLOW_DSN` | — | Postgres DSN (worker + handlers) |
| `KEELFLOW_REGISTRY` | — | dotted path `pkg.mod:registry` for dispatch |
| `KEELFLOW_TEST_DSN` | `...:5544/keelflow` | test DB |

## 16. Production checklist

- [ ] Handlers idempotent (upserts / natural keys / dedupe).
- [ ] Workers under a restart supervisor.
- [ ] One scheduler process per deployment.
- [ ] Kernel prefix ≠ `keelflow_`.
- [ ] `payload_max_bytes` reviewed for large payloads.
- [ ] SuperAdmin bound to localhost or behind a proxy — it has **no auth**.
- [ ] Permissions populated in `JobContext.permissions`; guards rely on it.
