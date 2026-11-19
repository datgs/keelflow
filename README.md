# keelflow

The keel of your business flows — a reusable **Postgres-native durable
workflow engine** for Python. Reference implementation of the
language-agnostic **keelflow spec** (`spec/`).

```bash
pip install keelflow
```

```python
async with conn.transaction(), conn.cursor() as cur:
    await cur.execute("UPDATE orders SET status='shipped' WHERE id=%s", (oid,))
    await jobs.builder("mail.send@v1").payload({"to": email}) \
        .context(ctx).enqueue(cur)
    # commit → domain write AND the job exist, atomically. Forever.
```

## Why Postgres instead of a message broker?

Business apps already have Postgres — the *source of truth for
consistency*. Adding RabbitMQ/Kafka splits that truth in two:

- **The two-write problem.** `UPDATE orders …` then `broker.publish` →
  crash between the two = a shipped order with no email job, or a
  phantom job for an order that rolled back. Brokers offer
  outbox-pattern workarounds; Postgres *is* the outbox. One `COMMIT`
  makes the domain write and the job intent visible together — or
  neither.
- **Consistency you can query.** Job state, run state, row status,
  audit — all in the same database as your data, inspectable with the
  SQL you already know. No `rabbitmqctl` archaeology at 3am.
- **Zero new infrastructure.** A broker is another stateful service to
  secure, monitor, upgrade, and pay for. If Postgres is up, keelflow is
  up; if it's down you have bigger problems than job delivery.
- **Locks & transactions for free.** `FOR UPDATE SKIP LOCKED` = fair,
  safe claiming; advisory locks = key-serialized execution; row locks =
  exactly-once workflow callbacks — primitives brokers don't give you.

The honest trade-off: a broker moves more messages/sec. keelflow's
niche is **business workflows** (hundreds–thousands of jobs/sec,
human-in-the-loop, auditable) — where correctness beats raw throughput,
and your DBA already knows the failure modes.

## What it is / isn't

**It is:** a library that adds durable execution semantics to an app
that owns a Postgres schema — jobs, DAGs, human tasks, state machines,
bulk imports — while your app keeps full control of its tables,
permissions, and transactions.

**It is not:**
- a workflow *platform* with its own control plane, UI cluster, or
  language SDKs to deploy (Temporal, Cadence, Netflix Conductor),
- a general message queue replacement for high-throughput event
  streaming (Kafka territory),
- an ETL/analytics orchestrator (Airflow, Dagster, Prefect) — those
  schedule data pipelines; keelflow executes domain transactions,
- a magic-replay framework — handlers are normal functions with a
  `ctx.tx` cursor, not rewound event histories.

## Design principles

1. **Transactional intent is the whole point.** Every caller-facing
   write (`enqueue`, `publish`, `transition`, `start`) takes your
   `tx`/`conn` and commits *with* it. Rollback → nothing leaked.
2. **Exactly-once effects, at-least-once execution.** The kernel may
   redeliver; handlers write domain effects inside the dispatch
   transaction (`ctx.tx`) which commits with the job finish — so
   retries are safe, and idempotent-by-construction patterns (natural
   keys, upserts) cover the rest.
3. **Delegate the boring machinery.** Claiming, leases, retries,
   leader election = Chancy. Everything that differentiates —
   transactional enqueue, DAG semantics, trigger callbacks, bulk
   row state — is keelflow's own ~4k LOC, 7 deps.
4. **The spec is portable, the code is honest.** `spec/` defines the
   contract (schema, enums, lifecycle) so another implementation can
   claim conformance; `src/` is the reference — no premature
   abstraction layers pretending multi-backend support.

## Honest limitations

- **Postgres only** (by design — that's the feature).
- **Alpha** (`0.1.x`): APIs can change between minor versions; pin
  `~=` or `<`.
- **Throughput ceiling**: built for business-app volumes — bulk
  measured at ~100k rows partition+process in ~10s locally
  (`.spikes/bulk_bench.py`), not a benchmark promise; profile your hot
  path before betting on it.
- **Single-node scheduler**: `run_scheduler` in exactly one process
  (slot dedup is DB-level, but leadership isn't).
- **Human/role task visibility is app-implemented** — the engine gives
  `my_tasks(actor, roles)` + permission guards; your app decides what
  "roles" mean.
- **SuperAdmin has no auth** — bind localhost or put it behind your own
  proxy.
- **Chancy ≤0.26 kernel**: a DB disconnect can crash the worker loop
  (upstream issue); run workers under a restart supervisor — systemd,
  Docker restart policy, or a process manager.

## What it gives you

- **Durable jobs** — transactional enqueue: job + outbox row commit with
  your data change, or nothing commits.
- **DAG workflows** — `needs`/`if:` guards/`${{ outputs }}` refs, YAML or
  code definitions, human steps park a run until a person completes them.
- **Bulk imports** — chunk + per-row status, `atomic='chunk'|'row'`,
  kill-resume of pending rows only, error queue.
- **Human tasks** — inbox (`user:`/`role:`), first-claim-wins, escalation.
- **Entity state machines** — guarded transitions, `creates_job` /
  `triggers_workflow` hooks, `allowed_actions`.
- **Outbox events, audit, RBAC hooks, cron scheduling,
  key-serialized execution** (`concurrency_key`), monitoring data APIs,
  minimal SuperAdmin UI, read-only MCP tools.

Kernel lifecycle (claim/retry/schedule) rides on **Chancy**; everything
differentiating is built in-house.

## Quickstart

```bash
docker run -d --name kf-pg -e POSTGRES_PASSWORD=postgres \
  -p 5544:5432 postgres:16-alpine
docker exec kf-pg createdb -U postgres keelflow

export KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow
uv run python examples/order_flow.py   # applies schema + runs a demo flow
```

Full walkthrough of every feature: **[docs/guide.md](docs/guide.md)**.
Complete demo app (SM + human tasks + DAG + failure paths):
**[examples/invoice_flow](examples/invoice_flow/)**.

## Layout

- `src/keelflow/sdk/` — contracts apps implement (JobHandler, JobContext, specs)
- `src/keelflow/engine/` — dispatch, DAG resolver, registry, scheduler,
  state machine, bulk, events, audit, monitoring
- `src/keelflow/postgres/` — schema + storage
- `src/keelflow/compiler/` — YAML → DAG/state-machine compiler
- `src/keelflow/admin/` — minimal SuperAdmin (dev/ops, no auth)
- `src/keelflow/mcp/` — read-only agent tools
- `spec/` — language-agnostic keelflow spec (port to .NET/Java/PHP)
- `docs/architecture-design-brief.md` — design source of truth
- `docs/plans/active/` — execution plan + acceptance evidence

## Dev

```bash
uv sync --dev
uv run pytest          # needs Postgres at $KEELFLOW_TEST_DSN (default :5544/keelflow)
uv run ruff check src tests
uv run mypy src/keelflow
```

Admin UI: `KEELFLOW_DSN=… uv run python -m keelflow.admin.app` →
http://127.0.0.1:8410 (read-only + retry button; no auth — bind locally).
