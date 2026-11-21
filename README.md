# keelflow

[![PyPI](https://img.shields.io/pypi/v/keelflow)](https://pypi.org/project/keelflow/)
[![Python](https://img.shields.io/pypi/pyversions/keelflow)](https://pypi.org/project/keelflow/)
[![CI](https://github.com/datgs/keelflow/actions/workflows/ci.yml/badge.svg)](https://github.com/datgs/keelflow/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

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
- **Cancellation is cooperative** — running jobs drain by default;
  mid-run stops only happen at `ctx.execution.checkpoint()` on job
  types that opt in, and external side effects never roll back.

## Feature matrix

| Capability | Status | Notes |
|---|---|---|
| Durable jobs, transactional enqueue | ✅ | job + outbox row commit with your data, or nothing |
| Payload schema validation | ✅ | pydantic, at enqueue **and** dispatch |
| Retry with backoff | ✅ | per job type (`max_attempts`), kernel-tracked |
| DAG workflows (`needs`, `if:`, `${{ }}` refs) | ✅ | parallel ready steps, code or YAML defs |
| Human steps in a DAG | ✅ | `human.*` types park the run (`blocked`) |
| External wait steps | ✅ | `wait.*` + `is_wait` park the run with no kernel claim; `signal`/`signal_failure` resolve atomically — provider auth + correlation stay app-owned |
| Business vs technical failure | ✅ | `decision=reject` → `rejected`; retry exhaustion → `failed` |
| Workflow triggers (SM → run → SM) | ✅ | `on_done/on_rejected/on_failed`, exactly-once callback |
| Resume failed/rejected runs | ✅ | `resume_run` in-place, `resume_step` |
| Entity state machines | ✅ | guards, `creates_job`, `allowed_actions`, row-lock serialization |
| Human task inbox | ✅ | `user:`/`role:` assignees, first-claim-wins, escalation |
| Bulk chunk/row processing | ✅ | `atomic='chunk'\|'row'`, kill-resume pending rows, error queue |
| Bulk set-based batch callback | ✅ | `process_batch` per chunk — one TX, one outcome per engine row id; ~4–5× the per-row path on write-heavy imports |
| Bulk job as DAG step | ✅ | fan-in advances the run atomically |
| Transactional outbox events | ✅ | per-subscriber dedup, at-least-once |
| `concurrency_key` serialized execution | ✅ | pg advisory locks |
| Cron scheduling | ✅ | window + overlap policies |
| Cooperative cancellation | ✅ | `JobService.cancel` (`job.cancel` perm); pre-start default + opt-in `ctx.execution.checkpoint()`; bulk chunks checkpoint per row, cascade to children |
| Dead-letter queue | ✅ | `keelflow_dlq` + `dlq_replay` — replay resumes the owning run |
| Retention & sweep | ✅ | `row_disposition` archive/keep/delete + `MonitoringService.sweep` (terminal jobs/runs, outbox, archive, DLQ) |
| Idempotent enqueue | ✅ | `unique_key` dedup over non-terminal jobs |
| Result size visibility | ✅ | `result_warning_bytes` soft + `result_max_bytes` opt-in hard cap |
| Audit trail | ✅ | append-only, `via` actor chains |
| Monitoring APIs / SuperAdmin UI | ✅ | data APIs; SuperAdmin = no-auth, localhost |
| Read-only MCP tools | ✅ | agent-inspectable jobs/runs/audit |
| YAML definition compiler | ✅ | validated at load: cycles, unknown types, guards |
| Language-agnostic spec | ✅ | `spec/` + conformance checklist |
| Single-TX dispatch (`ctx.tx`) | ✅ | handler writes + finish + advance in one commit |

Kernel lifecycle (claim/retry/schedule) rides on **Chancy**; everything
differentiating is built in-house.

## Roadmap

**0.1.x (alpha — now):** API stabilization; the surface above is
feature-complete for the spec. Expect minor breaking changes between
alphas — pin `keelflow~=0.1.0a1`.

**0.2** — hardening & ops
- [ ] Admin UI auth (token or proxy-ready session)
- [ ] `enqueue_many` batch API (5–10× enqueue throughput, same TX)
- [ ] DLQ requeue + sweep actions in SuperAdmin UI (engine `dlq_replay`/
      `sweep` APIs shipped in 0.1.x)
- [ ] Per-job-type concurrency limits (`ConcurrencySpec.max_workers` is
      declared but not yet enforced); queue metrics in SuperAdmin
- [x] External wait/signal steps (`wait.*` job type + `signal` API):
      a DAG step that parks the run until an external system resolves
      it — vendor callbacks, webhooks, async integrations; the app owns
      the call out, the engine owns the durable wait — **shipped**
- [ ] Run idempotency key — `wf.start(name, inputs, idem_key=...)`
      dedups `workflow.start` like `unique_key` does for jobs
- [ ] Declared-schema enforcement — optional `inputs_schema`/
      `output_schema` validated strict when present
- [ ] `cancel_run` API + run deadline — cancel a live DAG; parent
      propagation exists, run-level wrapper + timeout remain
- [ ] OpenTelemetry instrumentation (optional `keelflow[otel]` extra):
      W3C `traceparent` propagation through `jobs.context`, spans for
      enqueue/dispatch/handler/workflow-advance/fan-in/human-complete,
      attempt links across retries, job counters + duration histograms
      + queue-depth gauges — zero overhead when not enabled

**0.3** — growth & interop
- [ ] Durable timer step — `wait.until`/`sleep` inside a DAG
      (remind-after-3-days, escalation windows); builds on the `wait.*`
      step primitive from 0.2
- [ ] Child workflows — run-as-a-step (`subflow.*`), link run↔run;
      reuse the `blocked`/parent model proven by `human.*` and bulk
- [ ] Second-language conformance harness (spec test suite as JSON cases)
- [ ] LISTEN/NOTIFY wakeups for lower claim latency
- [ ] Cron timezone DST audit + per-type rate limits

**1.0** — stability promise
- [ ] Frozen `JobTypeSpec`/`StepSpec`/`ctx` public API + deprecation policy
- [ ] Migration-tested upgrades (alembic path CI)
- [ ] Published throughput/latency benchmark suite

**Deferred (intentionally, not forgotten)**
- [ ] Message-broker delivery adapter — `dispatch(job_id)` seam already
      exists; build when a real >thousands-jobs/s use case demands it
      ([why](docs/guide.md))
- [ ] Multi-backend storage — spec is portable; a second engine
      implementation lands when someone ports it
- [ ] Chancy 0.27+ kernel upgrade (breaking plugin API upstream)

## Quickstart

```bash
docker run -d --name kf-pg -e POSTGRES_PASSWORD=postgres \
  -p 5544:5432 postgres:16-alpine
docker exec kf-pg createdb -U postgres keelflow

export KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow
uv run python examples/order_flow.py   # applies schema + runs a demo flow
```

Full walkthrough of every feature: **[docs/guide.md](docs/guide.md)**.
Demo apps:
[examples/invoice_flow](examples/invoice_flow/) (SM + human tasks + DAG
+ failure paths),
[examples/supplier_onboarding](examples/supplier_onboarding/) (~20-step
conditional DAG, parallel checks, HITL gates),
[examples/bulk_import](examples/bulk_import/) (chunked bulk processing,
error queue, human map/approve),
[examples/rag_demo](examples/rag_demo/) (employee helpdesk RAG — durable
'LLM': timeout, retry backoff, DLQ replay, confidence-gated human review).

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
