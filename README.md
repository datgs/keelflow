# keelflow

The keel of your business flows — a reusable Postgres-native durable
workflow engine. Reference Python implementation of the **keelflow spec**
(`spec/`).

`pip install keelflow` (PyPI name: `keelflow`)

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
