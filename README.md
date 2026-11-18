# pyerp

Reusable Postgres-backed durable workflow engine (jobs, DAG workflows, bulk processing, human-in-the-loop tasks, entity state machines, outbox events, RBAC hooks, audit).

## Layout

- `src/pyerp/sdk/` — contracts applications implement (JobHandler, JobContext, specs)
- `src/pyerp/engine/` — worker loop, DAG resolver, registry, scheduler, state machine
- `src/pyerp/postgres/` — storage adapter + schema
- `src/pyerp/compiler/` — YAML → DAG / state-machine compiler
- `src/pyerp/admin/` — raw SuperAdmin UI (dev/ops)
- `src/pyerp/mcp/` — read-only agent tools
- `docs/architecture-design-brief.md` — design source of truth
- `docs/plans/active/` — current execution plan (discovery/approach/plan)

## Dev

```bash
uv sync --dev
uv run pytest
uv run ruff check
```
