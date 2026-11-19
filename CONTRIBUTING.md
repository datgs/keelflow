# Contributing to keelflow

Thanks for your interest. keelflow is in alpha (`0.1.x`) — API surfaces
can still change between minor versions.

## Dev setup

```bash
git clone https://github.com/datgs/keelflow
cd keelflow
uv sync --dev

# Postgres for tests (any local instance works)
docker run -d --name kf-pg -e POSTGRES_PASSWORD=postgres \
  -p 5544:5432 postgres:16-alpine
docker exec kf-pg createdb -U postgres keelflow
export KEELFLOW_TEST_DSN=postgresql://postgres:postgres@localhost:5544/keelflow
```

## Checks (all must pass before a PR)

```bash
uv run pytest            # integration tests need Postgres; they skip without it
uv run ruff check src tests examples
uv run mypy src/keelflow
```

CI runs the same three on every push (`.github/workflows/ci.yml`).

## Design rules that matter

- **Transactional intent is the core invariant.** A caller-facing API
  that mutates state takes `tx`/`conn`/cursor and writes inside it —
  never opens a private connection, never commits internally.
- **One TX per handler execution.** `dispatch` runs `handler.run` +
  finish + advance atomically; domain writes go through `ctx.tx`.
- **Idempotent handlers.** The kernel may redeliver; use upserts and
  natural keys, never assume exactly-once handler execution.
- **Kernel prefix != engine prefix.** Kernel tables use
  `keelflow_kernel_` (or a test prefix) — never `keelflow_`.
- **Schema changes** go in `src/keelflow/postgres/schema.sql` *and* a new
  `alembic/versions/` migration; keep `IF NOT EXISTS` idempotency.
- **`spec/` is normative.** Behavior changes that affect the contract
  (status enums, lifecycle, schema columns, guard grammar) must update
  the matching spec file in the same commit.
- Keep the core dependency-light: no web framework, no ORM in
  `src/keelflow`.

## Commit conventions

Small, focused commits with a task/step prefix (`T12:`, `D1:`,
`docs:`, `fix:`…). PRs should reference the spec section or plan task
they implement.

## Reporting issues

- Bugs: repro (test or script) + `keelflow`/`chancy` versions + Postgres
  version.
- The kernel is Chancy — kernel-level issues (claim/lease/recovery)
  belong upstream at `TkTech/chancy`; engine issues belong here.
  If unsure, file here and we'll triage.
