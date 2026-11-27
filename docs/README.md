# Documentation Map

Start with the smallest authoritative surface.

## Current Product

- `WORKFLOW.md`: request shape, planning, judgment, operation, validation, and
  completion.
- `architecture-design-brief.md`: historical draft design rationale; use it
  for background, not as the only current authority.
- `reliability.md`: durability, recovery, backend limits, and upgrade policy.
- `security.md`: data access, telemetry, and admin/MCP boundaries.
- `../spec/`: language-agnostic schema, lifecycle, context, and conformance
  contract.
- `../README.md`: product overview, quickstart, feature matrix, dispatch
  backends, and development commands.
- `decisions/`: accepted architecture and product choices; check status and
  follow-up notes when current code or active plans describe a later state.
- `plans/`: one durable working-memory document for work that needs it.
- [`patterns/encoding-invariants.md`](patterns/encoding-invariants.md): turn
  accepted architecture, reliability, security, and quality rules into native
  mechanical validation.
- `templates/`: optional decision, plan, runbook, and Harness-improvement
  structures.

## Consumer-Owned Truth

The consumer's README, product documents, architecture, code, tests, CI,
runtime signals, and application behavior remain authoritative. Harness does
not overwrite those with upstream product assumptions.

## Code And Validation

- `../src/keelflow/`: Python implementation.
- `../tests/`: behavior ownership and repository contract.
- `../examples/`: runnable application examples.
- `../alembic/`: versioned schema migrations.
- `../pyproject.toml`: package, tool, and pytest configuration.

## History

Historical implementation detail is preserved by Git history and completed
plans under `plans/completed/`.
