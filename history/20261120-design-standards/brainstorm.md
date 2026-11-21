# Design standards decisions

Date: 2026-11-20
Status: Decision record — no implementation started

## Cancellation

Decision: keep `JobHandler.on_cancel()` as an **experimental** extension point.
Do not add a cancellation token or mid-run interruption protocol now.

Normative behavior for the current design:

- A cancellation request observed before execution prevents the pending job
  from running and marks it `cancelled`.
- An already-running job is not interrupted. It may finish as `done` or
  `failed` after a cancellation request.
- Cancellation does not roll back external side effects.
- `kill_previous` is cooperative/pre-start cancellation, not a guarantee of
  force-killing an active handler.
- Applications needing finer-grained cancellation should split work into
  durable steps/chunks/checkpoints or implement an explicit domain protocol.
- `on_cancel()` must not be used for business invariants. It is best-effort
  and cannot be guaranteed after process termination.

## Broader design direction discussed

- PostgreSQL is the first-class native storage implementation.
- A future StorageAdapter/StoragePort may support MySQL or another database;
  adapters must satisfy behavior/conformance contracts rather than merely
  replace SQL syntax.
- Chancy is the current kernel implementation; a future KernelAdapter/Port
  should allow another queue kernel without changing workflow semantics.
- `inputs_schema` and `output_schema` are optional, but strict when declared.
- Heartbeat should be worker-managed; cancellation remains best-effort.
- Engine execution metadata should be separated into `ctx.execution`; app
  extensions remain in `ctx.extra`.
- Samples should use `ctx.tx` for domain reads/writes and avoid private DB
  connections inside handlers.

## Open decisions

- Tenant policy: single-tenant database for v1 versus optional shared
  multi-tenant isolation.
- Exact behavior when a declared output schema fails validation (non-retryable
  contract failure versus normal retry policy).
- Long-running handler transaction policy.
- Authorization boundary: framework primitives versus mandatory permission
  enforcement for all monitoring/admin mutations.
