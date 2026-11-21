---
status: proposed
last_reviewed: 2026-11-20
superseded_by: []
related_code:
  - src/keelflow/engine/workflow.py
  - src/keelflow/engine/human.py
  - src/keelflow/engine/enqueue.py
  - src/keelflow/sdk/specs.py
  - src/keelflow/postgres/schema.sql
---

# Approach: External wait/signal steps

## Recommendation

Add a first-class external-wait lifecycle that is intentionally separate from
`human.*` and ordinary worker jobs. A registered `wait.*` type is materialized as
a `blocked` step and a pending job row, but no Chancy job is pushed. The
application creates the external request and exposes its own authenticated
callback route. The route verifies the provider callback, translates it into
application meaning, and calls `signal(wait_job_id, result, ctx)`. The service
locks the wait, accepts exactly one terminal resolution, writes the result,
advances the owning workflow, and commits all of that in one transaction.

The wait job id is an engine reference, not an authentication credential. The
application owns the external correlation mapping, for example
`provider_transaction_id -> wait_job_id`. Keelflow does not generate, store, or
deliver webhook bearer tokens. The external system must never call a worker
process or a Keelflow HTTP endpoint directly: the route is always
`external system -> application -> Keelflow signal API`.

For v1, do not buffer unmatched signals in Keelflow. Return a typed `NotFound`
error and let the application/provider retry or use an application-owned inbox
when needed. This keeps the engine's durable state small and makes the trust
boundary clear: a signal is valid only after its wait row commits. A committed
`wait.opened` notification may help the application start external work, but it
contains metadata only and no engine secret.

Use explicit `JobTypeSpec.is_wait` (default `False`) rather than only checking
`type.startswith("wait.")`. Keep the public family convention `wait.*` and
validate that `is_wait=True` types use that namespace. This avoids accidental
parking of an ordinary type and keeps the lifecycle discoverable in YAML.

The signal API should support two terminal outcomes:

- `signal_success(wait_job_id, result, ctx)` -> job/step `done` and downstream
  advance;
- `signal_failure(wait_job_id, error, ctx)` -> job/step `failed` and normal run
  failure semantics.

The application should normally translate provider-level failures into one of
these outcomes. A domain rejection should be represented in the result and
handled by `if:` or a later business step; the engine must not guess what
"declined" means.

## Meaningful Alternatives

- **Reuse `human.*`** — minimal code, but it pollutes human inboxes, misstates
  audit actors, and enables user-oriented escalation on machine callbacks.
- **Use polling worker jobs** — no new lifecycle, but burns worker capacity,
  converts normal waiting into retries/failures, and creates poor observability.
- **Buffer unmatched signals in `keelflow_pending_signals`** — more tolerant of
  provider timing, but adds retention, replay, dedup, and cleanup semantics to
  the core. Defer; an application may own this inbox if its provider needs it.
- **Engine lookup by `(type, correlation_key)`** — convenient, but ambiguous
  when multiple runs wait on the same business key and turns application
  identity into a core routing contract. Defer; the application owns its
  provider-key-to-job-id mapping.
- **External system calling Keelflow directly** — rejected because the core
  cannot verify provider signatures, replay protection, tenant identity, or
  business interpretation. The application callback route is the integration
  boundary.

## Contracts and Integration

- Definition: a job type may opt into `is_wait=True`; its type must be in the
  `wait.*` namespace. It has an input schema and optional output schema, but no
  handler is invoked while waiting.
- Open: the workflow engine creates the job row in `pending`, marks the step
  `blocked`, and may emit a metadata-only `wait.opened` notification after
  commit.
- Resolve: the application passes the wait job id, result or failure, and a
  trusted actor context. The engine accepts only an open wait; repeated
  callbacks are rejected or made idempotent according to the finalized
  delivery-id contract.
- Security: `job.signal` permission is required. A service actor should be the
  normal caller. Provider authentication and business-key lookup happen in the
  application before this API is called.
- Atomicity: wait row terminalization, step output update, run advance, and any
  run terminal callback commit together.
- Cancellation: existing `JobService.cancel` supports `blocked` waits; a
  cancelled wait must not later accept a signal.
- Compatibility: no behavior change for `human.*`, normal jobs, or existing
  workflow definitions; migration is additive.

## Risk and Evidence

| Area | Risk | Evidence or validation |
|---|---|---|
| Early callback | medium | v1 open contract + explicit retry; test signal-before-open rejects and retry-after-open succeeds |
| Duplicate callback | high | lock + delivery-id/idempotency contract; concurrent signal test |
| Application trust boundary | high | signal requires trusted context and permission; direct external callback is outside core |
| Workflow atomicity | high | rollback test for signal/advance failure; output and next-step visibility test |
| Cancellation race | medium | cancel-vs-signal row-lock test; cancellation wins if committed first |
| Long waits | medium | monitoring query/README contract; no kernel row and no worker claim test |
| Migration | low | additive Alembic migration and clean-schema apply test |

## Migration, Operations, and Rollback

- Migration: add only wait lifecycle/delivery metadata required by the final
  signal contract through one additive Alembic revision; update the canonical
  schema and test truncation fixtures. Do not add token storage or alter
  existing job status values.
- Observability: expose wait type/status/age in monitoring; emit `wait.opened`
  and `wait.resolved` audit/outbox events without provider secrets. Document a
  stuck-wait query and a later timeout/timer follow-up.
- Rollback: code can be rolled back while existing wait rows remain only if the
  release policy retains the resolver and migration columns. Do not downgrade
  a migration after wait rows exist; if needed, disable new wait definitions
  and drain/cancel existing waits first.

## Accepted Assumptions

- Application integrations can authenticate and retry callbacks, and own
  provider-key-to-wait-job mapping.
- The engine is not an internet-facing webhook verifier; external systems call
  the application, never Keelflow directly.
- v1 handles one terminal callback per wait and does not model progress,
  heartbeats, or provider polling.
