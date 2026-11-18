---
status: proposed
last_reviewed: 2026-11-18
superseded_by: []
related_code: []
---

# Approach: pyerp — adopt kernel, own differentiators

## Recommendation
Build `pyerp` as a layered Python library: a thin SDK of contracts on top, a Postgres storage adapter, and an engine that reuses a mature Postgres job-queue kernel (Chancy or PgQueuer, decided by a benchmark spike) for claim/retry/lease mechanics. All differentiating layers — DAG workflows with `needs`/`if:`/outputs, chunk+row bulk semantics, versioned typed handlers, YAML compiler, human tasks, state machines, JobContext, audit hooks — are built by us.

This is "adopt the commodity, own the differentiator", and it is reversible: the SDK boundary means swapping kernels changes only `pyerp/postgres` + worker glue, never app handlers.

## Meaningful Alternatives
- **Celery + Redis/RabbitMQ** — rejected: reintroduces the dual-write problem (broker state vs Postgres business state), extra infra, and gives no durable row/chunk model.
- **Temporal / DBOS / Hatchet** — rejected: platform/replay model, wrong fit for our run-detail monitoring and data-centric state machine; also heavier operations.
- **Fully custom kernel** — deferred: possible but slower; we keep the option by hiding the kernel behind `Storage`/worker interfaces and benchmarking in T2. Switch cost is contained.

## Contracts and Integration
- Inputs: job payloads (Pydantic), workflow/state-machine YAML definitions, JobContext (actor, tenant, permissions, correlation, extra).
- Outputs: `jobs`, `chunks`, `row_status`, `step_runs`, `workflow_runs`, `outbox_events`, `audit_events` tables; SDK methods for enqueue/query/claim/complete; events emitted for downstream consumers.
- Compatibility: SDK surface is stable (`pyerp.sdk` only); engine internals free to change behind it.

## Risk and Evidence
| Area | Risk | Evidence or validation |
|---|---|---|
| Kernel reliability (Chancy/PgQueuer bugs) | medium | T2 benchmark spike: enqueue/claim throughput, kill-resume, duplicate-exec checks |
| Transactional intent correctness | high | T4 contract test: enqueue inside TX + rollback → no job |
| 500k-row chunk performance | medium | T8 integration test on synthetic 500k-row staging import |
| Key-serialized ordering (gap in all kernels) | medium | T8/T10 test: same `concurrency_key` never runs concurrently |
| YAML `if:` guard expressiveness | low | T7 spec keeps minimal comparison syntax only |
| RBAC/scope enforcement bypass | high | T11 negative tests: direct table read cannot skip `require` |

## Migration, Operations, and Rollback
- Migration: engine tables ship as Alembic migrations inside `pyerp/postgres`; apps run them in their own pipeline.
- Observability: `metrics.snapshot()` (queue depth, oldest pending, failure rate), structured logs with `job_id`/`correlation_id`, SuperAdmin read-only UI.
- Rollback: kernel swap is localized to `pyerp/postgres`; definitions and job history remain in Postgres.

## Accepted Assumptions
- Postgres is always the system of record; no external broker.
- All ambient execution data travels in `JobContext`, never in payloads.
- Human tasks share the same jobs table/state machine as machine jobs.
- The prototype is library-first — no domain tables or screens are built.
