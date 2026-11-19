# hd-flow spec — overview

Status: draft v0.1 · Reference implementation: `keelflow` (Python)

hd-flow is a durable workflow engine contract for business applications
(ERP-style): jobs, DAG workflows, bulk row processing, human tasks, entity
state machines, outbox events, audit — all transactional on a single
relational database.

A conforming implementation **MUST** satisfy every MUST below and pass
the conformance checklist in `07-conformance.md`.

## Core invariants

1. **Transactional intent.** Enqueueing a job, publishing an event, or
   recording an audit entry happens inside the caller's transaction.
   A rolled-back transaction leaves no phantom job/event. This is the
   single most important property — it is what makes the engine "durable"
   rather than "eventually consistent".

2. **Same-connection rule.** An API that accepts `tx`/`conn`/`session`
   runs on the caller's handle — never opens a private connection.
   Engines may open their own connections only for work that is already
   committed (worker dispatch, chunk fan-in, scheduler ticks).

3. **Kernel-agnostic lifecycle.** Job claim/retry/schedule may be
   delegated to a job kernel (Chancy, Hangfire, Quartz…) or built in,
   as long as the kernel satisfies the durable intent invariant and the
   job state machine in `02-job-lifecycle.md`.

4. **Strict state enums.** `pending → claimed → running → done|failed|
   cancelled|blocked`. Business state lives in payload/result — never
   in new engine statuses.

5. **Actor model everywhere.** Every mutating call carries a `JobContext`
   (see `06-context.md`) resolved from the caller — no ambient/global
   actor, no permission smuggling in payloads.

## Table prefix

Reference implementation uses `hd_flow_` prefix. A conforming
implementation MAY use a different prefix but MUST keep table names,
column semantics, and invariants from `01-schema.md`.
