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
  - examples/supplier_onboarding/src/supplier_onboarding/registry.py
  - examples/supplier_onboarding/src/supplier_onboarding/app.py
  - examples/supplier_onboarding/tests/test_supplier.py
---

# Discovery: External wait/signal steps

## Outcome and Scope

- Desired behavior: a workflow can park at an application-defined external
  wait step without claiming a worker; an application-owned integration can
  resolve the wait with a verified callback; resolution records outputs and
  advances the DAG atomically.
- Users: applications integrating payment, shipping, document, vendor, or
  other asynchronous external systems.
- Acceptance demo: extend `examples/supplier_onboarding` by replacing the
  synchronous mock bank check with an application-owned mock provider callback;
  this demonstrates a wait that blocks fan-in without claiming a worker.
- Non-goals: the engine does not call vendor APIs, host webhook routes, verify
  vendor signatures, interpret business outcomes, or provide a generic event
  broker. Durable timers are a later roadmap item.

## Evidence

- `src/keelflow/engine/workflow.py:305-340` — human steps already park a run
  without a kernel job; the workflow engine can reuse this lifecycle boundary.
- `src/keelflow/engine/workflow.py:568-595` — human step enqueue inserts a job
  row and marks the step `blocked`; no worker claim is created.
- `src/keelflow/engine/human.py:117-160` — human completion locks the job,
  writes the result, and calls `on_job_finished` in the same transaction.
- `src/keelflow/engine/enqueue.py:221-266` — cancellation already handles
  `blocked`/live jobs and is permission-checked; wait cancellation should use
  the same terminal path.
- `src/keelflow/sdk/specs.py:64-94` — `JobTypeSpec` has an explicit
  `is_human` lifecycle flag; a wait lifecycle should use an explicit marker,
  not only a string-prefix convention.
- `src/keelflow/postgres/schema.sql:4-40` — jobs have nullable lifecycle and
  uniqueness fields but no wait token or signal storage, which supports a
  small reference-based design.
- `src/keelflow/engine/events.py:29-50` — the transactional outbox can carry
  a post-commit `wait.opened` metadata notification to application code.
- `examples/supplier_onboarding/src/supplier_onboarding/registry.py:95-144` —
  the demo already models external checks as deterministic mock adapters; the
  bank verification path is the natural candidate for an external wait.
- `examples/supplier_onboarding/src/supplier_onboarding/registry.py:258-289` —
  the bank check feeds `risk_score` fan-in, making the blocked-before-callback
  and resumed-after-callback states visible.
- `examples/supplier_onboarding/src/supplier_onboarding/app.py:165-237` —
  the demo already owns application routes and human completion; a mock
  provider callback route can call the signal API from this application layer.
- `examples/supplier_onboarding/tests/test_supplier.py:142-193` — the existing
  full-run integration test is the right acceptance-test seam for the new wait.
- Temporal's official messaging documentation describes asynchronous Signals
  and distinguishes them from synchronous Updates:
  https://docs.temporal.io/sending-messages
- AWS Step Functions' official integration documentation describes callback
  tasks where an external process returns to the application-owned integration;
  the callback task can represent a third-party system:
  https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html

## Existing Contracts and Patterns

- `advance_run()` is the single writer for starting eligible steps and already
  recognizes `blocked` as a live workflow state.
- `on_job_finished()` is the atomic bridge from job outcome to step outputs and
  downstream scheduling.
- `JobContext` supports service actors and permissions; `job.signal` can be
  enforced without treating a webhook as a human actor.
- Transactional outbox delivery is at-least-once, so application subscribers
  must be idempotent.

## Constraints

- Technical: Postgres remains the official implementation; migration must be
  additive and old jobs must remain valid.
- Operations: a wait may last hours or days; it must consume no worker slot.
  Unresolved waits must be visible in monitoring and cancellable.
- Security/data: job ids are correlation references, not credentials. Vendor
  authentication, signature verification, replay protection, tenant isolation,
  and business authorization remain application-owned. The engine receives a
  trusted application context and enforces `job.signal`.
- Compatibility: existing `human.*` behavior and generic machine jobs must not
  change. Existing workflows keep working without a migration rewrite.

## Facts, Assumptions, Questions

- Verified: the current human-task implementation proves the park/resolve/
  advance transaction pattern.
- Verified: the current outbox is written in the same database and delivers
  after the creating transaction commits.
- Verified: supplier onboarding has a natural application-owned mock callback
  seam around bank verification and an existing integration test harness.
- Assumed: applications can retry a webhook when the callback is not accepted;
  this is the normal integration contract for providers.
- Open: whether v1 emits a metadata-only `wait.opened` notification; the exact
  public marker/name for the wait lifecycle; duplicate callback delivery
  semantics.

## Clarifications

- The recommended boundary is: external system -> application callback route
  -> verify/translate -> Keelflow signal API. The application owns the
  outbound call, callback endpoint, provider authentication, external-key to
  wait-job mapping, and business interpretation. Keelflow owns durable waiting,
  atomic resolution, cancellation, and DAG advancement.
