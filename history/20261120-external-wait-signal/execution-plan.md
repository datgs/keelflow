---
status: completed
last_reviewed: 2026-11-20
superseded_by: []
related_code:
  - src/keelflow/engine/workflow.py
  - src/keelflow/engine/human.py
  - src/keelflow/engine/enqueue.py
  - src/keelflow/sdk/specs.py
  - src/keelflow/sdk/services.py
  - src/keelflow/postgres/schema.sql
  - spec/02-job-lifecycle.md
  - spec/03-workflow.md
---

# Execution Plan: External wait/signal steps

Team: keelflow-core
Path: Standard, with a public-contract approval gate

## Outcome and Approach

Add a first-class `wait.*` workflow step for application-owned external
integrations. The step creates a durable blocked job without a kernel claim.
The application owns the vendor call, callback endpoint, signature verification,
external correlation mapping, and business interpretation. The callback always
flows:

`external system -> application callback route -> verify/translate -> Keelflow signal API`.

An external system never calls a worker process or Keelflow directly. The
application calls `signal(wait_job_id, result, ctx)` with a trusted service or
application context. The engine atomically accepts the resolution, stores typed
outputs, updates the step, advances the workflow, and runs any terminal
callback.

The plan does not add an engine HTTP endpoint, capability-token system, or
vendor adapter. It reuses the proven human park/complete pattern while
separating machine waits from human tasks.

## Decisions Required Before Implementation

**2026-11-20 — approved baseline**: D1 (application boundary, no buffer),
D2 (job-id reference, app-owned correlation), D3 (`wait.*` + `is_wait`).
Simplification: signals lock the wait row `FOR UPDATE` and first-wins, so
no delivery-id column and **no schema migration** are needed for v1.
`signal`/`signal_failure` take the caller's connection (like
`HumanTaskService.complete`) so the app can join domain writes and the
resolution in one transaction.

### D1 — Callback boundary and early signal policy

**Required boundary:** callbacks enter the application, not Keelflow. The
application verifies provider authentication/signatures, replay protection,
tenant identity, and business correlation. It then calls
`signal(wait_job_id, result, ctx)` with a trusted application context. Keelflow
enforces only its own wait lifecycle and `job.signal` permission.

**Recommended early-signal policy:** no Keelflow buffer in v1; reject unmatched
signals and require application/provider retry.

Why:

- A wait is valid only after its creating transaction commits. Accepting a
  signal before that creates a second state machine for an object that may
  later roll back.
- The application starts external work after wait creation commits. A
  metadata-only `wait.opened` notification may trigger that action; it carries
  no engine secret.
- A pending-signal table would need its own security, replay, expiry, dedup,
  retention, and cleanup rules.
- Providers normally retry webhooks. If a provider cannot retry, the
  application can own an inbox/outbox buffer for that integration.

Trade-off: applications must own callback retry and the correlation race. This
is deliberate; the framework is not a webhook broker.

Alternative: add `keelflow_pending_signals` later if a real provider cannot
retry and the required security, expiry, replay, and retention contract is
approved.

### D2 — Correlation and uniqueness

**Recommended:** the wait job id is the only engine v1 signal reference. There
is no `signal_by_key` and no engine-owned uniqueness policy for business keys.

The application maintains its own mapping, for example:

`provider_transaction_id -> wait_job_id`

Business keys remain application data and can be tenant-scoped, versioned, or
unique according to application rules. The engine receives the job id only
after the application has authenticated and correlated the provider callback.

Why:

- The framework does not need to become a webhook or credential manager.
- Business keys can have multiple concurrent attempts and are not globally
  unique across applications or tenants.
- `signal_by_key` would turn application identity into a core routing contract.
- The application can enforce its own business uniqueness without weakening
  engine lifecycle invariants.

Alternative: support an application correlation key only after a concrete
integration specifies tenant scope, retry, and ambiguity behavior.

### D3 — Public lifecycle name

**Recommended:** `wait.*` plus explicit `JobTypeSpec.is_wait=True`.

Rules:

- `is_wait=True` requires a `wait.` type;
- `is_wait` and `is_human` are mutually exclusive;
- a wait type has no running handler;
- workflow materialization creates a blocked job but no Chancy job;
- signal success makes the job/step `done`;
- signal failure makes the job/step `failed`.

`wait.*` leaves room for the later `wait.until` timer primitive. `external.*`
would describe the source rather than the durable state, and `signal.*` would
name the API rather than the wait. Neither is recommended.

## Tasks

| # | Task | Blocked by | Ownership | Output contract |
|---|---|---|---|---|
| 1 | Finalize public contract: `is_wait`, application callback boundary, job-id correlation, signal success/failure, duplicate delivery, permission, error types, and metadata event shape | D1-D3 approval | SDK/spec + contract design | Approved wait/signal contract in `spec/02`, `spec/03`, and a decision record |
| 2 | Add additive schema/migration for wait lifecycle and any finalized delivery-id metadata; update canonical schema and test fixtures | 1 | Postgres schema | Alembic head applies cleanly; existing rows/statuses remain valid |
| 3 | Materialize `wait.*` steps as blocked jobs with no Chancy push; optionally emit committed metadata-only `wait.opened` notification | 1, 2 | Workflow engine + events | Wait persists as `blocked`; no worker claim; application can identify the wait job |
| 4 | Implement signal service: enforce permission, validate wait type/status, lock the row, accept success/failure, apply duplicate policy, and call `on_job_finished` in the same transaction | 1, 2, 3 | SDK/service layer | One callback atomically produces job terminal state, step output/status, and downstream advance |
| 5 | Integrate cancellation, monitoring, audit/outbox events, and result-size policy without exposing provider secrets | 3, 4 | Engine ops surfaces | Cancelled waits reject later signals; open waits are queryable by age/type |
| 6 | Add contract/integration tests and a minimal application-owned callback adapter test | 2, 3, 4 | Tests/examples | Deterministic proof of accepted/rejected paths and application boundary |
| 7 | Upgrade `supplier_onboarding` as the acceptance demo: replace synchronous bank verification with `wait.bank_verify@v1`, add mock provider callback route + UI simulation, application-owned mapping, i18n, README/DAG updates, and end-to-end proof | 3, 4, 6 | Supplier demo | Visible blocked fan-in before callback and resumed `risk_score` after app callback |
| 8 | Update specs, guide, README roadmap/status, and release review | 1-7 | Integration owner | Documentation and complete validation evidence |

## Execution Guidance

- Critical path: D1-D3 -> schema/contract -> wait materialization -> signal
  transaction -> focused engine tests -> supplier demo -> docs.
- Safe parallel work after D1-D3: schema migration and spec/API skeleton can
  proceed separately, but signal implementation must wait for the finalized
  delivery-id contract. The supplier demo can start its UI/README shape after
  the application signal contract is stable.
- Shared hubs: `workflow.py`, `sdk/specs.py`, `schema.sql`, migration head,
  and supplier registry require one integration owner.
- Do not add timer scheduling, child workflows, signal buffering, engine
  business-key lookup, real vendor adapters, or a public HTTP webhook endpoint
  in this change. The demo's mock provider callback must enter the demo app
  first, then call Keelflow.

## Verification Strategy

- Focused contract tests:
  - `is_wait` namespace validation and mutual exclusion with `is_human`;
  - wait step creates a `blocked` job and no Chancy job;
  - application adapter authenticates/maps an external event to a wait job id;
  - supplier demo bank wait remains blocked while other checks finish and
    releases `risk_score` only after the app callback;
  - success and failure signals store the expected result/error;
  - unknown or non-wait job id is rejected;
  - cancelled, terminal, and already-resolved waits reject later signals;
  - duplicate delivery behavior is deterministic and safe;
  - concurrent signals have one winner;
  - signal transaction rollback leaves job, step, and run unchanged.
- Workflow integration:
  - signal output resolves `${{ steps.wait.outputs.* }}` and downstream
    `if:`;
  - wait survives worker restart because no worker owns it;
  - wait cancellation terminalizes the owning step/run consistently.
- Application-boundary proof:
  - a test application adapter receives `wait.opened` metadata if enabled,
    calls a fake external system, maps its callback to a wait job id, and
    retries when the wait is not yet accepted;
  - `supplier_onboarding` exposes a local mock-provider callback route; the UI
    simulation calls that route, not `jobs.signal()` directly;
  - provider signature verification is represented at the app boundary only
    (synthetic/local demo), not implemented in the engine.
- Repository checks:
  - `uv run pytest`;
  - `uv run ruff check src examples tests`;
  - `uv run mypy src/keelflow`;
  - `git diff --check`.

## Risks and Approval

- Accepted baseline risk: v1 requires application/provider callback retry
  instead of buffering unmatched signals.
- High risk: application trust boundary and duplicate callback semantics.
  Implementation must not begin until D1-D3 and the signal contract are
  approved.
- Medium risk: public API and schema migration; additive migration and explicit
  rollback/drain procedure required.
- Medium risk: demo code may accidentally bypass the application boundary;
  acceptance requires the simulated provider to call an app route first.
- Rollback: disable new `wait.*` definitions first; do not downgrade the
  migration while wait rows exist; retain resolver code until all open waits
  are cancelled or resolved.
- User approval: pending.

## Result

Implemented as designed (2026-11-20):

- `JobTypeSpec.is_wait` + `wait.*` namespace validation in
  `InMemoryRegistry` (mutually exclusive with `is_human`, no handler).
- `_enqueue_wait_step` in `engine/workflow.py`: blocked step + pending
  job row, no Chancy push, `wait.opened` outbox event.
- `JobService.signal`/`signal_failure` in `engine/enqueue.py`:
  `job.signal` permission, `FOR UPDATE` lock, first-wins,
  `on_job_finished` in the caller's transaction.
- `builder().enqueue()` rejects `wait.*` types.
- `supplier_onboarding` demo: `wait.bank_verify@v1` replaces the sync
  adapter; `demo_bank_requests` app correlation table;
  `send_bank_request` + `/integrations/bank/callback` app routes;
  `handle_bank_callback` shared with tests; i18n in all 5 locales;
  README updated.
- Tests: `tests/test_wait_signal.py` (6 tests — registration rules,
  enqueue guard, park/signal/advance, failure, guards, concurrent
  first-wins) plus updated `test_supplier.py` (wait blocks fan-in,
  app-boundary resolution).
- Docs: spec/02, spec/03, spec/07, guide §6, CHANGELOG, README matrix
  + roadmap (0.2 item checked).
- No schema migration needed — wait jobs ride on existing tables.
- Randomized event-sequence tests (12 seeds × 2–4 waits + 6-seed
  concurrent storm) added; they found and fixed a real bug —
  `jobs.cancel` on a pending wait/human job left the step `blocked`
  forever. `cancel` now advances the owning step in the same TX when
  the job itself terminalizes.

## Live Plan Rule

Workers may submit an evidence-backed plan-change proposal if provider retry
behavior, application correlation, or transaction locking invalidates an
assumption. Material scope, security, or public-contract changes require review
before execution continues.
