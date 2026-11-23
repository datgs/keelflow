---
status: active
last_reviewed: 2026-11-20
superseded_by: []
related_code:
  - src/keelflow/engine/worker.py
  - src/keelflow/sdk/context.py
  - src/keelflow/sdk/handler.py
  - src/keelflow/sdk/specs.py
  - spec/02-job-lifecycle.md
  - spec/03-workflow.md
---

# Execution Plan: Cooperative checkpoint cancellation

Date: 2026-11-20

## Status

Completed — approved and implemented (2026-11-20). The `test_e2e_scenario`
kill-resume flake noted here was fixed in `a76aa17` (2026-11-22): the test
now forces `Recovery.recover` after the dead worker's heartbeat lapse
instead of racing the Recovery poll. Verified 2026-11-23: 12/12 e2e runs +
25/25 isolated kill-resume stress iterations pass.

## Outcome

Keep pre-start cancellation as the default. Allow a job type that opts in to
cooperative mid-run cancellation to stop at explicit handler checkpoints. When
a checkpoint observes a committed cancellation request, it raises a dedicated
cancellation exception. The handler transaction rolls back; the worker records
`cancelled` and advances any owning workflow step in a separate transaction.
Ordinary jobs without checkpoints continue to drain to `done` or `failed`.

## Context and Evidence

- `src/keelflow/engine/worker.py:85-89` loads the job, checks
  `cancel_requested` at `src/keelflow/engine/worker.py:115-128`, and separately
  sets it to `running` at `src/keelflow/engine/worker.py:152`. Cancellation can
  race between the check and status change, so start/cancel needs one atomic
  decision; a separate re-check followed by an unconditional update is not
  sufficient.
- `src/keelflow/engine/worker.py:152-221` runs the handler and job/workflow
  completion in one transaction. An exception enters the generic retry/DLQ path
  at `src/keelflow/engine/worker.py:222-291`; cancellation therefore needs a
  distinct exception path or it will be retried as failure.
- `src/keelflow/sdk/context.py:10-27` carries `JobContext` and the handler
  transaction. There is no execution-checkpoint interface yet.
- `src/keelflow/sdk/handler.py:24-43` currently exposes `heartbeat()` and
  `on_cancel()`, but the worker does not invoke either hook.
- `src/keelflow/postgres/schema.sql:24` persists `cancel_requested`.
  `src/keelflow/engine/scheduler.py:97-107` sets it for an active job under
  `overlap="kill_previous"`; running jobs retain their status and drain.
- The SDK `JobService` protocol declares `cancel()` at
  `src/keelflow/sdk/services.py:155-169`, but no engine implementation of a
  public job-cancel operation was found. User approved implementing it as part
  of this feature so callers have a supported request path beyond cron.
- `spec/02-job-lifecycle.md:58-71` documents pre-start cancellation and drain
  behavior. `spec/03-workflow.md:104-107` has older, inconsistent hook wording
  and must be aligned when the contract changes.
- Chancy provides worker liveness/recovery. Its official recovery docs state
  abandoned execution cancellation is best-effort and late updates are fenced;
  this plan does not replace kernel worker heartbeat with handler heartbeat.
- Temporal's official activity docs deliver cancellation at worker heartbeats
  and offer wait-versus-proceed cancellation semantics. This is useful evidence
  for explicit acknowledgement, but keelflow chooses handler-owned safe-point
  checkpoints rather than periodic worker callbacks.

## Scope

In scope:

- An explicit checkpoint API for handlers, tentatively
  `await ctx.execution.checkpoint()`.
- An opt-in declaration on `JobTypeSpec`; default remains pre-start-only.
- A dedicated cancellation signal that is not treated as a retryable handler
  error.
- Rollback of the open handler transaction before recording cancellation.
- Workflow-step terminalization and durable status semantics.
- Clarification that `on_cancel()` remains experimental and is not invoked by
  the worker in this feature.
- Remove the unused handler `heartbeat()` hook; Chancy continues to own worker
  liveness/recovery.
- Focused unit/integration tests and updates to lifecycle/workflow specs and
  user guide.

Out of scope:

- Force-killing Python tasks or OS processes.
- Guaranteeing cleanup after process crash.
- Rolling back external side effects (HTTP, email, payment, filesystem, etc.).
- Progress payloads, checkpoint persistence/resume data, automatic heartbeat
  polling, or adaptive heartbeat cadence.
- A new cancel-run API. The already-declared job-cancel request operation is
  included in this feature; authorization policy remains to be decided.
- Invoking `on_cancel()` as a guaranteed callback.

## Confirmed Product/Contract Decision

- **Authorization for public cancellation:** require `job.cancel` via the
  existing permission service. System actor behavior follows the existing
  permission policy. Do not silently trust caller-supplied context permissions.

## Proposed Contract

1. A job must explicitly opt in before `ctx.execution.checkpoint()` observes
   and raises for a cancellation request. Non-opted-in jobs retain current
   drain-to-completion behavior.
2. The worker's transition into execution is the cancellation/start
   linearization point. Implement one atomic database operation that transitions
   an eligible pending/claimed job to `running` only when
   `cancel_requested=false`. If cancellation commits first, the handler must
   not run and the job is terminalized as cancelled. If the start transition
   commits first, the request is treated as a running-job request: default jobs
   drain; opted-in jobs may stop at a checkpoint. A separate check followed by
   an unconditional status update is not sufficient.
3. The checkpoint reads the durable request at the handler's explicit safe
   point. Proposed baseline: query the job row using the handler's existing
   PostgreSQL transaction/cursor at `READ COMMITTED`; a plain SELECT should not
   block the cancellation UPDATE. A checkpoint that begins before the request
   commits may miss it; a later checkpoint may observe it. No background
   polling or heartbeat interval is needed.
4. If the flag is set, checkpoint raises a dedicated `JobCancelled` signal.
   Proposed Python contract: subclass `BaseException`, not `Exception`, so
   ordinary `except Exception` blocks do not accidentally swallow it. It is
   control flow, not a handler failure, and must bypass retry/backoff and DLQ.
   Code that explicitly catches `BaseException` can still suppress it; document
   that handlers must not do so.
5. The exception exits the handler transaction, rolling back all domain writes
   made in that attempt. After rollback, worker records job `cancelled` and
   advances the owning step/run in a new transaction. Cancellation recording
   must be idempotent if kernel redelivers the dispatch.
6. Cancellation can race with completion. If no checkpoint observes the
   request before the handler returns and completion commits, normal completion
   wins (`done`/`failed`). A request accepted by a checkpoint wins and records
   `cancelled`.
7. `on_cancel()` remains experimental and is not called by this feature. Any
   future cleanup callback needs its own decision about transaction scope,
   failure behavior, idempotency, and external side effects.
8. A checkpoint provides cancellation responsiveness only when handler code
   reaches it. Long blocking calls and code that never checkpoints continue
   running; applications should place checkpoints between safe batches/units.

## Approach

Use the existing durable flag as the source of truth. A checkpoint query occurs
only when application code calls it, avoiding background task races and a new
cadence setting. Use a dedicated cancellation exception so the worker can keep
its existing generic exception/retry path unchanged for ordinary failures.
The handler transaction must unwind before the worker persists the terminal
cancel state. Keep Postgres `READ COMMITTED` behavior explicit; if the worker's
connection isolation or transaction structure prevents observing a concurrent
committed flag, revise the storage/read design before implementation.

## Risks and Recovery

- **Transaction and external side effects:** database writes roll back, but
  external calls do not. Handlers must use idempotency keys/outbox patterns for
  external effects and checkpoint before starting another unit of work.
- **Start/cancel race:** the atomic start transition and cancel request must
  serialize on the job row. Whichever commits first determines whether the job
  is pre-start-cancelled or running. Use a conditional update or equivalent
  row-lock transaction and test both orderings. Never use read-then-write
  without an atomic condition.
- **Race at finish:** cancellation after the final checkpoint may lose to
  successful completion. Document this; a stricter guarantee would require a
  serialized finish/cancel protocol and additional locking.
- **Workflow atomicity:** handler rollback and terminal cancellation are
  separate transactions by necessity. Recovery/re-delivery must converge to
  one terminal job and one workflow outcome.
- **Checkpoints inside nested transactions/locks:** confirm the query is safe
  while handler code holds locks; do not acquire a conflicting lock on the
  job row in the checkpoint path.
- **Compatibility:** adding opt-in changes public SDK/spec contracts. Default
  behavior must remain unchanged for existing registered job types.
- **Rollback:** revert the SDK/API and worker branch while leaving the existing
  `cancel_requested` column and pre-start behavior intact. No data migration is
  expected unless implementation evidence requires one.

## Tasks

### T1 — Finalize checkpoint contract and SDK surface

**Goal:** Define the opt-in and checkpoint behavior as a stable, unambiguous
contract.

**Context:** This is a public handler API and a new cancellation outcome; use
this plan's proposed contract and the existing `JobContext`/`JobTypeSpec`.

**Required outcomes:**
- Choose exact public name/location for the checkpoint and opt-in declaration.
- Define public `JobCancelled` control-flow type; prefer a `BaseException`
  subclass so ordinary `except Exception` does not swallow cancellation.
  Document that catching `BaseException` must re-raise it.
- Define that the API is async, raises only after observing the durable flag,
  and has no periodic polling/cadence.
- Define `JobService.cancel(job_id, ctx)` permission, idempotency, and
  pending-versus-running request behavior.
- Remove or explicitly deprecate the unused handler `heartbeat()` method.

**Constraints:** No cancellation token, background polling, forced task
cancellation, or `on_cancel()` invocation.

**Acceptance evidence:** SDK typing/API tests; docs show default-off behavior and
one opted-in handler example.

**Verification:** `uv run pytest tests/test_sdk.py -q`; `uv run mypy src/keelflow`.

**Ownership:** `src/keelflow/sdk/context.py`, `src/keelflow/sdk/specs.py`,
`src/keelflow/sdk/handler.py`, SDK exports, `tests/test_sdk.py`.

**Dependencies:** None.

### T2 — Implement cancellation request and transaction-safe checkpoints

**Goal:** The public cancellation operation durably requests cancellation; an
opted-in handler that observes it at a checkpoint stops without being retried,
rolls back its handler transaction, and is terminalized as cancelled.

**Required outcomes:**
- Implement `JobService.cancel(job_id, ctx)` with accepted authorization;
  duplicate requests are safe and terminal jobs are no-op.
- Pending jobs become cancelled before execution; claimed/running jobs retain
  status and get `cancel_requested=true`.
- Replace the worker's separate cancel check/status write with an atomic start
  transition conditioned on an eligible status and `cancel_requested=false`.
  A cancellation that wins must prevent handler invocation.
- Define the start-wins path: request is durably recorded; default jobs drain,
  opted-in jobs may stop at checkpoints.
- Replace the worker's separate cancel check/status write with an atomic start
  transition conditioned on an eligible status and `cancel_requested=false`.
  A cancellation that wins must prevent handler invocation.
- Define the start-wins path: request is durably recorded; default jobs drain,
  opted-in jobs may stop at checkpoints.
- Read the committed flag at a checkpoint without blocking the request writer
  or introducing a deadlock.
- Catch the dedicated cancellation signal outside the handler transaction.
- Record job cancellation and owning workflow-step outcome after rollback, in a
  new transaction; ensure redelivery is idempotent.
- Preserve current pre-start cancellation and ordinary running-job drain.
- Ensure cancellation bypasses retry and DLQ paths.

**Constraints:** No rollback claims for external effects; no call to
`on_cancel()`; no force-kill.

**Acceptance evidence:** Integration tests prove handler domain writes before
checkpoint roll back, job and workflow become cancelled, attempts do not
schedule retry/DLQ, and ordinary exceptions still use normal retry behavior.

**Verification:** focused PostgreSQL tests in worker/workflow suites; `uv run
pytest tests/test_engine_hardening.py tests/test_workflow.py -q`.

**Ownership:** `src/keelflow/engine/worker.py`, job service and checkpoint
implementation, worker/workflow/job-service tests.

**Dependencies:** T1 contract.

### T3 — Prove races, retries, and kernel recovery

**Goal:** Demonstrate cancellation remains safe under concurrency and
at-least-once dispatch.

**Required outcomes:**
- Use deterministic barriers to force both cancel/start orderings: cancel
  wins (handler is never invoked); start wins (request is recorded and the
  job follows default-drain or opted-in-checkpoint behavior).
- Test cancel requested before first checkpoint; at first checkpoint; after
  final checkpoint / during handler completion; and concurrent with workflow
  step advancement.
- Test kernel redelivery after cancellation commit does not run handler again
  or advance the workflow twice.
- Test a cancellation request for a non-opted-in job leaves it draining to
  normal completion.
- Document/checkpoint external side-effect limits and idempotency expectations.

**Acceptance evidence:** Deterministic PostgreSQL integration tests; no
sleep-only race proof where barriers/events can control timing.

**Verification:** focused cancellation tests plus existing recovery/e2e test.

**Ownership:** worker/workflow tests and test fixtures.

**Dependencies:** T2.

### T4 — Align specification and operator/developer guidance

**Goal:** Make executable behavior, normative spec, examples, and docs agree.

**Required outcomes:**
- Update `spec/02-job-lifecycle.md` and `spec/03-workflow.md` for opt-in
  checkpoints, exception semantics, transaction rollback, races, and ordinary
  drain default.
- Clarify `on_cancel()` is experimental/not invoked and remove heartbeat claims
  inconsistent with actual Chancy ownership.
- Add a concise example demonstrating checkpoints between safe units and
  warning that external effects are not rolled back.

**Acceptance evidence:** Documentation review against tests and public API;
no contradictory lifecycle claims remain.

**Verification:** `git diff --check`; full required test/lint/type checks.

**Ownership:** `spec/02-job-lifecycle.md`, `spec/03-workflow.md`,
`docs/guide.md`, `src/keelflow/sdk/handler.py`.

**Dependencies:** T1–T3.

## Execution Graph

```text
T1 contract
  ↓
T2 worker behavior
  ↓
T3 race/recovery proof
  ↓
T4 docs and full integration
```

## Verification Strategy

- Focused: cancellation-specific PostgreSQL tests, including transaction
  rollback and no retry/DLQ.
- Integration: deterministic cancel/start race tests prove both orderings and
  that cancel-first never invokes the handler; workflow cancellation outcome,
  redelivery idempotency, existing pre-start cancellation, and running-job
  drain tests.
- Repository checks: `uv run pytest -q`; `uv run ruff check src examples tests`;
  `uv run mypy src/keelflow`; `git diff --check`.
- Environment: PostgreSQL test database and Chancy test queue must be available;
  tests may skip if PostgreSQL is unavailable, so confirm actual DB-backed tests
  ran rather than treating skips as proof.

## Decisions

- 2026-11-20: User approved opt-in mid-run cancellation and an explicit
  checkpoint API; pre-start-only remains default.
- 2026-11-20: User approved an exception as the mechanism to force cooperative
  stop of the current handler logic.
- 2026-11-20: Do not use handler heartbeat for this feature; Chancy owns worker
  liveness. Use handler checkpoints instead.
- 2026-11-20: Keep `on_cancel()` experimental; do not invoke it until its
  lifecycle and transaction semantics are separately designed.
- 2026-11-20: User agreed that a checkpoint can raise an exception to stop the
  running handler. Proposed transaction semantics: that exception rolls back
  the handler DB transaction, then the worker records cancellation in a new
  transaction.
- 2026-11-20: User approved including the declared `JobService.cancel()` API
  in this feature, rather than leaving it as a separate prerequisite.
- 2026-11-20: Public `JobService.cancel()` requires the `job.cancel`
  permission through the existing permission service.

## Validation

- Design-plan validation: code paths and official Temporal/Chancy docs reviewed.
- Implementation validation: not started.

## Approval

Approved 2026-11-20 — implemented per T1–T4: `JobCancelled` +
`ExecutionContext.checkpoint()` (sdk/context.py), `cooperative_cancel`
opt-in (JobTypeSpec), `JobService.cancel` with `job.cancel` permission
(enqueue.py), atomic `_transition_to_running` + `_cancel_terminalize`
(worker.py), `heartbeat()` removed, spec 02/03 + guide updated, 6 new
regression tests.
