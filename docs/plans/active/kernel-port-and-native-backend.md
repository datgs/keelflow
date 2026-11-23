---
status: proposed
last_reviewed: 2026-11-23
superseded_by: []
related_code:
  - src/keelflow/sdk/services.py
  - src/keelflow/postgres/storage.py
  - src/keelflow/engine/enqueue.py
  - src/keelflow/engine/worker.py
  - src/keelflow/engine/bulk.py
  - src/keelflow/engine/workflow.py
  - src/keelflow/engine/scheduler.py
  - src/keelflow/engine/statemachine.py
  - docs/decisions/0001-kernel-chancy.md
---

# Execution Plan: Kernel port and native Postgres backend

Date: 2026-11-23
Path: Standard, architecture-sensitive

## Status

Planning complete; awaiting implementation approval. This plan does not
authorize code changes or a production-default switch.

## Outcome

Make the job-kernel boundary real and testable, then prove it with a second
Postgres backend implemented on keelflow's own durable job table. The existing
Chancy path remains the default and supported path during this work. Both
backends must satisfy the same behavior-level contract tests. Keep the native
backend experimental unless the evidence and a later decision justify support
as a production option.

The goal is not to claim generic portability. It is to expose coupling, clarify
who owns job state, and learn which semantics must be stable before choosing a
secondary database adapter.

## Discovery — verified evidence

- ADR 0001 accepts Chancy 0.26.0 as the kernel and lists its required
  capabilities: claim, retry/backoff, cancel, schedule, rate limit, priority,
  recovery, dedup, transactional enqueue (`docs/decisions/0001-kernel-chancy.md:11-27`).
  It says a future swap would affect "Storage + worker glue" (`:76-83`), but
  this is an architectural direction, not evidence that the seam is complete.
- The existing `Storage` protocol is not a usable adapter boundary yet:
  `PostgresStorage.claim`, `heartbeat`, `finish`, and `reclaim_expired` raise
  `NotImplementedError`; engine execution uses direct SQL (`src/keelflow/sdk/services.py:215-222`,
  `src/keelflow/postgres/storage.py:12-45`).
- Chancy integration is spread across enqueue, worker, bulk, workflow,
  scheduler, and state-machine code. It uses `push_ex`/`push_many_ex`, global
  `_kernel_client`, Chancy worker lifecycle, and Recovery plugin
  (`src/keelflow/engine/enqueue.py:99-104,170-185`,
  `src/keelflow/engine/worker.py:39-55,334-378`,
  `src/keelflow/engine/bulk.py:127-130,252-257`,
  `src/keelflow/engine/workflow.py:561`,
  `src/keelflow/engine/scheduler.py:130-132,170-183`,
  `src/keelflow/engine/statemachine.py:224`).
- Keelflow already stores job lifecycle fields including status, attempts,
  max-attempts, run time, claimant, lease, cancellation, and unique key in
  `keelflow_jobs` (`src/keelflow/postgres/schema.sql:5-39`), while Chancy stores
  a separate kernel row. A native backend could remove that second state store,
  but would make keelflow responsible for claim/lease/retry/recovery semantics.
- PostgreSQL-specific behavior is used beyond the kernel: JSONB, partial unique
  indexes, `FOR UPDATE SKIP LOCKED`, advisory locks, CTE/`unnest`, and
  LISTEN/NOTIFY (`src/keelflow/engine/bulk.py`, `worker.py`, `events.py`,
  `src/keelflow/postgres/schema.sql`). A MySQL adapter would therefore test a
  much broader storage/dialect boundary, not just the kernel port.
- The recent set-insert experiment was reverted: its measured gain was only
  ~1.3s/1M; permanent direct coupling to Chancy tables was judged not worth the
  gain (`docs/plans/completed/bulk-kernel-batch-inserts.md:255-280`). The port
  must call public backend operations, not reproduce Chancy internals.

## Recommendation and alternatives

**Recommended sequence:** (1) define the semantic kernel port and lifecycle
ownership; (2) move the current Chancy integration behind a Chancy adapter with
no behavior change; (3) implement an opt-in native Postgres backend against
`keelflow_jobs`; (4) run the same contract suite against both and compare
correctness, recovery behavior, operations, and performance. Do not switch the
default or promise production support in this scope.

**Do not combine MySQL with this proof.** A MySQL backend would exercise SQL
portability, JSON/index semantics, row locking, claiming, migration, event
notifications, and kernel behavior at once. If the longer-term goal is an
alternate database, make it a separate follow-up after this work identifies
which code is storage-portable. Candidate to assess then: MySQL, if a real
consumer requires it. An alternate Postgres kernel (e.g. PgQueuer) is another
candidate, but ADR 0001 records missing retries, cancellation, priority, and
rate limits; implementing those above it may cost more than the proof is worth.

## Scope and non-goals

In scope:

- A semantic `Kernel` contract for transactional dispatch enqueue, worker
  start/stop, retry scheduling, cancellation/recovery, and notifications needed
  by engine code. Define behavior rather than mirroring Chancy method names.
- A Chancy-backed implementation retaining current default behavior.
- An opt-in native Postgres implementation using `keelflow_jobs` as its single
  queue/lifecycle record, if the contract/design gate passes.
- Shared behavior tests for enqueue atomicity, claim, retries, cancellation,
  schedules, uniqueness, worker crash/reclaim, and workflow/bulk dispatch.
- An operations note and benchmark comparison; no default switch.

Out of scope:

- MySQL or any non-Postgres storage adapter.
- Changing public job/workflow behavior, retry policy, cancellation policy,
  ordering guarantees, or permission rules.
- Removing Chancy or dropping its current backend.
- Production support/SLA for native backend, default selection, or migration of
  existing Chancy installations.
- Refactoring all SQL behind a generic dialect/ORM.

## Contracts to settle before implementation

The first design task must make these ownership questions explicit; do not let
implementation pick them implicitly:

1. **Canonical job state:** Chancy mode currently has engine and kernel rows.
   Native mode should use `keelflow_jobs` alone. Define the mapping from each
   state/attempt/retry/cancel transition to one atomic DB action.
2. **At-least-once dispatch:** document the claim fence and idempotency boundary;
   stale workers must not commit engine results after reclaim.
3. **Retry semantics:** preserve engine `RetryPolicy`, retry timing, terminal
   failure/DLQ behavior, and workflow advancement. No kernel retry may race or
   double-count an engine attempt.
4. **Claim/lease/recovery:** define atomic pending→claimed/running, lease renewal,
   expiry/reclaim, owner identity, and concurrent-worker behavior. Must prove
   both crash recovery and stale-claim fencing.
5. **Cancellation:** preserve pre-start behavior, cooperative checkpoint
   behavior, and kill-previous semantics; do not force-kill handlers.
6. **Scheduling and uniqueness:** preserve run_at and cron enqueue; preserve
   `(type, unique_key)` active-job idempotency and any required claim ordering.
7. **Transactions and notifications:** enqueue must remain in the caller's
   transaction. Postgres notification behavior may differ internally but must
   have a polling fallback and no lost-wakeup correctness dependency.
8. **Capability differences:** rate limiting, worker tags, pause/resume, and
   Chancy plugins are part of the accepted ADR requirements. Either implement
   equivalent native behavior, identify exact gaps and keep native experimental,
   or stop; do not silently call the backends equivalent.

If these contracts imply a change to accepted public behavior or to ADR 0001,
stop for a user decision and update the decision record before implementation.

## Execution tasks

### T1 — Define the semantic port and behavior matrix

- **Goal:** State precisely what both kernel implementations promise and who
  owns each transition.
- **Context:** Current Chancy API use and incomplete `Storage` protocol above;
  ADR 0001 is accepted authority for required capabilities.
- **Required outcomes:** Map each required capability to engine call sites and
  tests; define method inputs, transaction ownership, return/error behavior,
  claim fencing, retry ownership, notifications, and capability gaps. Decide
  whether current SDK `Storage` is extended/replaced or a distinct kernel port
  is appropriate. No production behavior changes in this task.
- **Constraints:** Do not invent FIFO/order guarantees. Do not make native mode
  default. Do not hide missing ADR-required capabilities behind optional flags.
- **Acceptance evidence:** Contract table maps every ADR capability to both
  backend behavior or an explicit native-backend gap; duplicate-state and
  retry ownership are unambiguous; no unresolved policy choice remains.
- **Verification:** Design review against ADR 0001 and current integration tests.
- **Ownership:** `docs/decisions/0001-kernel-chancy.md`, SDK kernel contracts.
- **Dependencies:** None. Stop and request user judgment if ADR change is needed.

### T2 — Put Chancy behind the port without behavior change

- **Goal:** Prove the seam against the production backend before adding a
  second implementation.
- **Required outcomes:** Move scattered Chancy calls and worker lifecycle
  behind one adapter; remove engine dependence on Chancy-specific methods from
  the agreed ownership boundary; retain same transaction behavior, retries,
  notification, recovery plugins, queue configuration, and worker lifecycle.
- **Constraints:** Chancy remains default; no schema migration; no handler API
  change; no direct writes to Chancy tables.
- **Acceptance evidence:** Existing suite and e2e behavior unchanged; adapter
  tests show the port maps to public Chancy API; dependency is only imported
  by the Chancy adapter/bootstrap and any explicitly approved type boundary.
- **Verification:** Focused adapter tests, bulk/workflow/e2e suites, full pytest,
  ruff, mypy, diff check.
- **Ownership:** SDK kernel port, `src/keelflow/engine/` integration points,
  Chancy adapter/bootstrap, tests.
- **Dependencies:** T1.

### T3 — Implement native Postgres kernel as opt-in

- **Goal:** Execute work through `keelflow_jobs` without a parallel Chancy
  kernel row, using the same semantic port.
- **Required outcomes:** Implement claim, lease, heartbeat, reclaim, retry,
  notification/polling, worker lifecycle, and supported capability behavior
  according to T1. Preserve transactional enqueue and stale-claim fencing.
  Backend selection must be explicit and must not alter current default.
- **Constraints:** Postgres only; no table-schema direct writes to Chancy;
  no behavior changes; no production-support claim until capability matrix is
  complete.
- **Acceptance evidence:** Native-mode tests prove one canonical job row,
  atomic enqueue/rollback, concurrent single claim, retry budget/backoff,
  cancel races/checkpoints, killed-worker recovery, stale worker cannot finish
  after reclaim, unique-key dedup under concurrency, schedule behavior, and
  workflow/bulk fan-out/fan-in.
- **Verification:** PostgreSQL integration tests using deterministic barriers
  and worker-kill recovery; tests must fail if Postgres is unavailable rather
  than count skips as proof for these cases.
- **Ownership:** Native kernel, worker lifecycle, engine tests.
- **Dependencies:** T1 and T2.

### T4 — Shared conformance, operations, and comparison

- **Goal:** Prove both backends against one behavior suite and report what
  extensibility has actually been demonstrated.
- **Required outcomes:** Run shared conformance cases against Chancy and native
  Postgres. Measure throughput/latency and recovery time under identical
  workloads; report variance and operational trade-offs, not only speed. Update
  docs/guide and decision records with supported capability matrix and known
  limits. Keep Chancy default pending a separate decision.
- **Constraints:** Do not weaken tests to fit either backend. No MySQL work in
  this task.
- **Acceptance evidence:** Same required contract cases pass for both, or each
  known intentional capability gap is explicitly labeled experimental and not
  described as parity. Bench data includes environment/configuration and
  correctness results.
- **Verification:** Shared contract suite, e2e/recovery tests, full project
  checks, isolated Postgres benchmarks.
- **Ownership:** Shared test harness, docs/decision record, benchmark artifacts.
- **Dependencies:** T2 and T3.

Dependency graph: T1 → T2 → T3 → T4. T4 also depends on T2.

## Validation strategy

- Focused contract tests for each backend, with positive and negative cases.
- PostgreSQL integration tests for transaction atomicity, SKIP LOCKED claims,
  lease expiry/reclaim, fencing, concurrency, retries, cancellation, and
  notifications/poll fallback.
- End-to-end demos exercising workflow, bulk, scheduled work, human steps, and
  failure recovery in both modes.
- `uv run pytest -q`, `uv run ruff check src examples tests`,
  `uv run mypy src/keelflow`, `git diff --check`.
- Same isolated benchmark config for both backends; exact result oracle, run
  metadata, latency/throughput and recovery evidence. No fixed speed target.
- CI must run both conformance configurations before any production parity
  claim. Branch protection remains externally unverified unless checked.

## Risks and recovery

- **Architecture risk — high:** existing ADR chooses Chancy and its feature
  set. Keep native opt-in; revise ADR only after evidence and explicit approval.
- **Lifecycle correctness — high:** retries, cancel, lease, and stale worker
  fencing can corrupt business effects. Deterministic concurrency/recovery
  tests are required; no sleep-only race proof where barriers are possible.
- **Dual state — high:** Chancy mode remains dual-row; native mode must be
  single-source. Do not create a generic adapter that leaves duplicate state
  ownership ambiguous.
- **Scope creep — high:** MySQL combines a second kernel with dialect changes.
  Defer it until the kernel port proof is complete and an adopter/need is known.
- **Operational burden — medium/high:** two implementations double test and
  support matrix. Chancy stays default; native backend may remain experimental
  or be dropped if parity/maintenance cost is poor.
- **Rollback:** backend selection remains explicit; revert the native adapter
  independently. No existing Chancy data migration is planned. Do not claim
  switching existing deployments between backends is supported without a
  separate migration design.

## Recommendation for the next adapter after this plan

MySQL is the strongest test of **database** portability, but it is not the
best first adapter to prove extensibility: it combines SQL dialect, locking,
JSON, indexing, notifications, migrations, and kernel changes, and the current
product is explicitly Postgres-backed. After this plan, first use the evidence
to decide whether a database port is truly a product goal. If it is, choose
MySQL only with a concrete adopter and define a portable storage contract before
coding. If the goal is only to test the kernel seam, a second Postgres kernel is
more diagnostic; PgQueuer is a candidate, but its accepted ADR capability gaps
must be measured against the cost of implementing those features in keelflow.
No secondary adapter is authorized by this plan.

## Progress

- [x] Repository evidence gathered; confirmed current `Storage` is incomplete
      and Chancy coupling is distributed.
- [x] Plan defines staged extraction, Chancy parity, native backend, shared
      conformance, risks, and post-plan adapter options.
- [x] T1 — semantic kernel contract defined (see section below).
- [x] T2 — Chancy adapter extraction: `engine/kernel.py` port
      (`Kernel`, `KernelJobSpec`, `dispatch_spec`, `as_kernel`),
      `ChancyKernel` wrapper; engine call sites converted; 205 tests green.
- [x] T3 — opt-in native Postgres kernel: `engine/native_kernel.py`
      (`NativeKernel` + `NativeWorker`: SKIP LOCKED claim, per-job
      leases, reaper, heartbeat fencing, poll-based wakeup);
      9 conformance tests green in `tests/test_native_kernel.py`.
- [ ] T4 — conformance, operations, and benchmark comparison.

## Decisions and open gates

- 2026-11-23: Planning scope is Postgres-only; MySQL is a possible later
  database-port project, not part of kernel extensibility proof.
- 2026-11-23: Keep Chancy as default throughout this plan; native backend is
  opt-in and may remain experimental.
- Before T1 implementation, review whether the semantic contract can satisfy
  ADR 0001 without changing public behavior. If not, ask the user and update
  `docs/decisions/0001-kernel-chancy.md` first.
- Final implementation approval is still required before starting T1.

## Validation

- Planning: read repository workflow, accepted Chancy ADR, SDK `Storage`
  protocol/implementation, Chancy integration call sites, PostgreSQL-specific
  engine patterns, and the completed direct-kernel-write experiment.
- T2/T3 implemented 2026-11-23: `uv run ruff check src examples tests`,
  `uv run mypy src/keelflow`, `uv run pytest -q` → **214 passed**
  (205 existing incl. all Chancy-path integration tests, +9 native).
- Contract decisions resolved during T3 (recorded here per T1 gate):
  per-job `lease_until` renewal chosen over per-worker heartbeat; claim
  reuses the existing `'claimed'` status; dispatchable-type filter is
  registry-derived (`is_human`/`is_wait` excluded, `bulk.chunk@v1`
  always included); native `push_ex` dedup marks conflicts `cancelled`
  instead of chancy's silent orphan — observable, non-claimable either way.
- Still open for T4: shared-suite parameterization beyond the new
  conformance file, ops docs, benchmark comparison, capability-gap
  review (rate limits, pause, tags, priority, LISTEN wakeup).

## T1 semantic kernel contract (2026-11-23)

### Port shape

`keelflow.engine.kernel.Kernel` — engine-internal Protocol (not the SDK's
aspirational `Storage`, which remains unchanged and is a future boundary):

- `job(func, queue, kwargs, max_attempts, scheduled_at, unique_key=None,
  meta=None, priority=0)` — opaque dispatch-spec factory.
- `push_ex(cursor, job)` / `push_many_ex(cursor, jobs)` — dispatch rows inside
  the caller's transaction; on commit the worker may claim.
- `notify_pushed(cursor, queue)` — wake signal, same cursor.
- `create_worker(*, heartbeat_poll_interval, heartbeat_timeout,
  shutdown_timeout)` — backend-owned claim/invoke loop calling
  `dispatch(job_id, step_run_id)`; engine never claims jobs itself.
- `recover(worker, cursor)` — reclaim abandoned claims (called by the
  backend's maintenance loop; tests may force it).
- `migrate()`, `declare_queue(...)` — backend-owned schema/queue setup.

`configure(registry, dsn, kernel)` accepts the port. For compatibility the
Chancy adapter is created by `ChancyKernel(chancy_client)` and `configure()`
wraps any object exposing `.push_ex` for one release of leniency — tests pass
raw `chancy.Chancy` today.

### Capability mapping (ADR 0001 requirements → port)

| ADR capability | Owner | Chancy mode | Native mode |
|---|---|---|---|
| claim w/ SKIP LOCKED | kernel | chancy fetch_jobs | `UPDATE … FOR UPDATE SKIP LOCKED` on `keelflow_jobs` |
| retry w/ backoff | engine | engine push retry job | engine re-schedules `run_at` on same row |
| max_attempts | engine | engine checks attempts | same |
| cancellation | engine | `cancel_requested` + atomic `_transition_to_running` | same row, same path |
| `run_at` / cron | engine+kernel | `scheduled_at` + scheduler tick pushes | `run_at` predicate in claim SQL + same tick |
| dedup `unique_key` | engine | `keelflow_jobs` partial index | same index |
| recovery/kill-resume | kernel | Recovery plugin | reaper: `claimed`/long-`running` lease expiry → `pending` |
| LISTEN/NOTIFY | kernel | chancy `queue.pushed` | `pg_notify` + poll fallback |
| rate limit / queue pause / tags / metrics plugins | kernel | Chancy | **gap — native declares unsupported; stays experimental** |
| priority | kernel | kernel ORDER BY | claim ORDER BY run_at, id (gap: no priority) |
| transactional enqueue | contract | push_ex on caller cursor | engine-row-only push is the contract |

### Native backend state model (single row)

- `keelflow_jobs` is the only record. No kernel table.
- Claimable = `status='pending' AND run_at<=now()` AND type is dispatchable —
  **not** `is_human`, not `is_wait`, not bulk parent rows mid-fan-in. A
  dedicated claim predicate is required; a plain status filter would steal
  human tasks. Marker: claim only rows whose kernel job would exist today —
  T3 must encode this exactly (machine job types + `bulk.chunk@v1`; human
  and wait jobs are never claimed by workers under Chancy either).
- Claim: `UPDATE … SET status='claimed', claimed_by=w, lease_until=now()+lease`
  returning id → `dispatch`. `_transition_to_running` already accepts
  claimed→running.
- Retry: instead of pushing a new row, `UPDATE run_at` (same row) — removes
  the duplicate-delivery class entirely; kernel claim_id fencing is replaced
  by `claimed_by`+attempt generation fencing.
- Stale claim fencing: claim writes `(claimed_by, lease_until)`; finish writes
  verify ownership or the reclaim already won — T3 must implement the fence,
  not just the happy path.
- Reaper: rows `status IN ('claimed','running')` with `lease_until < now()`
  → `status='pending', claimed_by=NULL` (running rows need a lease too — T3
  must renew during handler execution; Chancy heartbeat covers whole worker,
  native needs a per-job or per-worker heartbeat design decision recorded).
- Notifications: `pg_notify` on enqueue commit + polling fallback
  (`polling_interval`) — lost-wakeup tolerance via run_at polling.

### Explicit open contract decisions (T3 must resolve before code)

1. Native worker lease model: per-job `lease_until` renewal during handler run
   (recommended — row already carries the column) vs per-worker heartbeat.
   Must pick and document.
2. Whether native claim sets `status='claimed'` (new observable state in
   monitoring) or claims a distinct flag. Current status enum already has
   'claimed' unused — recommended reuse.
3. Capability gaps accepted for experimental tag: rate limits, queue pause,
   tags, metrics plugin parity — all currently kernel-owned in Chancy.
