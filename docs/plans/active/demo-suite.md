---
status: in_progress
last_reviewed: 2026-11-19
related_code: [examples/invoice_flow, examples/supplier_onboarding]
---

## Progress

- [x] S0 Chancy issue review — `.spikes/chancy-issues-2026-11-19.md`;
      pinned `chancy==0.26.x`, supervisor-restart requirement documented.
- [x] S0a CI — `.github/workflows/ci.yml` (ruff + mypy + pytest on PG16).
- [x] Engine prerequisite contract — `50055a0` (triggers, resume_run,
      single-TX dispatch, ctx.tx, complete_and_transition).
- [x] D1 invoice_flow — `8e9fb8d`.
- [x] D2 supplier_onboarding — `59cd673`. Engine fixes uncovered:
      advance_run lost-wakeup (run row now FOR UPDATE), guard-skip
      livelock (post-loop terminal re-check), get_run needs dict_row +
      returns `needs`/`terminal_outcome`, human.claim dict_row.
- [ ] D3 bulk_import — next.
- [ ] Public readiness: security pass done (`90258f1`); remaining =
      tag `0.1.0a1` + PyPI release.

# Demo suite plan: invoice_flow + supplier_onboarding + bulk_import

## Outcome

This plan covers the demo suite, test code, and automation test coverage.
Also track the Chancy known-issues review here with an explicit phase and
scope decision; it must not become an untracked follow-up.

A staged demo suite with three distinct examples:

1. `invoice_flow`: focused state machine + HITL + DAG completion callback.
2. `supplier_onboarding`: realistic ~20-activity conditional DAG with
   parallel checks, joins, human gates, durable waits, and recovery paths.
3. `bulk_import`: upload → mapping HITL → row/chunk validation → approval
   HITL → chunk processing → downloadable error rows.

Build in that order. Supplier onboarding proves DAG composition at scale,
not a 20-step linear chain. Bulk import stays separate because it proves
row/chunk processing and error repair — a different capability story.

## Cross-cutting work: automated test and Chancy issue review

### Automated test code — required for every demo

Do not treat tests as a final polish step. Each feature task includes its
automated tests in the same change. The test layers are:

- **Unit tests:** workflow graph/guard behavior, role/decision mapping,
  state transitions, validation, idempotency keys, mock adapter outcomes.
- **Integration tests:** real Postgres + worker for transaction boundaries,
  callback/resume, parallel fan-out/join, human task claim/complete,
  retries and kill/restart recovery.
- **API/UI automation:** FastAPI test client for routes/forms, permissions,
  visible errors and state display; browser automation for one full
  user-visible happy path and key rejection/recovery paths where needed.
- **Regression automation:** every fixed engine/kernel-related defect gets
  a regression test that fails before the fix and passes after it.

Required checks per demo: `uv sync --dev`, focused tests, full pytest,
ruff, mypy. **CI is currently absent** (no `.github/workflows/` files).
Add GitHub Actions CI with Postgres and run these checks before demo work
is called complete. Include API/UI automation plus browser automation for
each demo's primary path; state exactly which browser paths are covered.
Do not claim UI automation from API tests alone.

### Chancy known-issues / bug-list review — required before implementation

**Do a bounded review before the first demo implementation**, not after
MVP. The engine's correctness depends on kernel behavior for claims,
retries, recovery and transactional enqueue; a known upstream defect could
change our adapter contract or invalidate the demo. This is a targeted risk
review, not a mandate to mirror or patch every Chancy issue.

1. Capture the review date and exact Chancy version/commit in the plan.
2. Review the upstream issue/bug list and release notes for relevant areas:
   transactional push/enqueue, claim/locking, retries/backoff, worker
   heartbeat/recovery, cancellation, scheduling, migrations and supported
   psycopg/Postgres versions.
3. For each candidate, classify: `not applicable`, `already fixed upstream`,
   `reproduced in our pinned version`, or `unverified`; record evidence and
   impact on KeelFlow.
4. Reproduce only plausible high-impact issues against KeelFlow's pinned
   version. If confirmed, add a regression test and either apply a narrow
   KeelFlow adapter workaround or pin/upgrade Chancy. Do not fork or patch
   Chancy internals unless explicitly approved.
5. Re-run the relevant integration/recovery tests after any version or
   adapter change. Track unrelated/upstream-only issues as deferred with
   revisit trigger; do not expand into a full upstream audit.

**Phase order:** Chancy targeted review + automation/CI readiness first;
then engine prerequisites and invoice_flow; then supplier_onboarding;
then bulk_import. Revisit Chancy issues when upgrading the dependency or
when a new reproduction points to kernel behavior.

## Demo 1: invoice_flow

A runnable FastAPI demo (`examples/invoice_flow/`) that lets a person
feel the state machine + HITL + DAG completion callback in one invoice
lifecycle. Submit → review inbox → posting workflow → GL human gate →
posted only after successful completion. Every mutation and domain write
has a defined transaction boundary and audit trail.

Demo actors: `u1` (submitter, owner + `inv.submit`), `fin1`
(review role `finance`, `inv.approve`), `gl1` (GL role `gl`, `inv.gl_check`),
`mgr` (finance + GL + manager roles). Keep roles distinct to prove
separation of duties: the first reviewer cannot approve their own GL
posting unless explicitly configured to do so.

## Screens

| Route | Screen | Engine surface exercised |
|---|---|---|
| `GET /` | Invoice list: state badge, allowed actions, linked run/status | `sm.current`, `sm.allowed_actions`, `monitoring` APIs (no direct engine-table reads) |
| `POST /invoices` | Create draft invoice | domain insert + initial state in one TX; audit |
| `GET /invoices/{id}` | Detail: state, actions, audit timeline, posting rows, linked workflow | `sm.current`, `audit.trace`, `runs_get` |
| `POST /invoices/{id}/act/{action}` | submit / approve / resubmit / repost / retry_post | `sm.transition`/resume in TX; invalid action/permission errors rendered, never 500 |
| `GET /inbox` | Reviewer inbox: open tasks for user/roles; task card labels review vs GL gate | `human.my_tasks` |
| `POST /tasks/{id}/claim` | Claim a task | `human.claim` — surface first-claim-wins conflict visibly |
| `POST /tasks/{id}/complete` | Complete review or GL check with decision + note | distinct service paths: review task applies approve/reject to SM atomically; GL step task completes its `step_run` and advances DAG atomically |
| `GET /runs/{id}` | Workflow run: step DAG, per-step status/outputs; reload page to refresh | `monitoring.runs_get` (no polling) |
| `GET /jobs/{id}` | Job detail: payload, context actor + `via`, errors | `monitoring.job_get` |

Optional stretch: `POST /inbox/escalate` — `escalate_overdue` demo
(reassign stale tasks to `role:manager`).

## Components

```
examples/invoice_flow/
├── pyproject.toml          # workspace member; dep: keelflow (path)
├── README.md
└── src/invoice_flow/
    ├── registry.py         # job types, post.wf@v1, invoice@v1 SM
    ├── seed.py             # schema + demo_invoices + 3 sample rows
    ├── app.py              # FastAPI routes above
    ├── worker.py           # kernel worker entry (dispatch DAG steps)
    └── templates/          # Jinja server-rendered; manual refresh in v1
```

Domain tables (owned only by the demo):

- `demo_invoices(id, number, amount, created_by)`
- `demo_invoice_ledger(id, invoice_id, account, amount, created_at)`
- `demo_invoice_postings(id, invoice_id, workflow_run_id, posted_by, payload, created_at)`

The app uses keelflow service APIs for engine reads/writes; it does not
query or mutate `keelflow_*` tables directly. Domain writes performed by
handlers use the transaction supplied by the worker execution contract.

## Current engine gaps / constraints to resolve first

This plan adds an engine capability, so treat the following as prerequisite
contracts—not assumptions that the current implementation already meets:

1. **Handler transaction contract.** `JobHandler.run(ctx, payload)` has no
   transaction argument (`src/keelflow/sdk/handler.py:19-21`). Domain
   handlers therefore cannot currently write invoice ledger/posting rows
   in the worker's transaction. Define a public execution context or
   explicit transaction parameter, then pass it to handlers without
   exposing psycopg-specific types in the SDK. Specify rollback/retry
   behavior: handler domain writes and job/step completion must commit or
   roll back together.
2. **One transaction owner.** `on_job_finished` currently commits internally
   (`src/keelflow/engine/workflow.py:240`), and human completion calls it
   after its own transaction (`src/keelflow/engine/human.py:77-104`). Refactor
   to caller-owned transaction composition before adding callbacks; no
   service called inside a route/worker may commit the caller's transaction.
3. **Human SM task linkage.** A transition-created human task is not a
   workflow step; current `HumanTaskService.complete` only advances a run
   when it finds a `step_run` (`src/keelflow/engine/human.py:95-104`). Add
   explicit action metadata to the task and a transactional completion
   operation that validates claimant + requested decision, completes the
   task, applies the corresponding SM transition, writes audit, and creates
   follow-up workflow/task atomically. Workflow `human.gl_check@v1` remains
   a step-run task and must only complete that step.
4. **Concurrent first transition.** `transition` locks an existing state
   row (`src/keelflow/engine/statemachine.py:113-120`), but cannot lock a
   missing row. Use a per-entity advisory/portable lock or insert-on-conflict
   initialization followed by `SELECT … FOR UPDATE`; prove two concurrent
   initial actions cannot both succeed.
5. **Authorization consistency.** `allowed_actions` currently constructs a
   context without the caller's permissions (`statemachine.py:100-108`),
   while `perm:own` is host-defined scope rather than a built-in transition
   check. Define the demo's authorization adapter: role/permission mapping,
   owner check against `created_by`, and use the same policy for rendering
   actions and enforcing POST actions. UI filtering is never the security
   boundary. Enforce separation of duties: `fin1` handles review, `gl1`
   handles GL approval; test that `fin1` cannot complete the GL gate.
6. **Run terminal/failure semantics.** Specify when retries are exhausted,
   how human GL rejection becomes a failed/declined terminal outcome, and
   how a callback failure is retried without repeating state/audit/task
   side effects. Persist callback state and entity/actor context; use a run
   row lock and a single transaction for terminal status + callback + audit.
7. **Resume semantics.** Existing `resume_step` resets a failed step and
   commits internally (`src/keelflow/engine/workflow.py:112-144`); it is not
   yet a `resume_run` contract. Decide whether resume reuses the failed
   step's job row or creates a new attempt linked to the same step_run;
   preserve successful siblings, make duplicate requests idempotent, and
   atomically return entity `posting_failed → posting` with the resume.
8. **Idempotent domain effects.** Worker retry can replay a handler after
   uncertain commit. Give ledger/posting writes stable idempotency keys
   (invoice + workflow run/step) with unique constraints; assert no duplicate
   posting on retry/recovery.
9. **Audit coverage.** Define required events for invoice create/edit,
   submit, review decision, resubmit, posting start/success/failure/resume,
   GL decision, ledger/posting writes. Each must share the corresponding
   transaction and retain actor `via` lineage; background callbacks use a
   system/job actor linked to the initiating reviewer.
10. **Public API and migration compatibility.** This is a pre-1.0 library,
    but schema/bootstrap and Alembic must agree. Include migration upgrade,
    downgrade, clean-install, and existing-schema upgrade tests. Avoid
    adding an arbitrary callback framework: implement the smallest typed
    workflow-trigger contract needed by this use case.

## Registry wiring

```text
JobTypes:   post.ledger@v1, post.record@v1 (idempotent domain handlers)
            human.review@v1 (is_human=True, decision form)
            human.gl_check@v1 (is_human=True, assignee role `gl`, GL decision form)
            human.posting_fix@v1 (is_human=True, assignee role `finance`, optional failure task)
Workflow:   post.wf@v1 → p1 (post.ledger@v1) → p2 (post.record@v1)
                         → p3 (human.gl_check@v1, role `gl`, needs p2)
StateMachine: invoice@v1
    draft ──submit──► in_review        guard perm inv.submit
                                       creates_job human.review@v1 (role:finance)
    in_review ──approve──► posting     guard perm inv.approve
                                       triggers_workflow { post.wf@v1,
                                         on_done: 'complete',
                                         on_failed: 'posting_failed' }
    in_review ──reject──► needs_fix    guard perm inv.approve
    needs_fix ──resubmit──► in_review  guard perm inv.submit (perm:own)
                                       creates_job human.review@v1 (role:finance)
    posting ──complete──► posted       engine-internal (workflow hook)
    posting ──review_rejected──► posting_rejected
                                       engine-internal (business outcome)
    posting ──posting_failed──► posting_failed
                                       engine-internal (technical failure)
    posting_rejected ──repost──► posting
                                       guard perm inv.approve
                                       resume same run at GL gate
    posting_failed ──retry_post──► posting
                                       guard perm inv.approve
                                       resume failed step (not re-trigger)
```

**Workflow completion hook (new engine feature; implement before demo).**
Current `TransitionSpec.triggers_workflow` is only a workflow-name string
and `on_job_finished` closes the run without a terminal callback. Add a
small typed `WorkflowTriggerSpec` (avoid a generic callback framework):

```text
workflow: post.wf@v1
on_done: complete
on_rejected: review_rejected       # business rejection, not a retryable failure
on_failed: posting_failed          # technical failure after retry exhaustion
failure_job: human.posting_fix@v1 # optional; assignee role from config
```

The triggering transition already identifies the entity and initiating
actor. Persist `triggered_by_entity`, callback actions, and serialized
actor/context on the workflow run in the same TX that creates the run.
The trigger contract is part of the versioned definition and validated
at load: workflow exists; callback actions are valid transitions from
the configured source state; failure job type exists and is human if it
is an inbox task. Do not accept arbitrary callable names or raw dicts.

Terminal semantics:

- The callback runs exactly once in the transaction that marks the run
  terminal. It applies the configured internal SM transition and audit
  record atomically with run status. Callback failure rolls back the
  terminal update so it can retry; serialize with a row lock on the run
  and persist callback completion in that same transaction.
- Callback uses the same database connection/transaction as run
  finalization; it must not open a second connection. `on_job_finished`
  currently commits internally, so refactor it before adding the callback:
  transaction ownership must be explicit and there must be one commit
  boundary, not nested commits.
- `posting → posted` only on successful run completion (after GL human
  step). `posting → posting_rejected` on explicit GL business rejection;
  it must not increment technical retry/DLQ counters. `posting →
  posting_failed` only after technical retries exhaust; this creates a
  finance fix-up task.
- `posting_failed → posting` resumes the same failed run/step, not a new
  workflow. Repeated resume requests must be idempotent.
- The callback transition is an engine-authorized transition: do not
  bypass transition validation/audit, but do not pretend it is an HTTP
  actor action. Record a system/job actor with `via` pointing to the
  initiating approver. Define a dedicated internal transition path that
  still checks `(from_state, action)` but does not require the human
  approver's permission to be re-evaluated as if they clicked again.
- Separate **business rejection** from **technical failure**. A GL
  reviewer choosing reject completes the human step normally with a
  structured decision; it must not raise or consume retry attempts. Persist
  run outcome (`succeeded | rejected | failed`) separately from execution
  status if needed. Fire `on_rejected` for the business result and
  `on_failed` only after technical retries exhaust. Rejection sets
  `posting_rejected`; a later `repost` resumes the same run at the GL gate
  after correction.

`post.wf@v1` (3 steps):

1. `post.ledger@v1` — write ledger entries to `demo_invoice_ledger`.
2. `post.record@v1` — write `demo_invoice_postings` row.
3. `human.gl_check@v1` — finance HITL inside the DAG; approval result
   completes step and unblocks the run. A rejection must fail/route to
   the configured failure path, not mark the run successful.

**Human-task linkage must be explicit.** The SM-created review task
carries invoice entity reference + originating action. Completing it
with `approve`/`reject` atomically completes the task and invokes the
corresponding allowed SM action. The `needs_fix → in_review` resubmission
creates a fresh review task. The workflow GL task is linked to its
step_run and completion advances the DAG (not the outer review SM action).

## Engine prerequisite contract

Before demo code, settle and test these engine API/data changes:

- Add `WorkflowTriggerSpec` to SDK and `TransitionSpec`; preserve plain
  `triggers_workflow: str` only if existing callers require it (otherwise
  change the contract now before a public release).
- Add run columns for trigger entity type/id, callback actions
  (`on_done`/`on_rejected`/`on_failed`), initiating context, terminal
  outcome, and callback completion/attempt state; include migration +
  schema bootstrap + downgrade.
- Refactor workflow completion so one caller-owned TX finalizes run,
  performs the configured state transition, writes audit, and inserts an
  optional human fix-up job. No hidden commit inside the callback path.
- Add `resume_run(run_id, from_step, ctx, tx)` for failed or rejected
  runs: preserve successful steps and run ID, create a distinct attempt
  record/job linked to the same step_run, and resume from the failed step
  or rejected human gate. In the same TX transition the entity from
  `posting_failed`/`posting_rejected` back to `posting`. Duplicate resume
  calls must be idempotent. Do not overload kernel retry attempts with
  human resume.
- Make workflow step handlers receive an explicit execution transaction
  if they write domain tables. The app must not open a separate
  transaction for `demo_invoice_ledger`/`demo_invoice_postings` writes.
  The tx contract composes with the host app's DB transaction on request-
  side SM + audit + task creation. Async workers own a distinct transaction
  after request commit; it must cover handler domain writes + job/step
  completion atomically.
- Locking: serialize state transitions per `(entity_type, entity_id)`;
  tests must cover two concurrent first transitions when no state row
  exists (the current `SELECT … FOR UPDATE` cannot lock a missing row).
- Add engine tests for trigger validation, run completion/failure,
  callback rollback/retry, callback exactly-once, audit linkage, nested
  human step completion, idempotent handler replay, entity locking,
  permission/ownership enforcement, and resume-same-run before building
  the demo.

## Implementation steps

0. **Engine prerequisite:** implement and pass the contract/tests above.
   Do not start UI wiring until callback and TX semantics are verified.
1. Workspace member + deps (`fastapi`, `uvicorn`, `jinja2`, multipart,
   test client dependency if needed); confirm workspace resolution with
   `uv sync --dev`.
2. `registry.py` — handlers, SM, workflow, schemas; validate all
   definitions at load time.
3. `seed.py` — idempotently apply engine schema + create demo tables +
   seed invoices and initial states; safe to run repeatedly.
4. Worker entry point — use distinct `keelflow_kernel_` prefix; ensure
   handlers receive the worker execution transaction for domain writes.
5. Routes and service orchestration: create/list/detail, submit/approve/
   reject/resubmit, inbox/claim/complete, run/job detail. Keep each
   state-changing route in one caller-owned transaction; no direct
   writes to engine tables.
6. Templates — shared shell, actor switcher, visible errors, manual
   refresh buttons; no polling/htmx in v1.
7. Integration test full path: submit → review claim/approve → posting
   workflow + GL task → GL complete → posted; second path review reject →
   fix → resubmit; third path GL reject → `posting_rejected` → correction →
   repost same run; technical failure → `posting_failed` → retry same run.
   Include transaction rollback, duplicate callback/resume, handler replay,
   wrong owner/role, and competing claims.
8. `README.md` runbook + 60-second demo script; update acceptance evidence.
9. Final plan gate: verify every UI route maps to a service contract,
   every domain write has an idempotency key + transaction boundary, and
   success/reject/technical-failure/resume paths all have integration tests.

## Acceptance

- End-to-end path executable from a clean local DB using documented
  commands; no manual psql setup.
- Invoice is `posting` while the DAG/GL task is active and becomes
  `posted` only after the GL task completes.
- Review rejection produces `needs_fix`; submitter can edit/resubmit,
  creating a new review task without corrupting the previous audit trail.
- Technical posting failure becomes `posting_failed`; resume continues
  the same run from its failed step. GL business rejection becomes
  `posting_rejected` without technical retry; `repost` resumes the same
  run at the GL gate. Successful completion alone sets `posted`.
- Review and GL decisions enforce separation of duties and assignee
  authorization server-side; changing the demo actor query parameter alone
  must not bypass task claimant checks.
- Callback is once-only under retry/replay; callback + run terminal
  status + audit are transactionally consistent.
- Domain ledger/posting rows are written by actual handlers using the
  worker's transaction and linked to the invoice; no fake `posted=true`
  output stands in for persistence.
- Invalid action/permission and claim race show readable UI errors,
  not 500s. Competing claim test proves only one claimant succeeds.
- Routes never write `keelflow_*` tables directly; caller TX is reused
  for app writes + engine service calls. Worker handlers use the worker's
  single TX for domain writes + engine completion (not the completed HTTP
  request session).
- `uv sync --dev`, clean-install + schema migration upgrade/downgrade,
  full pytest suite, ruff, and mypy pass.
- Runbook includes start/stop/cleanup commands, local-only bind,
  actor-switching caveat, and a reset command that truncates only demo-
  owned tables (never broad `keelflow_*` cleanup in a shared DB).
- `docs/plans/active/acceptance-evidence.md` updated to link the demo
  for items 1, 2, 5, 7.

## UI decisions (confirmed)

- Real ledger and posting tables, written by workflow handlers
- `reject → needs_fix → resubmit` loop included
- Posting workflow has nested GL human approval; final SM state waits for it
- GL business rejection is distinct from technical failure/retries
- Manual refresh buttons (no htmx polling in v1)
- Separation of duties: `fin1` reviews, `gl1` approves GL; no self-approval.
- Demo actor switcher only; no production authentication. Bind to localhost
  by default and display a persistent "demo only / no auth" notice.
- Seed every invoice's initial state explicitly, so concurrent first
  transitions cannot race on a missing state row; separately keep an
  engine-level test for safe first-transition behavior.

## Demo 2: supplier_onboarding (generic ~20-activity workflow)

### Why this scenario

Supplier onboarding is a common cross-industry process with realistic
parallel checks, conditional reviews, external waits, human approvals,
retries, and compensation. It complements `invoice_flow`: that demo
focuses on SM + nested HITL + terminal callback; this one proves DAG
composition and operational visibility at a larger scale.

The "20" is an approximate activity count, **not 20 mandatory sequential
steps**. The value is in a readable graph with parallel branches, joins,
conditionals, human gates, and recoverable waits. Avoid a long linear
workflow that only proves the engine can store 20 rows.

### Example workflow (~20 activities)

1. Receive onboarding request.
2. Check required documents are present.
3. Normalize supplier identity/address.
4. Detect possible duplicate supplier.
5. Verify tax identifier (mock adapter).
6. Verify bank details (mock adapter; never use real data).
7. Screen against sanctions/watchlists (mock adapter only).
8. Aggregate check results and calculate risk tier.
9. Request missing documents — HITL; wait/resume, do not create a DAG back-edge.
10. Security review if supplier needs system access — conditional HITL.
11. Business sponsor approval — HITL.
12. Procurement approval — HITL.
13. Finance approval above configured risk/value threshold — conditional HITL.
14. Legal review for high-risk/contract-required suppliers — conditional HITL.
15. Select category, currency, and payment terms.
16. Create vendor record in the demo domain database (idempotent).
17. Verify created vendor record.
18. Generate onboarding pack from fake/sample data.
19. Wait for supplier acknowledgement — external-wait simulation / HITL.
20. Activate vendor — final state transition after prerequisites complete.

Activities 4–7 run in parallel after normalization, then join at risk
aggregation. Steps 10, 13, and 14 are conditional. Missing-document
remediation is a durable wait and resume/new workflow attempt, not a
cycle in the DAG. Model vendor lifecycle separately as an SM
(`requested → checking → awaiting_approvals → onboarding → active`, with
`needs_info`, `rejected`, and `failed` paths).

All verification integrations are deterministic local mocks. Use only
synthetic names, tax IDs, and bank details; do not call real sanctions,
bank, email, or government services.

### Screens

| Screen | Purpose / engine behavior demonstrated |
|---|---|
| Request list | Supplier state, risk tier, current workflow/run status |
| New request | Submit synthetic supplier profile + fake document set |
| Supplier detail | Timeline, checks, approvals, current state and allowed actions |
| Check board | Parallel tax/bank/duplicate/watchlist mock results + join/risk summary |
| Inbox | Sponsor/procurement/finance/legal/security tasks, assignee and claim status |
| Approval detail | Decision + comment; role separation and first-claim-wins |
| Workflow view | ~20 activity DAG with branches, joins, skipped conditionals, human waits, retry state; display multiple active nodes together |
| Missing-info page | Requester uploads/fixes synthetic docs; resumes without a cyclic DAG |
| Vendor record | Final domain data, activation status, idempotency evidence |
| Operations view | Failed/retried jobs, run history, audit trail, manual refresh |

### Task sequence

| Task | Goal / output | Depends on | Proof |
|---|---|---|---|
| S0 — Chancy targeted review | Version-stamped issue triage for relevant kernel behavior; fixes/workarounds only for confirmed material issues | None | Review record with source evidence; regression test for each adopted fix; pinned version stated |
| S0a — test automation readiness | Add GitHub Actions CI (Postgres service + pytest/ruff/mypy) and test dependencies; define repeatable local commands | None | CI run green on branch; integration tests run against service DB |
| S1 — scenario contract | Reviewable YAML workflow graph: node IDs, dependencies, guards, schemas, output refs, roles, entity states, failure/recovery paths | Invoice engine prerequisites + demo shell | Loader accepts definition; graph review finds fan-out/join and no cycles |
| S2 — fake adapters + domain model | Synthetic fixtures, deterministic tax/bank/watchlist checks, vendor tables + idempotency constraints | S1 | No network calls; replay creates one vendor record |
| S3 — workflow + SM | Parallel checks, risk join, conditional reviews, vendor creation, acknowledgement wait, activation | S1, S2 | Integration tests cover success, branches, rejection, missing-info resume |
| S4 — UI/inbox | Request list/detail, checks, role inbox, missing-info form, run graph, vendor record | S3 | Browser demo shows parallel checks and HITL gates |
| S5 — recovery/evidence | Worker restart, retries, audit, runbook, acceptance evidence | S3, S4 | Restart/replay tests; no duplicate vendor activation |

### Plan / acceptance

- Build only after `invoice_flow` proves the shared app shell and the
  engine prerequisites in that plan are complete.
- Define exact node IDs, dependencies, guards, input/output schemas,
  human-task assignments, state transitions, and failure/recovery paths
  before implementation; keep the graph reviewable as YAML.
- Activity count is approximate; design must include at least one
  parallel fan-out/join, two conditionals, three human gates, one durable
  wait, one technical retry, and one business rejection.
- Success path reaches `active`; each conditional approval is exercised
  by fixtures that take both the true and false branch.
- Parallel checks demonstrably overlap, join only after all required
  outcomes, and safely retry without duplicate vendor records.
- Missing-document and supplier-acknowledgement waits survive worker
  restart and resume after human/external completion.
- Human approval is role-authorized; actor switcher is explicitly
  local-demo-only, bound to localhost, with persistent no-auth warning.
- Every external integration is a deterministic local mock with no
  network access; all supplier data is synthetic.
- Technical failure, business rejection, cancellation, and compensation
  are distinct outcomes with visible recovery instructions.
- Instance workflow UI combines the stored definition with persisted run
  state; it does not assume one current node. Show all active/running/
  blocked nodes, completed/skipped/failed nodes, edges, and branch results.
- Entire demo runs with local Postgres and deterministic mock adapters;
  tests cover clean seed, full success, conditional branches, rejection,
  missing-info resume, worker recovery, and idempotency.
- Automated browser test opens a specific supplier instance and verifies
  the graph shows parallel active nodes and its current HITL wait.
### Demo 3: bulk_import (separate scope, scheduled after demos 1–2)

Upload → HITL column mapping → per-row/chunk validation → validation
report → HITL import approval (proceed/fix/abort) → chunk processing →
downloadable CSV error rows. It focuses on bulk semantics and is
intentionally not folded into supplier onboarding.

Its future implementation scope is explicitly tracked here, not left as
an unplanned idea. Define mapping persistence, validation idempotency,
row repair/retry, file size/content-type limits, storage outside public
paths, safe cleanup, automated API/browser coverage, and CI evidence.
Use synthetic files only. Start after invoice_flow and supplier_onboarding
acceptance is complete; before then, do not change bulk product code for
the demo.
