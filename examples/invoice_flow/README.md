# invoice_flow — keelflow demo

Invoice lifecycle exercising **state machine + human-in-the-loop + DAG
workflow + workflow triggers** in one flow:

```
draft ──submit──► in_review ──approve──► posting ──on_succeeded──► posted
   │                │                      │
   │                │ creates_job:         │ triggers post.wf@v1:
   │                │ human.review@v1      │  ledger → record → gl_check(human)
   │                │ (role:finance)       │
   │                │                      ├─ GL reject → posting_rejected
   │                └─ reject → needs_fix  │   → repost resumes run
   └── resubmit ◄───┘                      └─ tech fail → posting_failed
                                              + human.posting_fix@v1 (role:gl)
```

- **Business rejection vs technical failure**: a GL `reject` completes the
  step normally (`decision=reject`) → run terminal outcome `rejected` →
  `on_rejected` transition. A handler exception burns retries → outcome
  `failed` → `on_failed` transition + `failure_job` fix-up task.
- **Single TX everywhere**: handler domain writes (`demo_ledger_postings`),
  job finish, run advance, and the SM callback all commit atomically. A
  failure leaves zero partial writes.
- **Exactly-once callback**: `callback_state` guards the SM transition on
  run completion; `resume_run` reopens the offending step in place (same
  run id, successful steps stay done).

## Screens

| Route | Screen |
|---|---|
| `GET /` | Invoice list + create |
| `GET /invoices/{no}` | State, allowed actions, runs, audit trail, chaos toggle |
| `GET /inbox` | Inbox — claim → approve/reject/re-post |
| `GET /runs/{id}` | Run DAG — step nodes + statuses |
| `GET /jobs/{id}` | Job drawer — payload/result/ctx `via` chain, retry |

## Actors (`?as=`)

| id | role | can | workflow step |
|---|---|---|---|
| `u1` | clerk | create, submit, resubmit invoices | — |
| `fin1` | finance | review/approve (role:finance) | `human.review` task from `submit` |
| `gl1` | GL | GL check + posting fix-ups (role:gl) | `gl_check`, `posting_fix` |
| `mgr` | manager | all permissions | any human step |

Switch via `?as=` in the header. Separation of duties: the submitter
cannot approve their own review; the GL gate belongs to `role:gl`.

UI language: `?lang=` picks `en`/`vi`/`es`/`zh-Hans`/`hi` and persists
via the `kf_lang` cookie (selector in the header; `?as=` is kept).
Identifiers, actor ids, and fixture values stay untranslated.

## Run

```bash
export KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow
uv sync                                # workspace member — uses local keelflow
uv run python -m invoice_flow.seed     # demo schema + keelflow schema + invoices
uv run python -m invoice_flow.worker & # kernel worker (dispatch DAG steps)
uv run uvicorn invoice_flow.app:app --port 8501
```

→ http://localhost:8501 — demo only, no auth, manual refresh.

## The 60-second demo

1. Create an invoice as `u1` → **submit** → `in_review`, a
   `human.review` task lands in `fin1`'s inbox.
2. Switch to `?as=fin1` → `/inbox` → claim → **approve** → invoice
   `posting` and the `post.wf@v1` DAG kicks off: `ledger` → `record` →
   `gl_check` task for `gl1`.
3. Switch to `?as=gl1` → claim → **approve** → invoice `posted`.
   Reject instead → `posting_rejected` (business rejection, no retries)
   → repost resumes the same run. Toggle the chaos flag for the
   technical-failure → `posting_failed` → resume path.

## Test

```bash
uv run pytest examples/invoice_flow
```

## SDK extension points it uses

All wiring lives in `registry.py` — these are the seams the library
exposes and this demo fills in:

| Extension point | Snippet |
|---|---|
| **`JobHandler.run`** — your code, runs in the engine's dispatch TX | `class PostLedgerH(JobHandler): async def run(self, ctx, payload: InvPayload): …` |
| **`ctx.tx`** — write domain rows *in the same commit* as job finish + workflow advance | `await ctx.tx.execute("INSERT INTO demo_ledger_postings … ON CONFLICT …")` |
| **`JobTypeSpec` + `register_job_type`** — declare a job type + its handler | `reg.register_job_type(JobTypeSpec(type="inv.post_ledger@v1", handler=PostLedgerH, …))` |
| **`HumanTaskOptions(assignee_role=…)`** — who may claim a human task | `human_task=HumanTaskOptions(assignee_role="gl")` |
| **`StateMachineDefinition` / `TransitionSpec`** — entity lifecycle as data | `reg.register_state_machine(StateMachineDefinition(name="invoice@v1", … transitions=(…)))` |
| **`creates_job`** — transition spawns a human task | `TransitionSpec("draft","in_review","submit", creates_job="human.review@v1")` |
| **`triggers_workflow` + `WorkflowTriggerSpec`** — SM transition kicks a DAG, outcome maps back | `triggers_workflow=WorkflowTriggerSpec(workflow="post.wf@v1", on_succeeded="posted", on_rejected=…, failure_job=…)` |
| **`WorkflowDefinition` / `StepSpec` + `needs`/`input` refs** — a DAG of steps | `StepSpec(id="gl_check", type="human.gl_check@v1", input={"journal_ref": "${{ steps.record.outputs.journal_ref }}"}, needs=("record",))` |
| **`human.*` step type** — step that blocks until a person acts in the Inbox | `StepSpec(id="gl_check", type="human.gl_check@v1", …)` |
| **`roles` argument** — pass trusted actor roles to task claim/completion | `roles=roles_for(request)` — the engine checks the assigned role; do not pass user-controlled role claims |
| **App-side services** — drive everything from your API in the caller TX | `EngineStateMachineService`, `WorkflowService`, `HumanTaskService`, `JobService` in `app.py` |
