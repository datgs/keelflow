# supplier_onboarding — keelflow demo 2

A ~13-step supplier onboarding DAG showing the parts `invoice_flow`
doesn't: **parallel check fan-out**, **conditional human review**
(`if:` guards on DAG steps → `blocked` vs `skipped`), **mock external
adapters** (deterministic, synthetic data only — no real integrations).

## What it demonstrates

| Feature | Where |
|---|---|
| 3 parallel machine steps + 1 external wait | `sanction`, `media`, `credit` are adapters; `bank` is `wait.bank_verify@v1` — parks the run, no worker |
| External-wait step (app-owned callback) | UI sends request → mock provider → `/integrations/bank/callback` → `jobs.signal(job_id, …)`; result row + resolution + fan-in commit in one TX |
| Fan-in step joining parallel results | `risk_score` reads all `demo_check_results` |
| Conditional HITL via `if:` guards | `comp_review` (risk≠low), `credit_signoff` (risk=high), `bank_conf` (risk=low) — others become `skipped` |
| Skipped steps satisfy `needs` | `erp` needs `comp_review`+`credit_signoff`; provision needs `bank_conf` — skip or block both work |
| Technical failure + `resume_run` | `fail_sanction` chaos flag → `sanction` fails → run `failed`; toggle off → resume |
| Business rejection | complete a human step with `decision=reject` → `terminal_outcome='rejected'` |
| Idempotent domain writes | `demo_check_results` upsert, `vendor_no` natural key |
| DAG UI | `/runs/<id>` renders dependency layers (parallel lanes), human nodes dashed, skipped greyed |

## The workflow — `sup.onboard@v1`

```mermaid
flowchart TD
    validate --> sanction
    validate --> media
    validate --> credit
    validate --> bank[[bank · wait.bank_verify]]
    bank -.->|provider callback → app → signal| risk_score
    sanction & media & credit --> risk_score
    risk_score -->|risk != low| comp_review[[comp_review · human]]
    risk_score -->|risk == high| credit_signoff[[credit_signoff · human]]
    risk_score -->|risk == low| bank_conf[[bank_conf · human]]
    comp_review & credit_signoff --> erp
    erp --> terms
    erp & bank_conf --> provision
    terms & provision --> activate
```

Four branches fan out after `validate` and join at `risk_score` — but
`bank` is an **external wait**: the step parks `blocked` with no worker
while the app sends a request to the mock bank provider. On the supplier
page, **send bank verification request** registers the app-owned
`demo_bank_requests` correlation, then **simulate provider callback**
posts to `/integrations/bank/callback` — the app route verifies the ref,
writes `demo_check_results.bank_verify`, and calls `jobs.signal` in one
transaction, which unblocks `risk_score`. The provider never touches
keelflow directly.

The risk tier picks the human gates (`if:` guards — non-selected ones show
as `skipped` but still satisfy `needs`). `erp`/`terms`/`provision` then
provision the supplier; `activate` flips it to `active`.

## Layout

```
src/supplier_onboarding/
  registry.py   job types, handlers (mock adapters), workflow def
  schema.sql    demo_suppliers, demo_vendor_accounts, demo_check_results
  seed.py       synthetic supplier data
  worker.py     worker process (claims machine jobs)
  app.py        FastAPI UI (suppliers / inbox / run DAG view)
  templates/    Jinja (no live updates — refresh manually)
tests/test_supplier.py  2 integration tests (needs Postgres)
```

## Run it (3 processes)

```bash
export KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow

# once — engine + demo schema + synthetic suppliers
uv run --package keelflow-demo-supplier python -m supplier_onboarding.seed

# worker — claims machine jobs
uv run --package keelflow-demo-supplier python -m supplier_onboarding.worker

# UI — http://127.0.0.1:8001/?as=u-proc1
uv run --package keelflow-demo-supplier \
    uvicorn supplier_onboarding.app:app --port 8001
```

## Actors (`?as=`)

| id | role | can | workflow step |
|---|---|---|---|
| `u-proc1` | procurement | start onboarding runs | — |
| `u-comp1` | compliance | `comp_review` tasks | `comp_review` |
| `u-fin1` | finance | `credit_signoff` + `bank_conf` | `credit_signoff`, `bank_conf` |
| `u-procadm` | provisioning | — | — |
| `u-mgr` | manager | all permissions | any human step |

UI language: `?lang=` picks `en`/`vi`/`es`/`zh-Hans`/`hi` and persists
via the `kf_lang` cookie (selector in the header; `?as=` is kept).
Identifiers, actor ids, and fixture values stay untranslated.

## Try the paths

1. **Happy path** — start onboarding on any supplier → watch the DAG page
   (`/runs/<id>`): 3 checks light up in parallel while `bank` stays
   `blocked` (no worker) → on the supplier page, **send bank
   verification request** then **simulate provider callback** →
   `bank` resolves → `risk_score` → exactly the human steps for that
   risk level `blocked`, the rest `skipped` → complete them in the
   inbox → `erp`/`terms`/`provision`/`activate` converge → supplier
   `active` with a vendor account.
2. **Technical failure** — toggle `fail_sanction` on the supplier page →
   start a run → `sanction` retries then fails → run `failed` → toggle
   off → `resume run` → completes in place.
3. **Business rejection** — complete a human step with `reject` → run
   `done` with `terminal_outcome=rejected` (not a technical failure —
   no retries burned).

## Chaos knobs

- `fail_sanction` flag on each supplier — the sanction adapter raises a
  simulated API timeout.

## Demo constraints

Synthetic data only · no auth (`?as=` switches actor) · binds localhost ·
manual refresh.

## SDK extension points it uses

Same seams as `invoice_flow`, plus the parts it adds — all in
`registry.py`:

| Extension point | Snippet |
|---|---|
| **`JobHandler.run`** — subclass a shared base for a family of adapters | `class _Adapter(JobHandler): async def run(self, ctx, payload: Sup): … await _record(ctx.uow, …)` then `class SanctionScreenH(_Adapter): check_type="sanction_screen"` |
| **`ctx.uow`** — write check results in the handler's dispatch TX | `await _record(ctx.uow, payload.supplier_id, self.check_type, result)` |
| **`ctx.uow` for a read inside a handler** | `s = await _sup_row(ctx.uow, payload.supplier_id)` |
| **`StepSpec.needs` fan-out** — N siblings under one parent run in parallel | `StepSpec("sanction",…, needs=("validate",))` `media`/`credit`/`bank` likewise → all 4 run concurrently |
| **`needs` fan-in** — a step waits on several parents | `StepSpec("risk_score",…, needs=("sanction","media","credit","bank"))` |
| **`StepSpec.if_expr`** — conditional HITL: false → `skipped`, true → `blocked` | `if_expr="${{ steps.risk_score.outputs.risk }} == 'high'"` |
| **skipped satisfies `needs`** — a branch you didn't take still unblocks dependents | `erp` needs `comp_review`+`credit_signoff`; only one runs, the other `skipped` → `erp` still proceeds |
| **`HumanTaskOptions(assignee_role)`** — which role may claim each human step | `human_task=HumanTaskOptions(assignee_role="finance")` etc. |
| **Business reject via human decision** — completes the run with `terminal_outcome='rejected'` (not a job failure) | `human.*` step completed with `decision="reject"` |
| **Actor roles for claim/completion** — app passes roles from its trusted actor map | `roles=_ACTOR_ROLES[ctx.actor.id]` — no user-supplied roles |
| **`resume_run`** — restart a `failed` run from the point of failure | `WorkflowService.resume_run(…)` on `/resume/{run_id}` |
| **`is_external_wait` wait step** — step parks `blocked`, no kernel claim, resolved by the app's own callback | `StepSpec("bank", "wait.bank_verify@v1", …)` + `JobTypeSpec(…, is_external_wait=True)` |
| **App-owned correlation** — engine signal uses `job_id`; the app keeps `provider_ref → job_id` in its own table | `demo_bank_requests` in `schema.sql`; `send_bank_request`/`handle_bank_callback` in `app.py` |
| **`jobs.signal` in caller TX** — domain row + wait resolution + DAG advance = one commit | `await jobs.signal(conn, job_id, result, svc_ctx)` in `handle_bank_callback` |

## SDK extension points it uses — code

The DAG's shape is pure data (`StepSpec`/`if_expr`) — the only code is
handler bodies:

```python
# one base, four adapters — each writes to ctx.uow and returns outputs
class _Adapter(JobHandler):
    check_type = "base"
    async def run(self, ctx, payload: Sup) -> JobResult:
        s = await _sup_row(ctx.uow, payload.supplier_id)
        result = await self.check(ctx, s)
        await _record(ctx.uow, payload.supplier_id, self.check_type, result)
        return JobResult(status="done", outputs=result)

# conditional human gate — engine decides run-vs-skip from the expr
StepSpec("credit_signoff", "human.credit_signoff@v1", input=i,
         needs=("risk_score",),
         if_expr="${{ steps.risk_score.outputs.risk }} == 'high'")
```
