# keelflow demo — financial approval

The largest demo: a **~23-step payment-run DAG** that exercises almost
every engine feature on a single run — parallel risk fan-out, a
sequential multi-level human approval chain, an external wait/signal
boundary (×3), a set-based `process_batch` payment-line step, GL
posting, reconciliation, audit, and cooperative cancel.

A **payment run** (`demo_pay_requests` + `demo_pay_lines`) is submitted
once; the DAG validates it, fans out five independent checks, scores
risk, routes through a risk/amount-conditional human approval chain,
posts to GL, pays the lines in bulk, executes at the bank via an
external callback, then reconciles and archives.

## Why it exists

The smaller demos each prove one feature. This one is the integration
proof — a realistic many-step workflow where parallel lanes, human
gates, external callbacks, and bulk processing all coexist in one run.

## The DAG (~23 steps)

```mermaid
flowchart TD
  validate --> dedup
  validate --> vendor
  vendor --> fx[wait.fx_quote - non-USD]
  vendor --> tax[fin.tax_calc]
  vendor --> budget[fin.budget_check]
  vendor --> sanction[fin.sanction_screen]
  vendor --> bank_pre[wait.bank_preverify]
  fx & tax & budget & sanction & bank_pre --> risk[fin.risk_score]
  risk --> mgr[human.ap_mgr]
  mgr --> ctrl[human.ap_ctrl - risk!=low]
  ctrl --> cfo[human.ap_cfo - risk=high]
  cfo --> comp[human.ap_comp - risk=high]
  mgr & ctrl & cfo & comp --> gl[fin.gl_post]
  gl --> accrual[fin.accrual]
  gl --> sched[fin.ap_schedule]
  sched --> treasury[fin.treasury_fund - urgent]
  accrual & sched & treasury --> paylines[bulk pay_lines]
  paylines --> exec[wait.bank_execute]
  exec --> reconcile[fin.reconcile]
  reconcile --> remit[fin.remit_advice]
  remit --> audit[fin.audit_archive]
  audit --> notify[fin.notify_complete]
```

| Layer | Steps | Feature exercised |
|---|---|---|
| Intake | `validate`, `dedup`, `vendor` | machine steps, dedup/idempotency guard, `if_expr` data |
| Risk fan-out | `fx`, `tax`, `budget`, `sanction`, `bank_pre` | parallel `needs`, `wait.*` (fx/bank_pre), conditional `fx` (non-USD) |
| Fan-in | `risk_score` | aggregate 5 check outputs → `low/medium/high` |
| Approval | `ap_mgr`→`ap_ctrl`→`ap_cfo`→`ap_comp` | sequential `human.*` role steps gated by `if_expr` on risk |
| Posting | `gl`, `accrual`, `ap_sched`, `treasury` | gated fan-in on approvals, conditional `treasury` (urgent) |
| Payment | `pay_lines`, `bank_exec` | `process_batch` bulk (one per-row error), `wait.*` external settle |
| Settle | `reconcile`, `remit`, `audit`, `notify` | post-exec chain, audit archive, status→`paid` |

## Feature coverage

| Feature | Where |
|---|---|
| parallel `needs` fan-out | 5 risk checks run concurrently |
| `wait.*` external steps | `fx_quote`, `bank_preverify`, `bank_execute` — resolved by app-owned `/integrations/{prov}/callback` routes |
| `human.*` role steps | `ap_mgr` (manager) → `ap_ctrl` (controller) → `ap_cfo` (cfo) → `ap_comp` (compliance) |
| `if:` conditional steps | `fx` (currency), `ap_ctrl`/`ap_cfo`/`ap_comp` (risk), `treasury` (urgent) |
| `process_batch` bulk | `fin.pay_lines@v1` — one `BatchRowResult` per engine row id; a blank `invoice_no` line produces one per-row business error |
| cooperative cancel | `cooperative_cancel=True` on machine/bulk types; **cancel run** button |
| retry/backoff | `sanction_screen` has `attempts=4`; `fail_sanction` chaos knob |
| audit | `PostgresAuditService` writes `submit`/`gl_post`/`paid` events |
| `unique_key`/idempotency | natural keys + `ON CONFLICT` on check results and provider requests |

## Run it

```bash
export KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow
uv run --package keelflow-demo-financial python -m financial_approval.seed
uv run --package keelflow-demo-financial python -m financial_approval.worker &
uv run --package keelflow-demo-financial \
    uvicorn financial_approval.app:app --port 8002
```

Then open `http://localhost:8002`. Switch actors with the `as` links
(clerk / manager / controller / cfo / comp / admin). To push a request
all the way through:

1. `?as=u-clerk` → open a request → **submit payment run**.
2. Machine checks + `bank_preverify` park — the request shows a
   **waiting external** wait; click **send bank_pre request**, then
   **simulate provider: OK** (the mock provider calls back).
3. Risk scoring fans in; the human chain blocks at `human.ap_mgr`.
   `?as=u-manager` → inbox → **approve**. If risk ≥ medium, `ap_ctrl`
   opens next (controller); at `high`, `ap_cfo` and `ap_comp` follow.
4. GL posts, lines pay in bulk, `bank_execute` parks — send + simulate
   the provider callback → reconcile → `paid`.

Seeded requests cover every branch: `PAY-0003` (120k USD) scores
**high** → full mgr→ctrl→cfo→comp chain; `PAY-0002` is VND + urgent →
`fx` wait + `treasury` step; `PAY-0001` has one blank `invoice_no` → one
per-row error inside the bulk pass.

## Tests

`tests/test_financial.py` drives real runs through a live worker +
Postgres: a 120k request through the full chain, a skipped-step
assertion (`fx` for USD), one per-row bulk error, and a small request
verifying the low-risk short chain (only `ap_mgr` blocks).

```bash
uv run --package keelflow-demo-financial \
    pytest examples/financial_approval/tests -v
```
