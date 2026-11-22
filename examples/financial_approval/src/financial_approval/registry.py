"""keelflow demo 5 — financial payment-run approval DAG (~26 steps).

The largest demo workflow: intake, parallel risk fan-out (machine +
external-wait), amount/risk-conditional human approval chain, GL
posting, a set-based `process_batch` payment-line bulk step, an external
bank-execution wait, then reconcile/audit/archive. Exercises nearly
every engine feature on one run:

  - parallel `needs` fan-out / fan-in          - `wait.*` external steps
  - `human.*` role steps + `if:` conditionals  - `process_batch` bulk
  - `failure` retry + cooperative cancel        - audit + status tracking

Run it:
    KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow \
        uv run --package keelflow-demo-financial \
            python -m financial_approval.seed
    uv run --package keelflow-demo-financial \
        python -m financial_approval.worker
    uv run --package keelflow-demo-financial \
        uvicorn financial_approval.app:app --port 8002
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from psycopg.rows import dict_row
from pydantic import BaseModel

from keelflow.engine import bulk
from keelflow.engine.audit import PostgresAuditService
from keelflow.engine.bulk import BulkJobHandler, BulkJobPayload
from keelflow.engine.registry import InMemoryRegistry
from keelflow.sdk import (
    AuditEvent,
    BatchRowResult,
    EntityRef,
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
    StepSpec,
    TaskUiSpec,
    WorkflowDefinition,
)

DSN_ENV = "KEELFLOW_DSN"

# ── actors / roles / permissions ──────────────────────────────────────────
# ?as=clerk (ap clerk) · ?as=manager · ?as=controller · ?as=cfo ·
# ?as=comp (compliance) · ?as=admin (all)
ROLES = {"clerk", "manager", "controller", "cfo", "compliance", "treasury"}
PERMISSIONS = {
    "u-clerk":      {"pay.submit"},
    "u-manager":    {"pay.approve_mgr"},
    "u-controller": {"pay.approve_ctrl"},
    "u-cfo":        {"pay.approve_cfo"},
    "u-comp":       {"pay.approve_comp"},
    "u-admin":      {"*"},
}
# every demo actor may also cancel (job.cancel checked in ctx)


# ── payload ───────────────────────────────────────────────────────────────
class Pay(BaseModel):
    request_id: int


class PayBulk(BulkJobPayload):
    """Bulk-step payload: BulkJobPayload supplies chunk_size/atomic."""
    request_id: int


# ── helpers ────────────────────────────────────────────────────────────────
def _h(key: str, *parts: str) -> int:
    """Stable hash — deterministic mock outcomes per request."""
    return int(hashlib.sha256(
        key.encode() + b"|" + "|".join(parts).encode()).hexdigest(), 16)


async def _pay_row(cur, request_id: int) -> dict | None:
    await cur.execute(
        "SELECT id, ref, vendor_id, currency, amount, urgent,"
        " fail_sanction, risk, status FROM demo_pay_requests WHERE id=%s",
        (request_id,))
    return await cur.fetchone()


async def _rec(cur, request_id: int, check_type: str, result: dict) -> None:
    await cur.execute(
        "INSERT INTO demo_fin_checks (request_id, check_type, result)"
        " VALUES (%s,%s,%s)"
        " ON CONFLICT (request_id, check_type) DO UPDATE"
        " SET result=EXCLUDED.result, created_at=now()",
        (request_id, check_type, json.dumps(result, default=str)))


def _status(ctx, request_id: int, status: str):
    return ctx.tx.execute(
        "UPDATE demo_pay_requests SET status=%s WHERE id=%s",
        (status, request_id))


def _audit(ctx, request_id: int, action: str, detail: dict) -> None:
    svc = PostgresAuditService()
    return svc.record(AuditEvent(
        actor=ctx.actor, action=action,
        entity=EntityRef(type="pay_request", id=str(request_id)),
        changes={"detail": ("", detail)},
        source="fin.demo"), ctx.tx)


# ── machine handlers ───────────────────────────────────────────────────────
class ValidateH(JobHandler):
    """Intake: sum lines, ensure non-zero, mark submitted."""

    async def run(self, ctx, payload: Pay) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) if hasattr(
            ctx.tx, "connection") else ctx.tx
        p = await _pay_row(cur, payload.request_id)
        if p is None:
            return JobResult(status="failed",
                             outputs={"reason": "request not found"})
        await cur.execute(
            "SELECT COALESCE(sum(amount),0) AS s, count(*) AS n"
            " FROM demo_pay_lines WHERE request_id=%s", (payload.request_id,))
        agg = await cur.fetchone()
        if int(agg["n"]) == 0:
            return JobResult(status="failed",
                             outputs={"reason": "no pay lines"})
        await _status(ctx, payload.request_id, "submitted")
        await _audit(ctx, payload.request_id, "submit",
                     {"lines": int(agg["n"])})
        return JobResult(status="done", outputs={
            "line_total": str(agg["s"]), "line_count": int(agg["n"]),
            "ref": p["ref"]})


class DedupCheckH(JobHandler):
    """Idempotency guard — flag if a *paid* request already exists for
    this vendor with the same amount (deterministic mock)."""

    async def run(self, ctx, payload: Pay) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) if hasattr(
            ctx.tx, "connection") else ctx.tx
        p = await _pay_row(cur, payload.request_id)
        dup = _h("dup", p["ref"], str(p["amount"])) % 97 == 0   # ~1%
        return JobResult(status="done", outputs={"possible_dup": dup})


class VendorEnrichH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) if hasattr(
            ctx.tx, "connection") else ctx.tx
        p = await _pay_row(cur, payload.request_id)
        await cur.execute(
            "SELECT name, country, bank_ref FROM demo_vendors WHERE id=%s",
            (p["vendor_id"],))
        v = await cur.fetchone()
        return JobResult(status="done", outputs={
            "vendor": v["name"], "country": v["country"],
            "bank_ref": v["bank_ref"],
            # expose request fields downstream if_expr guards need
            "currency": p["currency"], "urgent": p["urgent"]})


# ── parallel risk checks (machine) — deterministic mock adapters ────────
class TaxCalcH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) if hasattr(
            ctx.tx, "connection") else ctx.tx
        p = await _pay_row(cur, payload.request_id)
        rate = Decimal("0.10")
        tax = (p["amount"] * rate).quantize(Decimal("0.01"))
        res = {"tax": str(tax), "rate": str(rate),
               "country": "VN" if p["currency"] == "VND" else "intl"}
        await _rec(cur, payload.request_id, "tax_calc", res)
        return JobResult(status="done", outputs=res)


class BudgetCheckH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) if hasattr(
            ctx.tx, "connection") else ctx.tx
        p = await _pay_row(cur, payload.request_id)
        # deterministic budget: headroom derived from ref
        headroom = Decimal(50_000 + _h("bud", p["ref"]) % 250_000)
        ok = p["amount"] <= headroom
        res = {"headroom": str(headroom), "within_budget": ok}
        await _rec(cur, payload.request_id, "budget_check", res)
        return JobResult(status="done", outputs=res)


class SanctionScreenH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) if hasattr(
            ctx.tx, "connection") else ctx.tx
        p = await _pay_row(cur, payload.request_id)
        if p["fail_sanction"]:
            raise RuntimeError("sanction API timeout (simulated outage)")
        hit = _h("san", p["ref"], str(p["vendor_id"])) % 100 < 8
        res = {"list": "ofac/un consolidated", "hit": hit,
               "score": round(0.02 + (_h("s2", p["ref"]) % 30) / 100, 3)}
        await _rec(cur, payload.request_id, "sanction_screen", res)
        return JobResult(status="done", outputs=res)


# wait.fx_quote / wait.bank_preverify / wait.bank_execute are resolved by
# the app's own provider-callback routes (wait.* steps park the run).

class RiskScoreH(JobHandler):
    """Fan-in: combine the five check outputs → low|medium|high."""

    async def run(self, ctx, payload: Pay) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) if hasattr(
            ctx.tx, "connection") else ctx.tx
        p = await _pay_row(cur, payload.request_id)
        await cur.execute(
            "SELECT check_type, result FROM demo_fin_checks"
            " WHERE request_id=%s", (payload.request_id,))
        r = {row["check_type"]: row["result"] for row in await cur.fetchall()}
        amount = Decimal(str(p["amount"]))
        risk = "low"
        if (r["sanction_screen"]["hit"]
                or not r["budget_check"]["within_budget"]
                or r["bank_preverify"]["verified"] is False
                or amount >= 100_000):
            risk = "high"
        elif (amount >= 25_000
                or not r["bank_preverify"]["verified"]
                or r["sanction_screen"]["score"] > 0.2
                or p["currency"] != "USD"):
            risk = "medium"
        await ctx.tx.execute(
            "UPDATE demo_pay_requests SET risk=%s WHERE id=%s",
            (risk, payload.request_id))
        return JobResult(status="done", outputs={"risk": risk})


# ── GL + schedule machine steps (run only when every approval passed) ──
class GlPostH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) if hasattr(
            ctx.tx, "connection") else ctx.tx
        await cur.execute(
            "SELECT count(*) AS n FROM demo_pay_lines WHERE request_id=%s",
            (payload.request_id,))
        n = int((await cur.fetchone())["n"])
        await _rec(cur, payload.request_id, "gl_post",
                   {"entries": n, "journal": f"JE-{payload.request_id:05d}"})
        await _audit(ctx, payload.request_id, "gl_post", {"entries": n})
        return JobResult(status="done", outputs={"journal_entries": n})


class AccrualH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        return JobResult(status="done",
                         outputs={"accrual": f"ACR-{payload.request_id:05d}"})


class ApScheduleH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) if hasattr(
            ctx.tx, "connection") else ctx.tx
        p = await _pay_row(cur, payload.request_id)
        await _status(ctx, payload.request_id, "scheduled")
        terms = "immediate" if p["urgent"] else "net30"
        return JobResult(status="done", outputs={"schedule": terms})


class TreasuryFundH(JobHandler):
    """Only for urgent requests (if_expr)."""
    async def run(self, ctx, payload: Pay) -> JobResult:
        return JobResult(status="done",
                         outputs={"funding": "reserve-allocated"})


# ── bulk step: pay lines via process_batch (set-based) ─────────────────
class PayLinesBulkH(BulkJobHandler):
    """Stage this request's lines into keelflow_row_status inside the
    handler TX, then one set-based pass per chunk marks each line
    `ok`/`failed`. A line without an invoice_no is a per-row business
    error (siblings still commit). atomic='chunk' → uses process_batch.
    """

    async def run(self, ctx, payload: Pay) -> JobResult:
        cur = ctx.tx.connection.cursor() if hasattr(
            ctx.tx, "connection") else ctx.tx
        job_id = ctx.extra["job_id"]
        await cur.execute(
            "INSERT INTO keelflow_row_status (job_id, row_key, status, data)"
            " SELECT %s, 'line-'||id, 'pending',"
            " jsonb_build_object('line_id', id, 'invoice_no', invoice_no,"
            " 'amount', amount::text, 'request_id', request_id)"
            " FROM demo_pay_lines WHERE request_id=%s ORDER BY line_no",
            (job_id, payload.request_id))
        return await super().run(ctx, payload)

    async def process_row(self, ctx, row, tx):  # pragma: no cover - guard
        raise AssertionError("batch handler must run via process_batch")

    async def process_batch(self, ctx, rows, tx):
        out: list[BatchRowResult] = []
        ok_ids: list[int] = []
        for r in rows:
            d = r["data"]
            if not d.get("invoice_no"):
                out.append(BatchRowResult(
                    row_id=r["id"], error="missing invoice_no"))
            else:
                ok_ids.append(int(d["line_id"]))
                out.append(BatchRowResult(
                    row_id=r["id"], outputs={"line_id": d["line_id"]}))
        if ok_ids:
            await tx.executemany(
                "UPDATE demo_pay_lines SET gl_account='6000'"
                " WHERE id=%s", [(i,) for i in ok_ids])
        return out


class ReconcileH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        return JobResult(status="done", outputs={
            "reconciled": True, "method": "bank-stmt-match (mock)"})


class RemitAdviceH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        return JobResult(status="done",
                         outputs={"remittance": "advice-queued"})


class AuditArchiveH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        await _status(ctx, payload.request_id, "paid")
        await _audit(ctx, payload.request_id, "paid", {})
        return JobResult(status="done", outputs={"final_status": "paid"})


class NotifyCompleteH(JobHandler):
    async def run(self, ctx, payload: Pay) -> JobResult:
        return JobResult(status="done",
                         outputs={"notification": "ap-requestor-emailed"})


class _NoopH(JobHandler):
    async def run(self, ctx, payload) -> JobResult:  # pragma: no cover
        return JobResult(status="done")


# ── registry ──────────────────────────────────────────────────────────────
def build_registry() -> InMemoryRegistry:
    r = InMemoryRegistry()

    def j(type_: str, handler, *, human=False, wait=False, ui=None,
          attempts=3, coop=False):
        r.register_job_type(JobTypeSpec(
            type=type_, input_schema=Pay, handler=handler,
            retry=RetryPolicy(max_attempts=attempts, base_seconds=5),
            is_human=human, is_wait=wait, ui=ui,
            cooperative_cancel=coop))

    j("fin.validate@v1", ValidateH, coop=True)
    j("fin.dedup_check@v1", DedupCheckH)
    j("fin.vendor_enrich@v1", VendorEnrichH)
    j("fin.tax_calc@v1", TaxCalcH, coop=True)
    j("fin.budget_check@v1", BudgetCheckH)
    j("fin.sanction_screen@v1", SanctionScreenH, attempts=4)
    j("wait.fx_quote@v1", None, wait=True)
    j("wait.bank_preverify@v1", None, wait=True)
    j("wait.bank_execute@v1", None, wait=True)
    j("fin.risk_score@v1", RiskScoreH)
    j("fin.gl_post@v1", GlPostH)
    j("fin.accrual@v1", AccrualH)
    j("fin.ap_schedule@v1", ApScheduleH)
    j("fin.treasury_fund@v1", TreasuryFundH)
    bulk.register(r)                        # internal bulk.chunk@v1

    # bulk step: its input_schema carries chunk_size/atomic knobs
    r.register_job_type(JobTypeSpec(
        type="fin.pay_lines@v1", input_schema=PayBulk,
        handler=PayLinesBulkH,
        retry=RetryPolicy(max_attempts=3, base_seconds=5),
        cooperative_cancel=True))
    j("fin.reconcile@v1", ReconcileH)
    j("fin.remit_advice@v1", RemitAdviceH)
    j("fin.audit_archive@v1", AuditArchiveH)
    j("fin.notify_complete@v1", NotifyCompleteH)

    # conditional human approvals — `if:` guards decide at advance time
    j("human.ap_mgr@v1", _NoopH, human=True,
      ui=TaskUiSpec(assignee_role="manager"))
    j("human.ap_ctrl@v1", _NoopH, human=True,
      ui=TaskUiSpec(assignee_role="controller"))
    j("human.ap_cfo@v1", _NoopH, human=True,
      ui=TaskUiSpec(assignee_role="cfo"))
    j("human.ap_comp@v1", _NoopH, human=True,
      ui=TaskUiSpec(assignee_role="compliance"))

    i = {"request_id": "${{ inputs.request_id }}"}
    r.register_workflow(WorkflowDefinition(
        name="fin.pay_run@v1",
        steps=(
            # — intake —
            StepSpec("validate", "fin.validate@v1", input=i),
            StepSpec("dedup", "fin.dedup_check@v1", input=i,
                     needs=("validate",)),
            StepSpec("vendor", "fin.vendor_enrich@v1", input=i,
                     needs=("validate",)),
            # — parallel risk fan-out (5 nodes run concurrently) —
            StepSpec("fx", "wait.fx_quote@v1", input=i,
                     needs=("vendor",),
                     if_expr="${{ steps.vendor.outputs.currency }} != 'USD'"),
            StepSpec("tax", "fin.tax_calc@v1", input=i,
                     needs=("vendor",)),
            StepSpec("budget", "fin.budget_check@v1", input=i,
                     needs=("vendor",)),
            StepSpec("sanction", "fin.sanction_screen@v1", input=i,
                     needs=("vendor",)),
            StepSpec("bank_pre", "wait.bank_preverify@v1", input=i,
                     needs=("vendor",)),
            # — fan-in risk scoring —
            StepSpec("risk_score", "fin.risk_score@v1", input=i,
                     needs=("fx", "tax", "budget", "sanction", "bank_pre")),
            # — sequential multi-level human approval chain —
            # manager always; controller when risk!=low; CFO + compliance
            # only when risk==high. Each level blocks on the prior.
            StepSpec("ap_mgr", "human.ap_mgr@v1", input=i,
                     needs=("risk_score",)),
            StepSpec("ap_ctrl", "human.ap_ctrl@v1", input=i,
                     needs=("ap_mgr",),
                     if_expr="${{ steps.risk_score.outputs.risk }} != 'low'"),
            StepSpec("ap_cfo", "human.ap_cfo@v1", input=i,
                     needs=("ap_ctrl",),
                     if_expr="${{ steps.risk_score.outputs.risk }} == 'high'"),
            StepSpec("ap_comp", "human.ap_comp@v1", input=i,
                     needs=("ap_cfo",),
                     if_expr="${{ steps.risk_score.outputs.risk }} == 'high'"),
            # — GL + schedule (gated on every approval branch) —
            StepSpec("gl", "fin.gl_post@v1", input=i,
                     needs=("ap_mgr", "ap_ctrl", "ap_cfo", "ap_comp")),
            StepSpec("accrual", "fin.accrual@v1", input=i, needs=("gl",)),
            StepSpec("ap_sched", "fin.ap_schedule@v1", input=i,
                     needs=("gl",)),
            StepSpec("treasury", "fin.treasury_fund@v1", input=i,
                     needs=("ap_sched",),
                     if_expr="${{ steps.vendor.outputs.urgent }} == True"),
            # — bulk payment lines (set-based) —
            StepSpec("pay_lines", "fin.pay_lines@v1",
                     input={"request_id": "${{ inputs.request_id }}",
                            "atomic": "chunk", "chunk_size": 1000},
                     needs=("accrual", "ap_sched", "treasury")),
            # — external execution + settle —
            StepSpec("bank_exec", "wait.bank_execute@v1", input=i,
                     needs=("pay_lines",)),
            StepSpec("reconcile", "fin.reconcile@v1", input=i,
                     needs=("bank_exec",)),
            StepSpec("remit", "fin.remit_advice@v1", input=i,
                     needs=("reconcile",)),
            StepSpec("audit", "fin.audit_archive@v1", input=i,
                     needs=("remit",)),
            StepSpec("notify", "fin.notify_complete@v1", input=i,
                     needs=("audit",)),
        ),
    ))
    return r
