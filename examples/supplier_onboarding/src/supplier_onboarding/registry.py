"""keelflow demo 2 — supplier onboarding DAG.

~20-step workflow showing: intake normalization, parallel external
checks (mock adapters — deterministic, no real API calls), risk scoring,
CONDITIONAL human review (if: guards on DAG steps), ERP vendor creation,
activation + onboarding pack.

Run it:
    KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow \
        uv run --package keelflow-demo-supplier python -m supplier_onboarding.seed
    uv run --package keelflow-demo-supplier python -m supplier_onboarding.worker
    uv run --package keelflow-demo-supplier uvicorn supplier_onboarding.app:app --port 8001
"""
from __future__ import annotations

import hashlib
import json

from psycopg.rows import dict_row
from pydantic import BaseModel

from keelflow.engine.registry import InMemoryRegistry
from keelflow.sdk import (
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
    StepSpec,
    TaskUiSpec,
    WorkflowDefinition,
)

DSN_ENV = "KEELFLOW_DSN"

# ── actors / permissions ────────────────────────────────────────────────────
# ?as=proc1 (procurement) · ?as=comp1 (compliance) · ?as=fin1 (finance)
# ?as=procadm (provisioning admin) · ?as=mgr (all)
ROLES = {"procurement", "compliance", "finance", "provisioning", "manager"}
PERMISSIONS = {
    "u-proc1": {"sup.create"},
    "u-comp1": {"sup.comp_review"},
    "u-fin1": {"sup.credit_signoff", "sup.bank_conf"},
    "u-procadm": {"sup.provision"},
    "u-mgr": {"*"},
}


# ── payloads ────────────────────────────────────────────────────────────────
class Sup(BaseModel):
    supplier_id: int


# ── helpers ─────────────────────────────────────────────────────────────────
async def _sup_row(tx, supplier_id: int) -> dict:
    cur = tx.connection.cursor(row_factory=dict_row) if hasattr(
        tx, "connection") else tx
    await cur.execute(
        "SELECT id, name, country, tax_id, contact_email, bank_ref,"
        " fail_sanction FROM demo_suppliers WHERE id=%s", (supplier_id,))
    return await cur.fetchone()


def _h(key: str, *parts: str) -> int:
    """Stable hash — deterministic mock outcomes per supplier."""
    return int(hashlib.sha256(key.encode() + b"|" +
                              "|".join(parts).encode()).hexdigest(), 16)


async def _record(tx, supplier_id: int, check_type: str, result: dict) -> None:
    await tx.execute(
        "INSERT INTO demo_check_results (supplier_id, check_type, result)"
        " VALUES (%s, %s, %s)"
        " ON CONFLICT (supplier_id, check_type) DO UPDATE"
        " SET result=EXCLUDED.result, created_at=now()",
        (supplier_id, check_type, json.dumps(result)),
    )


# ── handlers ────────────────────────────────────────────────────────────────
class ValidateIntakeH(JobHandler):
    """Normalize + basic sanity (mock). Sets status → onboarding."""

    async def run(self, ctx, payload: Sup) -> JobResult:
        s = await _sup_row(ctx.tx, payload.supplier_id)
        if not s or "@" not in s["contact_email"]:
            return JobResult(status="failed",
                             outputs={"reason": "invalid contact email"})
        await ctx.tx.execute(
            "UPDATE demo_suppliers SET status='onboarding'"
            " WHERE id=%s AND status='requested'", (payload.supplier_id,))
        return JobResult(status="done", outputs={
            "legal_name": s["name"].strip().title(), "tax_ok": True})


class _Adapter(JobHandler):
    """Mock external adapter — deterministic result keyed by supplier."""

    check_type = "base"

    async def run(self, ctx, payload: Sup) -> JobResult:
        s = await _sup_row(ctx.tx, payload.supplier_id)
        result = await self.check(ctx, s)
        await _record(ctx.tx, payload.supplier_id, self.check_type, result)
        return JobResult(status="done", outputs=result)


class SanctionScreenH(_Adapter):
    check_type = "sanction_screen"

    async def check(self, ctx, s):
        # chaos knob — simulates an external adapter outage
        if s["fail_sanction"]:
            raise RuntimeError("sanction API timeout (simulated outage)")
        hit = _h("san", s["name"], s["tax_id"]) % 100 < 8
        return {"list": "ofac/un consolidated", "hit": hit,
                "score": 0.02 + (_h("s2", s["tax_id"]) % 30) / 100}


class AdverseMediaH(_Adapter):
    check_type = "adverse_media"

    async def check(self, ctx, s):
        n = _h("media", s["name"]) % 4
        return {"articles": n,
                "flagged": n >= 3,
                "sources": [f"wire-{i}" for i in range(n)]}


class CreditCheckH(_Adapter):
    check_type = "credit_check"

    async def check(self, ctx, s):
        score = 300 + _h("cr", s["tax_id"], s["country"]) % 550
        return {"score": score,
                "band": ("poor" if score < 550 else
                         "fair" if score < 700 else "good")}


# Bank verification is NOT a synchronous adapter — the mock provider
# calls the app back later. The workflow parks on `wait.bank_verify@v1`;
# the app-owned callback route resolves it via jobs.signal().


class RiskScoreH(JobHandler):
    """Fan-in: combines parallel check outputs → low|medium|high."""

    async def run(self, ctx, payload: Sup) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) if hasattr(
            ctx.tx, "connection") else ctx.tx
        await cur.execute(
            "SELECT check_type, result FROM demo_check_results"
            " WHERE supplier_id=%s", (payload.supplier_id,))
        r = {row["check_type"]: row["result"] for row in await cur.fetchall()}
        risk = "low"
        if r["sanction_screen"]["hit"] or r["adverse_media"]["flagged"] or r["credit_check"]["band"] == "poor":
            risk = "high"
        elif (r["credit_check"]["band"] == "fair"
              or not r["bank_verify"]["verified"]
              or r["adverse_media"]["articles"] > 0):
            risk = "medium"
        await ctx.tx.execute(
            "UPDATE demo_suppliers SET risk=%s WHERE id=%s",
            (risk, payload.supplier_id))
        return JobResult(status="done", outputs={
            "risk": risk, "summary": r})


class ErpVendorCreateH(JobHandler):
    """Mock ERP vendor provisioning — vendor_no natural key → idempotent."""

    async def run(self, ctx, payload: Sup) -> JobResult:
        s = await _sup_row(ctx.tx, payload.supplier_id)
        vendor_no = f"V-{payload.supplier_id:05d}"
        await ctx.tx.execute(
            "INSERT INTO demo_vendor_accounts"
            " (supplier_id, vendor_no, legal_name, tax_id)"
            " VALUES (%s, %s, %s, %s)"
            " ON CONFLICT (vendor_no) DO NOTHING",
            (payload.supplier_id, vendor_no, s["name"].title(), s["tax_id"]))
        return JobResult(status="done", outputs={"vendor_no": vendor_no})


class PaymentTermsH(JobHandler):
    async def run(self, ctx, payload: Sup) -> JobResult:
        s = await _sup_row(ctx.tx, payload.supplier_id)
        terms = "NET30" if _h("pt", s["name"]) % 2 == 0 else "NET45"
        vendor_no = f"V-{payload.supplier_id:05d}"
        await ctx.tx.execute(
            "UPDATE demo_vendor_accounts SET payment_terms=%s"
            " WHERE vendor_no=%s", (terms, vendor_no))
        return JobResult(status="done", outputs={"terms": terms})


class ProvisionAccessH(JobHandler):
    async def run(self, ctx, payload: Sup) -> JobResult:
        vendor_no = f"V-{payload.supplier_id:05d}"
        await ctx.tx.execute(
            "UPDATE demo_vendor_accounts SET bank_verified=true"
            " WHERE vendor_no=%s", (vendor_no,))
        return JobResult(status="done",
                         outputs={"portal_account": f"portal:{vendor_no}"})


class FinalActivateH(JobHandler):
    async def run(self, ctx, payload: Sup) -> JobResult:
        vendor_no = f"V-{payload.supplier_id:05d}"
        await ctx.tx.execute(
            "UPDATE demo_vendor_accounts SET activated_at=now()"
            " WHERE vendor_no=%s", (vendor_no,))
        await ctx.tx.execute(
            "UPDATE demo_suppliers SET status='active' WHERE id=%s",
            (payload.supplier_id,))
        return JobResult(status="done", outputs={"vendor_no": vendor_no,
                                                 "activated": True})


class _NoopH(JobHandler):
    async def run(self, ctx, payload) -> JobResult:  # pragma: no cover
        return JobResult(status="done")


# ── registry ────────────────────────────────────────────────────────────────
def build_registry() -> InMemoryRegistry:
    r = InMemoryRegistry()

    def j(type_: str, handler, *, human=False, wait=False, ui=None,
          attempts=3):
        r.register_job_type(JobTypeSpec(
            type=type_, input_schema=Sup, handler=handler,
            retry=RetryPolicy(max_attempts=attempts, base_seconds=5),
            is_human=human, is_wait=wait, ui=ui))

    j("sup.validate@v1", ValidateIntakeH)
    j("sup.sanction_screen@v1", SanctionScreenH, attempts=4)
    j("sup.adverse_media@v1", AdverseMediaH)
    j("sup.credit_check@v1", CreditCheckH)
    j("wait.bank_verify@v1", None, wait=True)   # resolved by app callback
    j("sup.risk_score@v1", RiskScoreH)
    j("sup.erp_vendor_create@v1", ErpVendorCreateH)
    j("sup.payment_terms@v1", PaymentTermsH)
    j("sup.provision_access@v1", ProvisionAccessH)
    j("sup.final_activate@v1", FinalActivateH)

    # conditional human steps — `if:` guards decide at advance time
    j("human.comp_review@v1", _NoopH, human=True,
      ui=TaskUiSpec(assignee_role="compliance"))
    j("human.credit_signoff@v1", _NoopH, human=True,
      ui=TaskUiSpec(assignee_role="finance"))
    j("human.bank_conf@v1", _NoopH, human=True,
      ui=TaskUiSpec(assignee_role="finance"))

    i = {"supplier_id": "${{ inputs.supplier_id }}"}
    r.register_workflow(WorkflowDefinition(
        name="sup.onboard@v1",
        steps=(
            StepSpec("validate", "sup.validate@v1", input=i),
            # — parallel check fan-out (4 machine nodes run concurrently) —
            StepSpec("sanction", "sup.sanction_screen@v1",
                     input=i, needs=("validate",)),
            StepSpec("media", "sup.adverse_media@v1",
                     input=i, needs=("validate",)),
            StepSpec("credit", "sup.credit_check@v1",
                     input=i, needs=("validate",)),
            StepSpec("bank", "wait.bank_verify@v1",
                     input=i, needs=("validate",)),
            StepSpec("risk_score", "sup.risk_score@v1", input=i,
                     needs=("sanction", "media", "credit", "bank")),
            # — conditional HITL branches —
            StepSpec("comp_review", "human.comp_review@v1", input=i,
                     needs=("risk_score",),
                     if_expr="${{ steps.risk_score.outputs.risk }} != 'low'"),
            StepSpec("credit_signoff", "human.credit_signoff@v1", input=i,
                     needs=("risk_score",),
                     if_expr="${{ steps.risk_score.outputs.risk }} == 'high'"),
            StepSpec("bank_conf", "human.bank_conf@v1", input=i,
                     needs=("risk_score",),
                     if_expr="${{ steps.risk_score.outputs.risk }} == 'low'"),
            # — provisioning fan-out —
            StepSpec("erp", "sup.erp_vendor_create@v1", input=i,
                     needs=("comp_review", "credit_signoff")),
            StepSpec("terms", "sup.payment_terms@v1", input=i,
                     needs=("erp",)),
            StepSpec("provision", "sup.provision_access@v1", input=i,
                     needs=("erp", "bank_conf")),
            # — converge —
            StepSpec("activate", "sup.final_activate@v1", input=i,
                     needs=("terms", "provision")),
        ),
    ))
    return r
