"""Registry + definitions for the invoice_flow demo.

Job types:
  inv.post_ledger@v1      domain write inside the handler TX (ctx.tx)
  inv.record_posting@v1   domain write inside the handler TX
  human.review@v1         finance review (creates_job on submit)
  human.gl_check@v1       GL check step in post.wf@v1 (role:gl)
  human.posting_fix@v1    failure_job on posting_failed (role:gl)

State machine invoice@v1 with a typed WorkflowTriggerSpec on approve.
Domain writes must be idempotent — a kernel retry re-runs the handler.
"""
from __future__ import annotations

import os
import uuid

import psycopg
from pydantic import BaseModel, Field

from keelflow.engine.registry import InMemoryRegistry
from keelflow.sdk import (
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
    StateMachineDefinition,
    StepSpec,
    TaskUiSpec,
    TransitionSpec,
    WorkflowDefinition,
    WorkflowTriggerSpec,
)

DSN_ENV = "KEELFLOW_DSN"

PERMISSIONS = {
    "u1":   {"inv.submit"},
    "fin1": {"inv.submit", "inv.approve"},
    "gl1":  {"inv.submit"},
    "mgr":  {"inv.submit", "inv.approve", "perm:*", "*"},
}
ROLES = {
    "u1":   [],
    "fin1": ["finance"],
    "gl1":  ["gl"],
    "mgr":  ["finance", "gl", "manager"],
}


class InvPayload(BaseModel):
    invoice_no: str
    simulate_failure: bool = False


class ReviewPayload(BaseModel):
    entity_type: str = "invoice"
    entity_id: str = ""
    sm_actions: dict[str, str] = Field(default_factory=dict)
    journal_ref: str | None = None


class PostLedgerH(JobHandler):
    """Insert the ledger posting inside the dispatch TX (ctx.tx).

    Idempotent by invoice_no UNIQUE — a retry upserts the same row.
    `fail_post` flag on the invoice (or payload) simulates a technical
    failure; deterministic so retries don't accidentally succeed.
    """

    async def run(self, ctx, payload: InvPayload):
        async with await psycopg.AsyncConnection.connect(
            os.environ[DSN_ENV]
        ) as probe:
            row = await (await probe.execute(
                "SELECT amount, fail_post FROM demo_invoices WHERE number=%s",
                (payload.invoice_no,),
            )).fetchone()
        if row is None:
            return JobResult(status="failed",
                             error=f"invoice {payload.invoice_no} missing")
        if payload.simulate_failure or row[1]:
            raise RuntimeError(
                f"ledger post simulated failure ({payload.invoice_no})")
        # domain write in the SAME TX as job finish + workflow advance
        await ctx.tx.execute(
            "INSERT INTO demo_ledger_postings"
            " (invoice_no, amount, posted_by)"
            " VALUES (%s, %s, %s)"
            " ON CONFLICT (invoice_no) DO UPDATE SET"
            " amount=EXCLUDED.amount, posted_at=now()",
            (payload.invoice_no, row[0], ctx.actor.id),
        )
        return JobResult(status="done",
                         outputs={"posted": True, "amount": float(row[0])})


class RecordPostingH(JobHandler):
    async def run(self, ctx, payload: InvPayload):
        ref = f"JV-{uuid.uuid5(uuid.NAMESPACE_DNS, payload.invoice_no)}"
        await ctx.tx.execute(
            "INSERT INTO demo_gl_records (invoice_no, journal_ref, recorded_by)"
            " VALUES (%s, %s, %s)"
            " ON CONFLICT (invoice_no) DO NOTHING",
            (payload.invoice_no, ref, ctx.actor.id),
        )
        return JobResult(status="done", outputs={"journal_ref": ref})


class _NoopH(JobHandler):
    """Human task types never dispatch — the inbox completes them."""

    async def run(self, ctx, payload):  # pragma: no cover
        return JobResult(status="done")


def build_registry() -> InMemoryRegistry:
    reg = InMemoryRegistry()
    reg.register_job_type(JobTypeSpec(
        type="inv.post_ledger@v1", input_schema=InvPayload,
        handler=PostLedgerH, retry=RetryPolicy(max_attempts=1)))
    reg.register_job_type(JobTypeSpec(
        type="inv.record_posting@v1", input_schema=InvPayload,
        handler=RecordPostingH))
    reg.register_job_type(JobTypeSpec(
        type="human.review@v1", input_schema=ReviewPayload,
        handler=_NoopH, is_human=True,
        ui=TaskUiSpec(assignee_role="finance")))
    reg.register_job_type(JobTypeSpec(
        type="human.gl_check@v1", input_schema=ReviewPayload,
        handler=_NoopH, is_human=True,
        ui=TaskUiSpec(assignee_role="gl")))
    reg.register_job_type(JobTypeSpec(
        type="human.posting_fix@v1", input_schema=ReviewPayload,
        handler=_NoopH, is_human=True,
        ui=TaskUiSpec(assignee_role="gl")))

    reg.register_state_machine(StateMachineDefinition(
        name="invoice@v1", entity="invoice",
        states=("draft", "in_review", "posting", "posted",
                "needs_fix", "posting_rejected", "posting_failed"),
        initial="draft",
        transitions=(
            TransitionSpec(
                "draft", "in_review", "submit",
                guard={"permission": "inv.submit",
                       "assignee_role": "finance",
                       "sm_actions": {"approve": "approve",
                                      "reject": "reject"}},
                creates_job="human.review@v1"),
            TransitionSpec(
                "in_review", "posting", "approve",
                guard={"permission": "inv.approve"},
                triggers_workflow=WorkflowTriggerSpec(
                    workflow="post.wf@v1",
                    on_done="posted",
                    on_rejected="posting_rejected",
                    on_failed="posting_failed",
                    failure_job="human.posting_fix@v1")),
            TransitionSpec("in_review", "needs_fix", "reject"),
            TransitionSpec("needs_fix", "in_review", "resubmit",
                           guard={"permission": "inv.submit"}),
            TransitionSpec("posting", "posted", "posted"),
            TransitionSpec("posting", "posting_rejected", "posting_rejected"),
            TransitionSpec("posting_rejected", "posting", "repost"),
            TransitionSpec("posting", "posting_failed", "posting_failed"),
            TransitionSpec("posting_failed", "posting", "retry_post"),
        ),
    ))

    reg.register_workflow(WorkflowDefinition(
        name="post.wf@v1",
        steps=(
            StepSpec(id="ledger", type="inv.post_ledger@v1",
                     input={"invoice_no": "${{ inputs.entity_id }}",
                            "simulate_failure": False}),
            StepSpec(id="record", type="inv.record_posting@v1",
                     input={"invoice_no": "${{ inputs.entity_id }}"},
                     needs=("ledger",)),
            StepSpec(id="gl_check", type="human.gl_check@v1",
                     input={"entity_type": "${{ inputs.entity_type }}",
                            "entity_id": "${{ inputs.entity_id }}",
                            "journal_ref": "${{ steps.record.outputs.journal_ref }}"},
                     needs=("record",)),
        ),
    ))
    return reg
