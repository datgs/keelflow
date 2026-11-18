import pytest
from pydantic import BaseModel
from pyerp.sdk import (
    Actor, ActorType, JobContext, JobStatus, JobResult,
    JobTypeSpec, RetryPolicy, ConcurrencySpec, WorkflowDefinition,
    StepSpec, StateMachineDefinition, TransitionSpec, JobHandler,
)


def test_actor_model():
    u = Actor.user("u_1")
    assert u.type == ActorType.USER
    j = Actor.job("j_9", via=u)
    assert j.via == u


def test_context_require():
    ctx = JobContext(actor=Actor.user("u_1"), permissions=frozenset({"a.b.c"}))
    ctx.require("a.b.c")
    with pytest.raises(PermissionError):
        ctx.require("x.y.z")


def test_context_wildcard():
    ctx = JobContext(actor=Actor.system(), permissions=frozenset({"*"}))
    ctx.require("anything.goes")


class Pay(BaseModel):
    invoice_id: str


def test_job_type_spec():
    spec = JobTypeSpec(
        type="invoice.match@v1",
        input_schema=Pay,
        retry=RetryPolicy(max_attempts=5),
        concurrency=ConcurrencySpec(key="payload.vendor_id"),
    )
    assert spec.retry.max_attempts == 5
    assert spec.concurrency.key == "payload.vendor_id"
    assert spec.payload_max_bytes == 8192


def test_workflow_definition():
    wf = WorkflowDefinition(
        name="invoice.processing@v1",
        steps=(
            StepSpec(id="match", type="invoice.match@v1", input={"x": 1}),
            StepSpec(id="post", type="invoice.post@v1", needs=("match",)),
        ),
    )
    assert wf.steps[1].needs == ("match",)


def test_state_machine_definition():
    sm = StateMachineDefinition(
        name="invoice@v1",
        entity="invoice",
        states=("draft", "approved"),
        transitions=(TransitionSpec("draft", "approved", "approve"),),
        initial="draft",
    )
    assert sm.initial == "draft"


def test_handler_is_abstract():
    class H(JobHandler):
        async def run(self, ctx, payload):
            return JobResult(status="done")

    import asyncio
    r = asyncio.run(H().run(JobContext(actor=Actor.system()), Pay(invoice_id="i1")))
    assert r.status == "done"
