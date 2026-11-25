import pytest
from pydantic import BaseModel

from keelflow.sdk import (
    Actor,
    ActorType,
    ConcurrencySpec,
    ExecutionContext,
    JobCancelled,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
    StateMachineDefinition,
    StepSpec,
    TransitionSpec,
    WorkflowDefinition,
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
    assert spec.progress_enabled is False
    assert spec.no_progress_timeout_seconds is None


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


def test_job_cancelled_is_not_exception_subclass():
    """BaseException subclass: ordinary `except Exception` in a handler
    cannot swallow a checkpoint cancellation."""
    assert issubclass(JobCancelled, BaseException)
    assert not issubclass(JobCancelled, Exception)


def test_cooperative_cancel_spec_default():
    assert JobTypeSpec(type="t@v1", input_schema=Pay,
                       handler=JobHandler).cooperative_cancel is False
    spec = JobTypeSpec(type="t2@v1", input_schema=Pay, handler=JobHandler,
                       cooperative_cancel=True)
    assert spec.cooperative_cancel is True


def test_checkpoint_requires_opt_in_and_tx():
    import asyncio

    async def main():
        exc = ExecutionContext(job_id="j1", cooperative_cancel=False)
        with pytest.raises(RuntimeError, match="cooperative_cancel"):
            await exc.checkpoint()
        exc2 = ExecutionContext(job_id="j1", cooperative_cancel=True, uow=None)
        with pytest.raises(RuntimeError, match="handler transaction"):
            await exc2.checkpoint()

    asyncio.run(main())


def test_progress_requires_opt_in_and_valid_counters():
    import asyncio

    async def main():
        from keelflow.sdk import ExecutionContext

        disabled = ExecutionContext(job_id="j1")
        with pytest.raises(RuntimeError, match="progress_enabled"):
            await disabled.report_progress(1, 2)
        enabled = ExecutionContext(job_id="j1", progress_enabled=True)
        with pytest.raises(ValueError, match="current/total"):
            await enabled.report_progress(3, 2)
        with pytest.raises(ValueError, match="256 characters"):
            await enabled.report_progress(1, 2, message="x" * 257)

    asyncio.run(main())


def test_handler_has_no_heartbeat_hook():
    """Heartbeat is kernel-owned (chancy worker liveness); the unused
    handler hook was removed — only the experimental on_cancel remains."""
    assert not hasattr(JobHandler, "heartbeat")
    assert hasattr(JobHandler, "on_cancel")
