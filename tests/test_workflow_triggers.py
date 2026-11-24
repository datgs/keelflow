"""Workflow-trigger callbacks (demo prerequisite contract).

Covers the demo-suite engine prerequisites:
- typed `triggers_workflow` persists metadata on the run;
- on_succeeded applies in the run's terminal transaction;
- GL business reject → run 'rejected' + on_rejected action;
- technical failure → on_failed + failure_job human task;
- resume_run re-executes a failed/rejected run in place;
- complete_and_transition links an SM-created human task to its action.
"""
from __future__ import annotations

import asyncio
import os

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy, Queue, Worker
from pydantic import BaseModel

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.engine.statemachine import EngineStateMachineService
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import (
    Actor,
    EntityRef,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
    StateMachineDefinition,
    StepSpec,
    TransitionSpec,
    WorkflowDefinition,
    WorkflowTriggerSpec,
)

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
CHANCY_PREFIX = "keelflow_test_"


class P(BaseModel):
    pass


class OkH(JobHandler):
    async def run(self, ctx, payload):
        return JobResult(status="done", outputs={"ok": True})


class FailH(JobHandler):
    """Declared-fail handler (deterministic 'posting_failed')."""

    async def run(self, ctx, payload):
        return JobResult(status="failed", error="simulated failure")


async def _conn(autocommit: bool = True):
    return await psycopg.AsyncConnection.connect(DSN, autocommit=autocommit)


@pytest_asyncio.fixture
async def db():
    try:
        conn = await _conn()
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq, keelflow_entity_state CASCADE"
        )
        await conn.execute(
            f"TRUNCATE {CHANCY_PREFIX}jobs, {CHANCY_PREFIX}workers,"
            f" {CHANCY_PREFIX}queues CASCADE"
        )
        yield conn


@pytest_asyncio.fixture
async def env(db):
    async with Chancy(DSN, prefix=CHANCY_PREFIX) as kernel:
        await kernel.migrate()
        await kernel.declare(
            Queue("default", concurrency=4, executor=Chancy.Executor.Async,
                  eager_polling=True),
            upsert=True,
        )
        reg = InMemoryRegistry()
        reg.register_permission("inv.approve")
        reg.register_job_type(JobTypeSpec(type="wf.step@v1", input_schema=P,
                                          handler=OkH))
        reg.register_job_type(JobTypeSpec(
            type="wf.fail@v1", input_schema=P, handler=FailH,
            retry=RetryPolicy(max_attempts=2)))
        reg.register_job_type(JobTypeSpec(
            type="human.review@v1", input_schema=P, handler=OkH,
            is_human=True))
        reg.register_job_type(JobTypeSpec(
            type="human.fix@v1", input_schema=P, handler=OkH, is_human=True))
        jobs = JobService(reg, kernel, PostgresStorage(DSN))
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        sm = EngineStateMachineService(reg, kernel)
        await sm.ensure_schema(db)
        human = HumanTaskService(reg, kernel, sm=sm)
        wf = WorkflowService(reg, jobs)
        yield {"reg": reg, "jobs": jobs, "sm": sm, "human": human, "wf": wf,
               "kernel": kernel}


def _invoice_sm(triggers_workflow=None) -> StateMachineDefinition:
    return StateMachineDefinition(
        name="invoice@v1", entity="invoice",
        states=("draft", "in_review", "posting", "posted",
                "posting_rejected", "posting_failed"),
        transitions=(
            TransitionSpec("draft", "in_review", "submit"),
            TransitionSpec("in_review", "posting", "approve",
                           guard={"permission": "inv.approve"},
                           triggers_workflow=triggers_workflow),
            TransitionSpec("posting", "posted", "posted"),
            TransitionSpec("posting", "posting_rejected", "posting_rejected"),
            TransitionSpec("posting_rejected", "posting", "repost"),
            TransitionSpec("posting", "posting_failed", "posting_failed"),
            TransitionSpec("posting_failed", "posting", "retry_post"),
        ),
        initial="draft",
    )


async def _to_posting(env, entity, ctx):
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        await env["sm"].transition(entity, "submit", ctx, cur)
        await env["sm"].transition(entity, "approve", ctx, cur)


async def _wait(db, sql, args, want, timeout=15.0):
    """Poll until first column equals `want`, or `want` callable(row) is true."""
    for _ in range(int(timeout * 10)):
        row = await (await db.execute(sql, args)).fetchone()
        if row:
            ok = want(row) if callable(want) else row[0] == want
            if ok:
                return row
        await asyncio.sleep(0.1)
    return row


@pytest.mark.asyncio
async def test_trigger_metadata_and_on_succeeded(env, db):
    """Typed trigger persists; on_succeeded fires in the run's terminal TX."""
    env["reg"].register_state_machine(_invoice_sm(
        triggers_workflow=WorkflowTriggerSpec(
            workflow="post.wf@v1",
            on_succeeded="posted",
            on_rejected="posting_rejected",
            on_failed="posting_failed",
            failure_job="human.fix@v1",
        )))
    env["reg"].register_workflow(WorkflowDefinition(
        name="post.wf@v1",
        steps=(StepSpec(id="s1", type="wf.step@v1"),),
    ))
    ctx = JobContext(actor=Actor.user("fin1"),
                     permissions=frozenset({"inv.approve"}))
    entity = EntityRef(type="invoice", id="inv-t1")
    await _to_posting(env, entity, ctx)

    run = await (await db.execute(
        "SELECT id, triggered_by_entity_id, trigger_config, callback_state"
        " FROM keelflow_workflow_runs WHERE workflow='post.wf@v1'"
    )).fetchone()
    assert run is not None
    assert run[1] == "inv-t1"
    assert run[2]["on_succeeded"] == "posted"
    assert run[2]["failure_job"] == "human.fix@v1"

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        await _wait(db, "SELECT status FROM keelflow_workflow_runs WHERE id=%s",
                    (run[0],), "done")
        row = await (await db.execute(
            "SELECT terminal_outcome, callback_state"
            " FROM keelflow_workflow_runs WHERE id=%s", (run[0],)
        )).fetchone()
        assert row[0] == "succeeded"
        assert row[1] == "done"
        # entity moved to posted via callback — inside terminal TX
        state = await (await db.execute(
            "SELECT state FROM keelflow_entity_state"
            " WHERE entity_type='invoice' AND entity_id='inv-t1'"
        )).fetchone()
        assert state[0] == "posted"
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_business_reject_outcome(env, db):
    """Human step decision='reject' → run 'rejected' → on_rejected fires."""
    env["reg"].register_state_machine(_invoice_sm(
        triggers_workflow=WorkflowTriggerSpec(
            workflow="post.wf@v1",
            on_succeeded="posted",
            on_rejected="posting_rejected",
            on_failed="posting_failed",
        )))
    env["reg"].register_workflow(WorkflowDefinition(
        name="post.wf@v1",
        steps=(
            StepSpec(id="s1", type="wf.step@v1"),
            StepSpec(id="gl", type="human.review@v1", needs=("s1",)),
        ),
    ))
    ctx = JobContext(actor=Actor.user("fin1"),
                     permissions=frozenset({"inv.approve"}))
    entity = EntityRef(type="invoice", id="inv-t2")
    await _to_posting(env, entity, ctx)

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        for _ in range(150):
            human_row = await (await db.execute(
                "SELECT id FROM keelflow_jobs WHERE type='human.review@v1'"
            )).fetchone()
            if human_row:
                break
            await asyncio.sleep(0.1)
        assert human_row
        await env["human"].complete(
            db, str(human_row[0]), Actor.user("gl1"),
            {"decision": "reject", "note": "bad GL code"}, roles=["gl"])

        row = await _wait(
            db, "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
            " WHERE workflow='post.wf@v1'", (),
            lambda r: r[0] == "done" and r[1] == "rejected")
        assert row[0] == "done" and row[1] == "rejected"
        state = await (await db.execute(
            "SELECT state FROM keelflow_entity_state"
            " WHERE entity_id='inv-t2'"
        )).fetchone()
        assert state[0] == "posting_rejected"

        # resume_run: reopen the rejected step, entity back to posting
        run_id = await (await db.execute(
            "SELECT id FROM keelflow_workflow_runs WHERE workflow='post.wf@v1'"
        )).fetchone()
        ctx2 = JobContext(actor=Actor.user("fin1"))
        await env["wf"].resume_run(db, str(run_id[0]), ctx2)
        # entity back to posting (repost transition applied internally)
        state = await (await db.execute(
            "SELECT state FROM keelflow_entity_state WHERE entity_id='inv-t2'"
        )).fetchone()
        assert state[0] == "posting"
        # note: no run_id needed — resume_run already re-enqueued the step
        for _ in range(150):
            n = await (await db.execute(
                "SELECT count(*) FROM keelflow_jobs"
                " WHERE type='human.review@v1' AND status='pending'"
            )).fetchone()
            if n[0] == 1:
                break
            await asyncio.sleep(0.1)
        assert n[0] == 1
        # approve this time → run completes → entity posted
        task = await (await db.execute(
            "SELECT id FROM keelflow_jobs WHERE type='human.review@v1'"
            " AND status='pending'"
        )).fetchone()
        await env["human"].complete(
            db, str(task[0]), Actor.user("gl1"), {"decision": "approve"},
            roles=["gl"])
        row = await _wait(
            db, "SELECT status FROM keelflow_workflow_runs WHERE id=%s",
            (str(run_id[0]),), "done")
        state = await (await db.execute(
            "SELECT state FROM keelflow_entity_state WHERE entity_id='inv-t2'"
        )).fetchone()
        assert state[0] == "posted"
        # exactly-once: second resume attempt on the same run fails
        with pytest.raises((ValueError, KeyError)):
            await env["wf"].resume_run(db, str(run_id[0]), ctx2)
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_technical_failure_callback(env, db):
    """Declared-fail step → run failed → on_failed + failure_job task."""
    env["reg"].register_state_machine(_invoice_sm(
        triggers_workflow=WorkflowTriggerSpec(
            workflow="post.wf@v1",
            on_succeeded="posted",
            on_failed="posting_failed",
            failure_job="human.fix@v1",
        )))
    env["reg"].register_workflow(WorkflowDefinition(
        name="post.wf@v1",
        steps=(StepSpec(id="s1", type="wf.fail@v1"),),
    ))
    ctx = JobContext(actor=Actor.user("fin1"),
                     permissions=frozenset({"inv.approve"}))
    entity = EntityRef(type="invoice", id="inv-t3")
    await _to_posting(env, entity, ctx)

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        row = await _wait(
            db, "SELECT status FROM keelflow_workflow_runs"
            " WHERE workflow='post.wf@v1'", (), "failed")
        assert row[0] == "failed"
        out = await (await db.execute(
            "SELECT terminal_outcome FROM keelflow_workflow_runs"
            " WHERE workflow='post.wf@v1'"
        )).fetchone()
        assert out[0] == "failed"
        state = await (await db.execute(
            "SELECT state FROM keelflow_entity_state WHERE entity_id='inv-t3'"
        )).fetchone()
        assert state[0] == "posting_failed"
        # failure_job human task was created in the same TX
        fix = await (await db.execute(
            "SELECT id, type FROM keelflow_jobs WHERE type='human.fix@v1'"
        )).fetchone()
        assert fix is not None

        # retry_post path: resume_run on the failed run
        run_id = await (await db.execute(
            "SELECT id FROM keelflow_workflow_runs WHERE workflow='post.wf@v1'"
        )).fetchone()
        await env["wf"].resume_run(
            db, str(run_id[0]), JobContext(actor=Actor.user("op")))
        # The step reopened and re-enqueued (its handler fails again, so the
        # entity may already be back in posting_failed — assert the run
        # reopened rather than a transient entity state).
        sr = await (await db.execute(
            "SELECT status, job_id FROM keelflow_step_runs WHERE run_id=%s",
            (run_id[0],)
        )).fetchone()
        assert sr[1] is not None  # new job enqueued by resume
        run_st = await (await db.execute(
            "SELECT status FROM keelflow_workflow_runs WHERE id=%s",
            (run_id[0],)
        )).fetchone()
        assert run_st[0] in ("running", "failed")
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_complete_and_transition(env, db):
    """SM creates_job task linked via sm_actions: decision → transition."""
    env["reg"].register_state_machine(StateMachineDefinition(
        name="invoice@v1", entity="invoice",
        states=("draft", "in_review", "approved", "rejected"),
        transitions=(
            TransitionSpec(
                "draft", "in_review", "submit",
                creates_job="human.review@v1",
                guard={"assignee_role": "fin",
                       "sm_actions": {"approve": "approve",
                                      "reject": "reject"}}),
            TransitionSpec("in_review", "approved", "approve"),
            TransitionSpec("in_review", "rejected", "reject"),
        ),
        initial="draft",
    ))
    ctx = JobContext(actor=Actor.user("u1"), permissions=frozenset({"*"}))
    entity = EntityRef(type="invoice", id="inv-t4")
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        await env["sm"].transition(entity, "submit", ctx, cur)

    task = await (await db.execute(
        "SELECT id FROM keelflow_jobs WHERE type='human.review@v1'"
    )).fetchone()
    job_id = str(task[0])
    await env["human"].claim(db, job_id, Actor.user("fin2"),
                             roles=["fin"])
    # bad decision rejected before any state change
    with pytest.raises(ValueError, match="not allowed"):
        await env["human"].complete_and_transition(
            db, job_id, Actor.user("fin2"), "maybe", ctx, roles=["fin"])
    # reject → task done + entity rejected atomically
    await env["human"].complete_and_transition(
        db, job_id, Actor.user("fin2"), "reject", ctx, note="no budget",
        roles=["fin"])
    status = await (await db.execute(
        "SELECT status FROM keelflow_jobs WHERE id=%s", (job_id,)
    )).fetchone()
    assert status[0] == "done"
    state = await env["sm"].current(entity, db)
    assert state == "rejected"
