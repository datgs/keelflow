"""invoice_flow integration test — full lifecycle against real Postgres.

Covers the demo's user journey:
  u1 submits → fin1 approves (via human.review) → posting DAG runs →
  gl1 completes gl_check → entity posted (SM callback on workflow done).

Plus the failure paths: GL business reject → repost; technical failure →
posting_failed + fix-up task → retry.

Requires Postgres at KEELFLOW_TEST_DSN (skipped when unavailable).
"""
from __future__ import annotations

import asyncio
import os

import psycopg
import pytest
import pytest_asyncio
from invoice_flow.registry import DSN_ENV, build_registry
from psycopg.rows import dict_row

from conftest import declare_default_queue, make_worker, open_kernel
from keelflow.engine import worker as worker_mod
from keelflow.engine.audit import PostgresAuditService
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.statemachine import EngineStateMachineService
from keelflow.engine.workflow import WorkflowService
from keelflow.sdk import Actor, EntityRef, JobContext

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow")
KERNEL_PREFIX = "keelflow_test_"
os.environ.setdefault(DSN_ENV, DSN)

DEMO_SCHEMA = """
CREATE TABLE IF NOT EXISTS demo_invoices (
    number TEXT PRIMARY KEY, vendor TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL, currency TEXT NOT NULL DEFAULT 'USD',
    fail_post BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS demo_ledger_postings (
    id BIGSERIAL PRIMARY KEY, invoice_no TEXT NOT NULL UNIQUE,
    amount NUMERIC(12,2) NOT NULL, posted_by TEXT NOT NULL,
    posted_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS demo_gl_records (
    invoice_no TEXT PRIMARY KEY, journal_ref TEXT NOT NULL,
    recorded_by TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now());
"""

CTX = {
    "u1": JobContext(actor=Actor.user("u1"), permissions=frozenset({"inv.submit"})),
    "fin1": JobContext(actor=Actor.user("fin1"),
                       permissions=frozenset({"inv.submit", "inv.approve"})),
    "gl1": JobContext(actor=Actor.user("gl1")),
}


async def _conn(autocommit=True):
    return await psycopg.AsyncConnection.connect(DSN, autocommit=autocommit)


async def _wait(db, sql, args, pred, timeout=15.0):
    for _ in range(int(timeout * 10)):
        row = await (await db.execute(sql, args)).fetchone()
        if row and pred(row):
            return row
        await asyncio.sleep(0.1)
    return row


@pytest_asyncio.fixture
async def db():
    try:
        conn = await _conn()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        from keelflow.postgres.schema import apply_async

        await conn.execute(DEMO_SCHEMA)
        await apply_async(conn)
        # fresh DB (CI): kernel tables must exist before we truncate them.
        async with open_kernel("chancy", prefix=KERNEL_PREFIX):
            pass
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq, keelflow_entity_state,"
            " demo_invoices, demo_ledger_postings, demo_gl_records CASCADE")
        await conn.execute(
            f"TRUNCATE {KERNEL_PREFIX}jobs, {KERNEL_PREFIX}workers,"
            f" {KERNEL_PREFIX}queues CASCADE")
        await conn.execute(
            "INSERT INTO demo_invoices (number, vendor, amount)"
            " VALUES ('INV-9001','Test Vendor', 100.00),"
            "        ('INV-9002','Fail Vendor', 50.00)")
        yield conn


@pytest_asyncio.fixture
async def env(db, kernel_kind):
    async with open_kernel(kernel_kind, prefix=KERNEL_PREFIX) as kernel:
        await declare_default_queue(kernel, kernel_kind, concurrency=4)
        reg = build_registry()
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        audit = PostgresAuditService()
        sm = EngineStateMachineService(reg, kernel, audit=audit)
        jobs = JobService(reg, kernel)
        human = HumanTaskService(reg, kernel, sm=sm)
        wf = WorkflowService(reg, jobs)
        yield {"sm": sm, "human": human, "wf": wf, "kernel": kernel, "kind": kernel_kind}


async def _invoice(db, no, state="draft"):
    await db.execute(
        "INSERT INTO keelflow_entity_state (entity_type, entity_id, state)"
        " VALUES ('invoice', %s, %s)"
        " ON CONFLICT (entity_type, entity_id) DO UPDATE SET state=%s",
        (no, state, state))


async def _state(db, no):
    row = await (await db.execute(
        "SELECT state FROM keelflow_entity_state"
        " WHERE entity_type='invoice' AND entity_id=%s", (no,))).fetchone()
    return row[0] if row else None


@pytest.mark.asyncio
async def test_happy_path_sm_to_posted(env, db):
    """submit → review task → approve → DAG → gl_check → posted."""
    await _invoice(db, "INV-9001")
    ent = EntityRef(type="invoice", id="INV-9001")

    async with (await _conn(False)) as c, c.transaction(), \
            c.cursor(row_factory=dict_row) as cur:
        await env["sm"].transition(ent, "submit", CTX["u1"], cur)
    assert await _state(db, "INV-9001") == "in_review"

    # review task assigned to role:finance; fin1 sees it, gl1 doesn't
    fin_tasks = await env["human"].my_tasks(
        db, Actor.user("fin1"), roles=["finance"])
    gl_tasks = await env["human"].my_tasks(
        db, Actor.user("gl1"), roles=["gl"])
    assert any(t["payload"]["entity_id"] == "INV-9001" for t in fin_tasks)
    assert not any(
        t["payload"].get("entity_id") == "INV-9001" and t["type"] == "human.review@v1"
        for t in gl_tasks)

    task = next(t for t in fin_tasks
                if t["payload"].get("entity_id") == "INV-9001")
    await env["human"].claim(db, str(task["id"]), Actor.user("fin1"),
                         roles=["finance"])
    # complete_and_transition: approve → posting + workflow triggered
    await env["human"].complete_and_transition(
            db, str(task["id"]), Actor.user("fin1"), "approve", CTX["fin1"],
            roles=["finance"])
    assert await _state(db, "INV-9001") == "posting"

    w = make_worker(env["kernel"], env["kind"], concurrency=4)
    await w.start()
    try:
        # ledger + record run, then gl_check appears in gl1's inbox
        for _ in range(150):
            gl_tasks = await env["human"].my_tasks(
                db, Actor.user("gl1"), roles=["gl"])
            gl_task = next(
                (t for t in gl_tasks if t["type"] == "human.gl_check@v1"),
                None)
            if gl_task:
                break
            await asyncio.sleep(0.1)
        assert gl_task is not None
        # separation of duties: run is blocked on a human while ledger+record done
        run = await (await db.execute(
            "SELECT status FROM keelflow_workflow_runs"
            " WHERE workflow='post.wf@v1'"
        )).fetchone()
        assert run[0] == "running"

        await env["human"].complete(
            db, str(gl_task["id"]), Actor.user("gl1"), {"decision": "approve"},
            roles=["gl"])
        # callback fires on_done → posted, inside terminal TX
        for _ in range(150):
            if await _state(db, "INV-9001") == "posted":
                break
            await asyncio.sleep(0.1)
        assert await _state(db, "INV-9001") == "posted"
        # domain side effects landed in the handler TX
        posting = await (await db.execute(
            "SELECT invoice_no FROM demo_ledger_postings"
            " WHERE invoice_no='INV-9001'")).fetchone()
        assert posting is not None
        gl = await (await db.execute(
            "SELECT journal_ref FROM demo_gl_records"
            " WHERE invoice_no='INV-9001'")).fetchone()
        assert gl is not None
        # audit trail covers the SM transitions (incl. callback on posted)
        actions = await (await db.execute(
            "SELECT array_agg(action ORDER BY id) FROM keelflow_audit"
            " WHERE entity_type='invoice' AND entity_id='INV-9001'"
        )).fetchone()
        assert "invoice.posted" in actions[0]
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_business_reject_and_repost(env, db):
    """GL reject → posting_rejected; repost resumes the same run → posted."""
    await _invoice(db, "INV-9001")
    ent = EntityRef(type="invoice", id="INV-9001")
    async with (await _conn(False)) as c, c.transaction(), \
            c.cursor(row_factory=dict_row) as cur:
        await env["sm"].transition(ent, "submit", CTX["u1"], cur)
    task = await (await db.execute(
        "SELECT id FROM keelflow_jobs WHERE type='human.review@v1'"
        " AND payload->>'entity_id'='INV-9001'")).fetchone()
    await env["human"].complete_and_transition(
        db, str(task[0]), Actor.user("fin1"), "approve", CTX["fin1"],
        roles=["finance"])

    w = make_worker(env["kernel"], env["kind"], concurrency=4)
    await w.start()
    try:
        for _ in range(150):
            row = await (await db.execute(
                "SELECT id FROM keelflow_jobs WHERE type='human.gl_check@v1'"
            )).fetchone()
            if row:
                break
            await asyncio.sleep(0.1)
        await env["human"].complete(
            db, str(row[0]), Actor.user("gl1"),
            {"decision": "reject", "note": "wrong cost center"},
            roles=["gl"])
        for _ in range(150):
            if await _state(db, "INV-9001") == "posting_rejected":
                break
            await asyncio.sleep(0.1)
        assert await _state(db, "INV-9001") == "posting_rejected"
        run = await (await db.execute(
            "SELECT id, terminal_outcome FROM keelflow_workflow_runs"
            " WHERE workflow='post.wf@v1'")).fetchone()
        assert run[1] == "rejected"

        # repost → resume_run re-executes the rejected gl_check step
        await env["wf"].resume_run(db, str(run[0]), CTX["fin1"])
        assert await _state(db, "INV-9001") == "posting"
        for _ in range(150):
            row = await (await db.execute(
                "SELECT id FROM keelflow_jobs WHERE type='human.gl_check@v1'"
                " AND status='pending'")).fetchone()
            if row:
                break
            await asyncio.sleep(0.1)
        await env["human"].complete(
            db, str(row[0]), Actor.user("gl1"), {"decision": "approve"},
            roles=["gl"])
        for _ in range(150):
            if await _state(db, "INV-9001") == "posted":
                break
            await asyncio.sleep(0.1)
        assert await _state(db, "INV-9001") == "posted"
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_technical_failure_and_fix_task(env, db):
    """fail_post → posting_failed + fix task; fix + resume → posted."""
    await _invoice(db, "INV-9002")
    await db.execute(
        "UPDATE demo_invoices SET fail_post=TRUE WHERE number='INV-9002'")
    ent = EntityRef(type="invoice", id="INV-9002")
    async with (await _conn(False)) as c, c.transaction(), \
            c.cursor(row_factory=dict_row) as cur:
        await env["sm"].transition(ent, "submit", CTX["u1"], cur)
    task = await (await db.execute(
        "SELECT id FROM keelflow_jobs WHERE type='human.review@v1'"
        " AND payload->>'entity_id'='INV-9002'")).fetchone()
    await env["human"].complete_and_transition(
        db, str(task[0]), Actor.user("fin1"), "approve", CTX["fin1"],
        roles=["finance"])

    w = make_worker(env["kernel"], env["kind"], concurrency=4)
    await w.start()
    try:
        for _ in range(150):
            if await _state(db, "INV-9002") == "posting_failed":
                break
            await asyncio.sleep(0.1)
        assert await _state(db, "INV-9002") == "posting_failed"
        # fix-up task created for role:gl (failure_job), posting row absent
        fix = await (await db.execute(
            "SELECT id, assignee FROM keelflow_jobs"
            " WHERE type='human.posting_fix@v1'")).fetchone()
        assert fix is not None
        posting = await (await db.execute(
            "SELECT 1 FROM demo_ledger_postings WHERE invoice_no='INV-9002'"
        )).fetchone()
        assert posting is None  # rolled back with the handler TX

        # clear the flag (gl fixed the data), then resume
        await db.execute(
            "UPDATE demo_invoices SET fail_post=FALSE WHERE number='INV-9002'")
        run = await (await db.execute(
            "SELECT id FROM keelflow_workflow_runs"
            " WHERE terminal_outcome='failed'")).fetchone()
        await env["wf"].resume_run(db, str(run[0]), CTX["gl1"])

        # ledger re-runs → gl_check appears again → approve → posted
        for _ in range(150):
            row = await (await db.execute(
                "SELECT id FROM keelflow_jobs WHERE type='human.gl_check@v1'"
                " AND status='pending'")).fetchone()
            if row:
                break
            await asyncio.sleep(0.1)
        assert row is not None
        await env["human"].complete(
            db, str(row[0]), Actor.user("gl1"), {"decision": "approve"},
            roles=["gl"])
        for _ in range(150):
            if await _state(db, "INV-9002") == "posted":
                break
            await asyncio.sleep(0.1)
        assert await _state(db, "INV-9002") == "posted"
    finally:
        await w.stop(timeout=5)
