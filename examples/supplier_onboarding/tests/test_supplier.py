"""Demo 2 integration test — supplier onboarding DAG against real Postgres.

Covers: parallel check fan-out (4 adapters), risk scoring, conditional
HITL branches (blocked vs skipped), ERP provisioning + activation,
terminal outcomes, technical failure → resume_run.

Requires Postgres at KEELFLOW_TEST_DSN (skipped when unavailable).
"""
from __future__ import annotations

import asyncio
import os
import time

import psycopg
import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from supplier_onboarding.registry import DSN_ENV, build_registry

from conftest import declare_default_queue, make_worker, open_kernel
from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import Actor, JobContext

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow")
KERNEL_PREFIX = "keelflow_test_"
os.environ.setdefault(DSN_ENV, DSN)

with open(os.path.join(os.path.dirname(__file__), "..", "src",
                       "supplier_onboarding", "schema.sql")) as _fh:
    DEMO_SCHEMA = _fh.read()

ACTOR = Actor.user("u-proc1")


def _ctx() -> JobContext:
    return JobContext(actor=ACTOR, tenant_id="demo",
                      permissions=frozenset({"sup.create"}))


async def _conn(autocommit=True):
    return await psycopg.AsyncConnection.connect(
        DSN, autocommit=autocommit, row_factory=dict_row)


@pytest_asyncio.fixture
async def db():
    try:
        conn = await psycopg.AsyncConnection.connect(
            DSN, autocommit=True, row_factory=dict_row)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        from keelflow.postgres.schema import apply_async

        await apply_async(conn)
        await conn.execute(DEMO_SCHEMA)
        # fresh DB (CI): kernel tables must exist before we truncate them.
        async with open_kernel("chancy", prefix=KERNEL_PREFIX):
            pass
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq, keelflow_entity_state,"
            " demo_suppliers, demo_vendor_accounts, demo_check_results,"
            " demo_bank_requests CASCADE")
        await conn.execute(
            f"TRUNCATE {KERNEL_PREFIX}jobs, {KERNEL_PREFIX}workers,"
            f" {KERNEL_PREFIX}queues CASCADE")
        yield conn


@pytest_asyncio.fixture
async def env(db, kernel_kind):
    async with open_kernel(kernel_kind, prefix=KERNEL_PREFIX) as kernel:
        await declare_default_queue(kernel, kernel_kind, concurrency=6)
        reg = build_registry()
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        jobs = JobService(reg, kernel, PostgresStorage(DSN))
        human = HumanTaskService(reg, kernel)
        yield {"wf": WorkflowService(reg, jobs), "jobs": jobs,
               "human": human, "kernel": kernel, "kind": kernel_kind}


async def _mk_supplier(db, name, bank="OK001", fail=False) -> int:
    await db.execute(
        "INSERT INTO demo_suppliers (name, country, tax_id, contact_email,"
        " bank_ref, fail_sanction, status)"
        " VALUES (%s,'DE',%s,'x@x.example',%s,%s,'requested')",
        (name, f"T-{name[:6]}", bank, fail))
    return (await (await db.execute(
        "SELECT max(id) AS id FROM demo_suppliers")).fetchone())["id"]


async def _wait_run(db, run_id, timeout=60.0):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = await (await db.execute(
            "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
            " WHERE id=%s", (run_id,))).fetchone()
        if r["status"] in ("done", "failed"):
            return r
        await asyncio.sleep(0.2)
    raise AssertionError(f"run {run_id} still running after {timeout}s")


async def _wait_step(db, run_id, step_id, status, timeout=60.0):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = await (await db.execute(
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id=%s", (run_id, step_id))).fetchone()
        if r and r["status"] == status:
            return r
        await asyncio.sleep(0.2)
    raise AssertionError(f"step {step_id} not {status} after {timeout}s")


async def _resolve_bank_wait(db, env, sid):
    """Simulate the app-owned integration: register the provider request
    in the app's correlation table, then run the same handler the
    /integrations/bank/callback route executes — in one transaction."""
    from supplier_onboarding.app import handle_bank_callback

    job = None
    for _ in range(150):
        job = await (await db.execute(
            "SELECT j.id FROM keelflow_jobs j"
            " JOIN keelflow_step_runs sr ON sr.job_id = j.id"
            " WHERE j.type='wait.bank_verify@v1'"
            "   AND j.status IN ('pending','claimed')"
            "   AND j.payload->>'supplier_id' = %s", (str(sid),)
        )).fetchone()
        if job:
            break
        await asyncio.sleep(0.2)
    assert job, "bank wait job never appeared"
    ref = f"bk-test-{sid}"
    await db.execute(
        "INSERT INTO demo_bank_requests (provider_ref, supplier_id, job_id)"
        " VALUES (%s, %s, %s)", (ref, sid, str(job["id"])))
    async with (await _conn(False)) as c:
        await handle_bank_callback(c, env["jobs"], ref, "verified")
        await c.commit()


async def _blocked_human_tasks(db, run_id):
    return await (await db.execute(
        "SELECT step_id, job_id FROM keelflow_step_runs"
        " WHERE run_id=%s AND status='blocked'", (run_id,))).fetchall()


async def _complete_humans(db, env, run_id, decision="approve"):
    async with (await _conn(autocommit=False)) as c:
        for row in await _blocked_human_tasks(db, run_id):
            await env["human"].complete(
                c, str(row["job_id"]), ACTOR, {"decision": decision},
                roles=["compliance", "finance"])
        await c.commit()


@pytest.mark.asyncio
async def test_parallel_checks_and_conditional_hitl(env, db):
    """Full run: 4 parallel adapters → risk → conditional HITL → ERP → active."""
    sid = await _mk_supplier(db, "parallel parts")

    w = make_worker(env["kernel"], env["kind"], concurrency=6)
    await w.start()
    try:
        async with (await _conn(False)) as c, c.transaction(), \
                c.cursor() as cur:
            ref = await env["wf"].start(
                "sup.onboard@v1", {"supplier_id": sid}, _ctx(), cur)

        # machine checks finish; bank parks 'blocked' — no worker
        # owns it until the app-owned provider callback signals it
        for s in ("sanction", "media", "credit"):
            await _wait_step(db, ref.id, s, "done")
        await _wait_step(db, ref.id, "bank", "blocked")
        await _wait_step(db, ref.id, "risk_score", "pending")
        await _resolve_bank_wait(db, env, sid)
        await _wait_step(db, ref.id, "bank", "done")
        await _wait_step(db, ref.id, "risk_score", "done")

        checks = await (await db.execute(
            "SELECT count(*) AS n FROM demo_check_results WHERE supplier_id=%s",
            (sid,))).fetchone()
        assert checks["n"] == 4           # each adapter wrote a result row

        risk = (await (await db.execute(
            "SELECT risk FROM demo_suppliers WHERE id=%s", (sid,))
        ).fetchone())["risk"]
        assert risk in ("low", "medium", "high")

        # conditional branches: exactly the right human steps are blocked,
        # the rest skipped — whatever the risk
        expect = {
            "comp_review": "blocked" if risk != "low" else "skipped",
            "credit_signoff": "blocked" if risk == "high" else "skipped",
            "bank_conf": "blocked" if risk == "low" else "skipped",
        }
        for step, want in expect.items():
            await _wait_step(db, ref.id, step, want)

        await _complete_humans(db, env, ref.id)
        r = await _wait_run(db, ref.id)
        assert r["status"] == "done" and r["terminal_outcome"] == "succeeded"

        v = await (await db.execute(
            "SELECT vendor_no, payment_terms, bank_verified, activated_at"
            " FROM demo_vendor_accounts WHERE supplier_id=%s", (sid,))
        ).fetchone()
        assert v["activated_at"] is not None
        sup = await (await db.execute(
            "SELECT status FROM demo_suppliers WHERE id=%s", (sid,))
        ).fetchone()
        assert sup["status"] == "active"
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_technical_failure_and_resume(env, db):
    """fail_sanction → sanction step fails → run failed; fix + resume_run
    re-executes in place to completion."""
    sid = await _mk_supplier(db, "fail parts", fail=True)

    w = make_worker(env["kernel"], env["kind"], concurrency=6)
    await w.start()
    try:
        async with (await _conn(False)) as c, c.transaction(), \
                c.cursor() as cur:
            ref = await env["wf"].start(
                "sup.onboard@v1", {"supplier_id": sid}, _ctx(), cur)
        r = await _wait_run(db, ref.id, timeout=90)
        assert r["status"] == "failed"
        assert (await _wait_step(db, ref.id, "sanction", "failed"))

        # downstream never ran — risk_score waited on all 4 checks
        risk = await (await db.execute(
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='risk_score'", (ref.id,))
        ).fetchone()
        assert risk["status"] in ("pending",)

        await db.execute(
            "UPDATE demo_suppliers SET fail_sanction=false WHERE id=%s", (sid,))
        await env["wf"].resume_run(db, ref.id, _ctx())

        await _wait_step(db, ref.id, "sanction", "done")
        # bank wait is still blocked — resolve it through the app
        # boundary, then fan-in can proceed
        await _resolve_bank_wait(db, env, sid)
        await _wait_step(db, ref.id, "risk_score", "done")
        await _complete_humans(db, env, ref.id)
        r2 = await _wait_run(db, ref.id)
        assert r2["status"] == "done"
    finally:
        await w.stop(timeout=5)
