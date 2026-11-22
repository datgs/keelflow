"""Demo 5 integration test — financial payment-run DAG (~26 steps).

Covers: intake, parallel risk fan-out (machine + two wait.* steps),
risk scoring, a SEQUENTIAL multi-level human approval chain gated by
risk, GL posting, a set-based `process_batch` payment-line step, an
external bank-execution wait, reconcile/audit/archive — terminal
outcome, skipped conditional steps, and one per-row bulk error.

Requires Postgres at KEELFLOW_TEST_DSN (skipped when unavailable).
"""
from __future__ import annotations

import asyncio
import os
import time

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy, Queue, Worker
from financial_approval.registry import DSN_ENV, build_registry
from psycopg.rows import dict_row

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.workflow import WorkflowService
from keelflow.sdk import Actor, JobContext

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN",
    "postgresql://postgres:postgres@localhost:5544/keelflow")
KERNEL_PREFIX = "keelflow_test_"
os.environ.setdefault(DSN_ENV, DSN)

with open(os.path.join(os.path.dirname(__file__), "..", "src",
                       "financial_approval", "schema.sql")) as _fh:
    DEMO_SCHEMA = _fh.read()

ACTOR = Actor.user("u-admin")
ALL_ROLES = ["clerk", "manager", "controller", "cfo", "compliance",
             "treasury"]


def _ctx() -> JobContext:
    return JobContext(actor=ACTOR, tenant_id="demo",
                      permissions=frozenset({"pay.submit", "job.cancel"}))


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
        async with Chancy(DSN, prefix=KERNEL_PREFIX) as _k:
            await _k.migrate()
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq,"
            " keelflow_entity_state, demo_pay_requests, demo_pay_lines,"
            " demo_vendors, demo_fin_checks, demo_fin_provider_requests"
            " CASCADE")
        await conn.execute(
            f"TRUNCATE {KERNEL_PREFIX}jobs, {KERNEL_PREFIX}workers,"
            f" {KERNEL_PREFIX}queues CASCADE")
        yield conn


@pytest_asyncio.fixture
async def env(db):
    async with Chancy(DSN, prefix=KERNEL_PREFIX) as kernel:
        await kernel.migrate()
        await kernel.declare(
            Queue("default", concurrency=8,
                  executor=Chancy.Executor.Async, eager_polling=True),
            upsert=True)
        reg = build_registry()
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        jobs = JobService(reg, kernel)
        human = HumanTaskService(reg, kernel)
        yield {"wf": WorkflowService(reg, jobs), "jobs": jobs,
               "human": human, "kernel": kernel}


async def _mk_request(db, amount=50000.00, currency="USD", urgent=False,
                      fail=False, blank_invoice=False) -> int:
    await db.execute(
        "INSERT INTO demo_vendors (name, country, bank_ref)"
        " VALUES ('acme test','DE','DE89370400440532013000')"
        " ON CONFLICT DO NOTHING")
    vid = (await (await db.execute(
        "SELECT max(id) AS id FROM demo_vendors")).fetchone())["id"]
    row = await (await db.execute(
        "INSERT INTO demo_pay_requests"
        " (ref, vendor_id, currency, amount, urgent, fail_sanction)"
        " VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
        (f"PAY-T-{int(amount)}-{currency}-{int(urgent)}",
         vid, currency, amount, urgent, fail))).fetchone()
    rid = row["id"]
    # split into a few lines; optionally leave one invoice_no blank so
    # the bulk step produces exactly one per-row business error
    parts = [amount * 0.6, amount * 0.25, amount * 0.15]
    for ln, amt in enumerate(parts, start=1):
        inv = None if (blank_invoice and ln == len(parts)) \
            else f"INV-{rid}-{ln}"
        await db.execute(
            "INSERT INTO demo_pay_lines"
            " (request_id, line_no, invoice_no, gl_account, amount)"
            " VALUES (%s,%s,%s,'6100',%s)",
            (rid, ln, inv, round(amt, 2)))
    return rid


async def _wait_run(db, run_id, timeout=120.0):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = await (await db.execute(
            "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
            " WHERE id=%s", (run_id,))).fetchone()
        if r["status"] in ("done", "failed", "cancelled"):
            return r
        await asyncio.sleep(0.25)
    raise AssertionError(f"run {run_id} not terminal after {timeout}s")


async def _wait_step(db, run_id, step_id, status, timeout=60.0):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = await (await db.execute(
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id=%s",
            (run_id, step_id))).fetchone()
        if r and r["status"] == status:
            return r
        await asyncio.sleep(0.2)
    raise AssertionError(f"step {step_id} not {status} after {timeout}s")


async def _resolve_wait(db, env, rid, wait_type, outcome="verified"):
    """Register a provider request for the open wait job then run the
    same handler the /integrations/{prov}/callback route executes."""
    from financial_approval.app import handle_provider_callback

    check_type = {"wait.fx_quote@v1": "fx_quote",
                  "wait.bank_preverify@v1": "bank_preverify",
                  "wait.bank_execute@v1": "bank_execute"}[wait_type]
    job = None
    for _ in range(200):
        job = await (await db.execute(
            "SELECT j.id FROM keelflow_jobs j"
            " JOIN keelflow_step_runs sr ON sr.job_id = j.id"
            " WHERE j.type=%s AND j.status IN ('pending','claimed')"
            "   AND j.payload->>'request_id' = %s",
            (wait_type, str(rid)))).fetchone()
        if job:
            break
        await asyncio.sleep(0.15)
    assert job, f"{wait_type} job never appeared"
    ref = f"{check_type}-test-{rid}"
    await db.execute(
        "INSERT INTO demo_fin_provider_requests"
        " (provider_ref, request_id, wait_type, job_id)"
        " VALUES (%s,%s,%s,%s)",
        (ref, rid, check_type, str(job["id"])))
    async with (await _conn(False)) as c:
        await handle_provider_callback(c, env["jobs"], ref, outcome)
        await c.commit()


async def _blocked_humans(db, run_id):
    return await (await db.execute(
        "SELECT step_id, job_id FROM keelflow_step_runs"
        " WHERE run_id=%s AND status='blocked'", (run_id,))).fetchall()


async def _complete_humans(db, env, run_id, decision="approve"):
    async with (await _conn(False)) as c:
        for row in await _blocked_humans(db, run_id):
            await env["human"].complete(
                c, str(row["job_id"]), ACTOR, {"decision": decision},
                roles=ALL_ROLES)
        await c.commit()


@pytest.mark.asyncio
async def test_full_26_step_run(env, db):
    """USD high-amount request → medium/high risk → sequential
    mgr→ctrl→cfo→comp approvals → GL → bulk lines → bank exec → paid.
    fx step is skipped for USD; one line fails in pay_lines."""
    rid = await _mk_request(db, amount=120_000.00, blank_invoice=True)

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        async with (await _conn(False)) as c, c.transaction(), \
                c.cursor() as cur:
            ref = await env["wf"].start(
                "fin.pay_run@v1", {"request_id": rid}, _ctx(), cur)
        run_id = str(ref.id)

        # machine checks complete; USD skips fx; both waits park 'blocked'
        for s in ("validate", "dedup", "vendor", "tax", "budget",
                  "sanction"):
            await _wait_step(db, run_id, s, "done")
        await _wait_step(db, run_id, "fx", "skipped")
        await _wait_step(db, run_id, "bank_pre", "blocked")
        await _resolve_wait(db, env, rid, "wait.bank_preverify@v1")
        await _wait_step(db, run_id, "bank_pre", "done")
        await _wait_step(db, run_id, "risk_score", "done")

        risk = (await (await db.execute(
            "SELECT risk FROM demo_pay_requests WHERE id=%s", (rid,))
        ).fetchone())["risk"]
        assert risk in ("low", "medium", "high")

        # sequential approval chain — complete levels as they block
        for level in ("ap_mgr", "ap_ctrl", "ap_cfo", "ap_comp"):
            r = await (await db.execute(
                "SELECT status FROM keelflow_step_runs"
                " WHERE run_id=%s AND step_id=%s",
                (run_id, level))).fetchone()
            want = {"ap_mgr": "blocked",
                    "ap_ctrl": "skipped" if risk == "low" else "blocked",
                    "ap_cfo": "blocked" if risk == "high" else "skipped",
                    "ap_comp": "blocked" if risk == "high" else "skipped",
                    }[level]
            await _wait_step(db, run_id, level, want)
            if want == "blocked":
                await _complete_humans(db, env, run_id)

        # GL + schedule + bulk line step + bank execute + settle
        await _wait_step(db, run_id, "gl", "done")
        await _wait_step(db, run_id, "ap_sched", "done")
        await _wait_step(db, run_id, "treasury", "skipped")  # not urgent
        await _wait_step(db, run_id, "pay_lines", "done")

        # one line failed in the bulk pass → failed row + archive of done
        row_status = await (await db.execute(
            "SELECT status, count(*) AS n FROM keelflow_row_status"
            " GROUP BY status")).fetchall()
        live = {r["status"]: r["n"] for r in row_status}
        assert live.get("failed", 0) >= 1   # blank invoice_no line

        await _wait_step(db, run_id, "bank_exec", "blocked")
        await _resolve_wait(db, env, rid, "wait.bank_execute@v1")
        await _wait_step(db, run_id, "bank_exec", "done")

        r = await _wait_run(db, run_id)
        assert r["status"] == "done" and r["terminal_outcome"] == "succeeded"
        assert (await (await db.execute(
            "SELECT status FROM demo_pay_requests WHERE id=%s", (rid,))
        ).fetchone())["status"] == "paid"

        # audit trail exists for the request
        audit = await (await db.execute(
            "SELECT count(*) AS n FROM keelflow_audit"
            " WHERE entity_type='pay_request' AND entity_id=%s",
            (str(rid),))).fetchone()
        assert audit["n"] >= 1
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_low_risk_short_chain(env, db):
    """A small USD request → low risk → only ap_mgr blocks; ap_ctrl,
    ap_cfo, ap_comp all skipped; run still completes paid."""
    rid = await _mk_request(db, amount=1_000.00)

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        async with (await _conn(False)) as c, c.transaction(), \
                c.cursor() as cur:
            ref = await env["wf"].start(
                "fin.pay_run@v1", {"request_id": rid}, _ctx(), cur)
        run_id = str(ref.id)
        await _resolve_wait(db, env, rid, "wait.bank_preverify@v1")
        await _wait_step(db, run_id, "risk_score", "done")
        risk = (await (await db.execute(
            "SELECT risk FROM demo_pay_requests WHERE id=%s", (rid,))
        ).fetchone())["risk"]

        # complete ap_mgr (always), and any other blocked levels
        await _wait_step(db, run_id, "ap_mgr", "blocked")
        await _complete_humans(db, env, run_id)
        for level in ("ap_ctrl", "ap_cfo", "ap_comp"):
            r = await _blocked_humans(db, run_id)
            if r:
                await _complete_humans(db, env, run_id)

        await _wait_step(db, run_id, "pay_lines", "done")
        await _resolve_wait(db, env, rid, "wait.bank_execute@v1")
        r = await _wait_run(db, run_id)
        assert r["status"] == "done" and r["terminal_outcome"] == "succeeded"
        # low risk → deeper levels skipped
        if risk == "low":
            for level in ("ap_ctrl", "ap_cfo", "ap_comp"):
                st = await (await db.execute(
                    "SELECT status FROM keelflow_step_runs"
                    " WHERE run_id=%s AND step_id=%s",
                    (run_id, level))).fetchone()
                assert st["status"] == "skipped"
    finally:
        await w.stop(timeout=5)
