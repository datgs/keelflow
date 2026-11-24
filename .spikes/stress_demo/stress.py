"""Multi-app stress driver — bulk_import + invoice_flow + supplier_onboarding
running concurrently on one worker pool against dedicated Postgres.

Covers the demo surface under sustained load:

- bulk_import: stage CSV (300–600 rows, ~8% bad) → imp.import@v1 →
  human.map → human.approve (80% approve / 20% reject) → bulk passes
  → finished imports DELETEed (import + raw rows; products upserted).
- invoice_flow: submit → human.review (fin approve) → post.wf DAG →
  gl_check (90% approve / 10% reject → posting_rejected → repost) →
  posting_failed entities get retry_post; 3% of invoices deleted as
  draft noise; occasional mid-run job cancel.
- supplier_onboarding: onboard → parallel checks → wait.bank_verify
  resolved via app-owned provider callback → risk branch → human
  approvals → active; 8% fail_sanction → resume_run recovery; noise
  supplier inserts/updates/deletes.
- chaos task: cancels a random non-terminal non-human job every ~45s.
- metrics every 30s: engine job statuses, run outcomes, DLQ, process
  heartbeats, demo table sizes, oldest pending age; JSONL per metric.

Safety: --allow-db + KEELFLOW_STRESS_DSN must name a dedicated database
(assert_safe_test_dsn); advisory lock prevents overlapping runs; own
kernel prefix keelflow_stress_.

    KEELFLOW_STRESS_DSN=postgresql://postgres:postgres@localhost:5544/keelflow_test \
        uv run python .spikes/stress_demo/stress.py --minutes 60 --allow-db
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import random
import sys
import time
import uuid

import psycopg
from chancy import Chancy, Queue, Worker
from psycopg.rows import dict_row

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..",
                                "examples", "bulk_import", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..",
                                "examples", "invoice_flow", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..",
                                "examples", "supplier_onboarding", "src"))

from bulk_import.registry import build_registry as build_bulk, sample_csv  # noqa: E402
from invoice_flow.registry import build_registry as build_inv  # noqa: E402
from supplier_onboarding.app import handle_bank_callback  # noqa: E402
from supplier_onboarding.registry import build_registry as build_sup  # noqa: E402

from keelflow.engine import worker as worker_mod  # noqa: E402
from keelflow.engine.audit import PostgresAuditService  # noqa: E402
from keelflow.engine.enqueue import JobService  # noqa: E402
from keelflow.engine.human import HumanTaskService  # noqa: E402
from keelflow.engine.scheduler import run_scheduler  # noqa: E402
from keelflow.engine.statemachine import EngineStateMachineService  # noqa: E402
from keelflow.engine.workflow import WorkflowService  # noqa: E402
from keelflow.postgres.schema import apply_async  # noqa: E402
from keelflow.postgres.storage import PostgresStorage  # noqa: E402
from keelflow.sdk import Actor, EntityRef, JobContext  # noqa: E402
from keelflow.testing import assert_safe_test_dsn  # noqa: E402

DSN_ENV = "KEELFLOW_STRESS_DSN"
KERNEL_PREFIX = "keelflow_stress_"
ACTOR = Actor.user("u-stress")
ALL_ROLES = ["operations", "reviewer", "finance", "gl",
             "compliance", "finance"]

OPS_CTX = JobContext(actor=ACTOR, tenant_id="stress",
                     permissions=frozenset({"imp.create", "sup.create",
                                            "job.cancel"}))
FIN_CTX = JobContext(actor=Actor.user("u-fin"), tenant_id="stress",
                     permissions=frozenset({"inv.submit", "inv.approve"}))
GL_CTX = JobContext(actor=Actor.user("u-gl"), tenant_id="stress")


def log(*a):
    print(time.strftime("[%H:%M:%S]"), *a, flush=True)


def merged_registry():
    """Merge the three demo registries into one — distinct type
    namespaces, so a single worker serves all flows."""
    base = build_bulk()
    for r in (build_inv(), build_sup()):
        base._job_types.update(r._job_types)
        base._workflows.update(r._workflows)
        base._state_machines.update(r._state_machines)
    return base


async def _conn(dsn, autocommit=True):
    return await psycopg.AsyncConnection.connect(
        dsn, autocommit=autocommit, row_factory=dict_row)


def _demo_schema(name: str) -> str:
    p = os.path.join(os.path.dirname(__file__), "..", "..", "examples",
                     name, "src", name, "schema.sql")
    with open(p) as fh:
        return fh.read()


# ── bulk_import driver ────────────────────────────────────────────────────

async def _stage_import(wc, n_rows, bad_frac) -> int:
    text = sample_csv(n_rows, bad_frac)
    rows = list(csv.DictReader(io.StringIO(text)))
    await wc.execute(
        "INSERT INTO demo_imports (filename, status, rows_total)"
        " VALUES ('stress.csv','uploaded',%s)", (len(rows),))
    iid = (await (await wc.execute(
        "SELECT max(id) AS id FROM demo_imports")).fetchone())["id"]
    async with wc.cursor() as cur:
        await cur.executemany(
            "INSERT INTO demo_import_raw (import_id, row_no, data)"
            " VALUES (%s,%s,%s)",
            [(iid, i + 1, json.dumps(r)) for i, r in enumerate(rows)])
    return iid


async def _bulk_step(rc, run_id, step):
    return await (await rc.execute(
        "SELECT status, job_id FROM keelflow_step_runs"
        " WHERE run_id=%s AND step_id=%s", (run_id, step))).fetchone()


async def _drive_import(dsn, env, spec, rnd, stats):
    iid = spec["iid"]
    outcome = {"kind": "import", "id": iid, "status": None,
               "anomalies": []}
    rc = await _conn(dsn)
    wc = await _conn(dsn)
    await wc.execute("SET lock_timeout = '10s'")
    try:
        async with wc.transaction(), wc.cursor() as cur:
            ref = await env["wf"].start("imp.import@v1",
                                        {"import_id": iid}, OPS_CTX, cur)
        run_id = str(ref.id)
        outcome["run_id"] = run_id
        resolved: set[str] = set()
        t0 = time.monotonic()
        while True:
            run = await (await rc.execute(
                "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
                " WHERE id=%s", (run_id,))).fetchone()
            if run["status"] in ("done", "failed", "cancelled"):
                outcome["status"] = run["status"]
                outcome["terminal_outcome"] = run["terminal_outcome"]
                break
            # human tasks blocked inside this run
            tasks = await (await rc.execute(
                "SELECT s.step_id, s.job_id, j.type FROM keelflow_step_runs s"
                " JOIN keelflow_jobs j ON j.id = s.job_id"
                " WHERE s.run_id=%s AND s.status='blocked'"
                "   AND j.type LIKE 'human.%%'", (run_id,))).fetchall()
            for t in tasks:
                jid = str(t["job_id"])
                if jid in resolved:
                    continue
                try:
                    if t["step_id"] == "map":
                        result = {"decision": "approve",
                                  "mapping": {"sku": "item_code",
                                              "name": "title",
                                              "price": "unit_price",
                                              "stock": "qty"}}
                    else:  # approve step: reject a share of imports
                        result = {"decision": "reject"
                                  if rnd.random() < 0.2 else "approve"}
                    await env["human"].complete(
                        wc, jid, ACTOR, result, roles=ALL_ROLES)
                    resolved.add(jid)
                except Exception as exc:  # noqa: BLE001
                    log(f"  import {iid}: human race {exc!r}")
            if time.monotonic() - t0 > 900:
                outcome["status"] = "timeout"
                outcome["anomalies"].append("run exceeded 900s")
                break
            await asyncio.sleep(rnd.uniform(0.4, 0.8))
        # anomaly checks: row accounting, domain status
        v = await _bulk_step(rc, run_id, "validate")
        if v and v["status"] == "done":
            counts = await (await rc.execute(
                "SELECT status, count(*) n FROM keelflow_row_status"
                " WHERE job_id=%s GROUP BY status", (v["job_id"],))).fetchall()
            live = {r["status"]: r["n"] for r in counts}
            archived = await (await rc.execute(
                "SELECT count(*) n FROM keelflow_row_archive"
                " WHERE job_id=%s", (v["job_id"],))).fetchone()
            if live.get("processing", 0) or live.get("pending", 0):
                outcome["anomalies"].append(
                    f"stranded rows {live}")
            total = archived["n"] + live.get("failed", 0)
            if spec["n_rows"] != total:
                outcome["anomalies"].append(
                    f"row accounting {total} != {spec['n_rows']}")
        imp = await (await rc.execute(
            "SELECT status FROM demo_imports WHERE id=%s", (iid,))
        ).fetchone()
        if outcome["status"] == "done" and \
                outcome["terminal_outcome"] == "succeeded" and \
                imp["status"] not in ("imported", "processed", "done"):
            outcome["anomalies"].append(
                f"demo_imports status {imp['status']}")
        stats["import_" + (outcome["status"] or "?")] = \
            stats.get("import_" + (outcome["status"] or "?"), 0) + 1
        # DELETE coverage: remove finished import + its raw rows
        if spec.get("delete", True):
            try:
                await wc.execute(
                    "DELETE FROM demo_import_raw WHERE import_id=%s", (iid,))
                await wc.execute(
                    "DELETE FROM demo_imports WHERE id=%s", (iid,))
                await wc.commit()
                stats["imports_deleted"] += 1
            except Exception as exc:  # noqa: BLE001
                outcome["anomalies"].append(f"delete failed {exc!r}")
                await wc.rollback()
        return outcome
    finally:
        await rc.close()
        await wc.close()


# ── invoice_flow driver ───────────────────────────────────────────────────

async def _drive_invoice(dsn, env, spec, rnd, stats):
    no = spec["no"]
    outcome = {"kind": "invoice", "id": no, "status": None,
               "anomalies": []}
    rc = await _conn(dsn)
    wc = await _conn(dsn)
    await wc.execute("SET lock_timeout = '10s'")
    ent = EntityRef(type="invoice", id=no)
    try:
        try:
            async with wc.transaction(), wc.cursor() as cur:
                await env["sm"].transition(ent, "submit", FIN_CTX, cur)
        except Exception as exc:  # noqa: BLE001
            outcome["status"] = "submit_failed"
            outcome["anomalies"].append(f"submit {exc!r}")
            return outcome
        t0 = time.monotonic()
        cancelled = False
        while True:
            st = await (await rc.execute(
                "SELECT state FROM keelflow_entity_state"
                " WHERE entity_type='invoice' AND entity_id=%s",
                (no,))).fetchone()
            state = st["state"] if st else "missing"
            if state == "posted":
                outcome["status"] = "posted"
                break
            if state == "missing":
                outcome["status"] = "lost"
                outcome["anomalies"].append("entity state vanished")
                break
            # resolve this invoice's blocked human tasks
            tasks = await (await rc.execute(
                "SELECT j.id, j.type FROM keelflow_jobs j"
                " WHERE j.status IN ('pending','blocked')"
                "   AND j.type LIKE 'human.%%'"
                "   AND j.payload->>'entity_id'=%s",
                (no,))).fetchall()
            for t in tasks:
                try:
                    if t["type"] == "human.review@v1":
                        await env["human"].complete_and_transition(
                            wc, str(t["id"]), Actor.user("u-fin"),
                            "approve", FIN_CTX, roles=ALL_ROLES)
                    else:  # gl_check / posting_fix — 10% business reject
                        decision = ("reject"
                                    if (t["type"] == "human.gl_check@v1"
                                        and rnd.random() < 0.1)
                                    else "approve")
                        await env["human"].complete(
                            wc, str(t["id"]), Actor.user("u-gl"),
                            {"decision": decision}, roles=ALL_ROLES)
                except Exception as exc:  # noqa: BLE001
                    log(f"  inv {no}: human race {exc!r}")
            # auto-repost rejected entities, auto-retry failed
            # postings — both go through resume_run so the broken step
            # re-enqueues in the same run (a bare SM transition would
            # strand the entity in 'posting')
            if state in ("posting_rejected", "posting_failed"):
                try:
                    run = await (await rc.execute(
                        "SELECT id, terminal_outcome"
                        " FROM keelflow_workflow_runs"
                        " WHERE triggered_by_entity_id=%s",
                        (no,))).fetchone()
                    if run and run["terminal_outcome"] == "cancelled":
                        # cancelled runs are not resumable — the entity
                        # has no supported recovery path (finding F1);
                        # mark once so the driver stops thrashing
                        if "cancel_unresumable" not in outcome["anomalies"]:
                            outcome["anomalies"].append(
                                "cancel_unresumable")
                            stats["inv_cancelled_unresumable"] = \
                                stats.get("inv_cancelled_unresumable",
                                          0) + 1
                    elif run:
                        if state == "posting_failed":
                            # the fix-task flow: gl clears the defect
                            # flag, then the run resumes
                            await wc.execute(
                                "UPDATE demo_invoices"
                                " SET fail_post=false WHERE number=%s",
                                (no,))
                            await wc.commit()
                        async with wc.transaction(), wc.cursor() as cur:
                            await env["wf"].resume_run(cur, str(run["id"]),
                                                       FIN_CTX)
                        if state == "posting_rejected":
                            stats["invoice_reposts"] += 1
                        else:
                            stats["invoice_retries"] += 1
                except Exception as exc:  # noqa: BLE001
                    log(f"  inv {no}: resume race {exc!r}")
            # occasional mid-run cancel of a step job
            if spec["cancel"] and not cancelled and \
                    time.monotonic() - t0 > rnd.uniform(2, 6):
                job = await (await rc.execute(
                    "SELECT j.id FROM keelflow_jobs j"
                    " WHERE j.status IN ('pending','claimed','running')"
                    "   AND j.type NOT LIKE 'human.%%'"
                    " ORDER BY j.created_at DESC LIMIT 1")).fetchone()
                if job:
                    try:
                        await env["jobs"].cancel(str(job["id"]), OPS_CTX)
                        cancelled = True
                        stats["cancels"] += 1
                    except Exception as exc:  # noqa: BLE001
                        log(f"  inv {no}: cancel race {exc!r}")
            if time.monotonic() - t0 > 900:
                outcome["status"] = "timeout"
                outcome["anomalies"].append(f"stuck in {state} 900s")
                break
            await asyncio.sleep(rnd.uniform(0.4, 0.8))
        # domain post-conditions
        if outcome["status"] == "posted":
            posting = await (await rc.execute(
                "SELECT invoice_no FROM demo_ledger_postings"
                " WHERE invoice_no=%s", (no,))).fetchone()
            if posting is None:
                outcome["anomalies"].append("no ledger posting row")
            audit = await (await rc.execute(
                "SELECT count(*) n FROM keelflow_audit"
                " WHERE entity_type='invoice' AND entity_id=%s",
                (no,))).fetchone()
            if audit["n"] < 2:
                outcome["anomalies"].append("thin audit trail")
        stats["inv_" + (outcome["status"] or "?")] = \
            stats.get("inv_" + (outcome["status"] or "?"), 0) + 1
        return outcome
    finally:
        await rc.close()
        await wc.close()


async def _mk_invoice(wc, seq, rnd, args):
    no = f"INV-S{args.run_id}-{seq:06d}"
    await wc.execute(
        "INSERT INTO demo_invoices (number, vendor, amount, fail_post)"
        " VALUES (%s, %s, %s, %s)",
        (no, f"Vendor-{rnd.randint(1, 40)}",
         round(rnd.uniform(50, 5000), 2), rnd.random() < 0.015))
    await wc.execute(
        "INSERT INTO keelflow_entity_state"
        " (entity_type, entity_id, state) VALUES ('invoice', %s, 'draft')"
        " ON CONFLICT (entity_type, entity_id) DO UPDATE SET state='draft'",
        (no,))
    return no


async def _noise_invoice(wc, rnd, stats):
    """INSERT → UPDATE → DELETE of a draft invoice — real CRUD churn."""
    no = f"INV-N{uuid.uuid4().hex[:8]}"
    await wc.execute(
        "INSERT INTO demo_invoices (number, vendor, amount)"
        " VALUES (%s,'Noise',10.00)", (no,))
    await wc.execute(
        "UPDATE demo_invoices SET amount=%s WHERE number=%s",
        (round(rnd.uniform(10, 999), 2), no))
    await wc.execute("DELETE FROM demo_invoices WHERE number=%s", (no,))
    await wc.execute(
        "DELETE FROM keelflow_entity_state"
        " WHERE entity_type='invoice' AND entity_id=%s", (no,))
    stats["noise_crud"] += 1


# ── supplier_onboarding driver ────────────────────────────────────────────

async def _drive_supplier(dsn, env, spec, rnd, stats):
    sid = spec["sid"]
    outcome = {"kind": "supplier", "id": sid, "status": None,
               "anomalies": []}
    rc = await _conn(dsn)
    wc = await _conn(dsn)
    await wc.execute("SET lock_timeout = '10s'")
    resolved_waits: set[str] = set()
    resumed = 0
    try:
        async with wc.transaction(), wc.cursor() as cur:
            ref = await env["wf"].start("sup.onboard@v1",
                                        {"supplier_id": sid}, OPS_CTX, cur)
        run_id = str(ref.id)
        outcome["run_id"] = run_id
        t0 = time.monotonic()
        while True:
            run = await (await rc.execute(
                "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
                " WHERE id=%s", (run_id,))).fetchone()
            if run["status"] in ("done", "cancelled"):
                outcome["status"] = run["status"]
                outcome["terminal_outcome"] = run["terminal_outcome"]
                break
            if run["status"] == "failed":
                if spec["fail"] and resumed == 0:
                    await wc.execute(
                        "UPDATE demo_suppliers SET fail_sanction=false"
                        " WHERE id=%s", (sid,))
                    await wc.commit()
                    async with wc.transaction():
                        await env["wf"].resume_run(wc, run_id, OPS_CTX)
                    resumed += 1
                    stats["supplier_resumes"] += 1
                    await asyncio.sleep(0.5)
                    continue
                outcome["status"] = "failed"
                outcome["anomalies"].append("run failed unexpectedly")
                break
            # resolve bank waits through the app-owned callback path
            waits = await (await rc.execute(
                "SELECT j.id FROM keelflow_jobs j"
                " JOIN keelflow_step_runs sr ON sr.job_id = j.id"
                " WHERE sr.run_id=%s AND j.type='wait.bank_verify@v1'"
                "   AND j.status IN ('pending','claimed')",
                (run_id,))).fetchall()
            for job in waits:
                jid = str(job["id"])
                if jid in resolved_waits:
                    continue
                try:
                    pref = f"bk-stress-{jid[:12]}"
                    await wc.execute(
                        "INSERT INTO demo_bank_requests"
                        " (provider_ref, supplier_id, job_id)"
                        " VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                        (pref, sid, jid))
                    await wc.commit()
                    await handle_bank_callback(wc, env["jobs"], pref,
                                               "verified")
                    await wc.commit()
                    resolved_waits.add(jid)
                except Exception as exc:  # noqa: BLE001
                    log(f"  sup {sid}: bank cb race {exc!r}")
            # human approvals (conditional on risk)
            tasks = await (await rc.execute(
                "SELECT s.job_id FROM keelflow_step_runs s"
                " JOIN keelflow_jobs j ON j.id = s.job_id"
                " WHERE s.run_id=%s AND s.status='blocked'"
                "   AND j.type LIKE 'human.%%'", (run_id,))).fetchall()
            for t in tasks:
                try:
                    await env["human"].complete(
                        wc, str(t["job_id"]), ACTOR,
                        {"decision": "approve"}, roles=ALL_ROLES)
                except Exception as exc:  # noqa: BLE001
                    log(f"  sup {sid}: human race {exc!r}")
            if time.monotonic() - t0 > 900:
                outcome["status"] = "timeout"
                outcome["anomalies"].append("run exceeded 900s")
                break
            await asyncio.sleep(rnd.uniform(0.4, 0.8))
        if outcome["status"] == "done":
            sup = await (await rc.execute(
                "SELECT status FROM demo_suppliers WHERE id=%s",
                (sid,))).fetchone()
            if sup["status"] != "active":
                outcome["anomalies"].append(
                    f"supplier status {sup['status']} != active")
            checks = await (await rc.execute(
                "SELECT count(*) n FROM demo_check_results"
                " WHERE supplier_id=%s", (sid,))).fetchone()
            if checks["n"] < 4:
                outcome["anomalies"].append(
                    f"only {checks['n']} check results")
        stats["sup_" + (outcome["status"] or "?")] = \
            stats.get("sup_" + (outcome["status"] or "?"), 0) + 1
        return outcome
    finally:
        await rc.close()
        await wc.close()


async def _noise_supplier(wc, rnd, stats):
    name = f"noise-{uuid.uuid4().hex[:8]}"
    await wc.execute(
        "INSERT INTO demo_suppliers (name, country, tax_id,"
        " contact_email, bank_ref, fail_sanction, status)"
        " VALUES (%s,'DE',%s,'noise@hdwebsoft.com','N001',false,'requested')",
        (name, f"T-{name[:10]}"))
    sid = (await (await wc.execute(
        "SELECT max(id) id FROM demo_suppliers")).fetchone())["id"]
    await wc.execute(
        "UPDATE demo_suppliers SET contact_email=%s WHERE id=%s",
        (f"upd-{uuid.uuid4().hex[:6]}@hdwebsoft.com", sid))
    await wc.execute("DELETE FROM demo_suppliers WHERE id=%s", (sid,))
    stats["noise_crud"] += 1


# ── chaos + metrics ───────────────────────────────────────────────────────

async def _chaos(dsn, env, rnd, stats, deadline):
    """Cancel a random non-terminal machine job every ~45s."""
    conn = await _conn(dsn)
    try:
        while time.monotonic() < deadline:
            await asyncio.sleep(rnd.uniform(25, 60))
            job = await (await conn.execute(
                "SELECT id FROM keelflow_jobs"
                " WHERE status IN ('pending','claimed')"
                "   AND type NOT LIKE 'human.%%'"
                "   AND type NOT LIKE 'wait.%%'"
                " ORDER BY random() LIMIT 1")).fetchone()
            if not job:
                continue
            try:
                await env["jobs"].cancel(str(job["id"]), OPS_CTX)
                stats["chaos_cancels"] += 1
                log(f"chaos: cancelled {str(job['id'])[:8]}")
            except Exception as exc:  # noqa: BLE001
                log(f"chaos cancel race: {exc!r}")
    finally:
        await conn.close()


async def _metrics(dsn, fh, deadline, stats):
    conn = await _conn(dsn)
    try:
        while time.monotonic() < deadline:
            row = await (await conn.execute(
                "SELECT status, count(*) n FROM keelflow_jobs"
                " GROUP BY status")).fetchall()
            runs = await (await conn.execute(
                "SELECT status, terminal_outcome, count(*) n"
                " FROM keelflow_workflow_runs GROUP BY 1,2")).fetchall()
            dlq = await (await conn.execute(
                "SELECT count(*) n FROM keelflow_dlq")).fetchone()
            waits = await (await conn.execute(
                "SELECT count(*) n FROM keelflow_jobs"
                " WHERE status IN ('pending','claimed')"
                "   AND (type LIKE 'wait.%%' OR type LIKE 'human.%%')"
                )).fetchone()
            oldest = await (await conn.execute(
                "SELECT extract(epoch FROM now() - min(created_at)) s"
                " FROM keelflow_jobs WHERE status='pending'")).fetchone()
            health = await (await conn.execute(
                "SELECT kind, count(*) n,"
                "  max(extract(epoch FROM now()-last_seen)) oldest_age"
                " FROM keelflow_process_health GROUP BY kind")).fetchall()
            rec = {"t": round(time.time(), 1),
                   "jobs": {r["status"]: r["n"] for r in row},
                   "runs": {f"{r['status']}/{r['terminal_outcome']}": r["n"]
                            for r in runs},
                   "dlq": dlq["n"], "open_waits_humans": waits["n"],
                   "oldest_pending_s": (float(oldest["s"])
                       if oldest["s"] is not None else None),
                   "health": {r["kind"]: [r["n"], r["oldest_age"]]
                              for r in health},
                   "stats": dict(stats)}
            fh.write(json.dumps(rec, default=str) + "\n")
            fh.flush()
            log(f"metrics: jobs={rec['jobs']} runs={rec['runs']} "
                f"dlq={dlq['n']} waits={waits['n']} "
                f"oldest={rec['oldest_pending_s']}")
            await asyncio.sleep(30)
    finally:
        await conn.close()


# ── main ─────────────────────────────────────────────────────────────────

async def _pool(dsn, env, specs_iter, make_task, concurrency, deadline,
                outcomes, stats, rnd):
    drivers: set[asyncio.Task] = set()
    while time.monotonic() < deadline or drivers:
        while len(drivers) < concurrency and \
                time.monotonic() < deadline:
            try:
                spec = await specs_iter()
            except Exception as exc:  # noqa: BLE001
                log(f"spec gen failed: {exc!r}")
                await asyncio.sleep(1)
                continue
            drivers.add(asyncio.create_task(
                make_task(dsn, env, spec, rnd, stats)))
        done = {t for t in drivers if t.done()}
        for t in done:
            drivers.discard(t)
            try:
                outcomes.append(t.result())
            except Exception as exc:  # noqa: BLE001
                outcomes.append({"kind": "?", "status": "driver_crash",
                                 "anomalies": [f"{exc!r}"]})
        await asyncio.sleep(0.5)


async def main_async(args) -> int:
    if not args.allow_db:
        print("refusing: pass --allow-db", file=sys.stderr)
        return 2
    dsn = os.environ.get(DSN_ENV, "")
    if not dsn:
        print(f"refusing: set {DSN_ENV}", file=sys.stderr)
        return 2
    try:
        assert_safe_test_dsn(dsn)
    except ValueError as exc:
        print(f"refusing: {exc}", file=sys.stderr)
        return 2

    # advisory lock so two stress runs never share a DB
    lock_conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    ok = await (await lock_conn.execute(
        "SELECT pg_try_advisory_lock(884422)")).fetchone()
    if not ok[0]:
        print("refusing: stress advisory lock held", file=sys.stderr)
        return 3

    rnd = random.Random(args.seed)
    stats = {"cancels": 0, "chaos_cancels": 0, "invoice_reposts": 0,
             "invoice_retries": 0, "supplier_resumes": 0,
             "imports_deleted": 0, "noise_crud": 0}
    outcomes: list[dict] = []
    log_path = os.path.join(os.path.dirname(__file__),
                            f"stress-{int(time.time())}.jsonl")
    conn = await _conn(dsn)
    try:
        await apply_async(conn)
        for name in ("bulk_import", "invoice_flow",
                     "supplier_onboarding"):
            await conn.execute(_demo_schema(name))

        storage = PostgresStorage(dsn)
        async with Chancy(dsn, prefix=KERNEL_PREFIX,
                          max_connection_pool_size=40) as kernel:
            await kernel.migrate()
            await kernel.declare(
                Queue("default", concurrency=args.workers,
                      executor=Chancy.Executor.Async, eager_polling=True),
                upsert=True)
            reg = merged_registry()
            worker_mod.configure(registry=reg, dsn=dsn, kernel=kernel)
            jobs = JobService(reg, kernel, storage)
            audit = PostgresAuditService(storage)
            sm = EngineStateMachineService(reg, kernel, audit=audit)
            human = HumanTaskService(reg, kernel, sm=sm)
            env = {"wf": WorkflowService(reg, jobs), "jobs": jobs,
                   "human": human, "sm": sm, "storage": storage}
            w = Worker(kernel, shutdown_timeout=10)
            await w.start()
            stop = asyncio.Event()
            sched = asyncio.create_task(
                run_scheduler(storage, reg, kernel, interval=15,
                              stop=stop))
            deadline = time.monotonic() + args.minutes * 60
            seq = {"i": 0}
            gen_conn = await _conn(dsn)

            async def next_import():
                seq["i"] += 1
                n_rows = rnd.randint(300, 600)
                iid = await _stage_import(gen_conn, n_rows, 0.08)
                return {"iid": iid, "n_rows": n_rows,
                        "delete": rnd.random() < 0.9}

            async def next_invoice():
                seq["i"] += 1
                no = await _mk_invoice(gen_conn, seq["i"], rnd, args)
                return {"no": no, "cancel": rnd.random() < 0.05}

            async def next_supplier():
                seq["i"] += 1
                name = f"stress-sup-{args.run_id}-{seq['i']}-{uuid.uuid4().hex[:5]}"
                fail = rnd.random() < 0.08
                await gen_conn.execute(
                    "INSERT INTO demo_suppliers (name, country, tax_id,"
                    " contact_email, bank_ref, fail_sanction, status)"
                    " VALUES (%s,'DE',%s,'stress@hdwebsoft.com','OK001',%s,"
                    " 'requested')", (name, f"T-{name[:12]}", fail))
                sid = (await (await gen_conn.execute(
                    "SELECT max(id) id FROM demo_suppliers")).fetchone())["id"]
                return {"sid": sid, "fail": fail}

            async def noise():
                while time.monotonic() < deadline:
                    try:
                        await _noise_invoice(gen_conn, rnd, stats)
                        await _noise_supplier(gen_conn, rnd, stats)
                        await gen_conn.commit()
                    except Exception as exc:  # noqa: BLE001
                        log(f"noise crud race: {exc!r}")
                        await gen_conn.rollback()
                    await asyncio.sleep(rnd.uniform(2, 6))

            fh = open(log_path, "a")
            try:
                log(f"stress start: {args.minutes}min workers={args.workers}"
                    f" log={log_path}")
                tasks = [
                    asyncio.create_task(_pool(dsn, env, next_import,
                                              _drive_import,
                                              args.imports, deadline,
                                              outcomes, stats, rnd)),
                    asyncio.create_task(_pool(dsn, env, next_invoice,
                                              _drive_invoice,
                                              args.invoices, deadline,
                                              outcomes, stats, rnd)),
                    asyncio.create_task(_pool(dsn, env, next_supplier,
                                              _drive_supplier,
                                              args.suppliers, deadline,
                                              outcomes, stats, rnd)),
                    asyncio.create_task(_chaos(dsn, env, rnd, stats,
                                               deadline)),
                    asyncio.create_task(_metrics(dsn, fh, deadline, stats)),
                    asyncio.create_task(noise()),
                ]
                await asyncio.gather(*tasks)
            finally:
                fh.close()
            stop.set()
            await w.stop(timeout=10)
            sched.cancel()

        # ── final report ────────────────────────────────────────────
        await _report(conn, outcomes, stats, args, log_path)
        return 0
    finally:
        await conn.close()
        await gen_conn.close()
        await lock_conn.execute("SELECT pg_advisory_unlock(884422)")
        await lock_conn.close()


async def _report(conn, outcomes, stats, args, log_path):
    def of(kind):
        return [o for o in outcomes if o.get("kind") == kind]

    anomalies = [(o.get("id"), a) for o in outcomes
                 for a in o.get("anomalies", [])]
    report = {"minutes": args.minutes, "log": log_path,
              "stats": stats, "anomalies": anomalies}
    for kind in ("import", "invoice", "supplier"):
        rows = of(kind)
        by: dict[str, int] = {}
        for o in rows:
            by[o["status"] or "?"] = by.get(o["status"] or "?", 0) + 1
        report[kind] = {"n": len(rows), "outcomes": by}
    tail = await (await conn.execute(
        "SELECT status, count(*) n FROM keelflow_jobs GROUP BY status")
    ).fetchall()
    report["final_jobs"] = {r["status"]: r["n"] for r in tail}
    tail_r = await (await conn.execute(
        "SELECT status, count(*) n FROM keelflow_workflow_runs"
        " GROUP BY status")).fetchall()
    report["final_runs"] = {r["status"]: r["n"] for r in tail_r}
    report["dlq"] = (await (await conn.execute(
        "SELECT count(*) n FROM keelflow_dlq")).fetchone())["n"]
    report["domain"] = {
        "imports": (await (await conn.execute(
            "SELECT count(*) n FROM demo_imports")).fetchone())["n"],
        "import_raw": (await (await conn.execute(
            "SELECT count(*) n FROM demo_import_raw")).fetchone())["n"],
        "products": (await (await conn.execute(
            "SELECT count(*) n FROM demo_products")).fetchone())["n"],
        "invoices": (await (await conn.execute(
            "SELECT count(*) n FROM demo_invoices")).fetchone())["n"],
        "ledger_postings": (await (await conn.execute(
            "SELECT count(*) n FROM demo_ledger_postings")).fetchone())["n"],
        "suppliers": (await (await conn.execute(
            "SELECT count(*) n FROM demo_suppliers")).fetchone())["n"],
        "check_results": (await (await conn.execute(
            "SELECT count(*) n FROM demo_check_results")).fetchone())["n"],
        "audit_rows": (await (await conn.execute(
            "SELECT count(*) n FROM keelflow_audit")).fetchone())["n"],
    }
    report_path = log_path.replace(".jsonl", "-report.json")
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    log("=== FINAL REPORT ===")
    log(json.dumps(report, indent=1, default=str))
    log(f"report: {report_path}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--minutes", type=float, default=60)
    p.add_argument("--workers", type=int, default=10)
    p.add_argument("--imports", type=int, default=2)
    p.add_argument("--invoices", type=int, default=3)
    p.add_argument("--suppliers", type=int, default=3)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--run-id", type=str,
                   default=uuid.uuid4().hex[:6])
    p.add_argument("--allow-db", action="store_true")
    args = p.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
