"""Financial-approval soak — many concurrent fin.pay_run@v1 runs.

Drives the ~23-step payment-run DAG end-to-end against real Postgres for
a long window, with randomized request data and injected chaos:

- N concurrent runs, each driven by an async driver that resolves its
  own wait.* steps (fx/bank_preverify/bank_execute) through the same
  app-owned callback path the UI uses, and completes blocked human
  approvals (mgr → ctrl → cfo → comp).
- Randomized inputs: amount (low/medium/high), currency (VND forces the
  fx wait step), urgent (treasury step), a blank invoice line (bulk
  per-row error), fail_sanction (handler outage → permanent failure →
  resume_run recovery).
- Chaos: ~4% of runs get a mid-run cooperative cancel.
- Anomaly checks per finished run: expected terminal outcome, request
  status, no stranded 'processing' bulk rows, audit trail present.
- Periodic stats: outcome counts, engine job inflight, kernel backlog.

Safety contract mirrors the ERP benchmark: requires an explicit
KEELFLOW_BENCH_DSN naming a dedicated database plus --allow-db, and
takes the same advisory lock so a soak and a benchmark can never write
to one database at once. Uses its own kernel prefix (keelflow_finsoak_)
and its own SOAK-* request refs — it never touches seeded demo rows.

Run:
    KEELFLOW_BENCH_DSN=postgresql://postgres:postgres@localhost:5544/keelflow \
        uv run --package keelflow-demo-financial \
            python .spikes/financial_soak/soak.py --minutes 60 \
            --concurrent 8 --workers 8 --allow-db
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
import uuid

import psycopg
from chancy import Chancy, Queue, Worker
from psycopg.rows import dict_row

# the demo package lives outside this spike dir — import it explicitly
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..",
                    "examples", "financial_approval", "src"))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..",
                    "examples", "bulk_import", "src"))

from bulk_import.bench import acquire_bench_lock
from financial_approval.app import handle_provider_callback
from financial_approval.registry import build_registry

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.schema import apply_async
from keelflow.sdk import Actor, JobContext

DSN_ENV = "KEELFLOW_BENCH_DSN"
KERNEL_PREFIX = "keelflow_finsoak_"
WORKFLOW = "fin.pay_run@v1"
ALL_ROLES = ["clerk", "manager", "controller", "cfo", "compliance",
             "treasury"]
ACTOR = Actor.user("u-soak")

_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "..",
                        "examples", "financial_approval",
                        "src", "financial_approval")
with open(os.path.join(_SQL_DIR, "schema.sql")) as _fh:
    DEMO_SCHEMA = _fh.read()

_WAIT_TO_CHECK = {
    "wait.fx_quote@v1": "fx_quote",
    "wait.bank_preverify@v1": "bank_preverify",
    "wait.bank_execute@v1": "bank_execute",
}


def log(*a):
    print(time.strftime("[%H:%M:%S]"), *a, flush=True)


def _ctx() -> JobContext:
    return JobContext(actor=ACTOR, tenant_id="soak",
                      permissions=frozenset({"pay.submit", "job.cancel"}))


async def _conn(dsn, autocommit=True):
    return await psycopg.AsyncConnection.connect(
        dsn, autocommit=autocommit, row_factory=dict_row)


async def _mk_request(conn, seq: int, rnd: random.Random,
                      vendor_ids: list[int]) -> dict:
    """Create one randomized request + lines; return its spec."""
    r = rnd.random()
    if r < 0.50:
        amount = round(rnd.uniform(1_000, 24_999), 2)          # low
    elif r < 0.85:
        amount = round(rnd.uniform(25_000, 99_999), 2)         # medium
    else:
        amount = round(rnd.uniform(100_000, 220_000), 2)       # high
    currency = "VND" if rnd.random() < 0.30 else "USD"
    urgent = rnd.random() < 0.25
    fail_sanction = rnd.random() < 0.08
    blank_invoice = rnd.random() < 0.12
    cancel = rnd.random() < 0.04
    vid = rnd.choice(vendor_ids)
    ref = f"SOAK-{seq:05d}-{uuid.uuid4().hex[:6]}"
    rid = (await (await conn.execute(
        "INSERT INTO demo_pay_requests"
        " (ref, vendor_id, currency, amount, urgent, fail_sanction)"
        " VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
        (ref, vid, currency, amount, urgent, fail_sanction))
    ).fetchone())["id"]
    n_lines = rnd.randint(5, 30)
    weights = [rnd.random() + 0.2 for _ in range(n_lines)]
    total = sum(weights)
    for ln, w in enumerate(weights, start=1):
        inv = None if (blank_invoice and ln == n_lines) \
            else f"INV-{ref}-{ln}"
        await conn.execute(
            "INSERT INTO demo_pay_lines"
            " (request_id, line_no, invoice_no, gl_account, amount)"
            " VALUES (%s,%s,%s,'6100',%s)",
            (rid, ln, inv, round(float(amount) * w / total, 2)))
    return {"rid": rid, "ref": ref, "currency": currency,
            "urgent": urgent, "fail_sanction": fail_sanction,
            "cancel": cancel, "n_lines": n_lines}


async def _run_status(conn, run_id: str) -> dict | None:
    return await (await conn.execute(
        "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
        " WHERE id=%s", (run_id,))).fetchone()


async def _open_waits(conn, run_id: str):
    return await (await conn.execute(
        "SELECT j.id, j.type FROM keelflow_jobs j"
        " JOIN keelflow_step_runs sr ON sr.job_id = j.id"
        " WHERE sr.run_id=%s AND j.type LIKE %s"
        "   AND j.status IN ('pending','claimed')",
        (run_id, 'wait.%'))).fetchall()


async def _blocked_humans(conn, run_id: str):
    return await (await conn.execute(
        "SELECT s.job_id FROM keelflow_step_runs s"
        " JOIN keelflow_jobs j ON j.id = s.job_id"
        " WHERE s.run_id=%s AND s.status='blocked'"
        "   AND j.type LIKE %s", (run_id, 'human.%'))).fetchall()


async def _resolve_wait(wc, env, spec, job) -> None:
    check = _WAIT_TO_CHECK[job["type"]]
    ref = f"{check}-{job['id'].hex[:12]}"
    await wc.execute(
        "INSERT INTO demo_fin_provider_requests"
        " (provider_ref, request_id, wait_type, job_id)"
        " VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (ref, spec["rid"], check, str(job["id"])))
    await wc.commit()
    await handle_provider_callback(wc, env["jobs"], ref, "verified")
    await wc.commit()


async def _complete_humans(wc, env, run_id: str) -> None:
    rows = await _blocked_humans(wc, run_id)
    if rows:
        log(f"  {run_id[:8]}: {len(rows)} blocked human(s)")
    for row in rows:
        try:
            # human.complete manages its own transaction (savepoint) —
            # mirrors the app route where each complete commits
            await env["human"].complete(
                wc, str(row["job_id"]), ACTOR,
                {"decision": "approve"}, roles=ALL_ROLES)
            log(f"  {run_id[:8]}: completed {str(row['job_id'])[:8]}")
        except Exception as exc:  # noqa: BLE001 - already resolved
            log(f"  human complete race on {run_id[:8]}: {exc!r}")


async def _drive(dsn, env, spec, rnd, stats) -> dict:
    """Drive one run to terminal; return its outcome dict.

    Reuses two connections (rc = read polling, wc = writes/callbacks) so
    a driver holds ~2 connections for its whole life instead of opening
    ~6 per poll — that pool pressure starved the kernel and froze runs.
    """
    rid = spec["rid"]
    outcome = {"ref": spec["ref"], "status": None, "anomalies": []}
    resolved: set[str] = set()
    cancelled = False
    resumed = 0
    t0 = time.monotonic()
    rc = await _conn(dsn)
    wc = await _conn(dsn)
    # bound every lock wait — a driver must never freeze indefinitely
    # on a FOR UPDATE while completing a human step or signaling a wait
    await wc.execute("SET lock_timeout = '10s'")
    try:
        try:
            async with wc.transaction(), wc.cursor() as cur:
                ref = await env["wf"].start(WORKFLOW, {"request_id": rid},
                                            _ctx(), cur)
            run_id = str(ref.id)
        except Exception as exc:  # noqa: BLE001
            outcome["status"] = "start_failed"
            outcome["anomalies"].append(f"start failed: {exc!r}")
            return outcome
        outcome["run_id"] = run_id
        loop_n = 0
        while True:
            loop_n += 1
            # a leaked open transaction would hold row locks and cascade
            # into DDL/reader starvation — detect + reset it each loop
            # IDLE is the only clean state; INTRANS/INERROR/ACTIVE mean
            # a leaked transaction is holding locks
            tx_st = wc.info.transaction_status
            if tx_st.name != "IDLE":
                log(f"  {spec['ref']}: wc in transaction "
                    f"(status={tx_st.name}) at loop {loop_n} — rolling back")
                try:
                    await wc.rollback()
                except Exception as e:  # noqa: BLE001
                    log(f"    rollback failed: {e!r}")
            row = await _run_status(rc, run_id)
            if row is None:
                outcome["status"] = "lost"
                outcome["anomalies"].append("run row vanished")
                return outcome
            if row["status"] in ("done", "cancelled"):
                outcome["status"] = row["status"]
                outcome["terminal_outcome"] = row["terminal_outcome"]
                break
            if row["status"] == "failed":
                if spec["fail_sanction"] and resumed == 0:
                    # permanent handler failure → fix flag + resume in place
                    await wc.execute(
                        "UPDATE demo_pay_requests"
                        " SET fail_sanction=false WHERE id=%s", (rid,))
                    await wc.commit()
                    async with wc.transaction():
                        await env["wf"].resume_run(wc, run_id, _ctx())
                    resumed += 1
                    stats["resumes"] += 1
                    continue
                outcome["status"] = "failed"
                outcome["anomalies"].append(
                    f"run failed unexpectedly ({spec['ref']})")
                break
            # resolve any newly-open wait jobs via the app-owned callback
            waits = await _open_waits(rc, run_id)
            for job in waits:
                jid = str(job["id"])
                if jid in resolved:
                    continue
                try:
                    await _resolve_wait(wc, env, spec, job)
                    resolved.add(jid)
                except Exception as exc:  # noqa: BLE001
                    log(f"  wait resolve race on {spec['ref']}: {exc!r}")
            # complete any newly-blocked human approvals
            try:
                await _complete_humans(wc, env, run_id)
            except Exception as exc:  # noqa: BLE001
                log(f"  human drive error on {spec['ref']}: {exc!r}")
            # inject a mid-run cancel once, a few seconds in
            if spec["cancel"] and not cancelled and \
                    time.monotonic() - t0 > rnd.uniform(2, 6):
                job = await (await rc.execute(
                    "SELECT j.id FROM keelflow_jobs j"
                    " JOIN keelflow_step_runs sr ON sr.job_id=j.id"
                    " WHERE sr.run_id=%s"
                    "   AND j.status IN ('pending','claimed','running')"
                    " ORDER BY j.created_at DESC LIMIT 1",
                    (run_id,))).fetchone()
                if job:
                    await env["jobs"].cancel(str(job["id"]), _ctx())
                    cancelled = True
                    stats["cancels"] += 1
            if time.monotonic() - t0 > 1800:
                outcome["status"] = "timeout"
                outcome["anomalies"].append(
                    "run exceeded 1800s driver budget")
                break
            await asyncio.sleep(rnd.uniform(0.4, 0.9))
        # post-run anomaly checks
        stuck = await (await rc.execute(
            "SELECT count(*) AS n FROM keelflow_row_status rs"
            " JOIN keelflow_jobs j ON j.id = rs.job_id"
            " JOIN keelflow_step_runs sr ON sr.job_id = j.id"
            " WHERE sr.run_id=%s AND rs.status='processing'",
            (run_id,))).fetchone()
        if stuck["n"]:
            outcome["anomalies"].append(
                f"{stuck['n']} stranded 'processing' bulk rows")
        req = await (await rc.execute(
            "SELECT status FROM demo_pay_requests WHERE id=%s",
            (rid,))).fetchone()
        if outcome["status"] == "done" and req["status"] != "paid":
            outcome["anomalies"].append(
                f"request status {req['status']} != paid")
        if outcome["status"] == "cancelled" and req["status"] == "paid":
            outcome["anomalies"].append("cancelled run marked paid")
        if outcome["status"] == "done":
            audit = await (await rc.execute(
                "SELECT count(*) AS n FROM keelflow_audit"
                " WHERE entity_type='pay_request' AND entity_id=%s",
                (str(rid),))).fetchone()
            if audit["n"] < 1:
                outcome["anomalies"].append("no audit trail")
        outcome["elapsed_s"] = round(time.monotonic() - t0, 1)
        return outcome
    finally:
        await rc.close()
        await wc.close()


async def _stats_snapshot(conn) -> dict:
    inflight = await (await conn.execute(
        "SELECT status, count(*) AS n FROM keelflow_jobs"
        " WHERE status IN ('pending','claimed','running')"
        " GROUP BY status")).fetchall()
    waits = await (await conn.execute(
        "SELECT count(*) AS n FROM keelflow_jobs"
        " WHERE type LIKE %s AND status IN ('pending','claimed')",
        ('wait.%',))).fetchone()
    return {r["status"]: r["n"] for r in inflight} | {
        "open_waits": waits["n"]}


async def main_async(args) -> int:
    if not args.allow_db:
        print("refusing: pass --allow-db to confirm the soak runs on a "
              "dedicated/local database", file=sys.stderr)
        return 2
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        print(f"refusing: set {DSN_ENV} to a dedicated database",
              file=sys.stderr)
        return 2
    # the advisory lock is held on a plain (tuple) connection —
    # acquire_bench_lock reads row[0], which a dict_row cursor lacks
    lock_conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True,
                                                 row_factory=dict_row)
    try:
        if not await acquire_bench_lock(lock_conn):
            log("refusing: benchmark/soak advisory lock already held")
            return 3
        await apply_async(conn)
        await conn.execute(DEMO_SCHEMA)
        # seed the 4 vendors the request generator picks from
        for name, country, bank in [
                ("acme parts gmbh", "DE", "DE89370400440532013000"),
                ("golden lotus trading", "VN", "VN070400001234"),
                ("meridian foods s.a.", "AR", "AR015005000123"),
                ("quantum alloy labs", "UK", "GB29NWBK601613")]:
            await conn.execute(
                "INSERT INTO demo_vendors (name, country, bank_ref)"
                " VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (name, country, bank))
        vids = [r["id"] for r in await (await conn.execute(
            "SELECT id FROM demo_vendors ORDER BY id")).fetchall()]
        return await _run(args, conn, dsn, vids)
    finally:
        await conn.close()
        await lock_conn.close()


async def _run(args, conn, dsn, vendor_ids) -> int:
    rnd = random.Random(args.seed)
    stats = {"done": 0, "cancelled": 0, "failed": 0, "timeout": 0,
             "cancels": 0, "resumes": 0}
    anomalies: list[str] = []
    outcomes: list[dict] = []
    # default kernel pool is 10 — 8 concurrent drivers polling +
    # resolving waits/humans starved it into PoolTimeout under load.
    async with Chancy(dsn, prefix=KERNEL_PREFIX,
                      max_connection_pool_size=max(
                          16, args.workers * 2 + args.concurrent)) as kernel:
        await kernel.migrate()
        await kernel.declare(
            Queue("default", concurrency=args.workers,
                  executor=Chancy.Executor.Async, eager_polling=True),
            upsert=True)
        reg = build_registry()
        worker_mod.configure(registry=reg, dsn=dsn, kernel=kernel)
        jobs = JobService(reg, kernel)
        human = HumanTaskService(reg, kernel)
        env = {"wf": WorkflowService(reg, jobs), "jobs": jobs,
               "human": human}
        w = Worker(kernel, shutdown_timeout=10)
        await w.start()
        deadline = time.monotonic() + args.minutes * 60
        seq = 0
        drivers: set[asyncio.Task] = set()
        last_log = 0.0
        log(f"soak start: {args.minutes}min concurrent={args.concurrent} "
            f"workers={args.workers} seed={args.seed}")
        try:
            while time.monotonic() < deadline:
                # top up concurrent run drivers
                while len(drivers) < args.concurrent:
                    seq += 1
                    spec = await _mk_request(conn, seq, rnd, vendor_ids)
                    drivers.add(asyncio.create_task(
                        _drive(dsn, env, spec, rnd, stats)))
                # collect finished drivers
                done = {t for t in drivers if t.done()}
                for t in done:
                    drivers.discard(t)
                    outcome = t.result()
                    outcomes.append(outcome)
                    st = outcome["status"]
                    stats[st] = stats.get(st, 0) + 1
                    if st == "failed" and spec_passed_fail(outcome):
                        stats["resumes"] += 1
                    for a in outcome["anomalies"]:
                        anomalies.append(f"{outcome['ref']}: {a}")
                        log("ANOMALY:", anomalies[-1])
                if time.monotonic() - last_log > 30:
                    last_log = time.monotonic()
                    snap = await _stats_snapshot(conn)
                    log(f"runs={len(outcomes)} "
                        f"done={stats['done']} cancelled={stats['cancelled']} "
                        f"failed={stats['failed']} active={len(drivers)} "
                        f"inflight={snap}")
            log("draining", len(drivers), "drivers…")
            drain_deadline = time.monotonic() + 180
            while drivers and time.monotonic() < drain_deadline:
                done = {t for t in drivers if t.done()}
                for t in done:
                    drivers.discard(t)
                    outcome = t.result()
                    outcomes.append(outcome)
                    stats[outcome["status"]] = \
                        stats.get(outcome["status"], 0) + 1
                    for a in outcome["anomalies"]:
                        anomalies.append(f"{outcome['ref']}: {a}")
                await asyncio.sleep(1)
            for t in drivers:
                t.cancel()
        finally:
            await w.stop(timeout=10)

    el = [o.get("elapsed_s", 0) for o in outcomes
          if o["status"] == "done"]
    report = {
        "minutes": args.minutes, "concurrent": args.concurrent,
        "workers": args.workers, "seed": args.seed,
        "runs": len(outcomes), "stats": stats,
        "done_elapsed": {
            "min": min(el) if el else None,
            "mean": round(sum(el) / len(el), 1) if el else None,
            "max": max(el) if el else None},
        "anomalies": anomalies,
    }
    out_path = os.path.join(os.path.dirname(__file__), "soak_result.json")
    await asyncio.to_thread(_write_report, out_path, report)
    log(f"SOAK DONE: runs={report['runs']} stats={stats} "
        f"anomalies={len(anomalies)} → {out_path}")
    return 0 if not anomalies else 1


def _write_report(path: str, report: dict) -> None:
    with open(path, "w") as fh:
        json.dump(report, fh, indent=2)


def spec_passed_fail(outcome: dict) -> bool:
    return any("failed unexpectedly" not in a
               for a in outcome.get("anomalies", [True]))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--minutes", type=float, default=60)
    p.add_argument("--concurrent", type=int, default=8,
                   help="concurrent fin.pay_run@v1 drivers")
    p.add_argument("--workers", type=int, default=8,
                   help="kernel queue concurrency")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--allow-db", action="store_true",
                   help="confirm soak runs on a dedicated/local DB")
    return asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
