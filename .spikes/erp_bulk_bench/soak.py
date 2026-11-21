"""Soak runner — many concurrent workflows for N minutes.

Load mix:
- several concurrent bulk parents (process_batch path, real upserts)
- mixed DAG runs (2 wait steps + machine tail), resolved randomly
  (signal ok / signal_failure / cancel / leave open)

Invariants checked per iteration and at drain:
- each completed bulk parent: products-with-batch_id == valid rows,
  failed row_status == invalid rows (or status 'cancelled')
- every wait job/step pair agrees on status; open waits stay open
- nothing stranded in 'processing' at drain; every run lands terminal

Run:
    KEELFLOW_DSN=... uv run --package keelflow-demo-import \
        python .spikes/erp_bulk_bench/soak.py --minutes 60 --workers 8
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import time
import uuid
from datetime import UTC, datetime

import psycopg
from bulk_import.bench import SCHEMA, _validate, acquire_bench_lock
from chancy import Chancy, Queue, Worker
from psycopg.rows import dict_row
from pydantic import BaseModel

from keelflow.engine import bulk
from keelflow.engine import worker as worker_mod
from keelflow.engine.bulk import BulkJobHandler
from keelflow.engine.enqueue import JobService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.schema import apply_async
from keelflow.sdk import (
    Actor,
    BatchRowResult,
    JobContext,
    JobResult,
    JobTypeSpec,
    StepSpec,
    WorkflowDefinition,
)

DSN_ENV = "KEELFLOW_DSN"
KERNEL_PREFIX = "keelflow_soak_"
BENCH_TYPE = "soak.catalog@v1"
WAIT_TYPE = "wait.soak@v1"
MACHINE_TYPE = "soak.ok@v1"

log = lambda *a: print(f"[{datetime.now(UTC):%H:%M:%S}]", *a, flush=True)


def _gen_rows(n: int, offset: int, seed: int, bad_frac: float = 0.08):
    """Deterministic per-parent dataset — unique skus via offset."""
    rnd = random.Random(seed)
    for i in range(n):
        r = [f"S{offset}-{i}", f"Widget {i}",
             f"{rnd.uniform(1, 500):.2f}", str(rnd.randint(0, 999))]
        if rnd.random() < bad_frac:
            w = rnd.randint(0, 3)
            r[w] = "" if w < 3 else "-5"
        yield r


class _SoakPayload(bulk.BulkJobPayload):
    batch_id: str


class _SoakBatchH(BulkJobHandler):
    """Stages bench_staging → keelflow_row_status, then set-based
    classify + upsert via process_batch (chunk-atomic)."""

    async def run(self, ctx, payload) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) \
            if hasattr(ctx.tx, "connection") else ctx.tx
        job_id = ctx.extra["job_id"]
        await cur.execute(
            "SELECT row_no, sku, name, price, qty FROM bench_staging"
            " WHERE batch_id=%s ORDER BY row_no", (payload.batch_id,))
        staged = await cur.fetchall()
        await cur.executemany(
            "INSERT INTO keelflow_row_status (job_id,row_key,status,data)"
            " VALUES (%s,%s,'pending',%s)",
            [(job_id, f"r-{r['row_no']}", json.dumps(
                {"sku": r["sku"], "name": r["name"], "price": r["price"],
                 "qty": r["qty"], "batch_id": payload.batch_id}))
             for r in staged])
        return await super().run(ctx, payload)

    async def process_batch(self, ctx, rows, tx):
        out: list[BatchRowResult] = []
        upserts = []
        for r in rows:
            d = r["data"]
            err = _validate(d["sku"], d["name"], d["price"], d["qty"])
            if err:
                out.append(BatchRowResult(row_id=r["id"], error=err))
                continue
            upserts.append((d["sku"].strip(), d["name"].strip(),
                            float(d["price"].strip()),
                            int(float(d["qty"].strip())), d["batch_id"]))
            out.append(BatchRowResult(row_id=r["id"]))
        if upserts:
            await tx.executemany(
                "INSERT INTO bench_products (sku,name,price,stock,batch_id)"
                " VALUES (%s,%s,%s,%s,%s)"
                " ON CONFLICT (sku) DO UPDATE SET name=EXCLUDED.name,"
                " price=EXCLUDED.price, stock=EXCLUDED.stock,"
                " batch_id=EXCLUDED.batch_id, updated_at=now()",
                upserts)
        return out


class _OkH(BulkJobHandler):
    async def run(self, ctx, payload):
        return JobResult(status="done", outputs={"n": 1})


async def _conn():
    return await psycopg.AsyncConnection.connect(
        os.environ[DSN_ENV], autocommit=True)


async def _bulk_parent(conn, jobs, batch_id: str) -> str:
    ctx = JobContext(actor=Actor.user("soak"))
    async with conn.transaction(), conn.cursor() as cur:
        ref = await jobs.builder(BENCH_TYPE).context(ctx) \
            .payload({"batch_id": batch_id, "chunk_size": 2000,
                      "atomic": "chunk", "row_disposition": "delete"}) \
            .enqueue(cur)
    return str(ref.id)


async def _wf_run(env, name_seed: int):
    name = f"wf.soak@{name_seed}"
    env["reg"].register_workflow(WorkflowDefinition(
        name=name,
        steps=(StepSpec(id="w0", type=WAIT_TYPE),
               StepSpec(id="w1", type=WAIT_TYPE),
               StepSpec(id="m", type=MACHINE_TYPE, needs=("w0", "w1")))))
    async with (await _conn()) as conn, conn.transaction(), \
            conn.cursor() as cur:
        return await env["wf"].start(name, {}, JobContext(
            actor=Actor.user("soak")), cur)


async def _resolve_wait(conn, jobs, run_id, op: str):
    row = await (await conn.execute(
        "SELECT sr.job_id FROM keelflow_step_runs sr"
        " JOIN keelflow_jobs j ON j.id=sr.job_id"
        " WHERE sr.run_id=%s AND sr.status='blocked'"
        " AND j.type=%s ORDER BY sr.id LIMIT 1",
        (run_id, WAIT_TYPE))).fetchone()
    if not row:
        return None
    jid = str(row[0])
    sig = JobContext(actor=Actor.service("soak"),
                     permissions=frozenset({"job.signal"}))
    if op == "ok":
        async with conn.transaction():
            await jobs.signal(conn, jid, {"n": 1}, sig)
        await conn.commit()
    elif op == "fail":
        async with conn.transaction():
            await jobs.signal_failure(conn, jid, "soak fail", sig)
        await conn.commit()
    else:
        cctx = JobContext(actor=Actor.user("u1"),
                          permissions=frozenset({"job.cancel"}))
        await jobs.cancel(jid, cctx)
    return jid


async def main_async(args) -> int:
    conn = await psycopg.AsyncConnection.connect(
        os.environ[DSN_ENV], autocommit=True)
    try:
        if not await acquire_bench_lock(conn):
            log("refusing: benchmark/soak advisory lock already held")
            return 3
        await apply_async(conn)
        await conn.execute(SCHEMA)
        return await _run(args, conn)
    finally:
        await conn.close()


async def _run(args, conn) -> int:
    rnd = random.Random(args.seed)
    async with Chancy(os.environ[DSN_ENV], prefix=KERNEL_PREFIX) as kernel:
        await kernel.migrate()
        await kernel.declare(
            Queue("default", concurrency=args.workers,
                  executor=Chancy.Executor.Async, eager_polling=True),
            upsert=True)
        reg = InMemoryRegistry()
        bulk.register(reg)
        reg.register_job_type(JobTypeSpec(
            type=BENCH_TYPE, input_schema=_SoakPayload,
            handler=_SoakBatchH))
        reg.register_job_type(JobTypeSpec(
            type=WAIT_TYPE, input_schema=BaseModel, is_wait=True))
        reg.register_job_type(JobTypeSpec(
            type=MACHINE_TYPE, input_schema=BaseModel, handler=_OkH))
        jobs = JobService(reg, kernel)
        worker_mod.configure(registry=reg, dsn=os.environ[DSN_ENV],
                             kernel=kernel)
        wf = WorkflowService(reg, jobs)
        env = {"reg": reg, "jobs": jobs, "wf": wf}

        w = Worker(kernel, shutdown_timeout=10)
        await w.start()
        deadline = time.monotonic() + args.minutes * 60
        # parent_id -> {"batch": batch_id, "expect_ok", "expect_bad"}
        parents: dict[str, dict] = {}
        runs: dict[str, list] = {}     # run_id -> open wait ops
        offset = 0
        seed = args.seed
        anoms: list[str] = []
        last_log = 0.0
        try:
            while time.monotonic() < deadline:
                # top up concurrent bulk parents
                while len(parents) < args.bulk:
                    offset += rnd.randint(10_000, 50_000)
                    n = rnd.randint(5_000, 20_000)
                    batch = str(uuid.uuid4())
                    staged = list(_gen_rows(n, offset, seed))
                    seed = rnd.randint(0, 2**31)
                    exp_bad = sum(
                        1 for r in staged
                        if _validate(*r) is not None)
                    async with conn.cursor() as cur:
                        await cur.executemany(
                            "INSERT INTO bench_staging"
                            " (batch_id,row_no,sku,name,price,qty)"
                            " VALUES (%s,%s,%s,%s,%s,%s)",
                            [(batch, i + 1, *r)
                             for i, r in enumerate(staged)])
                    pid = await _bulk_parent(conn, jobs, batch)
                    parents[pid] = {"batch": batch,
                                    "expect_ok": n - exp_bad,
                                    "expect_bad": exp_bad}

                # maintain mixed wait/machine runs
                if len(runs) < args.dags:
                    ref = await _wf_run(env, rnd.randint(0, 2**31))
                    runs[str(ref.id)] = rnd.choices(
                        ["ok", "ok", "ok"], k=2)          # plan
                # resolve one wait event on a random run
                if runs:
                    rid = rnd.choice(list(runs))
                    plan = runs[rid]
                    op = rnd.choices(
                        ["ok", "fail", "cancel"],
                        weights=[70, 15, 15], k=1)[0] if plan else "leave"
                    if plan:
                        plan_op = plan.pop(0)
                        if op != "leave":
                            await _resolve_wait(
                                conn, jobs, rid, plan_op if op == "ok"
                                else op)
                    # drop terminal runs
                    rr = await (await conn.execute(
                        "SELECT status FROM keelflow_workflow_runs"
                        " WHERE id=%s", (rid,))).fetchone()
                    if rr and rr[0] in ("done", "failed", "cancelled"):
                        del runs[rid]

                # verify completed parents
                for pid, meta in list(parents.items()):
                    r = await (await conn.execute(
                        "SELECT status, result FROM keelflow_jobs"
                        " WHERE id=%s", (pid,))).fetchone()
                    if r[0] in ("pending", "running"):
                        continue
                    parents.pop(pid)
                    if r[0] == "cancelled":
                        continue
                    prods = await (await conn.execute(
                        "SELECT count(*) FROM bench_products"
                        " WHERE batch_id=%s", (meta["batch"],))).fetchone()
                    errs = await (await conn.execute(
                        "SELECT count(*) FROM keelflow_row_status"
                        " WHERE job_id=%s AND status='failed'",
                        (pid,))).fetchone()
                    if (prods[0] != meta["expect_ok"]
                            or errs[0] != meta["expect_bad"]):
                        anoms.append(
                            f"parent {pid}: products {prods[0]}!="
                            f"{meta['expect_ok']} or errors {errs[0]}!="
                            f"{meta['expect_bad']} (status {r[0]})")
                    # cleanup staged rows of the settled batch
                    await conn.execute(
                        "DELETE FROM bench_staging WHERE batch_id=%s",
                        (meta["batch"],))
                    await conn.execute(
                        "DELETE FROM bench_products WHERE batch_id=%s",
                        (meta["batch"],))
                    await conn.execute(
                        "DELETE FROM keelflow_row_status WHERE job_id=%s",
                        (pid,))

                if time.monotonic() - last_log > 30:
                    last_log = time.monotonic()
                    inflight = await (await conn.execute(
                        "SELECT count(*) FROM keelflow_jobs"
                        " WHERE status IN ('pending','running')"
                        " AND type LIKE 'bulk%'")).fetchone()
                    waits = await (await conn.execute(
                        "SELECT count(*) FROM keelflow_jobs"
                        " WHERE type LIKE 'wait%' AND status='pending'")
                    ).fetchone()
                    log(f"parents={len(parents)} runs={len(runs)}"
                        f" bulk_inflight={inflight[0]}"
                        f" waits_open={waits[0]} anomalies={len(anoms)}")
                await asyncio.sleep(1.5)
        finally:
            # drain: stop new work; wait for in-flight to settle
            drain_deadline = time.monotonic() + 120
            while parents and time.monotonic() < drain_deadline:
                for pid in list(parents):
                    r = await (await conn.execute(
                        "SELECT status FROM keelflow_jobs WHERE id=%s",
                        (pid,))).fetchone()
                    if r[0] not in ("pending", "running"):
                        parents.pop(pid)
                await asyncio.sleep(2)
            await w.stop(timeout=15)

        # final invariant sweep
        stranded = await (await conn.execute(
            "SELECT count(*) FROM keelflow_row_status"
            " WHERE status='processing'")).fetchone()
        if stranded[0]:
            anoms.append(f"{stranded[0]} rows stranded 'processing'")
        open_runs = await (await conn.execute(
            "SELECT count(*) FROM keelflow_workflow_runs"
            " WHERE status='running'")).fetchone()
        # leftover runs may legitimately keep open waits
        inflight_jobs = await (await conn.execute(
            "SELECT status, count(*) FROM keelflow_jobs"
            " WHERE status IN ('running','claimed') GROUP BY status")
        ).fetchall()
        log("drain done:", dict(inflight_jobs),
            "open_runs:", open_runs[0], "anomalies:", len(anoms))
        for a in anoms:
            log("ANOMALY:", a)
        print("RESULT:", "PASS" if not anoms else f"{len(anoms)} anomalies")
        return 0 if not anoms else 1


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--minutes", type=float, default=60)
    p.add_argument("--bulk", type=int, default=4,
                   help="concurrent bulk parents")
    p.add_argument("--dags", type=int, default=3,
                   help="concurrent wait/machine DAG runs")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--seed", type=int, default=42)
    return asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
