"""Demo 3 integration test — bulk import against real Postgres.

Covers: CSV staging, HITL map, two bulk passes (validate → approve →
process), row-level atomicity (bad rows → error queue), idempotent
products upsert, abort path (reject → process skipped → aborted).

Requires Postgres at KEELFLOW_TEST_DSN (skipped when unavailable).
"""
from __future__ import annotations

import asyncio
import os
import time

import psycopg
import pytest
import pytest_asyncio
from bulk_import.registry import (
    DSN_ENV,
    build_registry,
    sample_csv,
)
from chancy import Chancy, Queue, Worker
from psycopg.rows import dict_row

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.workflow import WorkflowService
from keelflow.sdk import Actor, JobContext

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow")
KERNEL_PREFIX = "keelflow_test_"
os.environ.setdefault(DSN_ENV, DSN)

with open(os.path.join(os.path.dirname(__file__), "..", "src",
                       "bulk_import", "schema.sql")) as _fh:
    DEMO_SCHEMA = _fh.read()

OPS = Actor.user("u-ops1")
REV = Actor.user("u-rev1")


def _ctx() -> JobContext:
    return JobContext(actor=OPS, tenant_id="demo",
                      permissions=frozenset({"imp.create"}))


async def _conn(autocommit=True):
    return await psycopg.AsyncConnection.connect(
        DSN, autocommit=autocommit, row_factory=dict_row)


@pytest_asyncio.fixture
async def db():
    try:
        conn = await _conn()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        from keelflow.postgres.schema import apply_async

        await apply_async(conn)
        await conn.execute(DEMO_SCHEMA)
        # fresh DB (CI): kernel tables must exist before we truncate them.
        async with Chancy(DSN, prefix=KERNEL_PREFIX) as _k:
            await _k.migrate()
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_dlq, keelflow_entity_state,"
            " demo_imports, demo_import_raw, demo_products CASCADE")
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
        yield {"wf": WorkflowService(reg, jobs), "human": human,
               "kernel": kernel}


async def _mk_import(db, n_rows=120, bad_frac=0.1) -> int:
    """Stage a synthetic CSV — same path the app upload endpoint uses."""
    import csv
    import io
    import json

    text = sample_csv(n_rows, bad_frac)
    rows = list(csv.DictReader(io.StringIO(text)))
    await db.execute(
        "INSERT INTO demo_imports (filename, status, rows_total)"
        " VALUES ('sample.csv','uploaded',%s)", (len(rows),))
    iid = (await (await db.execute(
        "SELECT max(id) AS id FROM demo_imports")).fetchone())["id"]
    for i, r in enumerate(rows):
        await db.execute(
            "INSERT INTO demo_import_raw (import_id, row_no, data)"
            " VALUES (%s,%s,%s)", (iid, i + 1, json.dumps(r)))
    return iid


async def _wait_step(db, run_id, step_id, status, timeout=90):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = await (await db.execute(
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id=%s", (run_id, step_id))).fetchone()
        if r and r["status"] == status:
            return r
        await asyncio.sleep(0.3)
    raise AssertionError(f"step {step_id} not {status} after {timeout}s")


async def _wait_run(db, run_id, timeout=120):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = await (await db.execute(
            "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
            " WHERE id=%s", (run_id,))).fetchone()
        if r["status"] in ("done", "failed"):
            return r
        await asyncio.sleep(0.3)
    raise AssertionError(f"run still running after {timeout}s")


async def _blocked(db, run_id, step_id):
    r = await (await db.execute(
        "SELECT job_id FROM keelflow_step_runs"
        " WHERE run_id=%s AND step_id=%s AND status='blocked'",
        (run_id, step_id))).fetchone()
    return str(r["job_id"]) if r else None


@pytest.mark.asyncio
async def test_full_import_approve(env, db):
    """upload→map→validate(bad rows queued)→approve→process→imported."""
    iid = await _mk_import(db, n_rows=120, bad_frac=0.1)
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        async with (await _conn(False)) as c, c.transaction(), \
                c.cursor() as cur:
            ref = await env["wf"].start(
                "imp.import@v1", {"import_id": iid}, _ctx(), cur)

        # map task appears after preview
        await _wait_step(db, ref.id, "map", "blocked")
        job_id = await _blocked(db, ref.id, "map")
        async with (await _conn(False)) as c:
            await env["human"].claim(c, job_id, OPS)
            await env["human"].complete(
                c, job_id, OPS,
                {"decision": "approve",
                 "mapping": {"sku": "item_code", "name": "title",
                             "price": "unit_price", "stock": "qty"}})
            await c.commit()

        # validate bulk pass runs, bad rows → error queue
        await _wait_step(db, ref.id, "validate", "done")
        vjob = await (await db.execute(
            "SELECT job_id FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='validate'", (ref.id,))).fetchone()
        stats = await (await db.execute(
            "SELECT status, count(*) AS n FROM keelflow_row_status"
            " WHERE job_id=%s GROUP BY status", (vjob["job_id"],))).fetchall()
        by = {r["status"]: r["n"] for r in stats}
        assert by["done"] + by["failed"] == 120
        assert by["failed"] > 0            # synthetic bad rows detected

        # chunks fanned out
        nchunks = await (await db.execute(
            "SELECT count(*) AS n FROM keelflow_chunks WHERE job_id=%s",
            (vjob["job_id"],))).fetchone()
        assert nchunks["n"] == 1           # 120 rows < chunk_size 5000

        # approve → process runs only on approve
        await _wait_step(db, ref.id, "approve", "blocked")
        ajob = await _blocked(db, ref.id, "approve")
        async with (await _conn(False)) as c:
            await env["human"].complete(
                c, ajob, REV, {"decision": "approve"})
            await c.commit()

        r = await _wait_run(db, ref.id)
        assert r["status"] == "done" and r["terminal_outcome"] == "succeeded"

        prods = await (await db.execute(
            "SELECT count(*) AS n FROM demo_products WHERE imported_by=%s",
            (iid,))).fetchone()
        assert prods["n"] == by["done"]     # only valid rows imported
        imp = await (await db.execute(
            "SELECT status FROM demo_imports WHERE id=%s", (iid,))).fetchone()
        assert imp["status"] == "imported"
    finally:
        await w.stop(timeout=5)


@pytest.mark.asyncio
async def test_reject_skips_process_and_aborts(env, db):
    """Approve step 'reject' → process skipped, run done, import aborted."""
    iid = await _mk_import(db, n_rows=50, bad_frac=0.0)
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        async with (await _conn(False)) as c, c.transaction(), \
                c.cursor() as cur:
            ref = await env["wf"].start(
                "imp.import@v1", {"import_id": iid}, _ctx(), cur)
        await _wait_step(db, ref.id, "map", "blocked")
        job_id = await _blocked(db, ref.id, "map")
        async with (await _conn(False)) as c:
            await env["human"].complete(
                c, job_id, OPS,
                {"decision": "approve",
                 "mapping": {"sku": "item_code", "name": "title",
                             "price": "unit_price", "stock": "qty"}})
            await c.commit()
        await _wait_step(db, ref.id, "approve", "blocked")
        ajob = await _blocked(db, ref.id, "approve")
        async with (await _conn(False)) as c:
            await env["human"].complete(
                c, ajob, REV, {"decision": "reject"})
            await c.commit()

        r = await _wait_run(db, ref.id)
        assert r["status"] == "done" and r["terminal_outcome"] == "rejected"
        await _wait_step(db, ref.id, "process", "skipped")
        await _wait_step(db, ref.id, "finalize", "done")
        imp = await (await db.execute(
            "SELECT status FROM demo_imports WHERE id=%s", (iid,))).fetchone()
        assert imp["status"] == "aborted"
        prods = await (await db.execute(
            "SELECT count(*) AS n FROM demo_products WHERE imported_by=%s",
            (iid,))).fetchone()
        assert prods["n"] == 0
    finally:
        await w.stop(timeout=5)
