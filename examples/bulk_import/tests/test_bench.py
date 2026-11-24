"""Benchmark correctness + safety tests.

- both variants must produce identical outputs over the same staged data
  (product state + error queue) — exact oracle comparison
- output reads/deletes are scoped by batch_id (unrelated rows untouched)
- the shared advisory lock refuses a second writer
- runner refuses without --allow-db or an explicit KEELFLOW_BENCH_DSN

Requires Postgres at KEELFLOW_TEST_DSN (skipped when unavailable).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import uuid

import psycopg
import pytest
import pytest_asyncio
from bulk_import import bench
from chancy import Chancy, Queue

from keelflow.postgres.schema import apply_async

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN",
    "postgresql://postgres:postgres@localhost:5544/keelflow_test")
KERNEL_PREFIX = "keelflow_test_"


@pytest_asyncio.fixture
async def db():
    try:
        conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(bench.SCHEMA)
        async with Chancy(DSN, prefix=KERNEL_PREFIX) as _k:
            await _k.migrate()
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq,"
            " bench_staging, bench_products, bench_errors,"
            " bench_invocations, bench_run_batches CASCADE")
        await conn.execute(
            f"TRUNCATE {KERNEL_PREFIX}jobs, {KERNEL_PREFIX}workers,"
            f" {KERNEL_PREFIX}queues CASCADE")
        yield conn


async def _counts(db, batch_id: str):
    prods = await (await db.execute(
        "SELECT sku, name, price, stock"
        " FROM bench_products WHERE batch_id=%s ORDER BY sku",
        (batch_id,))).fetchall()
    sql_errs = await (await db.execute(
        "SELECT count(*) AS n FROM bench_errors WHERE batch_id=%s",
        (batch_id,))).fetchone()
    return {"products": [(r[0], r[1], r[2], r[3]) for r in prods],
            "n_products": len(prods), "sql_errors": sql_errs[0]}


async def _kernel() -> Chancy:
    kernel = Chancy(DSN, prefix=KERNEL_PREFIX)
    await kernel.migrate()
    await kernel.declare(
        Queue("default", concurrency=4,
              executor=Chancy.Executor.Async, eager_polling=True),
        upsert=True)
    return kernel


@pytest.mark.asyncio
async def test_variants_produce_equivalent_outputs(db):
    """keelflow and sql variants over one staged dataset must land on
    the same product state and the same rejected rows."""
    os.environ[bench.DSN_ENV] = DSN
    n = 300
    batch_id = str(uuid.uuid4())
    await bench._stage(db, batch_id, n)

    # SQL baseline first — its snapshot is the oracle.
    await bench._wipe_output(db, batch_id)
    sql_r = await bench.run_sql(db, batch_id)
    oracle = await _counts(db, batch_id)

    # Keelflow row variant — fresh output for this batch, same staging.
    await bench._wipe_output(db, batch_id)
    async with await _kernel() as kernel:
        kf_r = await bench.run_keelflow(
            db, kernel, batch_id, n_workers=1, chunk_size=100)
    got = await _counts(db, batch_id)

    kf_errs = await (await db.execute(
        "SELECT count(*) FROM keelflow_row_status"
        " WHERE job_id=%s AND status='failed'", (kf_r["job_id"],)
    )).fetchone()

    assert got["products"] == oracle["products"]
    assert kf_errs[0] == oracle["sql_errors"] == sql_r["errors"] > 0
    assert got["n_products"] == oracle["n_products"] == n - sql_r["errors"]


@pytest.mark.asyncio
async def test_batch_variant_matches_row_and_sql_oracle(db):
    """The process_batch set-based path must produce the identical
    product state and error rows as the per-row path and the SQL
    baseline over the same staged data."""
    os.environ[bench.DSN_ENV] = DSN
    n = 300
    batch_id = str(uuid.uuid4())
    await bench._stage(db, batch_id, n)

    await bench._wipe_output(db, batch_id)
    sql_r = await bench.run_sql(db, batch_id)
    oracle = await _counts(db, batch_id)

    async with await _kernel() as kernel:
        await bench._wipe_output(db, batch_id)
        kf_row = await bench.run_keelflow(
            db, kernel, batch_id, n_workers=1, chunk_size=100,
            batch=False)
        row_state = await _counts(db, batch_id)
        await bench._wipe_output(db, batch_id)
        kf_batch = await bench.run_keelflow(
            db, kernel, batch_id, n_workers=1, chunk_size=100,
            batch=True)
    batch_state = await _counts(db, batch_id)

    batch_errs = await (await db.execute(
        "SELECT count(*) FROM keelflow_row_status"
        " WHERE job_id=%s AND status='failed'", (kf_batch["job_id"],)
    )).fetchone()

    # all three paths agree exactly
    assert batch_state["products"] == row_state["products"] \
        == oracle["products"]
    assert (batch_errs[0] == kf_row["errors"] == sql_r["errors"]
            == oracle["sql_errors"] > 0)
    assert (batch_state["n_products"] == row_state["n_products"]
            == oracle["n_products"] == n - sql_r["errors"])


@pytest.mark.asyncio
async def test_exact_oracle_gate(db):
    """verify_* compare exact row-level outcomes, not just counts —
    and flag planted corruption."""
    n = 50
    batch_id = str(uuid.uuid4())
    await bench._stage(db, batch_id, n)
    exp_prods, exp_errs = bench._oracle(n)

    await bench.run_sql(db, batch_id)
    assert await bench.verify_sql(db, batch_id, n, exp_prods, exp_errs) == []

    # plant corruption: delete one product, tamper one real error row
    a_sku = next(iter(exp_prods))
    err_row_no = int(next(iter(exp_errs))[2:])
    await db.execute(
        "DELETE FROM bench_products WHERE batch_id=%s AND sku=%s",
        (batch_id, a_sku))
    await db.execute(
        "UPDATE bench_errors SET error='tampered'"
        " WHERE batch_id=%s AND row_no=%s", (batch_id, err_row_no))
    problems = await bench.verify_sql(
        db, batch_id, n, exp_prods, exp_errs)
    assert any("missing" in p for p in problems)
    assert any("tampered" in p for p in problems)


@pytest.mark.asyncio
async def test_output_scoping_ignores_and_preserves_other_batches(db):
    """_wipe_output/_products/verify only see their own batch_id —
    unrelated rows (e.g. a soak writing concurrently) are neither
    deleted nor counted."""
    other_batch = str(uuid.uuid4())
    await db.execute(
        "INSERT INTO bench_products (sku,name,price,stock,batch_id)"
        " VALUES ('OTHER-1','X',1.00,5,%s)", (other_batch,))
    await db.execute(
        "INSERT INTO bench_errors (batch_id,row_no,error)"
        " VALUES (%s,1,'foreign error')", (other_batch,))

    n = 30
    batch_id = str(uuid.uuid4())
    await bench._stage(db, batch_id, n)
    exp_prods, exp_errs = bench._oracle(n)

    await bench._wipe_output(db, batch_id)
    await bench.run_sql(db, batch_id)
    # own batch verified exactly; foreign rows never entered the gate
    assert await bench.verify_sql(db, batch_id, n,
                                  exp_prods, exp_errs) == []
    await bench._wipe_output(db, batch_id)
    foreign = await (await db.execute(
        "SELECT count(*) FROM bench_products WHERE batch_id=%s",
        (other_batch,))).fetchone()
    assert foreign[0] == 1
    foreign_errs = await (await db.execute(
        "SELECT count(*) FROM bench_errors WHERE batch_id=%s",
        (other_batch,))).fetchone()
    assert foreign_errs[0] == 1


@pytest.mark.asyncio
async def test_advisory_lock_refuses_second_writer(db):
    """bench and soak share one advisory lock: a second session on the
    same database cannot acquire it while the first holds it."""
    assert await bench.acquire_bench_lock(db) is True
    async with await psycopg.AsyncConnection.connect(
            DSN, autocommit=True) as other:
        assert await bench.acquire_bench_lock(other) is False


@pytest.mark.asyncio
async def test_advisory_lock_released_on_close(db):
    """After the holder disconnects, the lock is free again."""
    async with await psycopg.AsyncConnection.connect(
            DSN, autocommit=True) as other:
        assert await bench.acquire_bench_lock(other) is True
    assert await bench.acquire_bench_lock(db) is True


def test_refuses_without_allow_db_flag():
    """Benchmark must not run without explicit DB confirmation."""
    args = argparse.Namespace(allow_db=False)
    assert asyncio.run(bench.main_async(args)) == 2


def test_refuses_without_dedicated_dsn(monkeypatch):
    """No fallback to KEELFLOW_DSN: an explicit KEELFLOW_BENCH_DSN
    naming the dedicated benchmark DB is required."""
    monkeypatch.delenv(bench.DSN_ENV, raising=False)
    monkeypatch.setenv("KEELFLOW_DSN", DSN)
    args = argparse.Namespace(allow_db=True)
    assert asyncio.run(bench.main_async(args)) == 2
