"""ERP bulk runtime benchmark — 100k/500k/1M catalog import.

Compares three execution variants over one deterministic supplier-catalog
workload on the same staged data:

- ``keelflow-row`` — per-row ``process_row`` path: staging into
  ``keelflow_row_status``, chunked per-row validation, durable row
  status bookkeeping, SKU-keyed upsert, error queue.
- ``keelflow-batch`` — set-based ``process_batch`` chunk callback:
  same rules, one callback + one row-status flip batch per chunk.
- ``sql`` — set-based PostgreSQL baseline: INSERT…SELECT with a CASE
  classifier. A *data-path lower bound* — no job/chunk orchestration.

Safety contract (docs/plans/completed/benchmark-reliability.md):
- Requires an explicit ``KEELFLOW_BENCH_DSN`` — no fallback to the
  dev/test DSN — plus ``--allow-db``.
- Acquires a PostgreSQL session advisory lock (shared with soak.py) so
  a benchmark and a soak can never write to one database at once.
- Every output row is scoped by ``batch_id``; every read/delete uses a
  scoped predicate. Nothing is ever TRUNCATEd or globally deleted.
- A run's timing is only reported when its complete output matches the
  deterministic oracle (exact products, exact error rows) — a failed
  gate marks the run invalid, is excluded from summaries, and makes
  the command exit non-zero.

Run:
    KEELFLOW_BENCH_DSN=postgresql://postgres:postgres@localhost:5544/keelflow \
        uv run --package keelflow-demo-import python -m bulk_import.bench \
            --sizes 100000 --reps 1 --workers 1 --allow-db
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import os
import random
import statistics
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
from chancy import Chancy, Queue, Worker
from psycopg.rows import dict_row

from keelflow.engine import bulk
from keelflow.engine import worker as worker_mod
from keelflow.engine.bulk import BulkJobHandler
from keelflow.engine.enqueue import JobService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.postgres.schema import apply_async
from keelflow.sdk import (
    Actor,
    BatchRowResult,
    JobContext,
    JobResult,
    JobTypeSpec,
)

DSN_ENV = "KEELFLOW_BENCH_DSN"
KERNEL_PREFIX = "keelflow_bench_"
BENCH_TYPE = "bench.catalog@v1"

# Session advisory-lock key shared with .spikes/erp_bulk_bench/soak.py —
# exactly one of {benchmark, soak} may write to a database at a time.
BENCH_LOCK_KEY = 7701122334455

SCHEMA = """
CREATE TABLE IF NOT EXISTS bench_staging (
    batch_id uuid NOT NULL,
    row_no   int  NOT NULL,
    sku      text,
    name     text,
    price    text,
    qty      text,
    PRIMARY KEY (batch_id, row_no)
);
CREATE TABLE IF NOT EXISTS bench_products (
    sku      text PRIMARY KEY,
    name     text NOT NULL,
    price    numeric(10,2) NOT NULL,
    stock    int NOT NULL,
    batch_id uuid,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS bench_errors (
    batch_id uuid NOT NULL,
    row_no   int NOT NULL,
    error    text NOT NULL,
    PRIMARY KEY (batch_id, row_no)
);
CREATE TABLE IF NOT EXISTS bench_invocations (
    invocation_id uuid PRIMARY KEY,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    config      jsonb
);
CREATE TABLE IF NOT EXISTS bench_run_batches (
    invocation_id uuid NOT NULL REFERENCES bench_invocations,
    batch_id      uuid PRIMARY KEY,
    size          int NOT NULL
);
"""

# ── advisory lock ────────────────────────────────────────────────────


async def acquire_bench_lock(conn) -> bool:
    """Try the shared bench/soak advisory lock on *conn*'s session.

    Session-level: held until ``conn`` closes or is explicitly unlocked.
    Caller must keep this connection open for the whole run.
    """
    row = await (await conn.execute(
        "SELECT pg_try_advisory_lock(%s)", (BENCH_LOCK_KEY,))).fetchone()
    return bool(row and row[0])


# ── validation: Python and SQL must classify identically ─────────────


def _validate(sku: str, name: str, price_s: str, qty_s: str) -> str | None:
    """Returns error string or None — mirrors the CASE below exactly."""
    sku, name = (sku or "").strip(), (name or "").strip()
    price_s, qty_s = (price_s or "").strip(), (qty_s or "").strip()
    if not sku:
        return "missing sku"
    if not name:
        return "missing name"
    try:
        if float(price_s) <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return f"bad price: {price_s!r}"
    try:
        if float(qty_s) < 0:
            raise ValueError
    except (TypeError, ValueError):
        return f"bad qty: {qty_s!r}"
    return None


_SQL_ERR = """
    CASE
      WHEN btrim(sku) = '' THEN 'missing sku'
      WHEN btrim(name) = '' THEN 'missing name'
      WHEN price !~ '^-?[0-9]*\\.?[0-9]+$' OR price::numeric <= 0
          THEN 'bad price: ' || quote_literal(price)
      WHEN qty !~ '^-?[0-9]*\\.?[0-9]+$' OR qty::numeric < 0
          THEN 'bad qty: ' || quote_literal(qty)
      ELSE NULL
    END
"""

# ── dataset + oracle ─────────────────────────────────────────────────


def _rows(n: int, bad_frac: float = 0.08, seed: int = 42):
    rnd = random.Random(seed)
    for i in range(n):
        row = [f"SKU-{1000 + i}", f"Widget {i}",
               f"{rnd.uniform(1, 500):.2f}", str(rnd.randint(0, 999))]
        if rnd.random() < bad_frac:
            which = rnd.randint(0, 3)
            row[which] = "" if which < 3 else "-5"
        yield row


def _oracle(n: int) -> tuple[dict, dict]:
    """Deterministic expected outcome for the staged dataset.

    Returns (products, errors):
      products: sku -> (name, Decimal price, stock)
      errors:   "r-{row_no}" -> error text (keelflow row_key form)
    """
    products: dict[str, tuple] = {}
    errors: dict[str, str] = {}
    for i, r in enumerate(_rows(n)):
        err = _validate(*r)
        if err is not None:
            errors[f"r-{i + 1}"] = err
        else:
            products[r[0].strip()] = (
                r[1].strip(), Decimal(r[2].strip()),
                int(float(r[3].strip())))
    return products, errors


async def _stage(conn, batch_id: str, n: int) -> None:
    """Stage N deterministic rows into bench_staging (executemany)."""
    rows = [(batch_id, i + 1, *r) for i, r in enumerate(_rows(n))]
    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO bench_staging (batch_id, row_no, sku, name, price, qty)"
            " VALUES (%s,%s,%s,%s,%s,%s)", rows)


# ── in-process phase instrumentation ─────────────────────────────────
# The Chancy worker runs in-process, so handlers can drop wall-clock
# hooks here. These are approximations used to split queue delay,
# staging, callback work, and residual engine bookkeeping — they are
# NOT exact phase boundaries (see RESULTS.md for definitions).

_PARENT_META: dict[str, dict] = {}   # job_id -> {start, stage_s, partition_s}
_CB_TIMES: dict[str, list[float]] = {}  # batch_id -> callback durations


def _reset_hooks() -> None:
    _PARENT_META.clear()
    _CB_TIMES.clear()


# ── Keelflow variant ─────────────────────────────────────────────────


class _BenchPayload(bulk.BulkJobPayload):
    batch_id: str


class _BenchH(BulkJobHandler):
    """Stages bench_staging → keelflow_row_status inside the dispatch TX,
    then validates + upserts each row into bench_products."""

    async def run(self, ctx, payload) -> JobResult:
        cur = ctx.tx.connection.cursor(row_factory=dict_row) \
            if hasattr(ctx.tx, "connection") else ctx.tx
        job_id = str(ctx.extra["job_id"])
        meta = {"start": time.perf_counter(), "stage_s": 0.0,
                "parent_super_run_s": 0.0, "parent_tx_s": 0.0}
        _PARENT_META[job_id] = meta
        # Set-based staging: one INSERT … SELECT builds the same JSONB
        # `data` payload server-side instead of shipping 1M rows to
        # Python and back (identical content — verified by the exact
        # oracle gate).
        t = time.perf_counter()
        await cur.execute(
            "INSERT INTO keelflow_row_status (job_id,row_key,status,data)"
            " SELECT %s, 'r-'||row_no, 'pending',"
            " jsonb_build_object('sku', sku, 'name', name, 'price', price,"
            " 'qty', qty, 'batch_id', %s::text)"
            " FROM bench_staging WHERE batch_id=%s ORDER BY row_no",
            (job_id, str(payload.batch_id), payload.batch_id))
        meta["stage_s"] = time.perf_counter() - t
        t = time.perf_counter()
        res = await super().run(ctx, payload)
        meta["parent_super_run_s"] = time.perf_counter() - t
        meta["parent_tx_s"] = time.perf_counter() - meta["start"]
        return res

    async def process_row(self, ctx, row, tx):
        d = row["data"]
        t = time.perf_counter()
        try:
            err = _validate(d["sku"], d["name"], d["price"], d["qty"])
            if err:
                raise ValueError(err)
            await tx.execute(
                "INSERT INTO bench_products (sku,name,price,stock,batch_id)"
                " VALUES (%s,%s,%s,%s,%s)"
                " ON CONFLICT (sku) DO UPDATE SET name=EXCLUDED.name,"
                " price=EXCLUDED.price, stock=EXCLUDED.stock,"
                " batch_id=EXCLUDED.batch_id, updated_at=now()",
                (d["sku"].strip(), d["name"].strip(),
                 float(d["price"].strip()), int(float(d["qty"].strip())),
                 d["batch_id"]))
            return {"sku": d["sku"].strip()}
        finally:
            _CB_TIMES.setdefault(d["batch_id"], []).append(
                time.perf_counter() - t)


class _BenchBatchH(_BenchH):
    """Set-based twin: same classification rules, one executemany per
    chunk side. Outcomes correlate by ENGINE ROW ID."""

    async def process_row(self, ctx, row, tx):  # pragma: no cover - guard
        raise AssertionError("batch handler must run via process_batch")

    async def process_batch(self, ctx, rows, tx):
        t = time.perf_counter()
        try:
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
        finally:
            if rows:
                _CB_TIMES.setdefault(rows[0]["data"]["batch_id"], []
                                     ).append(time.perf_counter() - t)


async def run_keelflow(conn, kernel, batch_id: str, n_workers: int,
                       chunk_size: int, *, batch: bool = False) -> dict:
    reg = InMemoryRegistry()
    bulk.register(reg)
    reg.register_job_type(JobTypeSpec(
        type=BENCH_TYPE, input_schema=_BenchPayload,
        handler=_BenchBatchH if batch else _BenchH))
    jobs = JobService(reg, kernel)
    worker_mod.configure(registry=reg, dsn=os.environ[DSN_ENV],
                         kernel=kernel)
    ctx = JobContext(actor=Actor.user("bench"))

    t0 = time.perf_counter()
    async with conn.transaction(), conn.cursor() as cur:
        ref = await jobs.builder(BENCH_TYPE).context(ctx) \
            .payload({"batch_id": batch_id,
                      "chunk_size": chunk_size,
                      "atomic": "chunk" if batch else "row",
                      "row_disposition": "archive"}) \
            .enqueue(cur)
    t_enq_end = time.perf_counter()   # enqueue TX committed here
    t_enq = t_enq_end - t0
    job_id = str(ref.id)

    # true per-run concurrency: re-declare the queue at this level so
    # each measurement really runs at the requested worker count
    from keelflow.engine.native_kernel import NativeKernel

    if isinstance(kernel, NativeKernel):
        w = kernel.create_worker(concurrency=n_workers,
                                 poll_interval=0.02,
                                 heartbeat_interval=5.0)
    else:
        await kernel.declare(
            Queue("default", concurrency=n_workers,
                  executor=Chancy.Executor.Async, eager_polling=True),
            upsert=True)
        w = Worker(kernel, shutdown_timeout=5)
    await w.start()
    t0 = time.perf_counter()
    try:
        while True:
            row = await (await conn.execute(
                "SELECT status FROM keelflow_jobs WHERE id=%s", (ref.id,)
            )).fetchone()
            if row and row[0] in ("done", "failed", "cancelled"):
                break
            await asyncio.sleep(0.3)
        t_proc = time.perf_counter() - t0
    finally:
        await w.stop(timeout=10)

    errors = await (await conn.execute(
        "SELECT count(*) FROM keelflow_row_status"
        " WHERE job_id=%s AND status='failed'", (ref.id,))).fetchone()
    meta = _PARENT_META.get(job_id, {})
    cbs = _CB_TIMES.get(batch_id, [])
    return {"job_id": job_id, "enqueue_s": t_enq,
            # process_s: enqueue-commit → parent terminal (whole e2e).
            # Includes worker startup when it is not already running.
            "process_s": t_proc, "errors": errors[0],
            "status": row[0],
            # phase hooks (in-process approximations; see module docstring)
            # parent_queue_s: enqueue-commit → handler start (contains
            # worker startup + kernel claim latency).
            "parent_queue_s": (meta.get("start", t_enq_end) - t_enq_end
                               if meta else None),
            # parent_stage_s: app-owned row staging INSERT.
            "parent_stage_s": meta.get("stage_s"),
            # parent_super_run_s: whole engine partition+enqueue call.
            "parent_super_run_s": meta.get("parent_super_run_s"),
            # parent_tx_s: dispatch handler TX (stage + partition).
            "parent_tx_s": meta.get("parent_tx_s"),
            # callback_* aggregate worker-time — callbacks on different
            # chunks overlap, so sum can exceed wall time. Not a phase.
            "callback_sum_s": sum(cbs),
            "callback_max_s": max(cbs) if cbs else 0.0,
            "callback_calls": len(cbs)}


# ── SQL baseline variant ─────────────────────────────────────────────


async def run_sql(conn, batch_id: str) -> dict:
    t0 = time.perf_counter()
    async with conn.transaction(), conn.cursor() as cur:
        await cur.execute(
            f"INSERT INTO bench_errors (batch_id, row_no, error)"
            f" SELECT batch_id, row_no, {_SQL_ERR}"
            f" FROM bench_staging WHERE batch_id=%s AND {_SQL_ERR} IS NOT NULL",
            (batch_id,))
        await cur.execute(
            "INSERT INTO bench_products (sku,name,price,stock,batch_id)"
            f" SELECT btrim(sku), btrim(name), price::numeric,"
            f" qty::numeric::int, batch_id FROM bench_staging"
            f" WHERE batch_id=%s AND {_SQL_ERR} IS NULL"
            " ON CONFLICT (sku) DO UPDATE SET name=EXCLUDED.name,"
            " price=EXCLUDED.price, stock=EXCLUDED.stock,"
            " batch_id=EXCLUDED.batch_id, updated_at=now()",
            (batch_id,))
    t_proc = time.perf_counter() - t0
    errors = await (await conn.execute(
        "SELECT count(*) AS n FROM bench_errors WHERE batch_id=%s",
        (batch_id,))).fetchone()
    return {"process_s": t_proc, "errors": errors[0]}


# ── scoped output access + exact oracle gate ─────────────────────────


async def _wipe_output(conn, batch_id: str) -> None:
    """Remove THIS batch's outputs only — never a global delete."""
    await conn.execute(
        "DELETE FROM bench_products WHERE batch_id=%s", (batch_id,))
    await conn.execute(
        "DELETE FROM bench_errors WHERE batch_id=%s", (batch_id,))


async def _products(conn, batch_id: str) -> dict[str, tuple]:
    rows = await (await conn.execute(
        "SELECT sku, name, price, stock FROM bench_products"
        " WHERE batch_id=%s", (batch_id,))).fetchall()
    return {r[0]: (r[1], r[2], r[3]) for r in rows}


def _diff_products(got: dict, want: dict) -> list[str]:
    problems = []
    missing = sorted(set(want) - set(got))
    extra = sorted(set(got) - set(want))
    if missing:
        problems.append(f"{len(missing)} products missing "
                        f"(e.g. {missing[:5]})")
    if extra:
        problems.append(f"{len(extra)} unexpected products "
                        f"(e.g. {extra[:5]})")
    bad = [s for s in set(want) & set(got) if want[s] != got[s]]
    for s in sorted(bad)[:5]:
        problems.append(f"product {s}: got {got[s]} want {want[s]}")
    if len(bad) > 5:
        problems.append(f"... and {len(bad) - 5} more mismatched products")
    return problems


async def verify_sql(conn, batch_id: str, n: int,
                     exp_prods: dict, exp_errs: dict) -> list[str]:
    """Exact oracle gate for the SQL baseline. Returns problems ([]=ok)."""
    problems = _diff_products(await _products(conn, batch_id), exp_prods)
    errs = await (await conn.execute(
        "SELECT row_no, error FROM bench_errors WHERE batch_id=%s",
        (batch_id,))).fetchall()
    got = {f"r-{r[0]}": r[1] for r in errs}
    if got != exp_errs:
        missing = sorted(set(exp_errs) - set(got), key=lambda s: int(s[2:]))
        extra = sorted(set(got) - set(exp_errs), key=lambda s: int(s[2:]))
        if missing:
            problems.append(f"{len(missing)} errors missing "
                            f"(e.g. {missing[:5]})")
        if extra:
            problems.append(f"{len(extra)} unexpected errors "
                            f"(e.g. {extra[:5]})")
        diff = [k for k in set(got) & set(exp_errs)
                if got[k] != exp_errs[k]]
        for k in sorted(diff, key=lambda s: int(s[2:]))[:5]:
            problems.append(f"error {k}: got {got[k]!r} want {exp_errs[k]!r}")
    return problems


async def verify_keelflow(conn, job_id: str, batch_id: str, n: int,
                          exp_prods: dict, exp_errs: dict,
                          status: str) -> list[str]:
    """Exact oracle gate for keelflow variants. Returns problems ([]=ok)."""
    problems = _diff_products(await _products(conn, batch_id), exp_prods)
    if status != "done":
        problems.append(f"parent job status {status!r} != 'done'")
    rows = await (await conn.execute(
        "SELECT status, count(*) FROM keelflow_row_status"
        " WHERE job_id=%s GROUP BY status", (job_id,))).fetchall()
    live = dict(rows)
    for st in ("pending", "processing"):
        if live.get(st):
            problems.append(f"{live[st]} rows stranded in {st!r}")
    got_errs = dict(await (await conn.execute(
        "SELECT row_key, error FROM keelflow_row_status"
        " WHERE job_id=%s AND status='failed'", (job_id,))).fetchall())
    if got_errs != exp_errs:
        missing = sorted(set(exp_errs) - set(got_errs),
                         key=lambda s: int(s[2:]))
        extra = sorted(set(got_errs) - set(exp_errs),
                       key=lambda s: int(s[2:]))
        if missing:
            problems.append(f"{len(missing)} error rows missing "
                            f"(e.g. {missing[:5]})")
        if extra:
            problems.append(f"{len(extra)} unexpected error rows "
                            f"(e.g. {extra[:5]})")
        diff = [k for k in set(got_errs) & set(exp_errs)
                if got_errs[k] != exp_errs[k]]
        for k in sorted(diff, key=lambda s: int(s[2:]))[:5]:
            problems.append(
                f"error {k}: got {got_errs[k]!r} want {exp_errs[k]!r}")
    # done rows were archived at fan-in (row_disposition='archive')
    archived = await (await conn.execute(
        "SELECT count(*) FROM keelflow_row_archive WHERE job_id=%s",
        (job_id,))).fetchone()
    done_live = live.get("done", 0)
    want_ok = n - len(exp_errs)
    if archived[0] + done_live != want_ok:
        problems.append(f"done rows {archived[0] + done_live} != {want_ok}")
    return problems


async def _cleanup_engine_rows(conn, job_id: str) -> None:
    """Scoped engine-row cleanup for one finished parent job."""
    await conn.execute(
        "DELETE FROM keelflow_row_status WHERE job_id=%s", (job_id,))
    await conn.execute(
        "DELETE FROM keelflow_row_archive WHERE job_id=%s", (job_id,))


# ── runner ───────────────────────────────────────────────────────────


async def _run_size(conn, kernel, invocation: str, n: int, reps: int,
                    workers: list[int], chunk_size: int,
                    variants: list[str]) -> list[dict]:
    batch_id = str(uuid.uuid4())
    await conn.execute(
        "INSERT INTO bench_run_batches (invocation_id, batch_id, size)"
        " VALUES (%s,%s,%s)", (invocation, batch_id, n))
    t0 = time.perf_counter()
    await conn.execute(SCHEMA)
    await _stage(conn, batch_id, n)
    stage_s = time.perf_counter() - t0
    exp_prods, exp_errs = _oracle(n)
    results = [{"size": n, "batch_id": batch_id, "stage_s": stage_s,
                "expected_errors": len(exp_errs), "runs": []}]
    # combos: sql baseline once per rep + keelflow per (mode, workers);
    # order alternates across reps to cancel warm-cache bias
    combos: list[tuple[str, int]] = []
    if "keelflow-row" in variants:
        combos += [("keelflow-row", w) for w in workers]
    if "keelflow-batch" in variants:
        combos += [("keelflow-batch", w) for w in workers]
    if "sql" in variants:
        combos.append(("sql", 0))
    for rep in range(reps):
        order = combos if rep % 2 == 0 else list(reversed(combos))
        for variant, w in order:
            _reset_hooks()
            await _wipe_output(conn, batch_id)
            if variant == "sql":
                r = await run_sql(conn, batch_id)
                problems = await verify_sql(
                    conn, batch_id, n, exp_prods, exp_errs)
            else:
                r = await run_keelflow(
                    conn, kernel, batch_id, w, chunk_size,
                    batch=variant == "keelflow-batch")
                problems = await verify_keelflow(
                    conn, r["job_id"], batch_id, n,
                    exp_prods, exp_errs, r.get("status"))
                await _cleanup_engine_rows(conn, r["job_id"])
            rec = {"variant": variant, "workers": w, "rep": rep,
                   "process_s": r["process_s"],
                   "rows_s": n / r["process_s"], "errors": r["errors"],
                   "status": r.get("status", "done"),
                   "correct": not problems,
                   "phases": {k: v for k, v in r.items()
                              if k.endswith("_s") or k == "callback_calls"},
                   "diagnostics": problems}
            results[0]["runs"].append(rec)
            if problems:
                print(f"[gate FAIL] {variant} w{w} rep{rep}: "
                      + "; ".join(problems[:4]), file=sys.stderr)
    await conn.execute(
        "DELETE FROM bench_staging WHERE batch_id=%s", (batch_id,))
    await conn.execute(
        "DELETE FROM bench_products WHERE batch_id=%s", (batch_id,))
    await conn.execute(
        "DELETE FROM bench_errors WHERE batch_id=%s", (batch_id,))
    return results


def _git_meta() -> tuple[str | None, bool | None]:
    try:
        rev = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=False, capture_output=True, text=True,
            timeout=5).stdout.strip()
        dirty = bool(subprocess.run(
            ["git", "status", "--porcelain"],
            check=False, capture_output=True, text=True,
            timeout=5).stdout.strip())
        return rev or None, dirty
    except Exception:  # noqa: BLE001
        return None, None


async def _env_metadata(conn) -> dict:
    pg = {}
    for s in ("server_version", "max_connections", "shared_buffers",
              "work_mem", "synchronous_commit"):
        try:
            row = await (await conn.execute(f"SHOW {s}")).fetchone()
            pg[s] = row[0] if row else None
        except Exception:  # noqa: BLE001 - settings may be restricted
            pg[s] = None
    rev, dirty = await asyncio.to_thread(_git_meta)
    try:
        ver = importlib.metadata.version("keelflow")
    except importlib.metadata.PackageNotFoundError:
        ver = None
    return {"host": os.uname().sysname + "/" + os.uname().machine,
            "python": sys.version.split()[0],
            "time": datetime.now(UTC).isoformat(),
            "postgres": pg, "git": {"rev": rev, "dirty": dirty},
            "keelflow_version": ver}


def _report(results: list[dict], env: dict, config: dict) -> dict:
    out = {"env": env, "config": config, "sizes": []}
    for blk in results:
        by_v: dict[tuple, list] = {}
        for r in blk["runs"]:
            if r["correct"]:
                by_v.setdefault((r["variant"], r["workers"]),
                                []).append(r["process_s"])
        summary = [{"variant": v, "workers": w, "n": len(ts),
                    "mean_s": statistics.mean(ts),
                    "median_s": statistics.median(ts),
                    "min_s": min(ts), "max_s": max(ts),
                    "rows_s_mean": blk["size"] / statistics.mean(ts)}
                   for (v, w), ts in sorted(by_v.items())]
        n_bad = sum(1 for r in blk["runs"] if not r["correct"])
        out["sizes"].append({**blk, "summary": summary,
                             "n_invalid_runs": n_bad})
    out["all_correct"] = all(s["n_invalid_runs"] == 0 for s in out["sizes"])
    return out


def _md(results: dict) -> str:
    lines = ["# ERP bulk benchmark\n",
             (f"env: `{results['env']['host']}` "
              f"py `{results['env']['python']}` "
              f"pg `{results['env']['postgres'].get('server_version')}` "
              f"run `{results['env']['time']}`\n"),
             (f"config: `{json.dumps(results['config'], sort_keys=True)}`"
              f" — all_correct=`{results['all_correct']}`\n"),
             ("Phase fields (approximate, in-process hooks): "
              "`enqueue_s` parent enqueue TX · `parent_queue_s` "
              "enqueue-commit → handler start (incl. worker startup) · "
              "`parent_stage_s` app-owned row staging · "
              "`parent_super_run_s` engine partition+enqueue · "
              "`parent_tx_s` dispatch handler TX · "
              "`callback_sum_s`/`callback_max_s` aggregate worker-time "
              "(overlaps across workers — not a wall-time phase).\n")]
    for s in results["sizes"]:
        lines.append(f"## {s['size']:,} rows (stage {s['stage_s']:.1f}s,"
                     f" expected bad {s['expected_errors']:,},"
                     f" invalid runs {s['n_invalid_runs']})\n")
        lines.append("| variant | workers | n | mean s | median s | "
                     "callback sum s | rows/s mean | correct |"
                     "\n|---|---|---|---|---|---|---|---|")
        byv: dict[tuple, list[dict]] = {}
        for r in s["runs"]:
            byv.setdefault((r["variant"], r["workers"]), []).append(r)
        for (v, w), rs in sorted(byv.items()):
            ok_rs = [r for r in rs if r["correct"]]
            ts = [r["process_s"] for r in ok_rs]
            cb = [r["phases"].get("callback_sum_s") or 0
                  for r in ok_rs]
            all_ok = len(ok_rs) == len(rs)
            if ts:
                lines.append(
                    f"| {v} | {w} | {len(ts)} "
                    f"| {statistics.mean(ts):.1f} "
                    f"| {statistics.median(ts):.1f} "
                    f"| {statistics.mean(cb):.1f} "
                    f"| {s['size'] / statistics.mean(ts):,.0f} |"
                    f" {'yes' if all_ok else 'NO'} |")
            else:
                lines.append(f"| {v} | {w} | 0 | — | — | — | — |"
                             f" NO |")
        lines.append("")
    return "\n".join(lines)


async def main_async(args) -> int:
    if not args.allow_db:
        print("refusing: pass --allow-db to confirm benchmark runs on a "
              "dedicated/local database", file=sys.stderr)
        return 2
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        print(f"refusing: set {DSN_ENV} to an explicit dedicated "
              "benchmark database (no fallback to KEELFLOW_DSN)",
              file=sys.stderr)
        return 2
    conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    try:
        if not await acquire_bench_lock(conn):
            print("refusing: another benchmark/soak holds the advisory "
                  "lock on this database", file=sys.stderr)
            return 3
        invocation = str(uuid.uuid4())
        await apply_async(conn)
        await conn.execute(SCHEMA)
        config = {"invocation_id": invocation, "sizes": args.sizes,
                  "reps": args.reps, "workers": args.workers,
                  "chunk": args.chunk, "variants": args.variants,
                  "row_disposition": "archive", "seed": 42,
                  "kernel_backend": args.kernel,
                  "kernel_prefix": KERNEL_PREFIX if args.kernel == "chancy"
                  else None}
        await conn.execute(
            "INSERT INTO bench_invocations (invocation_id, config)"
            " VALUES (%s,%s)", (invocation, json.dumps(config)))
        env = await _env_metadata(conn)
        if args.kernel == "native":
            from contextlib import asynccontextmanager

            from keelflow.engine.native_kernel import NativeKernel

            @asynccontextmanager
            async def _kernel():
                k = NativeKernel(dsn)
                await k.migrate()
                yield k
        else:
            from contextlib import asynccontextmanager

            @asynccontextmanager
            async def _kernel():
                async with Chancy(dsn, prefix=KERNEL_PREFIX) as client:
                    await client.migrate()
                    yield client
        async with _kernel() as kernel:
            all_results = []
            for n in args.sizes:
                all_results.extend(
                    await _run_size(conn, kernel, invocation, n,
                                    args.reps, args.workers, args.chunk,
                                    args.variants))
            rep = _report(all_results, env, config)
            os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
            md_path = os.path.splitext(args.out)[0] + ".md"
            # sync file writes, done once at the end — keep them off the
            # event loop via a thread for consistency with ASYNC230.
            await asyncio.to_thread(_write_results, args.out, md_path, rep)
            print(_md(rep))
            print(f"wrote {args.out} and {md_path}")
        await conn.execute(
            "UPDATE bench_invocations SET finished_at=now()"
            " WHERE invocation_id=%s", (invocation,))
        return 0 if rep["all_correct"] else 1
    finally:
        await conn.close()


def _write_results(json_path: str, md_path: str, rep: dict) -> None:
    with open(json_path, "w") as fh:
        json.dump(rep, fh, indent=2)
    with open(md_path, "w") as fh:
        fh.write(_md(rep))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--sizes", type=int, nargs="+",
                   default=[100_000], help="row counts (e.g. 100000 500000)")
    p.add_argument("--reps", type=int, default=3,
                   help="measured repetitions per size×variant")
    p.add_argument("--workers", type=int, nargs="+", default=[1, 4, 8],
                   help="queue concurrency levels for keelflow variant")
    p.add_argument("--chunk", type=int, default=1000,
                   help="bulk chunk_size (keelflow variant)")
    p.add_argument("--variants", nargs="+",
                   choices=["keelflow-row", "keelflow-batch", "sql"],
                   default=["keelflow-row", "keelflow-batch", "sql"],
                   help="execution variants to measure (default all)")
    p.add_argument("--kernel", choices=["chancy", "native"],
                   default="chancy",
                   help="dispatch backend for keelflow-* variants")
    p.add_argument("--out", default=".spikes/erp_bulk_bench/result.json")
    p.add_argument("--allow-db", action="store_true",
                   help="confirm benchmark runs on a dedicated/local DB")
    return asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
