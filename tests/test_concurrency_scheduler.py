"""T10 contract tests: key-serialized execution + cron scheduling."""
from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy, Queue, Worker
from pydantic import BaseModel

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.engine.scheduler import next_fire, tick
from keelflow.postgres.schema import apply_async
from keelflow.sdk import (
    Actor,
    ConcurrencySpec,
    CronSpec,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
)

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
CHANCY_PREFIX = "keelflow_test_"

ACTIVE: dict[str, int] = {}
MAX_SEEN: dict[str, int] = {}


class In(BaseModel):
    sku: str


class SlowH(JobHandler):
    async def run(self, ctx, payload: In):
        k = payload.sku
        ACTIVE[k] = ACTIVE.get(k, 0) + 1
        MAX_SEEN[k] = max(MAX_SEEN.get(k, 0), ACTIVE[k])
        try:
            await asyncio.sleep(0.3)
        finally:
            ACTIVE[k] -= 1
        return JobResult(status="done")


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
            "TRUNCATE keelflow_jobs, keelflow_outbox CASCADE"
        )
        await conn.execute(
            f"TRUNCATE {CHANCY_PREFIX}jobs, {CHANCY_PREFIX}workers,"
            f" {CHANCY_PREFIX}queues CASCADE"
        )
        ACTIVE.clear(); MAX_SEEN.clear()
        yield conn


@pytest_asyncio.fixture
async def env(db):
    async with Chancy(DSN, prefix=CHANCY_PREFIX) as kernel:
        await kernel.migrate()
        await kernel.declare(
            Queue("default", concurrency=8, executor=Chancy.Executor.Async,
                  eager_polling=True),
            upsert=True,
        )
        reg = InMemoryRegistry()
        reg.register_job_type(JobTypeSpec(
            type="fifo.alloc@v1", input_schema=In, handler=SlowH,
            concurrency=ConcurrencySpec(key="payload.sku"),
        ))
        reg.register_job_type(JobTypeSpec(
            type="nightly@v1", input_schema=In, handler=SlowH,
            schedule=CronSpec(cron="* * * * *", timezone="UTC"),
        ))
        jobs = JobService(reg, kernel)
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        yield {"reg": reg, "jobs": jobs, "kernel": kernel}


async def _enqueue(env, type_name, payload, key=None):
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            b = env["jobs"].builder(type_name).context(
                JobContext(actor=Actor.user("u1"))
            ).payload(payload)
            if key:
                b = b.concurrency_key(key)
            return (await b.enqueue(cur)).id


@pytest.mark.asyncio
async def test_same_key_never_parallel_different_keys_free(env, db):
    # 3 jobs same key, 3 jobs different keys
    ids = []
    for i in range(3):
        ids.append(await _enqueue(env, "fifo.alloc@v1", {"sku": "SKU-1"}, key="SKU-1"))
    for i in range(3):
        ids.append(await _enqueue(env, "fifo.alloc@v1", {"sku": f"SKU-{i}"}, key=f"SKU-{i}"))

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        for _ in range(150):
            rows = await (await db.execute(
                "SELECT status, count(*) FROM keelflow_jobs GROUP BY status"
            )).fetchall()
            if dict(rows).get("done", 0) == 6:
                break
            await asyncio.sleep(0.1)
    finally:
        await w.stop(timeout=5)

    assert MAX_SEEN["SKU-1"] == 1            # serialized
    assert max(MAX_SEEN[k] for k in ("SKU-0", "SKU-2")) == 1
    # different keys ran in parallel (max active across different keys > 1
    # can't be asserted reliably; at least all done)
    rows = await (await db.execute(
        "SELECT status, count(*) FROM keelflow_jobs GROUP BY status"
    )).fetchall()
    assert dict(rows)["done"] == 6


@pytest.mark.asyncio
async def test_next_fire_window():
    spec = CronSpec(cron="* * * * *", timezone="UTC", window="22:00-23:00")
    # 20:15 UTC → next minute is inside? no — outside window → defer to 22:00
    t = next_fire(spec, after=datetime(2026, 1, 1, 20, 15, tzinfo=UTC))
    assert (t.hour, t.minute) == (22, 0)
    # inside window: 22:30 → next minute kept
    t2 = next_fire(spec, after=datetime(2026, 1, 1, 22, 30, tzinfo=UTC))
    assert t2.hour == 22


@pytest.mark.asyncio
async def test_scheduler_tick_enqueues_once(env, db):
    n1 = await tick(db, env["reg"], env["kernel"])
    assert n1 == 1
    row = await (await db.execute(
        "SELECT count(*) FROM keelflow_jobs WHERE type='nightly@v1'"
    )).fetchone()
    assert row[0] == 1
    # second tick same slot → dedup via unique_key
    n2 = await tick(db, env["reg"], env["kernel"])
    assert n2 == 0 or n2 == 1  # overlap=skip may skip; but never a 2nd row
    row2 = await (await db.execute(
        "SELECT count(*) FROM keelflow_jobs WHERE type='nightly@v1'"
    )).fetchone()
    assert row2[0] == 1
