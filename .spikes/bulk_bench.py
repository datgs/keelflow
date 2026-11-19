"""Bulk throughput benchmark (must-prove #3, scaled).

Stages N rows, runs one bulk job with chunk_atomic, measures
stage/partition/process/fan-in wall time on a real worker.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

import psycopg
from chancy import Chancy, Queue, Worker

from keelflow.engine import worker as worker_mod
from keelflow.engine.bulk import BulkJobHandler, BulkJobPayload
from keelflow.engine.bulk import register as register_bulk
from keelflow.engine.enqueue import JobService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.postgres.schema import apply_async
from keelflow.sdk import Actor, JobContext, JobTypeSpec

DSN = os.environ.get(
    "KEELFLOW_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
PREFIX = "keelflow_bench_"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000


class Noop(BulkJobHandler):
    async def process_row(self, ctx, row, tx):
        return {"ok": True}


async def main() -> None:
    conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    await apply_async(conn)
    await conn.execute(
        "TRUNCATE keelflow_jobs, keelflow_chunks, keelflow_row_status,"
        " keelflow_outbox CASCADE"
    )

    async with Chancy(DSN, prefix=PREFIX) as kernel:
        await kernel.migrate()
        await kernel.declare(
            Queue("default", concurrency=8, executor=Chancy.Executor.Async,
                  eager_polling=True),
            upsert=True,
        )
        reg = InMemoryRegistry()
        register_bulk(reg)
        reg.register_job_type(JobTypeSpec(
            type="bench@v1", input_schema=BulkJobPayload, handler=Noop))
        jobs = JobService(reg, kernel)
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        ctx = JobContext(actor=Actor.user("bench"))

        t0 = time.perf_counter()
        async with conn.transaction(), conn.cursor() as cur:
            ref = await jobs.builder("bench@v1").context(ctx) \
                .payload({"chunk_size": 1000, "atomic": "chunk"}) \
                .enqueue(cur)
        # stage rows (copy-style insert)
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO keelflow_row_status (job_id, row_key, status, data)"
                " SELECT %s, 'r' || g, 'pending', '{}'"
                " FROM generate_series(1, %s) g",
                (ref.id, N),
            )
        t_stage = time.perf_counter() - t0

        w = Worker(kernel, shutdown_timeout=5)
        await w.start()
        t_run0 = time.perf_counter()
        try:
            while True:
                row = await (await conn.execute(
                    "SELECT status FROM keelflow_jobs WHERE id=%s", (ref.id,)
                )).fetchone()
                if row[0] in ("done", "failed"):
                    break
                await asyncio.sleep(0.25)
            t_run = time.perf_counter() - t_run0
        finally:
            await w.stop(timeout=10)

        await conn.execute(
            f"TRUNCATE {PREFIX}jobs, {PREFIX}workers, {PREFIX}queues CASCADE"
        )
        counts = await (await conn.execute(
            "SELECT status, count(*) FROM keelflow_row_status WHERE job_id=%s"
            " GROUP BY status",
            (ref.id,),
        )).fetchall()
        print(f"rows: {N}")
        print(f"stage+enqueue: {t_stage:.1f}s ({N / t_stage:,.0f} rows/s)")
        print(f"process:       {t_run:.1f}s ({N / t_run:,.0f} rows/s)")
        print(f"row statuses:  {dict(counts)}")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

# truncate chancy tables last (created by migrate, dropped on cleanup)

