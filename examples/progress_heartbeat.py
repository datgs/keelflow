"""Long-running job + progress heartbeat demo.

A `demo.long_job@v1` handler simulates a job that runs for tens of
minutes: each work unit sleeps, then every REPORT interval it calls
`ctx.execution.report_progress(current, total)` — the application's
"still alive, still making progress" signal. The job type opts in with
`progress_enabled=True` and `no_progress_timeout_seconds` — when no
report lands inside that window the attempt is cancelled, its UoW
rolled back, and the existing retry policy takes over.

Two modes (needs Postgres, same as order_flow):

    # healthy run — reports every 15s, finishes normally
    KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow \
        python examples/progress_heartbeat.py

    # stuck run — never reports; watchdog fails the attempt after the
    # stale window, engine reschedules, retries exhaust → DLQ
    KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow \
        python examples/progress_heartbeat.py --stuck

Tuning (production-shaped defaults: report every 15s, stale after 180s,
~40 min job — shorten via env for a quick look):

    DEMO_RUN_SECONDS=2400      total simulated work   (default 300)
    DEMO_REPORT_EVERY_S=15     heartbeat cadence      (default 15)
    DEMO_STALE_S=180           no-progress timeout    (default 180)
    DEMO_BACKOFF_S=30          retry backoff base     (default 30)

The monitor loop prints job status, latest progress, report age, and
whether `list_stale_progress` considers it stale — application progress,
distinct from the worker process heartbeat in keelflow_process_health.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import UTC, datetime

import psycopg
from chancy import Chancy, Queue, Worker
from pydantic import BaseModel

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.monitoring import MonitoringService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import (
    Actor,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
)

DSN = os.environ.get(
    "KEELFLOW_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
RUN_SECONDS = float(os.environ.get("DEMO_RUN_SECONDS", "300"))
REPORT_EVERY_S = float(os.environ.get("DEMO_REPORT_EVERY_S", "15"))
STALE_S = float(os.environ.get("DEMO_STALE_S", "180"))
BACKOFF_S = float(os.environ.get("DEMO_BACKOFF_S", "30"))
UNIT_S = 1.0  # one unit of simulated work per second


class P(BaseModel):
    total: int
    report_every_s: float


class LongJobH(JobHandler):
    """Ticks one unit per second; heartbeats progress every REPORT_S.

    The cadence is application-chosen: it must stay well under
    `no_progress_timeout_seconds`, otherwise honest-but-slow stretches
    look like a stall.
    """

    async def run(self, ctx, payload: P):
        last_report = time.monotonic()
        for current in range(1, payload.total + 1):
            await asyncio.sleep(UNIT_S)  # simulated work unit
            if (current == payload.total
                    or time.monotonic() - last_report
                    >= payload.report_every_s):
                await ctx.execution.report_progress(
                    current, payload.total,
                    message=f"unit {current} done")
                last_report = time.monotonic()
        return JobResult(status="done",
                         outputs={"units": payload.total})


class StuckJobH(JobHandler):
    """Same long job but never reports progress — the watchdog must
    fire after `no_progress_timeout_seconds` of silence."""

    async def run(self, ctx, payload: P):
        await asyncio.Event().wait()  # stuck forever
        return JobResult(status="done")


def _spec(type_name: str, handler) -> JobTypeSpec:
    return JobTypeSpec(
        type=type_name, input_schema=P, handler=handler,
        progress_enabled=True,
        no_progress_timeout_seconds=STALE_S,
        retry=RetryPolicy(max_attempts=2, backoff="fixed",
                          base_seconds=BACKOFF_S, jitter=False))


async def main() -> None:
    stuck = "--stuck" in sys.argv
    conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    await apply_async(conn)

    async with Chancy(DSN, prefix="keelflow_kernel_") as kernel:
        await kernel.migrate()
        await kernel.declare(
            Queue("default", concurrency=8, executor=Chancy.Executor.Async,
                  eager_polling=True),
            upsert=True,
        )

        reg = InMemoryRegistry()
        reg.register_job_type(_spec("demo.long_job@v1", LongJobH))
        reg.register_job_type(_spec("demo.stuck_job@v1", StuckJobH))

        storage = PostgresStorage(DSN)
        jobs = JobService(reg, kernel, storage)
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        mon = MonitoringService(jobs, storage=storage)

        type_name = "demo.stuck_job@v1" if stuck else "demo.long_job@v1"
        total = int(RUN_SECONDS / UNIT_S)
        ctx = JobContext(actor=Actor.user("demo"))
        async with conn.transaction(), conn.cursor() as cur:
            ref = await jobs.builder(type_name).context(ctx) \
                .payload({"total": total,
                          "report_every_s": REPORT_EVERY_S}) \
                .enqueue(cur)
        print(f"enqueued {type_name} job={ref.id} "
              f"(total={total} units × {UNIT_S:.0f}s ≈ "
              f"{total * UNIT_S / 60:.0f} min, "
              f"report every {REPORT_EVERY_S:g}s, "
              f"stale after {STALE_S:g}s)")

        w = Worker(kernel, shutdown_timeout=5)
        await w.start()
        try:
            while True:
                job = await mon.get_job(conn, ref.id)
                stale = await mon.list_stale_progress(conn)
                is_stale = any(str(r["id"]) == ref.id for r in stale)
                prog = job.get("progress") or {}
                progress_at = job.get("progress_at")
                age = (f"{(datetime.now(UTC) - progress_at).total_seconds():.0f}s"
                       if progress_at else "-")
                print(
                    f"status={job['status']:<8} attempts={job['attempts']} "
                    f"progress={prog.get('current')}/{prog.get('total')} "
                    f"last_report={age} stale={is_stale} "
                    f"error={(job.get('error') or '')[:60]}")
                if job["status"] in ("done", "failed", "cancelled"):
                    break
                await asyncio.sleep(2)
        except KeyboardInterrupt:
            print("interrupted — job keeps running in the worker")
        finally:
            await w.stop(timeout=5)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
