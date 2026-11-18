"""PgQueuer 2.0.0 kernel benchmark (T2 spike).

Subcommands:
  install            create pgqueuer schema objects (pgq install equivalent)
  enqueue [--jobs N] enqueue N jobs via batched Queries.enqueue, report inserts/s
  worker             run consumer until Ctrl+C (used by drain/dedup/latency/kill-resume)
  drain [--jobs N]   enqueue N, run consumer in drain mode, report jobs/s
  latency [--samples N]  enqueue one job at a time, measure enqueue→handler-start
  dedup [--jobs N]   enqueue N jobs sharing one dedupe_key; verify single execution
  kill-resume        worker picks a slow job; we SIGKILL it and verify re-dispatch

Run from .spikes/kernel with: ../.venv/bin/python bench_pgq.py <cmd>
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import statistics
import sys
import time
import uuid

import asyncpg
import psycopg

from pgqueuer import PgQueuer, Queries
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.types import QueueExecutionMode
from pgqueuer.models import Job

DSN = os.environ.get(
    "BENCH_DSN", "postgresql://postgres:postgres@localhost:5544/pyerp"
)


def count_execs(kernel: str, event: str = "start") -> int:
    with psycopg.connect(DSN) as conn:
        return conn.execute(
            "SELECT count(*) FROM bench_execs WHERE kernel=%s AND event=%s",
            (kernel, event),
        ).fetchone()[0]


def count_results(kernel: str) -> int:
    with psycopg.connect(DSN) as conn:
        return conn.execute(
            "SELECT count(*) FROM bench_results WHERE kernel=%s AND done_ts IS NOT NULL",
            (kernel,),
        ).fetchone()[0]


async def wait_for(pred, timeout: float, poll: float = 0.05, desc: str = "") -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(poll)
    print(f"TIMEOUT waiting for {desc}", file=sys.stderr)
    return False


def make_pgq(conn: asyncpg.Connection) -> PgQueuer:
    pgq = PgQueuer(AsyncpgDriver(conn))

    @pgq.entrypoint("fast", concurrency_limit=200)
    async def fast(job: Job) -> None:  # noqa: D103 — pure noop, no DB write
        return None

    @pgq.entrypoint("noop", concurrency_limit=200)
    async def noop(job: Job) -> None:
        rec = json.loads(job.payload) if job.payload else {}
        job_id = rec.get("job_id", str(job.id))
        exec_id = uuid.uuid4().hex[:12]
        wconn = await asyncpg.connect(DSN)
        try:
            await wconn.execute(
                "INSERT INTO bench_execs (kernel, job_id, exec_id, event)"
                " VALUES ('pgq', $1, $2, 'start')",
                job_id,
                exec_id,
            )
            if rec.get("enqueue_ts") is not None:
                await wconn.execute(
                    "INSERT INTO bench_results (kernel, job_id, enqueue_ts, start_ts, done_ts)"
                    " VALUES ('pgq', $1, to_timestamp($2), clock_timestamp(), clock_timestamp())"
                    " ON CONFLICT (job_id) DO UPDATE SET"
                    "   start_ts = EXCLUDED.start_ts, done_ts = EXCLUDED.done_ts",
                    job_id,
                    rec["enqueue_ts"],
                )
            else:
                await wconn.execute(
                    "INSERT INTO bench_results (kernel, job_id, start_ts, done_ts)"
                    " VALUES ('pgq', $1, clock_timestamp(), clock_timestamp())"
                    " ON CONFLICT (job_id) DO UPDATE SET done_ts = clock_timestamp()",
                    job_id,
                )
            await wconn.execute(
                "INSERT INTO bench_execs (kernel, job_id, exec_id, event)"
                " VALUES ('pgq', $1, $2, 'done')",
                job_id,
                exec_id,
            )
        finally:
            await wconn.close()

    @pgq.entrypoint("slow")
    async def slow(job: Job) -> None:
        rec = json.loads(job.payload)
        job_id = rec["job_id"]
        exec_id = uuid.uuid4().hex[:12]
        wconn = await asyncpg.connect(DSN)
        try:
            await wconn.execute(
                "INSERT INTO bench_execs (kernel, job_id, exec_id, event)"
                " VALUES ('pgq', $1, $2, 'start')",
                job_id,
                exec_id,
            )
            await asyncio.sleep(rec.get("seconds", 60))
            await wconn.execute(
                "INSERT INTO bench_execs (kernel, job_id, exec_id, event)"
                " VALUES ('pgq', $1, $2, 'done')",
                job_id,
                exec_id,
            )
        finally:
            await wconn.close()

    return pgq


async def cmd_install() -> None:
    print("use: ../.venv/bin/pgq install --pg-dsn", DSN)


async def cmd_enqueue(jobs: int) -> None:
    conn = await asyncpg.connect(DSN)
    try:
        queries = Queries(AsyncpgDriver(conn))
        payloads = [
            json.dumps({"job_id": f"p-{i}"}).encode() for i in range(jobs)
        ]
        t0 = time.perf_counter()
        ids = await queries.enqueue(
            ["noop"] * jobs, payloads, [0] * jobs
        )
        dt = time.perf_counter() - t0
        n = len([i for i in ids if i is not None])
        print(f"pgq: enqueued {n} jobs in {dt:.2f}s = {n / dt:.0f} jobs/s")
    finally:
        await conn.close()


async def cmd_worker(heartbeat_timeout: float, batch_size: int) -> None:
    conn = await asyncpg.connect(DSN)
    try:
        pgq = make_pgq(conn)
        import datetime as dt

        await pgq.run(
            heartbeat_timeout=dt.timedelta(seconds=heartbeat_timeout),
            batch_size=batch_size,
        )
    finally:
        await conn.close()


async def cmd_drain(jobs: int, entrypoint: str = "fast") -> None:
    conn = await asyncpg.connect(DSN)
    try:
        queries = Queries(AsyncpgDriver(conn))
        payloads = [
            json.dumps({"job_id": f"p-{i}"}).encode() for i in range(jobs)
        ]
        await queries.enqueue([entrypoint] * jobs, payloads, [0] * jobs)
        pgq = make_pgq(conn)
        import datetime as dt

        t0 = time.perf_counter()
        await pgq.run(
            mode=QueueExecutionMode.drain,
            batch_size=100,
            heartbeat_timeout=dt.timedelta(seconds=30),
        )
        elapsed = time.perf_counter() - t0
        if entrypoint == "fast":
            done = conn2_count_successful()
            starts = done
        else:
            done = count_results("pgq")
            starts = count_execs("pgq", "start")
        print(
            f"pgq: drained {done}/{jobs} jobs in {elapsed:.2f}s"
            f" = {jobs / elapsed:.0f} jobs/s (exec starts:"
            f" {starts}, dup-execs:"
            f" {starts - jobs})"
        )
    finally:
        await conn.close()


def conn2_count_successful() -> int:
    with psycopg.connect(DSN) as conn:
        return conn.execute(
            "SELECT count(*) FROM pgqueuer_log WHERE status='successful'"
        ).fetchone()[0]


def started(job_id: str) -> bool:
    with psycopg.connect(DSN) as conn:
        return conn.execute(
            "SELECT 1 FROM bench_results WHERE job_id=%s AND start_ts IS NOT NULL",
            (job_id,),
        ).fetchone() is not None


async def cmd_latency(samples: int) -> None:
    conn = await asyncpg.connect(DSN)
    enq = await asyncpg.connect(DSN)
    try:
        pgq = make_pgq(conn)
        runner = asyncio.create_task(pgq.run())
        await asyncio.sleep(1)
        queries = Queries(AsyncpgDriver(enq))
        lat = []
        for _ in range(samples):
            job_id = f"lat-{uuid.uuid4().hex[:8]}"
            await queries.enqueue(
                "noop",
                json.dumps({"job_id": job_id, "enqueue_ts": time.time()}).encode(),
            )
            if await wait_for(lambda: started(job_id), 10, 0.01, job_id):
                with psycopg.connect(DSN) as c:
                    row = c.execute(
                        "SELECT EXTRACT(EPOCH FROM (start_ts - enqueue_ts))*1000"
                        " FROM bench_results WHERE job_id=%s",
                        (job_id,),
                    ).fetchone()
                if row:
                    lat.append(row[0])
        runner.cancel()
        try:
            await runner
        except (asyncio.CancelledError, Exception):
            pass
        if lat:
            lat.sort()
            print(
                f"pgq: latency n={len(lat)} "
                f"median={statistics.median(lat):.1f}ms "
                f"p95={lat[int(len(lat) * 0.95) - 1]:.1f}ms "
                f"max={max(lat):.1f}ms"
            )
    finally:
        await enq.close()
        await conn.close()


async def cmd_dedup(jobs: int) -> None:
    conn = await asyncpg.connect(DSN)
    try:
        queries = Queries(AsyncpgDriver(conn))
        key = f"dedup-{uuid.uuid4().hex[:8]}"
        payloads = [
            json.dumps({"job_id": f"dup-{i}"}).encode() for i in range(jobs)
        ]
        ids = await queries.enqueue(
            ["noop"] * jobs,
            payloads,
            [0] * jobs,
            dedupe_key=[key] * jobs,
            on_conflict="skip",
        )
        kept = len([i for i in ids if i is not None])
        # drain
        pgq = make_pgq(conn)
        await pgq.run(mode=QueueExecutionMode.drain)
        print(
            f"pgq: enqueued {jobs} jobs with dedupe_key={key} on_conflict=skip;"
            f" accepted rows: {kept}; executions: {count_execs('pgq', 'start')}"
        )
    finally:
        await conn.close()


async def cmd_kill_resume() -> None:
    import datetime as dt
    import subprocess

    conn = await asyncpg.connect(DSN)
    try:
        queries = Queries(AsyncpgDriver(conn))
        job_id = f"slow-{uuid.uuid4().hex[:8]}"
        await queries.enqueue(
            "slow",
            json.dumps({"job_id": job_id, "seconds": 25}).encode(),
        )
        proc = subprocess.Popen(
            [
                sys.executable,
                "bench_pgq.py",
                "worker",
                "--heartbeat-timeout",
                "5",
            ],
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        try:
            await wait_for(
                lambda: count_execs("pgq", "start") >= 1,
                timeout=30,
                desc="job picked up",
            )
            t_kill = time.monotonic()
            proc.send_signal(signal.SIGKILL)
            proc.wait()
            print(f"killed worker pid {proc.pid}; waiting for re-dispatch…")
            pgq = make_pgq(conn)
            runner = asyncio.create_task(
                pgq.run(heartbeat_timeout=dt.timedelta(seconds=5))
            )
            try:
                await wait_for(
                    lambda: count_execs("pgq", "done") >= 1,
                    timeout=120,
                    poll=0.5,
                    desc="job re-run done",
                )
            finally:
                runner.cancel()
                import contextlib

                with contextlib.suppress(Exception, asyncio.CancelledError):
                    await runner
            elapsed = time.monotonic() - t_kill
            starts = count_execs("pgq", "start")
            dones = count_execs("pgq", "done")
            print(
                f"pgq: kill→done in {elapsed:.1f}s; exec starts={starts}"
                f" (re-executions={starts - 1}), dones={dones}"
            )
        finally:
            if proc.poll() is None:
                proc.kill()
    finally:
        await conn.close()


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("install", "enqueue", "drain", "latency", "dedup", "kill-resume"):
        s = sub.add_parser(name)
        s.add_argument("--jobs", type=int, default=1000)
        s.add_argument("--samples", type=int, default=20)
        s.add_argument(
            "--func", default="fast",
            help="entrypoint for drain (fast=no DB write, noop=record execs)",
        )
    w = sub.add_parser("worker")
    w.add_argument("--heartbeat-timeout", type=float, default=30)
    w.add_argument("--batch-size", type=int, default=50)
    args = p.parse_args()

    if args.cmd == "install":
        coro = cmd_install()
    elif args.cmd == "enqueue":
        coro = cmd_enqueue(args.jobs)
    elif args.cmd == "drain":
        coro = cmd_drain(args.jobs, args.func)
    elif args.cmd == "latency":
        coro = cmd_latency(args.samples)
    elif args.cmd == "dedup":
        coro = cmd_dedup(args.jobs)
    elif args.cmd == "kill-resume":
        coro = cmd_kill_resume()
    else:
        coro = cmd_worker(args.heartbeat_timeout, args.batch_size)
    asyncio.run(coro)


if __name__ == "__main__":
    main()
