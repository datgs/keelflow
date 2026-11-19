"""Chancy 0.26.0 kernel benchmark (T2 spike).

Subcommands:
  enqueue [--jobs N]     push N jobs via push_many, report inserts/sec
  worker                 run a worker until Ctrl+C (used by drain/dedup/latency/kill-resume)
  drain [--jobs N]       enqueue N, then run worker until all done; report jobs/sec
  latency [--samples N]  enqueue one job at a time, measure enqueue→handler-start
  dedup [--jobs N]       enqueue N jobs sharing one unique_key; verify single execution
  kill-resume            worker claims a slow job; we kill it and verify recovery

Run from .spikes/kernel with: ../.venv/bin/python bench_chancy.py <cmd>
"""
from __future__ import annotations

import argparse
import asyncio
import os
import signal
import statistics
import sys
import time
import uuid

import psycopg

from chancy import Chancy, Job, Queue, Worker

DSN = os.environ.get(
    "BENCH_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
QUEUE = "bench"
PREFIX = "chancy_bench_"


def make_job(
    job_id: str, enqueue_ts: float | None = None, func: str = "bench_jobs.noop"
) -> Job:
    return Job(
        func=func,
        queue=QUEUE,
        kwargs={"job_id": job_id, "enqueue_ts": enqueue_ts}
        if func == "bench_jobs.noop"
        else {"job_id": job_id},
        meta={"bench": True},
    )


async def setup() -> Chancy:
    # Recovery poll 5s (default 60s) so kill-resume exercises don't take minutes.
    from chancy.plugins.recovery import Recovery

    chancy = Chancy(DSN, prefix=PREFIX, plugins=[Recovery(poll_interval=5)])
    await chancy.migrate()
    await chancy.declare(
        Queue(
            QUEUE,
            concurrency=200,
            executor="chancy.executors.thread.ThreadedExecutor",
            polling_interval=1,
            eager_polling=True,
        ),
        upsert=True,
    )
    return chancy


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


async def cmd_enqueue(jobs: int) -> None:
    async with await setup() as chancy:
        batch = [make_job(f"c-{i}") for i in range(jobs)]
        t0 = time.perf_counter()
        n = 0
        async for refs in chancy.push_many(batch, batch_size=1000):
            n += len(refs)
        dt = time.perf_counter() - t0
        print(f"chancy: pushed {n} jobs in {dt:.2f}s = {n / dt:.0f} jobs/s")


async def run_worker(heartbeat_poll: int = 30, heartbeat_timeout: int = 90) -> None:
    chancy = await setup()
    worker = Worker(
        chancy,
        heartbeat_poll_interval=heartbeat_poll,
        heartbeat_timeout=heartbeat_timeout,
        shutdown_timeout=5,
    )
    await worker.start()
    await worker.wait_for_shutdown()


async def cmd_drain(jobs: int, func: str = "bench_jobs.fast") -> None:
    async with await setup() as chancy:
        async for _ in chancy.push_many(
            [make_job(f"c-{i}", func=func) for i in range(jobs)], batch_size=1000
        ):
            pass
        chancy.log.info("enqueued %d jobs, starting worker", jobs)
        await asyncio.sleep(0.5)  # let enqueue txn settle
        t0 = time.perf_counter()
        w = Worker(chancy, shutdown_timeout=10)
        await w.start()
        # For 'fast' func (no DB writes), completion is counted via the
        # chancy jobs table itself; for 'noop', use bench_results.
        def done_count() -> int:
            if func == "bench_jobs.fast":
                with psycopg.connect(DSN) as conn:
                    return conn.execute(
                        f"SELECT count(*) FROM {PREFIX}jobs WHERE state='succeeded'"
                    ).fetchone()[0]
            return count_results("chancy")

        ok = await wait_for(
            lambda: done_count() >= jobs,
            timeout=jobs / 100 + 300,
            poll=0.2,
            desc=f"{jobs} done",
        )
        dt = time.perf_counter() - t0
        await w.stop(timeout=10)
        starts = count_execs("chancy", "start")
        print(
            f"chancy: drained {done_count()}/{jobs} jobs in {dt:.2f}s"
            f" = {jobs / dt:.0f} jobs/s (exec starts: {starts}, dup-execs: {starts - jobs})"
            + ("" if ok else " [TIMED OUT]")
        )


async def cmd_latency(samples: int) -> None:
    async with await setup() as chancy:
        w = Worker(chancy)
        await w.start()
        await asyncio.sleep(1)  # worker ready, listening
        lat = []
        for i in range(samples):
            job_id = f"lat-{uuid.uuid4().hex[:8]}"
            t0 = time.time()  # enqueue_ts used by handler to compute db-time latency
            await chancy.push(make_job(job_id, enqueue_ts=t0))
            t_commit = time.time()
            ok = await wait_for(
                lambda: started(job_id),
                timeout=10,
                poll=0.01,
                desc=f"job {job_id} start",
            )
            if not ok:
                continue
            # measure against wall clock commit → observed start via bench_results
            with psycopg.connect(DSN) as conn:
                row = conn.execute(
                    "SELECT EXTRACT(EPOCH FROM (start_ts - enqueue_ts))*1000 FROM bench_results WHERE job_id=%s",
                    (job_id,),
                ).fetchone()
            if row:
                lat.append(row[0])
            _ = t_commit
        await w.stop(timeout=5)
        if lat:
            lat.sort()
            print(
                f"chancy: latency n={len(lat)} "
                f"median={statistics.median(lat):.1f}ms "
                f"p95={lat[int(len(lat) * 0.95) - 1]:.1f}ms "
                f"max={max(lat):.1f}ms"
            )
        else:
            print("chancy: no latency samples")


def started(job_id: str) -> bool:
    with psycopg.connect(DSN) as conn:
        return conn.execute(
            "SELECT 1 FROM bench_results WHERE job_id=%s AND start_ts IS NOT NULL",
            (job_id,),
        ).fetchone() is not None


async def cmd_dedup(jobs: int) -> None:
    async with await setup() as chancy:
        key = f"dedup-{uuid.uuid4().hex[:8]}"
        batch = [
            make_job(f"dup-{i}").with_unique_key(key) for i in range(jobs)
        ]
        async for refs in chancy.push_many(batch, batch_size=1000):
            pushed = len(refs)
        with psycopg.connect(DSN) as conn:
            rows = conn.execute(
                f"SELECT count(*) FROM {PREFIX}jobs WHERE unique_key=%s", (key,)
            ).fetchone()[0]
        w = Worker(chancy)
        await w.start()
        await wait_for(
            lambda: count_execs("chancy", "done") >= 1, timeout=30, desc="dedup done"
        )
        await w.stop(timeout=5)
        print(
            f"chancy: pushed {pushed} jobs with unique_key={key};"
            f" rows in jobs table: {rows}; executions started:"
            f" {count_execs('chancy', 'start')}"
        )


async def cmd_kill_resume() -> None:
    import subprocess

    async with await setup() as chancy:
        job_id = f"slow-{uuid.uuid4().hex[:8]}"
        await chancy.push(
            Job(
                func="bench_jobs.slow",
                queue=QUEUE,
                kwargs={"job_id": job_id, "seconds": 60},
                max_attempts=3,
            )
        )
        # Start a real worker subprocess, wait for the job to start, then SIGKILL.
        proc = subprocess.Popen(
            [sys.executable, "bench_chancy.py", "worker",
             "--heartbeat-poll", "2", "--heartbeat-timeout", "10"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        try:
            await wait_for(
                lambda: count_execs("chancy", "start") >= 1,
                timeout=30,
                desc="job picked up",
            )
            t_kill = time.monotonic()
            proc.send_signal(signal.SIGKILL)
            proc.wait()
            print(f"killed worker pid {proc.pid}; waiting for recovery…")
            # New worker takes over via Recovery plugin (runs on leader).
            await run_worker(heartbeat_poll=2, heartbeat_timeout=10)
        finally:
            if proc.poll() is None:
                proc.kill()
        _ = t_kill


async def cmd_worker(heartbeat_poll: int, heartbeat_timeout: int) -> None:
    await run_worker(heartbeat_poll=heartbeat_poll, heartbeat_timeout=heartbeat_timeout)


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("enqueue", "drain", "latency", "dedup", "kill-resume"):
        s = sub.add_parser(name)
        s.add_argument("--jobs", type=int, default=1000)
        s.add_argument("--samples", type=int, default=20)
        s.add_argument(
            "--func", default="bench_jobs.fast",
            help="dotted job func for drain (fast=no DB write, noop=record execs)",
        )
    w = sub.add_parser("worker")
    w.add_argument("--heartbeat-poll", type=int, default=30)
    w.add_argument("--heartbeat-timeout", type=int, default=90)
    args = p.parse_args()

    if args.cmd == "enqueue":
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
        coro = cmd_worker(args.heartbeat_poll, args.heartbeat_timeout)
    asyncio.run(coro)


if __name__ == "__main__":
    main()
