"""Job functions for the Chancy benchmark.

Must be importable by dotted path from both the benchmark process and the
worker process (Chancy resolves `func` as an import string). Each job
record records `clock_timestamp()` in Postgres so latency numbers come from
one clock (the DB), not from two processes' wall clocks.
"""
from __future__ import annotations

import time
import uuid

import psycopg

DSN = "postgresql://postgres:postgres@localhost:5544/pyerp"


def fast(job_id: str) -> None:
    """Pure noop: no DB write. Measures kernel claim overhead only."""


def noop(job_id: str, enqueue_ts: float | None = None) -> None:
    exec_id = uuid.uuid4().hex[:12]
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO bench_execs (kernel, job_id, exec_id, event)"
            " VALUES ('chancy', %s, %s, 'start')",
            (job_id, exec_id),
        )
        if enqueue_ts is not None:
            conn.execute(
                "INSERT INTO bench_results (kernel, job_id, enqueue_ts, start_ts, done_ts)"
                " VALUES ('chancy', %s, to_timestamp(%s), clock_timestamp(), clock_timestamp())"
                " ON CONFLICT (job_id) DO UPDATE SET"
                "   start_ts = EXCLUDED.start_ts, done_ts = EXCLUDED.done_ts",
                (job_id, enqueue_ts),
            )
        else:
            conn.execute(
                "INSERT INTO bench_results (kernel, job_id, start_ts, done_ts)"
                " VALUES ('chancy', %s, clock_timestamp(), clock_timestamp())"
                " ON CONFLICT (job_id) DO UPDATE SET done_ts = clock_timestamp()",
                (job_id,),
            )
        conn.execute(
            "INSERT INTO bench_execs (kernel, job_id, exec_id, event)"
            " VALUES ('chancy', %s, %s, 'done')",
            (job_id, exec_id),
        )


def slow(job_id: str, seconds: float = 30.0) -> None:
    exec_id = uuid.uuid4().hex[:12]
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO bench_execs (kernel, job_id, exec_id, event)"
            " VALUES ('chancy', %s, %s, 'start')",
            (job_id, exec_id),
        )
    time.sleep(seconds)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO bench_execs (kernel, job_id, exec_id, event)"
            " VALUES ('chancy', %s, %s, 'done')",
            (job_id, exec_id),
        )
