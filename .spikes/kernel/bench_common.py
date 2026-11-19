"""Shared helpers for the kernel spike (T2: Chancy vs PgQueuer).

Usage:
    python bench_common.py reset          # drop & recreate the spike results schema
    python bench_common.py stats          # print exec/result counters

DSN is read from BENCH_DSN, defaulting to the local spike postgres.
"""
import os
import sys

import psycopg

DSN = os.environ.get(
    "BENCH_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bench_execs (
    kernel   text NOT NULL,
    job_id   text NOT NULL,
    exec_id  text NOT NULL,
    event    text NOT NULL,          -- 'start' | 'done'
    ts       timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS bench_results (
    kernel      text NOT NULL,
    job_id      text PRIMARY KEY,
    enqueue_ts  timestamptz,
    start_ts    timestamptz,
    done_ts     timestamptz
);
"""

STATS_SQL = """
SELECT kernel, event, count(*) FROM bench_execs GROUP BY kernel, event ORDER BY kernel, event;
SELECT kernel, count(*) FROM bench_results GROUP BY kernel;
SELECT kernel, count(*) AS jobs_with_dup_execs FROM (
    SELECT kernel, job_id, count(*) c
    FROM bench_execs WHERE event = 'start'
    GROUP BY kernel, job_id HAVING count(*) > 1
) d GROUP BY kernel;
"""

RESET_SQL = "DROP TABLE IF EXISTS bench_execs, bench_results;"


def reset() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(RESET_SQL)
        conn.execute(SCHEMA_SQL)
    print("bench tables reset")


def stats() -> None:
    with psycopg.connect(DSN) as conn:
        for stmt in STATS_SQL.strip().split(";"):
            stmt = stmt.strip()
            if not stmt:
                continue
            print(f"-- {stmt.splitlines()[0].strip()}")
            for row in conn.execute(stmt).fetchall():
                print("   ", row)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "stats"
    if cmd == "reset":
        reset()
    elif cmd == "stats":
        stats()
    else:
        print(__doc__)
