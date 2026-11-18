# Kernel benchmark spike (T2)

Compares **Chancy 0.26.0** vs **PgQueuer 2.0.0** as the adopted job kernel for
pyerp. Decision memo: `docs/decisions/0001-kernel-chancy.md`.

## Setup

- Postgres: `postgres:16-alpine` container `pyerp-postgres`, DSN
  `postgresql://postgres:postgres@localhost:5544/pyerp` (override via `BENCH_DSN`)
- Venv: `.spikes/kernel/.venv` with `chancy==0.26.0`, `pgqueuer==2.0.0`,
  `psycopg[binary]`, `asyncpg`
- Schema: `pgq install` (pgqueuer objects); chancy self-migrates on `Chancy(DSN).migrate()`
- Bench tables: `python bench_common.py reset` creates `bench_execs`/`bench_results`

## Commands

Run from `.spikes/kernel/`:

```bash
# chancy
.venv/bin/python bench_chancy.py enqueue --jobs 5000
.venv/bin/python bench_chancy.py drain --jobs 5000 --func bench_jobs.fast
.venv/bin/python bench_chancy.py latency --samples 20
.venv/bin/python bench_chancy.py dedup --jobs 100
.venv/bin/python bench_chancy.py kill-resume

# pgqueuer
.venv/bin/python bench_pgq.py enqueue --jobs 5000
.venv/bin/python bench_pgq.py drain --jobs 20000 --func fast
.venv/bin/python bench_pgq.py latency --samples 20
.venv/bin/python bench_pgq.py dedup --jobs 100
.venv/bin/python bench_pgq.py kill-resume
```

`--func fast` = pure noop handler (kernel overhead only);
`--func bench_jobs.noop` / `noop` = handler writes start/done rows to bench
tables (more representative).

## Results (2026-11-18, M-series Mac, postgres:16-alpine, localhost)

| Test | Chancy 0.26.0 | PgQueuer 2.0.0 |
|---|---|---|
| Enqueue (5k, batch) | ~6,100 jobs/s | ~60,000 jobs/s |
| Drain 5k, no-DB handler | ~2,060 jobs/s | ~20,400 jobs/s (20k) |
| Drain 5k, DB-writing handler | ~397 jobs/s | — |
| Enqueue→handler latency | median 11.7ms, p95 21.9ms | median ~9.4ms, p95 36.8ms |
| Dedup key (100 pushes, 1 key) | 1 row, 1 execution | 1 row, 1 execution |
| Kill-resume (SIGKILL mid-job) | re-picked in ~60s (Recovery poll 5s + heartbeat 10s cfg) | re-picked ~5s heartbeat timeout, done 55s |
| At-least-once under kill | 2 starts / 1 done — confirmed | 2 starts / 1 done — confirmed |
| Job PK | UUID | BIGSERIAL (2.0 widened int4) |

## Config gotchas found

- **Chancy `eager_polling=True` is required** for drain throughput: default loop
  fetches at most `concurrency` jobs per `polling_interval` tick (~15 jobs/s at
  16/1s). With eager polling it re-fetches on each completion.
- **PgQueuer `concurrency_limit` is global per entrypoint** (across all workers),
  not per-worker. `=0` means unlimited. Values set the total in-flight cap.
- **PgQueuer worker and producer need separate connections** — sharing one
  asyncpg connection between `pgq.run()` and `Queries.enqueue` corrupts the
  protocol state (`InternalClientError`).
- **PgQueuer `mode=drain` does not wait for stale picked jobs** — it exits when
  nothing is queued, even if a killed worker's job is still `picked`. Not a
  correctness bug (a live continuous worker re-picks it), but affects test design.
- **Chancy recovery latency** = `heartbeat_timeout` (default 90s) + Recovery
  plugin `poll_interval` (default 60s). Both tunable; ~60s observed at 10s/5s.
- **PgQueuer reclaim** is inline in the dequeue query (any live worker re-picks
  heartbeats older than `heartbeat_timeout`, default 30s).
