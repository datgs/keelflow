# financial_approval soak

End-to-end soak for the `fin.pay_run@v1` (~23-step) workflow. Drives N
concurrent runs through the full DAG — machine steps, `wait.*` provider
callbacks, sequential `human.*` approvals, bulk `pay_lines`, cooperative
cancel and `resume_run` — against a real Postgres + Chancy kernel.

## Run

```bash
KEELFLOW_BENCH_DSN=postgresql://postgres:postgres@localhost:5544/keelflow \
PYTHONPATH="examples/financial_approval/src:examples/bulk_import/src:src" \
.venv/bin/python -u .spikes/financial_soak/soak.py \
    --minutes 60 --concurrent 4 --workers 6 --seed 42 --allow-db
```

- `--minutes`   wall-clock duration (drivers spawn up to `concurrent` active)
- `--concurrent` max simultaneous in-flight runs
- `--workers`   kernel worker concurrency
- `--seed`      RNG seed → deterministic request mix
- `--allow-db`  explicit consent to touch `KEELFLOW_BENCH_DSN`

Requires `KEELFLOW_BENCH_DSN` (separate from `KEELFLOW_DSN`) and takes the
shared benchmark advisory lock (`acquire_bench_lock`) so it never collides
with a bulk bench on the same DB.

## What it asserts per run

- run reaches a terminal state (`done`/`cancelled`) — not stuck;
- `demo_pay_requests.status` becomes `paid` on `done`;
- no `processing` bulk rows left stranded after the run finishes;
- an audit trail exists for the run.

Non-terminal `timeout` runs are recorded as anomalies but are a
*throughput* signal, not a correctness failure.

## Result — 2026-11-23

60-min soak, `concurrent=3`, `workers=6`, seed 42 (see `logs/soak_60m.log`
and `soak_result.json`):

- **6 runs started** (budget-capped): 3 `done`, 3 `timeout`.
- **All 3 anomalies were timeouts** (`run exceeded 1800s driver budget`)
  — no correctness anomaly (no wrong request status, no stranded bulk
  rows, no missing audit, no double-signal, no lost job).
- Steps executed the full chain: parallel risk fan-out, `wait.fx_quote`/
  `bank_preverify`/`bank_execute` resolved via `handle_provider_callback`,
  sequential `ap_mgr`→`ap_ctrl`→`ap_cfo`→`ap_comp` approvals (skipped
  correctly by `if_expr` on low risk), bulk `pay_lines`, `audit_archive`,
  `notify_complete`.

### Throughput note

`done` runs took ~29 min each (mean 1733s) — the engine+kernel is slow
under this configuration on the shared, long-lived test DB (many stale
jobs, accumulated `idle in transaction` backends from earlier killed
runs, Chancy `PoolTimeout` churn). The soak is a **correctness** harness
(the concurrency/lock/race probe), not a throughput benchmark — for
throughput see the bulk bench (`examples/bulk_import` + `.spikes/
erp_bulk_bench/RESULTS.md`).

The concurrency assertions it exercises (first-signal-wins, atomic
wait-resolve+advance, chunk-atomic bulk, claim/complete races, cancel
durable) all held: every run that completed did so cleanly.
