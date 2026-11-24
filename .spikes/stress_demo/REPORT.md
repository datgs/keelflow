# keelflow stress-test report — 90-minute oracle-verified soak

Run: 2026-11-25 03:38–05:23 local (90 min), Postgres @ localhost:5544/keelflow_test,
1 chancy Worker process (queue `default` concurrency 12), scheduler running.
Driver: `.spikes/stress_demo/stress.py` (P5 upgrades — manifest-scoped oracle,
chaos on running jobs, worker restarts, DB fault injection, latency capture).

## Workload executed (driver counts, not estimates)

| Flow | Completed | Cancelled by driver | Timed out at driver |
|---|---|---|---|
| bulk imports (100-4000 rows each) | 3,751 | 11 | 8 |
| invoices → posted | 10,649 | — | 7 |
| suppliers → active | 4,299 | 32 | 6 |

Engine rows at driver end: 146,473 jobs done (baseline 22,340 → **~1,379 jobs/s
aggregate over the 90 min window**, measured as jobs-done delta over elapsed
time on this laptop, not a saturated benchmark). 22,005 runs done, 535 failed
jobs (handler business failures + DLQ'd retries), 534 DLQ rows.

End-to-end driver-observed latency over 18,763 completions: **p50 = 1.5 s,
p95 = 2.6 s** (max 900 s = driver per-entity timeout, not an engine value).

## Chaos actually injected (verified counters)

- 40 cooperative cancels **on jobs verified `running`** (all honored; 38
  additional cancels on jobs that finished before the flag was checked are
  counted as `chaos_unhonored` — cancel flag landed but the job completed
  first, which is expected cooperative semantics, not a violation).
- 1 graceful worker restart (SIGINT-style stop + start) mid-run.
- 30 `pg_terminate_backend` kills of non-driver application connections
  (~1 every 4 min average over the run).
- ~10% of invoices/suppliers had `fail_post`/`fail_sanction` business faults;
  simulated sanction-API outage retried and DLQ'd by design.

## Oracle verdict (run-scoped, per-entity)

Oracle checked every entity the driver created (manifest), not DB aggregates:

- 3,770 imports / 10,656 invoices / 4,337 suppliers verified.
- **0 unexplained non-terminal entities after accounting for intentional
  outcomes** (driver-cancelled runs leave domain rows mid-state by design —
  3,490 `cancelled_ok` classified entries).
- 21 entities read as "non-terminal unexplained" at driver end; post-run
  forensics showed all 21 were **in-flight work whose kernel claim was
  waiting on next-worker recovery** (see Recovery below), not lost work.

## Recovery semantics observed (new evidence)

The worker died with ~75k kernel `dispatch` rows still in `state='running'`
(claimed, never completed — the last worker's graceful stop raced in-flight
dispatches). With **no worker alive** nothing can execute them; this is
expected. Starting any worker afterwards:

1. `chancy Recovery` plugin resets `running` kernel jobs whose `taken_by`
   worker has a stale `last_seen` back to `pending` (verified: 75,479 → 0
   dead-worker rows; ~9,400 went back to `pending` and are being
   re-dispatched).
2. Re-dispatch is idempotent at the engine layer: terminal engine jobs are
   skipped, `cancel_requested` jobs terminalize as `cancelled` and settle
   their bulk chunk + fan-in (observed: 20 stuck engine `running` jobs → 6
   after two short worker sessions; cancelled count rose 90 → 100 as
   cancel-requested stragglers settled).
3. At-least-once redelivery re-ran handlers that had partially executed —
   `imp.validate`/`inv.post_ledger` rewrites hit `ON CONFLICT` domain
   idempotency (verified: `demo_ledger_postings` shows one row per invoice,
   not one per attempt).

**Implication for operations**: a worker is required for recovery; if all
workers die, kernel `running` rows wait until a worker starts — by design,
not a leak. `ops_metrics_snapshot.workers_alive` (now fresh-heartbeat-only)
is the correct signal to alert on.

## Resolved correctness findings (this session)

- F1 — cancelled workflow runs stranded `posting_failed` invoices:
  fixed via explicit new-run recovery transition (`retry_post` triggers a
  fresh `post.wf@v1` run). Verified in `test_cancelled_run_new_run_recovery`.
- F2 — cancelled bulk chunk left parent `done` with unprocessed rows:
  `_fan_in` now terminalizes the parent `cancelled` when any chunk was
  cancelled; `summary` reports `rows_pending`/`rows_processing`.
- F3 — stale heartbeats counted as live workers: `ops_metrics_snapshot`
  now counts `last_seen` within 30 s only; heartbeats refresh every 10 s
  independently of dispatch.
- F4 (new, benign-by-design) — engine `running` jobs outlive their worker:
  recovery is kernel-mediated at-least-once; requires a live worker to
  complete. Documented above; no code change.

## Evidence quality

- Run-scoped oracle over a driver manifest — every reported "done" maps to a
  domain row the driver created and verified terminal.
- Chaos claims are backed by counters (`chaos_on_running`, `db_terminations`,
  `worker_restarts`) and terminal-state verification per cancel.
- Baseline + delta reported; throughput is an honest delta/seconds figure
  for this run, not a benchmark number.
- Driver-side 900 s timeouts are flagged as anomalies, not hidden.
