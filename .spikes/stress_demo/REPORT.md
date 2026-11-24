# Stress Demo Report — 2026-11-24 (revised)

Exploratory soak of three demo apps against dedicated Postgres
(`keelflow_test` on :5544), chancy kernel, **one Worker process** with
queue concurrency 12 (not 12 worker processes). Driver:
`stress.py`. Raw artifacts: `stress-*-report.json`, `stress-*.jsonl`,
`run*.log` (local, untracked).

## What the evidence does and does not show

This is a **soak, not a production-readiness certification**. The
numbers below are whole-database snapshots taken at run end, including
rows left by earlier attempts; they are not per-run throughput or
completeness counts.

| claim in v1 | status after review |
|---|---|
| ~246k jobs processed | **aggregate DB count**, not per-run |
| "zero lost jobs" | not proven — no run-scoped oracle |
| "at-least-once proven benign" | one observed self-heal, not a proof |
| "28–55 jobs/s sustained" | ~28/s avg from DB `done` deltas only; no latency or capacity data |
| "no pool starvation" | no pool errors logged, but pool wait never measured |
| "heartbeat + pool healthy" | `workers_alive` counted stale rows; heartbeat only fires on dispatch |
| chaos cancels | targeted `pending`/`claimed` jobs only, cancel result unchecked |

## Verified engine behavior under load

- One `(job_id, idx)` duplicate-chunk insert on bulk-parent redelivery
  → unique constraint → retry → early-return (single observed case;
  demonstrates the guard exists, not that every redelivery is benign).
- Retries → DLQ → fix-flag → `resume_run` worked for `fail_sanction`
  (8%) and `fail_post` (~1.5%) across both runs.
- Cooperative cancel lands: cancelled step → run `done/cancelled` →
  trigger callback applies `on_failed`.

## Findings (drove the correctness work)

### F1 — cancelled workflow run strands the entity

`resume_run` refuses `cancelled`; a cancelled `post.wf` left invoices
in `posting_failed` with no supported recovery. **Resolved:** new-run
recovery path shipped — `posting_failed → posting` (`retry_post`) now
triggers a fresh `post.wf` run via `WorkflowTriggerSpec`; domain writes
are idempotent; `resume_run` still refuses cancelled runs on purpose.

### F2 — cancelled chunk reported parent 'done'

A chaos-cancelled chunk left 382 rows `pending` while the parent sealed
`done`. **Resolved:** fan-in now terminalizes the parent `cancelled`
when any chunk is cancelled; `rows_pending`/`rows_processing` are in
the result summary. Spec 04 updated.

### F3 — liveness metrics were misleading

`workers_alive` counted stale heartbeat rows; heartbeat fired only on
dispatch. **Resolved:** D3 contract implemented — 10s heartbeat loop
independent of dispatch, 30s stale threshold; `workers_alive` counts
fresh rows only, `workers_stale` and `schedulers_alive` added.

## Corrected run numbers (whole-DB deltas, exploratory)

- run3 (60 min): `done` jobs 71k→172k; runs done 7.3k (includes prior
  attempts' rows; imports 3824, invoices 7725, suppliers 3359 driven).
- run5 (30 min): +49k `done` jobs; invoices 3679 posted incl. 56
  fix-resume retries, suppliers 1723, imports 1880.
- 2 invoices stranded pre-F1-fix (cancelled run, unresumable);
  1 supplier failed on legit business reject (bad email).

## Reproduce

    KEELFLOW_STRESS_DSN=postgresql://postgres:postgres@localhost:5544/keelflow_test \
        uv run python .spikes/stress_demo/stress.py \
        --minutes 60 --workers 12 --imports 3 --invoices 5 \
        --suppliers 5 --allow-db
