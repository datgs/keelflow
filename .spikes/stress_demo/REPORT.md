# Stress Demo Report — 2026-11-24

Sustained multi-app load against dedicated Postgres
(`keelflow_test` on :5544), chancy kernel, 12 workers, 15s scheduler.
Driver: `stress.py` (this dir). Raw artifacts: `stress-*-report.json`,
`stress-*.jsonl`, `run*.log` (local, untracked).

## Runs

| run | duration | outcome |
|-----|----------|---------|
| run3 | 60 min | clean — final report below |
| run4/5 | 13 + 30 min | rerun with driver fixes + technical-failure coverage |

## Coverage exercised (all three demos concurrently)

- **bulk_import**: CSV staging → map/approve human steps → validate +
  process bulk passes → archive/error-queue → delete finished imports.
  3824 runs in 60 min; ~1.7M raw rows staged; 3417 imports deleted.
- **invoice_flow**: submit → finance review → ledger/record DAG →
  gl_check → posted/rejected repost / posting_failed fix+resume.
  7725 posted (60 min) + 3679 (30 min); 846 reposts; 56 retries.
- **supplier_onboarding**: 4 parallel checks → app-owned bank callback
  resolves wait.* → conditional HITL → active. 3359 done (60 min);
  269 fail_sanction resumes; business rejects exercise rejection path.
- **Chaos**: 14 engine-level mid-run job cancels + 8 in run5; noise
  CRUD (insert/update/delete supplier+invoice rows) ~2700 ops.
- **Observability under load**: `keelflow_process_health` worker +
  scheduler heartbeats, `ops_metrics_snapshot` pending/waiting/DLQ
  fields all exercised continuously.

## Engine results (60-min run3)

- 172k+ `keelflow_jobs` processed total; 22.4k workflow runs done.
- Zero lost jobs: every driver-detectable anomaly traced below.
- Retries/DLQ work exactly as designed: `fail_sanction` (8%) and
  `fail_post` (~1.5%) jobs retry → DLQ → fix-flag → `resume_run` →
  terminal success (269 + 131 supplier resumes, 56 invoice retries).
- At-least-once proven benign: kernel redelivery of a bulk parent
  caused one `(job_id, idx)` duplicate-chunk insert → unique
  constraint → retry → early-return (no double work).
- Cooperative cancel lands correctly: chaos cancels → step 'cancelled'
  → run 'done/cancelled' → trigger callback applies `on_failed`
  transition.

## Findings

### F1 — no supported recovery for a workflow-cancelled entity (gap)

When a `post.wf` run is cancelled mid-flight (chaos), the entity lands
in `posting_failed` and `resume_run` refuses the 'cancelled' outcome
(`ValueError: not resumable`). The fix-task flow only covers technical
failures. Two invoices stranded permanently in run3, one in run5
(marked `cancel_unresumable` — driver no longer hot-loops).
**Options**: allow `resume_run` on 'cancelled' with an explicit
repost intent, or document that a cancelled run requires a new
submission and offer a `needs_fix`-style SM path. **Open decision.**

### F2 — cancelled chunk inside a 'succeeded' bulk pass leaves
pending rows (cancel semantics)

A chaos-cancelled chunk in `imp.validate` left 382 rows 'pending'
(chunked) while the parent sealed 'done' — archive + failed didn't
cover all rows (`row accounting 0 != 382` before the process pass
re-staged them). Rows weren't lost (process re-staged + completed
them; `demo_products` ended consistent), but a cancelled bulk step
can complete "successfully" while rows remain unprocessed.
**Options**: fan-in could mark the parent's outcome degraded when a
chunk was cancelled, or docs should state the semantic. **Open
decision — flagged for spec.**

### F3 — driver artifacts, not engine bugs

- `x.example` emails were rejected by `sup.validate` as 'invalid
  contact email' (validator works — 2 business rejects across runs).
- `human.gl_check`/`wait.bank_verify`/`human.approve` leftovers:
  driver-killed runs from earlier iterations, not engine leaks.

### Performance signal

~28-55 jobs/s sustained incl. human/wait resolution, no pool
starvation at `max_connection_pool_size=40`, oldest-pending grew only
on intentionally-stranded wait jobs. `demo_import_raw` reached ~2M
rows; bulk partition + fan-in stayed stable.

## Reproduce

    KEELFLOW_STRESS_DSN=postgresql://postgres:postgres@localhost:5544/keelflow_test \
        uv run python .spikes/stress_demo/stress.py \
        --minutes 60 --workers 12 --imports 3 --invoices 5 \
        --suppliers 5 --allow-db
