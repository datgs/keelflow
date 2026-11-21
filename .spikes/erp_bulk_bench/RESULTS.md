# ERP bulk import benchmark — findings

Date: 2026-11-21 (v3 — reliability-fixed protocol; see notes below)
Environment: macOS (Darwin/arm64, M-series), Python 3.12.13, Postgres 16
(docker, localhost:5544), in-process Chancy worker, per-run queue
concurrency actually re-declared at each level (v1 wiring bug fixed),
`chunk_size=1000`, `row_disposition='archive'`, `atomic='row'` for the
per-row path / `atomic='chunk'` for the batch path.
Workload: supplier catalog import — validate `sku`/`name`/`price`/`qty`,
SKU-keyed upsert into `bench_products`, invalid rows to a persistent
error queue. ~8% deterministic bad rows. Same staged data for all
variants; every timed run gated on the expected valid/invalid split.

## v3 protocol (benchmark-reliability plan)

- Requires `KEELFLOW_BENCH_DSN` explicitly — no fallback to the dev/test
  DSN — plus `--allow-db`.
- Benchmark and `soak.py` share one PostgreSQL session advisory lock
  (`pg_try_advisory_lock`); a second writer refuses before any writes.
- All output reads/writes/deletes are scoped by `batch_id`; no global
  DELETE/TRUNCATE in the runner. Engine rows cleaned by `job_id` only.
- Correctness gate is now **exact**: full per-SKU product set, full
  per-row error set (`row_key` → error text), terminal parent status,
  no stranded rows, done rows accounted via archive. A failed run is
  excluded from summaries and the command exits non-zero.
- Per-run phase fields (in-process hooks, approximations):
  `enqueue_s`, `parent_queue_s` (enqueue → handler start),
  `parent_stage_s`, `parent_partition_s`, `callback_s` (summed
  process_row/process_batch wall time). Residual = engine bookkeeping
  + chunk queue + fan-in/retention.

Reproduce:
```bash
KEELFLOW_BENCH_DSN=postgresql://postgres:postgres@localhost:5544/keelflow \
  uv run --package keelflow-demo-import python -m bulk_import.bench \
      --sizes 100000 500000 --reps 3 --workers 1 4 8 \
      --allow-db --out .spikes/erp_bulk_bench/result.json
```

**Never run `soak.py` against the same database concurrently** — the
advisory lock enforces this; use a separate database if both are needed.

## Results (v4 — set-based partition + INSERT…SELECT staging)

Same v3 protocol; engine partition now uses one set-based statement
(claim + number + chunk insert + row assignment via `unnest`, hash
join) and the demo stages rows with `INSERT … SELECT` building JSONB
server-side. All runs pass the exact oracle gate.

### 1,000,000 rows (stage ≈6.5s, expected bad 80,458; reduced matrix w4+w8)

| variant | workers | mean s | rows/s | vs v3 |
|---|---|---|---|---|
| keelflow-batch | 4 | 61.9 | ~16,164 | -8% time |
| keelflow-batch | 8 | 60.5 | ~16,521 | -13% time |
| keelflow-row | 4 | 365.5 | ~2,736 | ~-1% |
| keelflow-row | 8 | 292.7 | ~3,416 | ~-0.6% |
| sql baseline | — | 6.9 | ~145,100 | ~same |

v4 1M phases (rep means, seconds):

| phase | v3 | v4 | change |
|---|---|---|---|
| parent stage (INSERT…SELECT) | ~17 | ~9 | -47% |
| parent super().run (partition+chunk jobs+push) | ~20 | ~17 | -15% |
| parent dispatch TX total | ~37 | ~26 | -30% |
| batch total w8 | ~69 | ~60 | -13% |

Notes:

- First set-based attempt was **slower** (measured `super_run` ~80s):
  joining row→chunk through a data-modifying CTE forced a nested loop
  with ~250M comparisons (EXPLAIN evidence). Passing chunk ids via
  `unnest` lets the join hash — that is what shipped.
- `push_many_ex` is used for call-site simplicity and one queue
  notification; Chancy 0.26 still inserts one kernel row per job —
  that part of the remaining ~17s `super_run` is kernel-row inserts.
- Remaining 1M batch cost (~60s): staging ~9s + partition ~17s +
  callback ~15s (aggregate, overlapped) + residual ~20s (chunk
  dispatch/row-status/fan-in archive).

### 100,000 / 500,000 (v4, all gates pass)

100k: batch w8 ~16.8k r/s · row w8 ~3.5k · sql ~155k.
500k: batch w8 ~11.4k r/s · row w8 ~3.2k · sql ~156k.
(v4 500k numbers are slightly below v3's — run-to-run DB variance;
both measured with the same exact oracle gate.)

## Results (v3 — exact oracle gate, dedicated-DSN + lock, all runs valid)

### 100,000 rows (stage ≈0.6s, expected bad 8,019)

| variant | workers | mean s | callback s | rows/s |
|---|---|---|---|---|
| keelflow-batch | 1 | 12.6 | 1.0 | ~7,918 |
| keelflow-batch | 4 | 6.8 | 1.2 | ~14,729 |
| keelflow-batch | 8 | 6.5 | 1.5 | ~15,425 |
| keelflow-row | 1 | 84.4 | 12.5 | ~1,185 |
| keelflow-row | 4 | 36.2 | 19.0 | ~2,761 |
| keelflow-row | 8 | 29.3 | 29.6 | ~3,410 |
| sql baseline | — | 0.7 | — | ~149,954 |

### 500,000 rows (stage ≈3.4s, expected bad 40,283)

| variant | workers | mean s | callback s | rows/s |
|---|---|---|---|---|
| keelflow-batch | 1 | 50.7 | 5.3 | ~9,860 |
| keelflow-batch | 4 | 36.5 | 6.7 | ~13,709 |
| keelflow-batch | 8 | 38.9 | 10.2 | ~12,869 |
| keelflow-row | 1 | 417.6 | 61.5 | ~1,197 |
| keelflow-row | 4 | 178.4 | 95.8 | ~2,802 |
| keelflow-row | 8 | 145.9 | 151.2 | ~3,427 |
| sql baseline | — | 3.2 | — | ~155,808 |

### 1,000,000 rows (stage ≈6.6s, expected bad 80,458; reduced matrix w4+w8)

| variant | workers | mean s | callback s | rows/s |
|---|---|---|---|---|
| keelflow-batch | 4 | 73.0 | 12.8 | ~13,704 |
| keelflow-batch | 8 | 69.2 | 15.2 | ~14,456 |
| keelflow-row | 4 | 359.3 | 189.0 | ~2,783 |
| keelflow-row | 8 | 294.6 | 303.7 | ~3,395 |
| sql baseline | — | 6.7 | — | ~148,191 |

1M phase breakdown (rep means; seconds):

| phase | row w8 | batch w8 |
|---|---|---|
| parent stage (1M executemany INSERT into `keelflow_row_status`) | ~17 | ~17 |
| parent partition (partition + enqueue 1,000 chunks) | ~19.5 | ~19.6 |
| callback (validate+upsert, summed across workers) | ~304 | ~15 |
| residual (chunk queue + row-status flips + fan-in/retention + poll) | ~‑36→0 | ~17 |
| **total** | ~295 | ~69 |

Notes on phase readings:

- `callback_s` is summed per-callback wall time **across concurrent
  workers** — it can exceed wall time when workers overlap (row w8:
  304s summed ≈ saturating ~8 workers over ~295s, i.e. the per-row
  `process_row` TX is the bottleneck there).
- In the **batch path the application callback is only ~15s of ~69s**;
  the dominant measured costs are now framework staging/partition
  (~37s total) plus ~17s of residual engine bookkeeping (chunk
  dispatch, set-based row-status flips, fan-in archive of ~920k rows).
- `parent_stage_s` (~17s for 1M rows via `executemany`) and
  `parent_partition_s` (~20s) are framework-owned and are the next
  optimization candidates (e.g. COPY-based staging).

## Results (v2 — superseded measurement protocol; kept for history)

> v3 note: these were measured before the exact-oracle gate existed,
> under a count-only check on shared tables. They pass the old gate and
> are believed uncontaminated (no soak ran concurrently); the v3 rerun
> above confirms the same throughput picture.
>
> **The earlier 1M run (`result_1m.json`) is INVALID**: it ran
> concurrently with `soak.py` on the same tables, and the v2 runner
> counted/deleted `bench_products` globally, so several reps failed the
> count gate (e.g. extra rows attributed to the batch). Timings from
> that file must not be quoted — the v3 1M table above replaces it.

### 100,000 rows (stage ≈0.6s, expected bad 8,019)

| variant | workers | mean s | rows/s |
|---|---|---|---|
| keelflow-batch | 1 | 14.1 | ~7,116 |
| keelflow-batch | 4 | 6.6 | ~15,187 |
| keelflow-batch | 8 | 6.5 | ~15,401 |
| keelflow-row | 1 | 87.1 | ~1,148 |
| keelflow-row | 4 | 35.6 | ~2,807 |
| keelflow-row | 8 | 29.0 | ~3,447 |
| sql baseline | — | 0.6 | ~155,897 |

### 500,000 rows (stage ≈3.1s, expected bad 40,283)

| variant | workers | mean s | rows/s |
|---|---|---|---|
| keelflow-batch | 1 | 48.2 | ~10,379 |
| keelflow-batch | 4 | 36.4 | ~13,750 |
| keelflow-batch | 8 | 34.5 | ~14,492 |
| keelflow-row | 1 | 415.7 | ~1,203 |
| keelflow-row | 4 | 179.6 | ~2,784 |
| keelflow-row | 8 | 149.4 | ~3,346 |
| sql baseline | — | 3.3 | ~152,638 |

## v2 conclusions

- **`process_batch` ≈ 4–5× the per-row path** at equal worker counts
  (100k: 15.4k vs 3.4k rows/s at 8 workers; 500k: 14.5k vs 3.3k), and
  it scales with worker concurrency while the row path barely does.
- Remaining gap to the SQL floor (~10×) is the engine's durable
  row/chunk bookkeeping (status flips + archive at fan-in) plus the
  per-row `BatchRowResult` Python classification loop — the price of
  per-row error queues, resumability, and audit.
- v1's "concurrency doesn't help" reading was a **wiring artifact**
  (queue declared once at max concurrency; per-level argument unused).
  With per-level declaration, both paths scale with worker count.

## v1 results (superseded — concurrency wiring invalid)

Environment: macOS (Darwin/arm64, M-series), Python 3.12.13, Postgres 16
(docker, localhost:5544), single in-process Chancy worker process,
`atomic='row'`, `chunk_size=1000`, `row_disposition='archive'`.

Error counts identical across variants (8,019 @100k / 40,283 @500k) —
the Python `_validate` and SQL `CASE` classifier agree, and the
correctness test (`examples/bulk_import/tests/test_bench.py`) asserts
identical product state and error counts.

## What the numbers mean

- **keelflow ≈ 3.5k rows/s** regardless of worker concurrency (1/4/8).
  The bottleneck is the per-row work *inside* `process_row`: validation
  + one `UPDATE keelflow_row_status` (processing → done) + one domain
  upsert per row, per transaction. Kernel fan-out scales chunks, but the
  per-row SQL round-trips inside the dispatch TX dominate.

  v2 NOTE: the "regardless of concurrency" part was a benchmark wiring
  artifact (queue declared once at max level). With per-level queue
  declaration the row path scales: 1.1k (w1) → 3.4k rows/s (w8). The
  per-row round-trip cost per row stands; superseded tables above.
- **sql baseline ≈ 150k rows/s** — ~45× faster — because one
  `INSERT…SELECT` with a `CASE` classifier validates and upserts the
  whole set in a single round-trip, with no per-row status bookkeeping.
- The gap is **data-path overhead, not orchestration**: the SQL
  baseline does no job/chunk bookkeeping and no durable row status. A
  fairer "same features" comparison would still be far slower than pure
  SQL, but a per-row engine API is the architectural limiter here.

## Conclusion (workload-specific)

For the classic ERP complaint — *"bulk imports are slow"* — the honest
answer this benchmark supports is:

- Keelflow's per-row `process_row` model is correct, durable, resumable,
  and auditable, but it is a **~3.5k rows/s** ceiling for write-heavy
  imports because each row costs multiple SQL round-trips inside the
  dispatch transaction.
- Set-based PostgreSQL processing of the same workload is ~45× faster.
- **v2: the `process_batch` chunk callback shipped** (see
  `docs/plans/active/bulk-batch-callback.md`) — one set-based callback
  per chunk, one outcome per engine row id, whole-chunk TX. It reaches
  ~14.5k rows/s at 8 workers (≈4–5× the row path, scaling with
  concurrency) while keeping chunk/retry/error-queue machinery. The
  remaining ~10× gap to raw SQL is the durable bookkeeping itself.

Caveats: local docker Postgres, synthetic uniform rows, single worker
process, `atomic='row'`; `atomic='chunk'` semantics differ (one TX per
chunk, whole-chunk rollback on failure). Numbers are workload-specific —
not a general "keelflow vs n8n" claim.
