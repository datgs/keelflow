# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-23T06:30:14.543219+00:00`

config: `{"chunk": 1000, "invocation_id": "e13d4216-d1e6-4ad9-b1a5-f0c596c3f29e", "kernel_prefix": "keelflow_bench_", "reps": 2, "row_disposition": "archive", "seed": 42, "sizes": [500000], "variants": ["keelflow-batch"], "workers": [8]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue TX · `parent_queue_s` enqueue-commit → handler start (incl. worker startup) · `parent_stage_s` app-owned row staging · `parent_super_run_s` engine partition+enqueue · `parent_tx_s` dispatch handler TX · `callback_sum_s`/`callback_max_s` aggregate worker-time (overlaps across workers — not a wall-time phase).

## 500,000 rows (stage 3.3s, expected bad 40,283, invalid runs 0)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 8 | 2 | 25.3 | 25.3 | 6.5 | 19,800 | yes |
