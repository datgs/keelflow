# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-22T16:21:43.542507+00:00`

config: `{"chunk": 1000, "invocation_id": "cf1952af-1c9f-40ab-b9d7-d7f6e84f8af6", "kernel_prefix": "keelflow_bench_", "reps": 3, "row_disposition": "archive", "seed": 42, "sizes": [1000000], "variants": ["keelflow-batch"], "workers": [1, 2, 4, 8, 16]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue TX · `parent_queue_s` enqueue-commit → handler start (incl. worker startup) · `parent_stage_s` app-owned row staging · `parent_super_run_s` engine partition+enqueue · `parent_tx_s` dispatch handler TX · `callback_sum_s`/`callback_max_s` aggregate worker-time (overlaps across workers — not a wall-time phase).

## 1,000,000 rows (stage 8.1s, expected bad 80,458, invalid runs 0)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 1 | 3 | 92.5 | 95.6 | 10.6 | 10,806 | yes |
| keelflow-batch | 2 | 3 | 69.5 | 69.2 | 11.1 | 14,379 | yes |
| keelflow-batch | 4 | 3 | 67.0 | 66.2 | 13.3 | 14,917 | yes |
| keelflow-batch | 8 | 3 | 64.7 | 64.8 | 16.2 | 15,457 | yes |
| keelflow-batch | 16 | 3 | 59.9 | 57.7 | 22.9 | 16,690 | yes |
