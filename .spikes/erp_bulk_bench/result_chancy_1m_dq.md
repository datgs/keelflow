# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-23T10:55:24.666336+00:00`

config: `{"chunk": 1000, "invocation_id": "6e4457b2-f7b8-45da-b612-979ad0f9e32d", "kernel_backend": "chancy", "kernel_prefix": "keelflow_bench_", "reps": 3, "row_disposition": "archive", "seed": 42, "sizes": [1000000], "variants": ["keelflow-batch"], "workers": [8]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue TX · `parent_queue_s` enqueue-commit → handler start (incl. worker startup) · `parent_stage_s` app-owned row staging · `parent_super_run_s` engine partition+enqueue · `parent_tx_s` dispatch handler TX · `callback_sum_s`/`callback_max_s` aggregate worker-time (overlaps across workers — not a wall-time phase).

## 1,000,000 rows (stage 6.5s, expected bad 80,458, invalid runs 0)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 8 | 3 | 54.4 | 54.5 | 13.9 | 18,391 | yes |
