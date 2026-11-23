# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-23T06:29:07.436380+00:00`

config: `{"chunk": 1000, "invocation_id": "1d2b55fb-12af-4deb-b335-12435aa8fa78", "kernel_prefix": "keelflow_bench_", "reps": 2, "row_disposition": "archive", "seed": 42, "sizes": [500000], "variants": ["keelflow-batch"], "workers": [8]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue TX · `parent_queue_s` enqueue-commit → handler start (incl. worker startup) · `parent_stage_s` app-owned row staging · `parent_super_run_s` engine partition+enqueue · `parent_tx_s` dispatch handler TX · `callback_sum_s`/`callback_max_s` aggregate worker-time (overlaps across workers — not a wall-time phase).

## 500,000 rows (stage 3.4s, expected bad 40,283, invalid runs 0)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 8 | 2 | 24.3 | 24.3 | 6.8 | 20,604 | yes |
