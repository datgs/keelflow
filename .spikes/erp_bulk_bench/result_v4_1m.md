# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-21T20:20:35.669004+00:00`

config: `{"chunk": 1000, "invocation_id": "a86a612f-4086-47fe-b8c0-e80cfc6acad4", "kernel_prefix": "keelflow_bench_", "reps": 3, "row_disposition": "archive", "seed": 42, "sizes": [1000000], "workers": [4, 8]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue TX · `parent_queue_s` enqueue-commit → handler start (incl. worker startup) · `parent_stage_s` app-owned row staging · `parent_super_run_s` engine partition+enqueue · `parent_tx_s` dispatch handler TX · `callback_sum_s`/`callback_max_s` aggregate worker-time (overlaps across workers — not a wall-time phase).

## 1,000,000 rows (stage 6.5s, expected bad 80,458, invalid runs 0)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 4 | 3 | 61.9 | 61.9 | 13.2 | 16,164 | yes |
| keelflow-batch | 8 | 3 | 60.5 | 60.3 | 16.2 | 16,521 | yes |
| keelflow-row | 4 | 3 | 365.5 | 350.6 | 192.5 | 2,736 | yes |
| keelflow-row | 8 | 3 | 292.7 | 291.0 | 312.8 | 3,416 | yes |
| sql | 0 | 3 | 6.9 | 6.8 | 0.0 | 145,100 | yes |
