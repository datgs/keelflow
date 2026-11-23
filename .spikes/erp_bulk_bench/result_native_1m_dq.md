# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-23T10:44:31.079475+00:00`

config: `{"chunk": 1000, "invocation_id": "974bead2-d2cb-4819-8089-0f8e261198d5", "kernel_backend": "native", "kernel_prefix": null, "reps": 3, "row_disposition": "archive", "seed": 42, "sizes": [1000000], "variants": ["keelflow-batch"], "workers": [8]}` — all_correct=`False`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue TX · `parent_queue_s` enqueue-commit → handler start (incl. worker startup) · `parent_stage_s` app-owned row staging · `parent_super_run_s` engine partition+enqueue · `parent_tx_s` dispatch handler TX · `callback_sum_s`/`callback_max_s` aggregate worker-time (overlaps across workers — not a wall-time phase).

## 1,000,000 rows (stage 7.0s, expected bad 80,458, invalid runs 1)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 8 | 2 | 58.5 | 58.5 | 16.2 | 17,080 | NO |
