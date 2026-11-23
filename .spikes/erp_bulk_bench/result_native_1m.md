# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-23T08:20:35.654588+00:00`

config: `{"chunk": 1000, "invocation_id": "100c2086-b8f9-4e3c-a05a-360b8926a069", "kernel_backend": "native", "kernel_prefix": null, "reps": 3, "row_disposition": "archive", "seed": 42, "sizes": [1000000], "variants": ["keelflow-batch"], "workers": [8]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue TX · `parent_queue_s` enqueue-commit → handler start (incl. worker startup) · `parent_stage_s` app-owned row staging · `parent_super_run_s` engine partition+enqueue · `parent_tx_s` dispatch handler TX · `callback_sum_s`/`callback_max_s` aggregate worker-time (overlaps across workers — not a wall-time phase).

## 1,000,000 rows (stage 6.5s, expected bad 80,458, invalid runs 0)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 8 | 3 | 59.3 | 61.3 | 15.9 | 16,872 | yes |
