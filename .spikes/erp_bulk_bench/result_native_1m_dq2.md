# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-23T10:50:20.909502+00:00`

config: `{"chunk": 1000, "invocation_id": "41db033c-e75f-42fc-a4fd-7c8806bc5f99", "kernel_backend": "native", "kernel_prefix": null, "reps": 5, "row_disposition": "archive", "seed": 42, "sizes": [1000000], "variants": ["keelflow-batch"], "workers": [8]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue TX · `parent_queue_s` enqueue-commit → handler start (incl. worker startup) · `parent_stage_s` app-owned row staging · `parent_super_run_s` engine partition+enqueue · `parent_tx_s` dispatch handler TX · `callback_sum_s`/`callback_max_s` aggregate worker-time (overlaps across workers — not a wall-time phase).

## 1,000,000 rows (stage 6.6s, expected bad 80,458, invalid runs 0)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 8 | 5 | 55.1 | 54.7 | 15.6 | 18,153 | yes |
