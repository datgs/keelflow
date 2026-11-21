# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-21T18:23:44.116216+00:00`

config: `{"chunk": 1000, "invocation_id": "6fa4c369-8cbd-4359-a610-191ef5649b52", "kernel_prefix": "keelflow_bench_", "reps": 1, "row_disposition": "archive", "seed": 42, "sizes": [20000], "workers": [1, 4]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue TX · `parent_queue_s` enqueue-commit → handler start (incl. worker startup) · `parent_stage_s` app-owned row staging · `parent_super_run_s` engine partition+enqueue · `parent_tx_s` dispatch handler TX · `callback_sum_s`/`callback_max_s` aggregate worker-time (overlaps across workers — not a wall-time phase).

## 20,000 rows (stage 0.1s, expected bad 1,613, invalid runs 0)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 1 | 1 | 1.8 | 1.8 | 0.2 | 11,061 | yes |
| keelflow-batch | 4 | 1 | 1.5 | 1.5 | 0.3 | 13,262 | yes |
| keelflow-row | 1 | 1 | 16.8 | 16.8 | 2.5 | 1,189 | yes |
| keelflow-row | 4 | 1 | 7.2 | 7.2 | 3.8 | 2,771 | yes |
| sql | 0 | 1 | 0.1 | 0.1 | 0.0 | 153,563 | yes |
