# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-21T18:24:15.942360+00:00`

config: `{"chunk": 1000, "invocation_id": "252e1265-ab0b-4b27-8291-6dc9ec29217b", "kernel_prefix": "keelflow_bench_", "reps": 3, "row_disposition": "archive", "seed": 42, "sizes": [100000, 500000], "workers": [1, 4, 8]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue TX · `parent_queue_s` enqueue-commit → handler start (incl. worker startup) · `parent_stage_s` app-owned row staging · `parent_super_run_s` engine partition+enqueue · `parent_tx_s` dispatch handler TX · `callback_sum_s`/`callback_max_s` aggregate worker-time (overlaps across workers — not a wall-time phase).

## 100,000 rows (stage 0.6s, expected bad 8,019, invalid runs 0)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 1 | 3 | 8.6 | 8.5 | 1.1 | 11,683 | yes |
| keelflow-batch | 4 | 3 | 6.4 | 6.4 | 1.3 | 15,713 | yes |
| keelflow-batch | 8 | 3 | 6.0 | 6.1 | 1.4 | 16,764 | yes |
| keelflow-row | 1 | 3 | 87.2 | 88.4 | 12.4 | 1,147 | yes |
| keelflow-row | 4 | 3 | 35.6 | 35.5 | 19.1 | 2,810 | yes |
| keelflow-row | 8 | 3 | 28.6 | 28.6 | 29.5 | 3,499 | yes |
| sql | 0 | 3 | 0.6 | 0.6 | 0.0 | 155,186 | yes |

## 500,000 rows (stage 3.3s, expected bad 40,283, invalid runs 0)

| variant | workers | n | mean s | median s | callback sum s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 1 | 3 | 61.3 | 63.1 | 5.4 | 8,159 | yes |
| keelflow-batch | 4 | 3 | 45.1 | 45.0 | 6.3 | 11,098 | yes |
| keelflow-batch | 8 | 3 | 44.0 | 44.2 | 7.4 | 11,371 | yes |
| keelflow-row | 1 | 3 | 429.6 | 430.4 | 61.2 | 1,164 | yes |
| keelflow-row | 4 | 3 | 186.9 | 186.8 | 94.5 | 2,675 | yes |
| keelflow-row | 8 | 3 | 156.1 | 156.0 | 151.3 | 3,204 | yes |
| sql | 0 | 3 | 3.2 | 3.2 | 0.0 | 155,509 | yes |
