# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-21T16:27:04.812363+00:00`

config: `{"chunk": 1000, "invocation_id": "f4bdc910-fe6f-49d4-90fa-b20da541c138", "kernel_prefix": "keelflow_bench_", "reps": 3, "row_disposition": "archive", "seed": 42, "sizes": [100000, 500000], "workers": [1, 4, 8]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue · `parent_queue_s` enqueue → handler start · `parent_stage_s` row staging · `parent_partition_s` partition+chunk enqueue · `callback_s` summed process_row/process_batch duration · residual = engine bookkeeping + chunk queue + fan-in.

## 100,000 rows (stage 0.6s, expected bad 8,019, invalid runs 0)

| variant | workers | n | mean s | median s | callback mean s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 1 | 3 | 12.6 | 14.3 | 1.0 | 7,918 | yes |
| keelflow-batch | 4 | 3 | 6.8 | 6.7 | 1.2 | 14,729 | yes |
| keelflow-batch | 8 | 3 | 6.5 | 6.4 | 1.5 | 15,425 | yes |
| keelflow-row | 1 | 3 | 84.4 | 84.2 | 12.5 | 1,185 | yes |
| keelflow-row | 4 | 3 | 36.2 | 35.8 | 19.0 | 2,761 | yes |
| keelflow-row | 8 | 3 | 29.3 | 29.5 | 29.6 | 3,410 | yes |
| sql | 0 | 3 | 0.7 | 0.7 | 0.0 | 149,954 | yes |

## 500,000 rows (stage 3.4s, expected bad 40,283, invalid runs 0)

| variant | workers | n | mean s | median s | callback mean s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 1 | 3 | 50.7 | 47.3 | 5.3 | 9,860 | yes |
| keelflow-batch | 4 | 3 | 36.5 | 36.0 | 6.7 | 13,709 | yes |
| keelflow-batch | 8 | 3 | 38.9 | 33.5 | 10.2 | 12,869 | yes |
| keelflow-row | 1 | 3 | 417.6 | 417.9 | 61.5 | 1,197 | yes |
| keelflow-row | 4 | 3 | 178.4 | 176.9 | 95.8 | 2,802 | yes |
| keelflow-row | 8 | 3 | 145.9 | 146.2 | 151.2 | 3,427 | yes |
| sql | 0 | 3 | 3.2 | 3.1 | 0.0 | 155,808 | yes |
