# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-21T16:20:12.806595+00:00`

config: `{"chunk": 1000, "invocation_id": "747b7bfa-bd81-4ff3-a9ac-9212ced55c8b", "kernel_prefix": "keelflow_bench_", "reps": 1, "row_disposition": "archive", "seed": 42, "sizes": [20000], "workers": [1, 4]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue · `parent_queue_s` enqueue → handler start · `parent_stage_s` row staging · `parent_partition_s` partition+chunk enqueue · `callback_s` summed process_row/process_batch duration · residual = engine bookkeeping + chunk queue + fan-in.

## 20,000 rows (stage 0.1s, expected bad 1,613, invalid runs 0)

| variant | workers | n | mean s | median s | callback mean s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 1 | 1 | 2.1 | 2.1 | 0.2 | 9,485 | yes |
| keelflow-batch | 4 | 1 | 1.5 | 1.5 | 0.3 | 13,264 | yes |
| keelflow-row | 1 | 1 | 17.1 | 17.1 | 2.5 | 1,168 | yes |
| keelflow-row | 4 | 1 | 7.2 | 7.2 | 3.7 | 2,771 | yes |
| sql | 0 | 1 | 0.1 | 0.1 | 0.0 | 152,718 | yes |
