# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` pg `16.16` run `2026-11-21T17:20:20.094896+00:00`

config: `{"chunk": 1000, "invocation_id": "04d004e0-35ab-41fe-bccf-785976808843", "kernel_prefix": "keelflow_bench_", "reps": 3, "row_disposition": "archive", "seed": 42, "sizes": [1000000], "workers": [4, 8]}` — all_correct=`True`

Phase fields (approximate, in-process hooks): `enqueue_s` parent enqueue · `parent_queue_s` enqueue → handler start · `parent_stage_s` row staging · `parent_partition_s` partition+chunk enqueue · `callback_s` summed process_row/process_batch duration · residual = engine bookkeeping + chunk queue + fan-in.

## 1,000,000 rows (stage 6.6s, expected bad 80,458, invalid runs 0)

| variant | workers | n | mean s | median s | callback mean s | rows/s mean | correct |
|---|---|---|---|---|---|---|---|
| keelflow-batch | 4 | 3 | 73.0 | 73.7 | 12.8 | 13,704 | yes |
| keelflow-batch | 8 | 3 | 69.2 | 69.5 | 15.2 | 14,456 | yes |
| keelflow-row | 4 | 3 | 359.3 | 357.5 | 189.0 | 2,783 | yes |
| keelflow-row | 8 | 3 | 294.6 | 295.2 | 303.7 | 3,395 | yes |
| sql | 0 | 3 | 6.7 | 6.7 | 0.0 | 148,191 | yes |
