# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` run `2026-11-21T08:34:22.036797+00:00`

## 500,000 rows (stage 3.1s)

| variant | workers | n | mean s | median s | rows/s mean | errors |
|---|---|---|---|---|---|---|
| keelflow | 1 | 1 | 157.9 | 157.9 | 3,166 | [40283] |
| keelflow | 4 | 1 | 269.4 | 269.4 | 1,856 | [40283] |
| keelflow | 8 | 1 | 142.9 | 142.9 | 3,498 | [40283] |
| sql | 0 | 1 | 3.2 | 3.2 | 157,383 | [40283] |
