# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` run `2026-11-21T08:24:40.447476+00:00`

## 100,000 rows (stage 0.6s)

| variant | workers | n | mean s | median s | rows/s mean | errors |
|---|---|---|---|---|---|---|
| keelflow | 1 | 1 | 28.3 | 28.3 | 3,534 | [8019] |
| keelflow | 4 | 1 | 29.5 | 29.5 | 3,389 | [8019] |
| keelflow | 8 | 1 | 28.3 | 28.3 | 3,532 | [8019] |
| sql | 0 | 1 | 0.6 | 0.6 | 156,776 | [8019] |
