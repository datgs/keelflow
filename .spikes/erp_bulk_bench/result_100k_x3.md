# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` run `2026-11-21T08:38:52.931638+00:00`

## 100,000 rows (stage 0.6s)

| variant | workers | n | mean s | median s | rows/s mean | errors |
|---|---|---|---|---|---|---|
| keelflow | 1 | 3 | 28.7 | 28.6 | 3,482 | [8019] |
| keelflow | 4 | 3 | 29.0 | 29.2 | 3,444 | [8019] |
| keelflow | 8 | 3 | 28.7 | 28.6 | 3,482 | [8019] |
| sql | 0 | 3 | 0.7 | 0.7 | 146,348 | [8019] |
