# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` run `2026-11-21T13:05:17.841228+00:00`

## 500,000 rows (stage 3.2s, expected bad 40,283)

| variant | workers | n | mean s | median s | rows/s mean | correct |
|---|---|---|---|---|---|---|
| keelflow-batch | 1 | 3 | 48.2 | 49.5 | 10,379 | yes |
| keelflow-batch | 4 | 3 | 36.4 | 35.7 | 13,750 | yes |
| keelflow-batch | 8 | 3 | 34.5 | 34.0 | 14,492 | yes |
| keelflow-row | 1 | 3 | 415.7 | 415.8 | 1,203 | yes |
| keelflow-row | 4 | 3 | 179.6 | 181.1 | 2,784 | yes |
| keelflow-row | 8 | 3 | 149.4 | 149.9 | 3,346 | yes |
| sql | 0 | 3 | 3.3 | 3.3 | 152,638 | yes |
