# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` run `2026-11-21T12:21:40.663971+00:00`

## 100,000 rows (stage 0.6s, expected bad 8,019)

| variant | workers | n | mean s | median s | rows/s mean | correct |
|---|---|---|---|---|---|---|
| keelflow-batch | 1 | 3 | 14.1 | 14.0 | 7,116 | yes |
| keelflow-batch | 4 | 3 | 6.6 | 6.7 | 15,187 | yes |
| keelflow-batch | 8 | 3 | 6.5 | 6.4 | 15,401 | yes |
| keelflow-row | 1 | 3 | 87.1 | 87.8 | 1,148 | yes |
| keelflow-row | 4 | 3 | 35.6 | 35.8 | 2,807 | yes |
| keelflow-row | 8 | 3 | 29.0 | 28.9 | 3,447 | yes |
| sql | 0 | 3 | 0.6 | 0.7 | 155,897 | yes |
