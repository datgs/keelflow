# ERP bulk benchmark

env: `Darwin/arm64` py `3.12.13` run `2026-11-21T15:42:52.421034+00:00`

## 1,000,000 rows (stage 6.5s, expected bad 80,458)

| variant | workers | n | mean s | median s | rows/s mean | correct |
|---|---|---|---|---|---|---|
| keelflow-batch | 1 | 3 | 151.9 | 149.6 | 6,584 | NO |
| keelflow-batch | 4 | 3 | 107.5 | 113.8 | 9,298 | NO |
| keelflow-batch | 8 | 3 | 102.3 | 109.6 | 9,779 | NO |
| keelflow-row | 1 | 3 | 918.6 | 853.4 | 1,089 | NO |
| keelflow-row | 4 | 3 | 483.8 | 505.0 | 2,067 | NO |
| keelflow-row | 8 | 3 | 430.2 | 458.8 | 2,325 | NO |
| sql | 0 | 3 | 8.6 | 9.0 | 116,609 | NO |
