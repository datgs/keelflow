# Bulk throughput benchmark (must-prove #3)

Environment: M-series Mac, Postgres 16 (docker), one worker,
`concurrency=8`, `eager_polling=True`, handler = noop (`process_row`
returns `{}`), `atomic='chunk'`, `chunk_size=1000`.

| Stage | Rows | Wall | Rate |
|---|---|---|---|
| stage + enqueue | 100,000 | 0.6 s | ~163k rows/s |
| process (5 chunks) | 100,000 | 10.0 s | ~10k rows/s |

Extrapolated to the 500k must-prove target: **~50 s process time** for a
noop handler — well inside any agreed window. Real handlers (SQL writes
per row) will dominate; the engine overhead is bounded by the chunk
transaction itself.

Status transitions observed live: parent `pending → running → done`,
chunks `pending → running → done`, rows `pending → processing → done`.

Reproduce: `PYERP_DSN=… uv run python .spikes/bulk_bench.py 100000`.
