# bulk_import — keelflow demo 3

CSV import pipeline showing **bulk row/chunk processing inside a DAG**:
upload → preview → HITL column mapping → bulk **validate** pass →
validation report → HITL **approve** → bulk **process** pass → finalize
+ downloadable error CSV.

## What it demonstrates

| Feature | Where |
|---|---|
| Bulk jobs as workflow steps | `imp.validate@v1`, `imp.process@v1` are `BulkJobHandler` types inside `imp.import@v1` — fan-in advances the run atomically |
| Handler-staged rows | `_ImportBulk.run` stages `demo_import_raw` → `keelflow_row_status` in `ctx.tx` before partitioning (parent + chunk enqueue atomic) |
| Row-level atomicity + error queue | `atomic='row'` — bad rows → `failed` with `error`, don't block siblings; `/imports/N/errors.csv` exports them |
| Chunk fan-out/fan-in | `chunk_size=50` → multiple `bulk.chunk@v1` jobs + `keelflow_chunks` bookkeeping |
| HITL in the middle of a run | `human.map` (column mapping feeds later steps via `${{ outputs.mapping }}`), `human.approve` (approve/reject gate) |
| Conditional continuation | `process` only runs `if:` approve decision is `approve`; reject → skipped → `finalize` → `aborted` |
| Idempotent domain writes | `demo_products` upsert on `sku` natural key |

## Layout

```
src/bulk_import/
  registry.py   job types, bulk handlers, imp.import@v1 DAG
  schema.sql    demo_imports, demo_import_raw, demo_products
  seed.py       schema setup only (imports created via UI)
  worker.py     worker process
  app.py        FastAPI UI (upload, map/approve actions, progress bars,
                error CSV, run DAG view)
tests/test_import.py  2 integration tests (needs Postgres)
```

## Run it (3 processes)

```bash
export KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow

uv run --package keelflow-demo-import python -m bulk_import.seed
uv run --package keelflow-demo-import python -m bulk_import.worker
uv run --package keelflow-demo-import \
    uvicorn bulk_import.app:app --port 8002
```

Open http://127.0.0.1:8002/?as=u-ops1 → **generate sample.csv**
(250 rows, ~8% intentionally bad) → start import.

## Actors (`?as=`)

| id | role | can |
|---|---|---|
| `u-ops1` | operations | upload, start, map, approve |
| `u-rev1` | reviewer | approve |
| `u-mgr` | manager | all |

## The story to tell

1. Upload → preview detects columns → **map** task blocks the run;
   mapping flows to both bulk passes via step outputs.
2. **validate** fans the rows into chunks → each row checked →
   per-step progress bars show done/failed; **approve** shows the
   validation report (N valid / M failed).
3. Approve → **process** re-queues rows; valid ones upsert into
   `demo_products`, failed ones land in the error queue →
   **download errors.csv**. Reject → `process` skipped, import `aborted`.

## Demo constraints

Synthetic/sample CSVs only · file size/row limits enforced · no auth ·
binds localhost · manual refresh.
