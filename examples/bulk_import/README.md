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
| Row retention | `row_disposition='archive'` (default) — done staging rows move to `keelflow_row_archive` at fan-in, so the live table only keeps the error queue; parent `result` keeps `rows_done`/`archived_rows` totals |
| Chunk fan-out/fan-in | `chunk_size=50` → multiple `bulk.chunk@v1` jobs + `keelflow_chunks` bookkeeping |
| HITL in the middle of a run | `human.map` (column mapping feeds later steps via `${{ outputs.mapping }}`), `human.approve` (approve/reject gate) |
| Conditional continuation | `process` only runs `if:` approve decision is `approve`; reject → skipped → `finalize` → `aborted` |
| Idempotent domain writes | `demo_products` upsert on `sku` natural key |

## The workflow — `imp.import@v1`

```mermaid
flowchart TD
    preview --> map[[map · human]]
    map --> validate[[validate · bulk]]
    validate --> approve[[approve · human]]
    approve -->|approve| process[[process · bulk]]
    process --> finalize
    approve -.->|reject · process skipped| finalize
```

`validate` and `process` are bulk parents — each fans out into
`bulk.chunk@v1` jobs and fans back in. Two human gates (`map`, `approve`)
block the run until someone acts; the column mapping chosen in `map`
feeds both bulk passes via step outputs. Rejecting at `approve` skips
`process` and finalizes the import as `aborted`.

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

| id | role | can | workflow step |
|---|---|---|---|
| `u-ops1` | operations | upload, start, map | `map` |
| `u-rev1` | reviewer | approve only | `approve` |
| `u-mgr` | manager | all | any human step |

Separation of duties is the point: `map` is assigned to `role:operations`
while `approve` is assigned to `role:reviewer` — `u-ops1` cannot approve
its own import.

UI language: `?lang=` picks `en`/`vi`/`es`/`zh-Hans`/`hi` and persists
via the `kf_lang` cookie (selector in the header; `?as=` is kept).
Identifiers, actor ids, and fixture values stay untranslated.

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

## SDK extension points it uses

This demo's signature: it plugs **`BulkJobHandler`** into a DAG step, so
a 250k-row import is just one node in a workflow.

| Extension point | Snippet |
|---|---|
| **`BulkJobHandler`** — subclass for chunked row processing; engine fans out to `bulk.chunk` workers + fans back in | `class ValidateRowsH(_ImportBulk)` `class ProcessRowsH(_ImportBulk)` |
| **`process_row(ctx, row, tx)`** — per-row business logic; engine owns chunking, retries, error queue | `async def process_row(self, ctx, row, tx): … if not sku: raise ValueError("missing sku")` |
| **Stage rows before `super().run()`** — hook in `_ImportBulk.run` writes `keelflow_row_status` inside `ctx.tx`, then engine partitions | `await cur.executemany("INSERT INTO keelflow_row_status …", batch); return await super().run(ctx, payload)` |
| **Row `data` self-contained** — chunk workers' ctx has no parent payload, so each row carries cells+mapping+import_id | `{"cells":…,"mapping":…,"import_id":…}` |
| **`BulkJobPayload` via `${{ steps.*.outputs }}`** — feed a human step's output (mapping) into the bulk step's payload | `"mapping": "${{ steps.map.outputs.mapping }}"` |
| **`atomic` + `chunk_size`** — engine bulk knobs on the step input | `"atomic": "row", "chunk_size": 5000` |
| **bulk-as-DAG-step** — `imp.validate`/`imp.process` are normal `StepSpec`s; engine links the bulk parent to the step run | `StepSpec("process", "imp.process@v1", input=b, needs=("approve",), if_expr=…)` |
| **`if_expr` on the bulk step** — only run the process pass when approved | `if_expr="${{ steps.approve.outputs.decision }} == 'approve'"` |
| **`bulk.progress(conn, job_id)`** — engine-owned % / rate / ETA for the parent job | `await bulk.progress(conn, s["job_id"])` in `app.py` |
| **`bulk.register(r)`** — registers the internal `bulk.chunk@v1` worker | `bulk.register(r)` |

## SDK extension points it uses — code

```python
class _ImportBulk(BulkJobHandler):
    async def run(self, ctx, payload: ImpBulk) -> JobResult:
        # stage rows in the same TX the engine will partition from
        await cur.executemany(
            "INSERT INTO keelflow_row_status (job_id,row_key,status,data)"
            " VALUES (%s,%s,'pending',%s)", batch)
        return await super().run(ctx, payload)

class ProcessRowsH(_ImportBulk):
    async def process_row(self, ctx, row, tx):
        d, mapping = row["data"]["cells"], row["data"]["mapping"]
        sku = str(d.get(mapping["sku"], "")).strip()
        if not sku: raise ValueError("missing sku")   # → error queue
        await tx.execute("INSERT INTO demo_products … ON CONFLICT (sku) …")
        return {"sku": sku}                            # row marked done
```
