"""keelflow demo 3 — bulk CSV import.

Upload → preview → HITL column mapping → bulk VALIDATE pass (atomic row,
errors → error queue) → validation report → HITL approve (proceed/abort)
→ bulk PROCESS pass → finalize. Both bulk passes are workflow steps —
the engine's fan-in advances the run atomically.

Run it:
    KEELFLOW_DSN=... uv run --package keelflow-demo-import \
        python -m bulk_import.seed
    uv run --package keelflow-demo-import python -m bulk_import.worker
    uv run --package keelflow-demo-import \
        uvicorn bulk_import.app:app --port 8002
"""
from __future__ import annotations

import csv
import io
import json
import random

from psycopg.rows import dict_row
from pydantic import BaseModel

from keelflow.engine import bulk
from keelflow.engine.bulk import BulkJobHandler, BulkJobPayload
from keelflow.engine.registry import InMemoryRegistry
from keelflow.sdk import (
    JobHandler,
    JobResult,
    JobTypeSpec,
    StepSpec,
    TaskUiSpec,
    WorkflowDefinition,
)

DSN_ENV = "KEELFLOW_DSN"

# ── actors / permissions ────────────────────────────────────────────────────
# ?as=ops1 (upload + map) · ?as=rev1 (approver) · ?as=mgr (all)
PERMISSIONS = {
    "u-ops1": {"imp.create", "imp.map", "imp.approve"},
    "u-rev1": {"imp.approve"},
    "u-mgr": {"*"},
}
ROLES = {"operations", "reviewer", "manager"}

# target product fields for mapping
FIELDS = ("sku", "name", "price", "stock")


# ── payloads ────────────────────────────────────────────────────────────────
class ImpId(BaseModel):
    import_id: int


class ImpBulk(BulkJobPayload):
    import_id: int
    mapping: dict[str, str]


class FinalPayload(BaseModel):
    import_id: int
    decision: str                       # approve | reject | abort


# ── helpers ─────────────────────────────────────────────────────────────────
def _dict_cur(tx):
    return (tx.connection.cursor(row_factory=dict_row)
            if hasattr(tx, "connection") else tx)


def sample_csv(n_rows: int = 250, bad_frac: float = 0.08) -> str:
    """Deterministic synthetic CSV — 4 source columns, ~8% bad rows."""
    rnd = random.Random(42)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["item_code", "title", "unit_price", "qty"])
    for i in range(n_rows):
        row = [f"SKU-{1000 + i}", f"Widget {i}",
               f"{rnd.uniform(1, 500):.2f}", rnd.randint(0, 999)]
        if rnd.random() < bad_frac:
            which = rnd.randint(0, 3)
            row[which] = "" if which < 3 else "-5"   # blank or bad qty
        w.writerow(row)
    return out.getvalue()


def _num(v) -> float | None:
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


# ── handlers ────────────────────────────────────────────────────────────────
class PreviewH(JobHandler):
    """Detect headers + row count from staged raw rows → feeds human.map."""

    async def run(self, ctx, payload: ImpId) -> JobResult:
        cur = _dict_cur(ctx.tx)
        await cur.execute(
            "SELECT data FROM demo_import_raw WHERE import_id=%s"
            " ORDER BY row_no LIMIT 1", (payload.import_id,))
        first = await cur.fetchone()
        await cur.execute(
            "SELECT count(*) AS n FROM demo_import_raw WHERE import_id=%s",
            (payload.import_id,))
        n = (await cur.fetchone())["n"]
        cols = list((first["data"] if first else {}).keys())
        return JobResult(status="done", outputs={
            "columns": cols, "rows_total": n,
            "fields": list(FIELDS)})


class _ImportBulk(BulkJobHandler):
    """Stages this pass's rows from demo_import_raw into
    keelflow_row_status inside the handler TX (before partitioning).
    Row data carries cells + mapping + import_id so chunk workers —
    whose ctx has no parent payload — can process them self-contained."""

    async def run(self, ctx, payload: ImpBulk) -> JobResult:
        cur = _dict_cur(ctx.tx)
        job_id = ctx.extra["job_id"]
        await cur.execute(
            "SELECT row_no, data FROM demo_import_raw WHERE import_id=%s",
            (payload.import_id,))
        rows = await cur.fetchall()
        for r in rows:
            data = {"cells": r["data"], "mapping": payload.mapping,
                    "import_id": payload.import_id}
            await cur.execute(
                "INSERT INTO keelflow_row_status (job_id, row_key, status, data)"
                " VALUES (%s, %s, 'pending', %s)",
                (job_id, f"row-{r['row_no']}", json.dumps(data)))
        return await super().run(ctx, payload)


class ValidateRowsH(_ImportBulk):
    """Bulk pass 1 — validate rows. atomic='row': bad rows go to the error
    queue without blocking good ones."""

    async def process_row(self, ctx, row, tx):
        d, mapping = row["data"]["cells"], row["data"]["mapping"]
        sku = str(d.get(mapping["sku"], "")).strip()
        name = str(d.get(mapping["name"], "")).strip()
        price = _num(d.get(mapping["price"]))
        qty = _num(d.get(mapping["stock"]))
        if not sku:
            raise ValueError("missing sku")
        if not name:
            raise ValueError("missing name")
        if price is None or price <= 0:
            raise ValueError(f"bad price: {d.get(mapping['price'])!r}")
        if qty is None or qty < 0:
            raise ValueError(f"bad qty: {d.get(mapping['stock'])!r}")
        return {"sku": sku}


class ProcessRowsH(_ImportBulk):
    """Bulk pass 2 — upsert valid rows into demo_products (sku natural
    key → idempotent). Invalid rows fail again here: the process pass's
    error queue IS the downloadable error CSV."""

    async def process_row(self, ctx, row, tx):
        payload = row["data"]
        d, mapping = payload["cells"], payload["mapping"]
        sku = str(d.get(mapping["sku"], "")).strip()
        name = str(d.get(mapping["name"], "")).strip()
        price = _num(d.get(mapping["price"]))
        qty = _num(d.get(mapping["stock"]))
        if not sku or not name:
            raise ValueError("missing sku/name")
        if price is None or price <= 0:
            raise ValueError(f"bad price: {d.get(mapping['price'])!r}")
        if qty is None or qty < 0:
            raise ValueError(f"bad qty: {d.get(mapping['stock'])!r}")
        await tx.execute(
            "INSERT INTO demo_products (sku, name, price, stock, imported_by)"
            " VALUES (%s,%s,%s,%s,%s)"
            " ON CONFLICT (sku) DO UPDATE SET"
            " name=EXCLUDED.name, price=EXCLUDED.price,"
            " stock=EXCLUDED.stock, updated_at=now()",
            (sku, name, price, int(qty), payload["import_id"]))
        return {"sku": sku}


class FinalizeH(JobHandler):
    """Set final import status + record totals on demo_imports."""

    async def run(self, ctx, payload: FinalPayload) -> JobResult:
        status = "imported" if payload.decision == "approve" else "aborted"
        await ctx.tx.execute(
            "UPDATE demo_imports SET status=%s WHERE id=%s",
            (status, payload.import_id))
        return JobResult(status="done", outputs={"status": status})


class _NoopH(JobHandler):
    async def run(self, ctx, payload) -> JobResult:  # pragma: no cover
        return JobResult(status="done")


# ── registry ────────────────────────────────────────────────────────────────
def build_registry() -> InMemoryRegistry:
    r = InMemoryRegistry()
    bulk.register(r)                       # internal bulk.chunk@v1

    r.register_job_type(JobTypeSpec(
        type="imp.preview@v1", input_schema=ImpId, handler=PreviewH))
    r.register_job_type(JobTypeSpec(
        type="imp.validate@v1", input_schema=ImpBulk,
        handler=ValidateRowsH))
    r.register_job_type(JobTypeSpec(
        type="imp.process@v1", input_schema=ImpBulk,
        handler=ProcessRowsH))
    r.register_job_type(JobTypeSpec(
        type="imp.finalize@v1", input_schema=FinalPayload,
        handler=FinalizeH))
    r.register_job_type(JobTypeSpec(
        type="human.map@v1", input_schema=ImpId, handler=_NoopH,
        is_human=True, ui=TaskUiSpec(assignee_role="operations")))
    r.register_job_type(JobTypeSpec(
        type="human.approve@v1", input_schema=ImpId, handler=_NoopH,
        is_human=True, ui=TaskUiSpec(assignee_role="reviewer")))

    i = {"import_id": "${{ inputs.import_id }}"}
    b = {"import_id": "${{ inputs.import_id }}",
         "mapping": "${{ steps.map.outputs.mapping }}",
         "atomic": "row", "chunk_size": 50}
    r.register_workflow(WorkflowDefinition(
        name="imp.import@v1",
        steps=(
            StepSpec("preview", "imp.preview@v1", input=i),
            StepSpec("map", "human.map@v1", input=i,
                     needs=("preview",)),
            StepSpec("validate", "imp.validate@v1", input=b,
                     needs=("map",)),
            StepSpec("approve", "human.approve@v1", input=i,
                     needs=("validate",)),
            StepSpec("process", "imp.process@v1", input=b,
                     needs=("approve",),
                     if_expr="${{ steps.approve.outputs.decision }} == 'approve'"),
            StepSpec("finalize", "imp.finalize@v1",
                     input={"import_id": "${{ inputs.import_id }}",
                            "decision": "${{ steps.approve.outputs.decision }}"},
                     needs=("process",)),
        ),
    ))
    return r
