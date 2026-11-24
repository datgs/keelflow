"""Demo 3 UI — bulk CSV import. Upload → map → validate → approve →
process → downloadable error CSV.

    uv run --package keelflow-demo-import \
        uvicorn bulk_import.app:app --port 8002

Demo only: no auth, binds localhost, synthetic/sample data. Refresh
manually (no live updates).
"""
from __future__ import annotations

import csv
import io
import json
import os
from contextlib import asynccontextmanager

import chancy
import psycopg
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from psycopg.rows import dict_row

from keelflow.engine import bulk
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import Actor, JobContext

from . import i18n
from .registry import FIELDS, PERMISSIONS, build_registry, sample_csv

DSN = os.environ.get("KEELFLOW_DSN",
                     "postgresql://postgres:postgres@localhost:5544/keelflow")
TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")

MAX_BYTES = 64 * 1024 * 1024
MAX_ROWS = 500_000

_ACTORS = ("u-ops1", "u-rev1", "u-mgr")
_ACTOR_NAMES = {
    "u-ops1": "Oscar — Operations",
    "u-rev1": "Rita — Reviewer",
    "u-mgr":  "Maya — Manager (all)",
}
_ACTOR_ROLES = {
    "u-ops1": ["operations"],
    "u-rev1": ["reviewer"],
    "u-mgr": ["operations", "reviewer", "manager"],
}


def _ctx(request: Request) -> JobContext:
    uid = request.query_params.get("as") or "u-ops1"
    if uid not in _ACTORS:
        uid = "u-ops1"
    return JobContext(actor=Actor.user(uid), tenant_id="demo",
                      permissions=PERMISSIONS.get(uid, set()))


@asynccontextmanager
async def _lifespan(app: FastAPI):
    registry = build_registry()
    kernel = chancy.Chancy(DSN, prefix="keelflow_kernel_imp_")
    await kernel.migrate()
    await kernel.declare(
        chancy.Queue("default", concurrency=8,
                     executor=chancy.Chancy.Executor.Async,
                     eager_polling=True),
        upsert=True,
    )
    jobs = JobService(registry, kernel, PostgresStorage(DSN))
    app.state.jobs = jobs
    app.state.registry = registry
    app.state.wf = WorkflowService(registry, jobs)
    app.state.human = HumanTaskService(registry, kernel)
    async with kernel:
        yield


app = FastAPI(title="keelflow demo · bulk import", lifespan=_lifespan)
templates = Jinja2Templates(directory=TEMPLATES)
templates.env.globals.update(max=max)


async def _conn():
    return await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row)


def _nav(request: Request) -> dict:
    uid = request.query_params.get("as") or "u-ops1"
    return {"as": uid, "actors": sorted(_ACTORS),
            "names": _ACTOR_NAMES,
            "perms": sorted(PERMISSIONS.get(uid, set())),
            "perms_of": {a: sorted(PERMISSIONS.get(a, set()))
                         for a in _ACTORS}}


def _layers(steps: list[dict]) -> list[list[dict]]:
    placed: set[str] = set()
    layers: list[list[dict]] = []
    remaining = list(steps)
    while remaining:
        layer = [s for s in remaining
                 if set(s["needs"]).issubset(placed)]
        if not layer:
            layers.append(remaining)
            break
        placed.update(s["step_id"] for s in layer)
        layers.append(layer)
        remaining = [s for s in remaining if s["step_id"] not in placed]
    return layers


async def _stage_csv(conn, filename: str, text: str) -> int:
    """Parse CSV → demo_imports + demo_import_raw, one TX. Returns id."""
    reader = csv.DictReader(io.StringIO(text))
    rows = [r for _, r in zip(range(MAX_ROWS + 1), reader, strict=False)]
    if not rows or not reader.fieldnames:
        raise ValueError("empty CSV or no header row")
    if len(rows) > MAX_ROWS:
        raise ValueError(f"too many rows (max {MAX_ROWS})")
    async with conn.transaction(), conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO demo_imports (filename, status, rows_total)"
            " VALUES (%s,'uploaded',%s) RETURNING id",
            (filename, len(rows)))
        row = await cur.fetchone()
        iid = row[0] if not isinstance(row, dict) else row["id"]
        for i, r in enumerate(rows):
            await cur.execute(
                "INSERT INTO demo_import_raw (import_id, row_no, data)"
                " VALUES (%s,%s,%s)",
                (iid, i + 1, json.dumps(r)))
    return iid


# ── imports list / upload ───────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def imports(request: Request):
    async with await _conn() as conn:
        rows = await (await conn.execute(
            "SELECT * FROM demo_imports ORDER BY id DESC")).fetchall()
        runs = await (await conn.execute(
            "SELECT DISTINCT ON ((inputs->>'import_id')::int)"
            "   id, inputs->>'import_id' AS imp, status, terminal_outcome"
            " FROM keelflow_workflow_runs WHERE workflow='imp.import@v1'"
            " ORDER BY (inputs->>'import_id')::int, created_at DESC"
        )).fetchall()
        n_products = await (await conn.execute(
            "SELECT count(*) AS n FROM demo_products")).fetchone()
        tasks = await (await conn.execute(
            "SELECT count(*) AS n FROM keelflow_jobs"
            " WHERE type LIKE 'human.%'"
            " AND status IN ('pending','claimed')")).fetchone()
    return i18n.render(templates, request, "imports.html", {
        "nav": _nav(request), "imports": rows,
        "runs": {int(r["imp"]): r for r in runs},
        "n_products": n_products["n"], "open_tasks": tasks["n"]})


@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile | None = File(None),  # noqa: B008
    generate: str | None = Form(None),
):
    ctx = _ctx(request)
    ctx.require("imp.create")
    if generate:
        text = sample_csv()
        filename = "sample.csv"
    else:
        if file is None or not file.filename:
            return RedirectResponse(f"/?as={ctx.actor.id}", status_code=303)
        if not file.filename.lower().endswith(".csv"):
            return RedirectResponse(f"/?as={ctx.actor.id}", status_code=303)
        raw = await file.read()
        if len(raw) > MAX_BYTES:
            return RedirectResponse(f"/?as={ctx.actor.id}", status_code=303)
        text = raw.decode("utf-8", errors="replace")
        filename = file.filename
    async with await _conn() as conn:
        try:
            iid = await _stage_csv(conn, filename, text)
        except ValueError:
            return RedirectResponse(f"/?as={ctx.actor.id}", status_code=303)
        await conn.commit()
    return RedirectResponse(f"/imports/{iid}?as={ctx.actor.id}",
                            status_code=303)


@app.post("/imports/{iid}/start")
async def start(request: Request, iid: int):
    ctx = _ctx(request)
    ctx.require("imp.create")
    async with await _conn() as conn, conn.transaction(), \
            conn.cursor() as cur:
        await cur.execute(
            "UPDATE demo_imports SET status='importing'"
            " WHERE id=%s AND status='uploaded'", (iid,))
        await app.state.wf.start(
            "imp.import@v1", {"import_id": iid}, ctx, cur)
    return RedirectResponse(f"/imports/{iid}?as={ctx.actor.id}",
                            status_code=303)


# ── import detail: DAG + step views ─────────────────────────────────────────

async def _step_stats(conn, steps: list[dict]) -> dict[str, dict]:
    """Live progress for bulk steps (engine bulk.progress → %/rate/ETA)."""
    out = {}
    for s in steps:
        if s["job_id"] and s["type"].startswith("imp."):
            out[s["step_id"]] = await bulk.progress(conn, s["job_id"])
    return out


@app.get("/imports/{iid}", response_class=HTMLResponse)
async def detail(request: Request, iid: int):
    async with await _conn() as conn:
        imp = await (await conn.execute(
            "SELECT * FROM demo_imports WHERE id=%s", (iid,))).fetchone()
        if imp is None:
            return RedirectResponse("/?as=u-ops1", status_code=303)
        run = await (await conn.execute(
            "SELECT id FROM keelflow_workflow_runs"
            " WHERE inputs->>'import_id'=%s"
            " ORDER BY created_at DESC LIMIT 1", (str(iid),))).fetchone()
        detail_ = (await app.state.wf.get_run(conn, str(run["id"]))
                   if run else None)
        stats = (await _step_stats(conn, detail_["steps"])
                 if detail_ else {})
        sample = await (await conn.execute(
            "SELECT data FROM demo_import_raw WHERE import_id=%s"
            " ORDER BY row_no LIMIT 3", (iid,))).fetchall()
        # for the map step UI: source columns from preview step
        cols = []
        if detail_:
            prev = next((s for s in detail_["steps"]
                         if s["step_id"] == "preview"), None)
            if prev and prev["outputs"]:
                cols = prev["outputs"].get("columns", [])
        n_products = await (await conn.execute(
            "SELECT count(*) AS n FROM demo_products"
            " WHERE imported_by=%s", (iid,))).fetchone()
    layers = _layers(detail_["steps"]) if detail_ else []
    wf_diagram = None
    blocked = []
    if detail_:
        blocked = [st for st in detail_["steps"]
                   if st["status"] in ("blocked", "claimed")
                   and st["type"].startswith("human.")]
        meta = {}
        for st in detail_["steps"]:
            if st["type"].startswith("human."):
                spec = app.state.registry.job_type(st["type"])
                meta[st["type"]] = {
                    "assignee": getattr(spec.human_task, "assignee_role", None)
                    if spec.human_task else None}
        from keelflow.engine.diagram import workflow_svg
        wf_diagram = workflow_svg(
            app.state.registry.workflow(detail_["workflow"]),
            step_status={st["step_id"]: st["status"]
                         for st in detail_["steps"]},
            job_type_meta=meta)
    return i18n.render(templates, request, "import.html", {
        "nav": _nav(request), "imp": imp, "run": detail_,
        "layers": layers, "stats": stats, "sample": sample,
        "columns": cols, "fields": FIELDS,
        "n_products": n_products["n"], "wf_diagram": wf_diagram,
        "blocked": blocked})


# ── human tasks (map + approve — rendered inside the import page) ──────────

@app.post("/imports/{iid}/map")
async def do_map(request: Request, iid: int,
                 job_id: str = Form(...),
                 map_sku: str = Form(""), map_name: str = Form(""),
                 map_price: str = Form(""), map_stock: str = Form("")):
    ctx = _ctx(request)
    actor = Actor.user(ctx.actor.id)
    mapping = {"sku": map_sku, "name": map_name,
               "price": map_price, "stock": map_stock}
    async with await _conn() as conn:
        try:
            await app.state.human.claim(
                conn, job_id, actor, roles=_ACTOR_ROLES.get(ctx.actor.id, []))
            await app.state.human.complete(
                conn, job_id, actor,
                {"decision": "approve", "mapping": mapping},
                roles=_ACTOR_ROLES.get(ctx.actor.id, []))
            await conn.execute(
                "UPDATE demo_imports SET mapping=%s WHERE id=%s",
                (json.dumps(mapping), iid))
            await conn.commit()
        except PermissionError as e:
            return RedirectResponse(
                f"/imports/{iid}?as={ctx.actor.id}&err={e}", status_code=303)
    return RedirectResponse(f"/imports/{iid}?as={ctx.actor.id}",
                            status_code=303)


@app.post("/imports/{iid}/approve")
async def do_approve(request: Request, iid: int,
                     job_id: str = Form(...),
                     decision: str = Form(...)):
    ctx = _ctx(request)
    actor = Actor.user(ctx.actor.id)
    async with await _conn() as conn:
        try:
            await app.state.human.claim(
                conn, job_id, actor, roles=_ACTOR_ROLES.get(ctx.actor.id, []))
            await app.state.human.complete(
                conn, job_id, actor, {"decision": decision},
                roles=_ACTOR_ROLES.get(ctx.actor.id, []))
            await conn.commit()
        except PermissionError as e:
            return RedirectResponse(
                f"/imports/{iid}?as={ctx.actor.id}&err={e}", status_code=303)
    return RedirectResponse(f"/imports/{iid}?as={ctx.actor.id}",
                            status_code=303)


# ── error CSV download ──────────────────────────────────────────────────────

@app.get("/imports/{iid}/errors.csv", response_class=PlainTextResponse)
async def errors(iid: int):
    async with await _conn() as conn:
        run = await (await conn.execute(
            "SELECT job_id FROM keelflow_step_runs sr"
            " JOIN keelflow_workflow_runs r ON r.id=sr.run_id"
            " WHERE r.inputs->>'import_id'=%s AND sr.step_id='process'"
            " ORDER BY r.created_at DESC LIMIT 1", (str(iid),))).fetchone()
        if not run or not run["job_id"]:
            return ""
        rows = await (await conn.execute(
            "SELECT row_key, data, error FROM keelflow_row_status"
            " WHERE job_id=%s AND status='failed' ORDER BY row_key",
            (run["job_id"],))).fetchall()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["row_key", "error", "cells"])
    for r in rows:
        cells = r["data"]["cells"] if r["data"] else {}
        w.writerow([r["row_key"], r["error"], json.dumps(cells)])
    return PlainTextResponse(out.getvalue(), media_type="text/csv")


# ── generic inbox + run/job views (same as other demos) ─────────────────────

@app.get("/inbox", response_class=HTMLResponse)
async def inbox(request: Request):
    uid = request.query_params.get("as") or "u-ops1"
    async with await _conn() as conn:
        tasks = await app.state.human.my_tasks(
            conn, Actor.user(uid), roles=_ACTOR_ROLES.get(uid, []))
    return i18n.render(templates, request, "inbox.html", {
        "nav": _nav(request), "tasks": tasks, "uid": uid})


@app.get("/runs/{run_id}", response_class=HTMLResponse)
async def run(request: Request, run_id: str):
    async with await _conn() as conn:
        detail = await app.state.wf.get_run(conn, run_id)
    return i18n.render(templates, request, "run.html", {
        "nav": _nav(request), "run": detail,
        "steps": detail["steps"], "layers": _layers(detail["steps"])})


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job(request: Request, job_id: str):
    async with await _conn() as conn:
        r = await (await conn.execute(
            "SELECT * FROM keelflow_jobs WHERE id=%s", (job_id,))).fetchone()
        # bulk.progress stays correct after fan-in cleanup moves 'done'
        # rows to keelflow_row_archive (default row_disposition).
        try:
            stats = await bulk.progress(conn, job_id)
        except KeyError:
            stats = {}
        archived = await (await conn.execute(
            "SELECT count(*) AS n FROM keelflow_row_archive WHERE job_id=%s",
            (job_id,))).fetchone()
        chunks = await (await conn.execute(
            "SELECT idx, status, result FROM keelflow_chunks"
            " WHERE job_id=%s ORDER BY idx", (job_id,))).fetchall()
    if archived["n"]:
        stats = dict(stats) | {"archive": archived["n"]}
    return i18n.render(templates, request, "job.html", {
        "nav": _nav(request), "job": r,
        "stats": stats,
        "chunks": chunks,
        "can_cancel": r and r["status"] in ("pending", "claimed", "running"),
    })


@app.post("/jobs/{job_id}/cancel")
async def job_cancel(request: Request, job_id: str):
    """Cooperative cancel — records a durable request; pending jobs
    cancel outright, running chunks stop at the next row checkpoint."""
    ctx = _ctx(request)
    try:
        await request.app.state.jobs.cancel(job_id, ctx)
    except PermissionError as e:
        return RedirectResponse(
            f"/jobs/{job_id}?as={ctx.actor.id}&err={e}", status_code=303)
    return RedirectResponse(f"/jobs/{job_id}?as={ctx.actor.id}",
                            status_code=303)
