"""Demo 2 UI — FastAPI app. Supplier onboarding DAG with parallel checks
and conditional human review.

    uv run --package keelflow-demo-supplier \
        uvicorn supplier_onboarding.app:app --port 8001

Demo only: no auth, binds localhost. Refresh manually (no live updates).
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

import chancy
import psycopg
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from psycopg.rows import dict_row

from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.workflow import WorkflowService
from keelflow.sdk import Actor, JobContext

from .registry import PERMISSIONS, ROLES, build_registry

DSN = os.environ.get("KEELFLOW_DSN",
                     "postgresql://postgres:postgres@localhost:5544/keelflow")
TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")

# actor ids seen in this demo
_ACTORS = ("u-proc1", "u-comp1", "u-fin1", "u-procadm", "u-mgr")


def _ctx(request: Request) -> JobContext:
    uid = request.query_params.get("as") or "u-proc1"
    if uid not in _ACTORS:
        uid = "u-proc1"
    return JobContext(actor=Actor.user(uid),
                      tenant_id="demo",
                      permissions=PERMISSIONS.get(uid, set()))


@asynccontextmanager
async def _lifespan(app: FastAPI):
    registry = build_registry()
    kernel = chancy.Chancy(DSN, prefix="keelflow_kernel_sup_")
    await kernel.migrate()
    await kernel.declare(
        chancy.Queue("default", concurrency=8,
                     executor=chancy.Chancy.Executor.Async,
                     eager_polling=True),
        upsert=True,
    )
    jobs = JobService(registry, kernel)
    app.state.wf = WorkflowService(registry, jobs)
    app.state.human = HumanTaskService(registry, kernel)
    async with kernel:
        yield

app = FastAPI(title="keelflow demo · supplier onboarding", lifespan=_lifespan)
templates = Jinja2Templates(directory=TEMPLATES)


async def _conn():
    return await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row)


def _nav(request: Request) -> dict:
    uid = request.query_params.get("as") or "u-proc1"
    return {"as": uid, "actors": sorted(_ACTORS),
            "perms": sorted(PERMISSIONS.get(uid, set()))}


# ── suppliers ───────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def suppliers(request: Request):
    async with await _conn() as conn:
        rows = await (await conn.execute(
            "SELECT * FROM demo_suppliers ORDER BY id")).fetchall()
        runs = await (await conn.execute(
            "SELECT DISTINCT ON ((inputs->>'supplier_id')::int)"
            "   id, inputs->>'supplier_id' AS sup, status, terminal_outcome,"
            "   created_at"
            " FROM keelflow_workflow_runs"
            " WHERE workflow='sup.onboard@v1'"
            " ORDER BY (inputs->>'supplier_id')::int, created_at DESC"
        )).fetchall()
        open_tasks = await (await conn.execute(
            "SELECT COUNT(*) FROM keelflow_jobs"
            " WHERE type LIKE 'human.%' AND status IN ('pending','claimed')"
        )).fetchone()
    run_by_sup = {int(r["sup"]): r for r in runs}
    return templates.TemplateResponse(request, "suppliers.html", {
        "nav": _nav(request), "suppliers": rows, "runs": run_by_sup,
        "open_tasks": open_tasks["count"],
    })


@app.get("/suppliers/{sid}", response_class=HTMLResponse)
async def supplier(request: Request, sid: int):
    async with await _conn() as conn:
        s = await (await conn.execute(
            "SELECT * FROM demo_suppliers WHERE id=%s", (sid,))).fetchone()
        checks = await (await conn.execute(
            "SELECT * FROM demo_check_results WHERE supplier_id=%s"
            " ORDER BY check_type", (sid,))).fetchall()
        runs = await (await conn.execute(
            "SELECT id, status, terminal_outcome, created_at"
            " FROM keelflow_workflow_runs"
            " WHERE inputs->>'supplier_id'=%s ORDER BY created_at DESC",
            (str(sid),))).fetchall()
        vendor = await (await conn.execute(
            "SELECT * FROM demo_vendor_accounts WHERE supplier_id=%s",
            (sid,))).fetchone()
    return templates.TemplateResponse(request, "supplier.html", {
        "nav": _nav(request), "s": s, "checks": checks,
        "runs": runs, "vendor": vendor,
    })


@app.post("/suppliers/{sid}/onboard")
async def onboard(request: Request, sid: int):
    ctx = _ctx(request)
    ctx.require("sup.create")
    async with await _conn() as conn, conn.transaction(), \
            conn.cursor() as cur:
        await app.state.wf.start(
            "sup.onboard@v1", {"supplier_id": sid}, ctx, cur)
    return RedirectResponse(f"/suppliers/{sid}?as={ctx.actor.id}",
                            status_code=303)


@app.post("/suppliers/{sid}/toggle-fail")
async def toggle_fail(request: Request, sid: int):
    ctx = _ctx(request)
    async with await _conn() as conn:
        await conn.execute(
            "UPDATE demo_suppliers SET fail_sanction = NOT fail_sanction"
            " WHERE id=%s", (sid,))
        await conn.commit()
    return RedirectResponse(f"/suppliers/{sid}?as={ctx.actor.id}",
                            status_code=303)


@app.post("/suppliers/{sid}/resume/{run_id}")
async def resume(request: Request, sid: int, run_id: str):
    ctx = _ctx(request)
    async with await _conn() as conn:
        await app.state.wf.resume_run(conn, run_id, ctx)
        await conn.commit()
    return RedirectResponse(f"/suppliers/{sid}?as={ctx.actor.id}",
                            status_code=303)


# ── inbox ───────────────────────────────────────────────────────────────────

@app.get("/inbox", response_class=HTMLResponse)
async def inbox(request: Request):
    uid = request.query_params.get("as") or "u-proc1"
    async with await _conn() as conn:
        tasks = await app.state.human.my_tasks(
            conn, Actor.user(uid), roles=ROLES)
        # attach supplier names for readability
        for t in tasks:
            sid = (t.get("payload") or {}).get("supplier_id")
            if sid:
                r = await (await conn.execute(
                    "SELECT name FROM demo_suppliers WHERE id=%s",
                    (int(sid),))).fetchone()
                t["supplier_name"] = r["name"] if r else f"#{sid}"
    return templates.TemplateResponse(request, "inbox.html", {
        "nav": _nav(request), "tasks": tasks, "uid": uid})


@app.post("/inbox/{job_id}/complete")
async def complete_task(request: Request, job_id: str,
                        decision: str = Form(...),
                        note: str = Form("")):
    ctx = _ctx(request)
    actor = Actor.user(ctx.actor.id)
    async with await _conn() as conn:
        try:
            await app.state.human.claim(conn, job_id, actor)
        except PermissionError:
            pass  # already claimed by us or role task
        await app.state.human.complete(
            conn, job_id, actor,
            {"decision": decision, "note": note})
        await conn.commit()
    return RedirectResponse(f"/inbox?as={ctx.actor.id}", status_code=303)


# ── run DAG view ────────────────────────────────────────────────────────────

def _layers(steps: list[dict]) -> list[list[dict]]:
    """Order steps into dependency layers (for parallel-lane display)."""
    placed: set[str] = set()
    layers: list[list[dict]] = []
    remaining = list(steps)
    while remaining:
        layer = [s for s in remaining
                 if set(s["needs"]).issubset(placed)]
        if not layer:                     # safety: cycle → dump rest
            layers.append(remaining)
            break
        placed.update(s["step_id"] for s in layer)
        layers.append(layer)
        remaining = [s for s in remaining if s["step_id"] not in placed]
    return layers


@app.get("/runs/{run_id}", response_class=HTMLResponse)
async def run(request: Request, run_id: str):
    async with await _conn() as conn:
        detail = await app.state.wf.get_run(conn, run_id)
        for s in detail["steps"]:
            if s["job_id"]:
                s["job"] = await (await conn.execute(
                    "SELECT id, type, status, attempts, result"
                    " FROM keelflow_jobs WHERE id=%s",
                    (s["job_id"],))).fetchone()
    return templates.TemplateResponse(request, "run.html", {
        "nav": _nav(request), "run": detail,
        "steps": detail["steps"],
        "layers": _layers(detail["steps"])}) 


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job(request: Request, job_id: str):
    async with await _conn() as conn:
        r = await (await conn.execute(
            "SELECT * FROM keelflow_jobs WHERE id=%s", (job_id,))).fetchone()
        audit = await (await conn.execute(
            "SELECT * FROM keelflow_audit WHERE job_id=%s"
            " ORDER BY created_at", (job_id,))).fetchall()
    return templates.TemplateResponse(request, "job.html", {
        "nav": _nav(request), "job": r, "audit": audit})
