"""invoice_flow demo — FastAPI app.

Domain data lives in plain app tables (`demo_invoices`); engine data in
`keelflow_*`. The app never writes engine tables directly — all mutations
go through SM / HumanTaskService / JobService in the caller's TX.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from psycopg.rows import dict_row

from keelflow.engine.human import HumanTaskService
from keelflow.engine.monitoring import MonitoringService
from keelflow.engine.statemachine import EngineStateMachineService
from keelflow.engine.audit import PostgresAuditService
from keelflow.sdk import Actor, EntityRef, JobContext

from .registry import build_registry

DSN = os.environ["KEELFLOW_DSN"]
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.registry = build_registry()
    app.state.sm = EngineStateMachineService(app.state.registry)
    app.state.human = HumanTaskService(app.state.registry)
    app.state.monitoring = MonitoringService()
    app.state.audit = PostgresAuditService()
    yield


app = FastAPI(title="keelflow invoice demo", lifespan=lifespan)


async def conn():
    return await psycopg.AsyncConnection.connect(DSN, autocommit=True)


def actor(request: Request) -> Actor:
    return Actor.user(request.query_params.get("as", "u1"))


ROLES = {"u1": [], "fin1": ["finance"], "mgr": ["finance", "manager"]}


def ctx_for(request: Request) -> JobContext:
    uid = request.query_params.get("as", "u1")
    perms = {"u1": {"inv.submit", "perm:own"},
             "fin1": {"inv.approve"},
             "mgr": {"inv.submit", "inv.approve"}}.get(uid, set())
    return JobContext(actor=Actor.user(uid), permissions=frozenset(perms))


def back(request: Request, path: str) -> RedirectResponse:
    q = request.query_params.get("as")
    return RedirectResponse(f"{path}?as={q}" if q else path, status_code=303)


# ── invoices ─────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def list_invoices(request: Request):
    c = await conn()
    cur = await c.execute(
        "SELECT i.id, i.number, i.amount, s.state"
        " FROM demo_invoices i"
        " LEFT JOIN keelflow_entity_state s"
        "   ON s.entity_type='invoice' AND s.entity_id=i.id::text"
        " ORDER BY i.id"
    )
    rows = await cur.fetchall()
    me = actor(request)
    enriched = []
    for rid, number, amount, state in rows:
        actions = await app.state.sm.allowed_actions(
            EntityRef(type="invoice", id=str(rid)), me, c)
        enriched.append({"id": rid, "number": number, "amount": amount,
                         "state": state or "draft", "actions": actions})
    await c.close()
    return templates.TemplateResponse(
        "invoices.html", {"request": request, "rows": enriched,
                          "me": me.id})


@app.get("/invoices/{iid}", response_class=HTMLResponse)
async def invoice_detail(request: Request, iid: int):
    c = await conn()
    row = await (await c.execute(
        "SELECT id, number, amount FROM demo_invoices WHERE id=%s", (iid,)
    )).fetchone()
    state = await app.state.sm.current(EntityRef(type="invoice", id=str(iid)), c)
    audit = await app.state.audit.trace(c, EntityRef(type="invoice", id=str(iid)))
    steps = await (await c.execute(
        "SELECT sr.step_id, sr.status, sr.job_id, wr.id AS run_id"
        " FROM keelflow_step_runs sr"
        " JOIN keelflow_workflow_runs wr ON wr.id = sr.run_id"
        " WHERE wr.inputs->>'entity_id' = %s ORDER BY sr.created_at",
        (str(iid),),
    )).fetchall()
    await c.close()
    return templates.TemplateResponse(
        "invoice.html", {"request": request, "inv": row,
                         "state": state or "draft", "audit": audit,
                         "steps": steps})


@app.post("/invoices/{iid}/act/{action}")
async def act(request: Request, iid: int, action: str):
    c = await conn()
    ctx = ctx_for(request)
    try:
        async with c.transaction(), c.cursor() as cur:
            await app.state.sm.transition(
                EntityRef(type="invoice", id=str(iid)), action, ctx, cur)
        msg = "ok"
    except (ValueError, PermissionError) as e:
        msg = str(e)
    await c.close()
    return back(request, f"/invoices/{iid}" + (f"?msg={msg}" if msg != "ok" else ""))


# ── inbox ────────────────────────────────────────────────────────────────

@app.get("/inbox", response_class=HTMLResponse)
async def inbox(request: Request):
    c = await conn()
    me = actor(request)
    tasks = await app.state.human.my_tasks(c, me, roles=ROLES.get(me.id, []))
    await c.close()
    return templates.TemplateResponse(
        "inbox.html", {"request": request, "tasks": tasks, "me": me.id})


@app.post("/tasks/{tid}/claim")
async def claim(request: Request, tid: str):
    c = await conn()
    try:
        await app.state.human.claim(c, tid, actor(request))
        err = None
    except PermissionError as e:
        err = str(e)
    await c.close()
    return back(request, "/inbox" + (f"?err={err}" if err else ""))


@app.post("/tasks/{tid}/complete")
async def complete(request: Request, tid: str,
                   decision: str = Form("approve"), note: str = Form("")):
    c = await conn()
    try:
        await app.state.human.complete(
            c, tid, actor(request), {"decision": decision, "note": note})
        err = None
    except (PermissionError, KeyError) as e:
        err = str(e)
    await c.close()
    return back(request, "/inbox" + (f"?err={err}" if err else ""))


# ── monitoring ───────────────────────────────────────────────────────────

@app.get("/runs/{rid}", response_class=HTMLResponse)
async def run_detail(request: Request, rid: str):
    c = await conn()
    run = await app.state.monitoring.runs_get(c, rid)
    await c.close()
    return templates.TemplateResponse(
        "run.html", {"request": request, "run": run})


@app.get("/jobs/{jid}", response_class=HTMLResponse)
async def job_detail(request: Request, jid: str):
    c = await conn()
    job = await app.state.monitoring.job_get(c, jid)
    await c.close()
    return templates.TemplateResponse(
        "job.html", {"request": request, "job": job})


@app.post("/invoices")
async def create_invoice(request: Request,
                         number: str = Form(...), amount: float = Form(...)):
    c = await conn()
    await c.execute(
        "INSERT INTO demo_invoices (number, amount) VALUES (%s, %s)",
        (number, amount))
    await c.close()
    return back(request, "/")
