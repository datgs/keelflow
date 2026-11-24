"""invoice_flow demo — FastAPI app.

    KEELFLOW_DSN=postgresql://... uv run uvicorn invoice_flow.app:app --port 8501

Demo only: no authentication. The acting user is chosen with `?as=<uid>`
(u1=clerk, fin1=finance, gl1=GL, mgr=manager). All engine mutations go
through SM / HumanTaskService / WorkflowService inside a real TX; the app
never writes engine tables directly.
"""
from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import chancy
import psycopg
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from psycopg.rows import dict_row

from keelflow.engine.audit import PostgresAuditService
from keelflow.engine.diagram import state_machine_svg, workflow_svg
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.monitoring import MonitoringService
from keelflow.engine.statemachine import EngineStateMachineService
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import Actor, EntityRef, JobContext

from .i18n import render
from .registry import DSN_ENV, PERMISSIONS, ROLES, build_registry

DSN = os.environ.get(
    DSN_ENV, "postgresql://postgres:postgres@localhost:5544/keelflow")
KERNEL_PREFIX = os.environ.get("KEELFLOW_KERNEL_PREFIX", "keelflow_kernel_inv_")

templates = Jinja2Templates(
    directory=str(Path(__file__).parent / "templates"))
templates.env.filters["json_pretty"] = lambda v: json.dumps(
    v, indent=2, default=str)
templates.env.globals["perms_of"] = lambda uid: sorted(
    PERMISSIONS.get(uid, set()))


@asynccontextmanager
async def lifespan(app: FastAPI):
    registry = build_registry()
    kernel_cm = chancy.Chancy(DSN, prefix=KERNEL_PREFIX)
    kernel = await kernel_cm.__aenter__()
    await kernel.migrate()
    await kernel.declare(
        chancy.Queue("default", concurrency=8,
                     executor=chancy.Chancy.Executor.Async),
        upsert=True,
    )
    audit = PostgresAuditService()
    app.state.kernel = kernel
    app.state.registry = registry
    app.state.jobs = JobService(registry, kernel, PostgresStorage(DSN))
    app.state.sm = EngineStateMachineService(registry, kernel, audit=audit)
    app.state.human = HumanTaskService(registry, kernel,
                                       sm=app.state.sm)
    app.state.wf = WorkflowService(registry, app.state.jobs)
    app.state.monitoring = MonitoringService()
    app.state.audit = audit
    try:
        yield
    finally:
        await kernel_cm.__aexit__(None, None, None)


app = FastAPI(title="keelflow invoice demo", lifespan=lifespan)


async def conn():
    return await psycopg.AsyncConnection.connect(DSN, autocommit=True)


def ctx_for(request: Request) -> JobContext:
    uid = request.query_params.get("as", "u1")
    return JobContext(actor=Actor.user(uid),
                      permissions=frozenset(PERMISSIONS.get(uid, set())))


def actor_for(request: Request) -> Actor:
    return Actor.user(request.query_params.get("as", "u1"))


def roles_for(request: Request) -> list[str]:
    return ROLES.get(request.query_params.get("as", "u1"), [])


def back(request: Request, path: str) -> RedirectResponse:
    me = request.query_params.get("as", "u1")
    sep = "&" if "?" in path else "?"
    return RedirectResponse(f"{path}{sep}as={me}", status_code=303)


# ── invoices ─────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def invoice_list(request: Request):
    c = await conn()
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT i.number, i.vendor, i.amount, i.currency, i.fail_post,"
            " COALESCE(e.state, 'draft') AS state"
            " FROM demo_invoices i"
            " LEFT JOIN keelflow_entity_state e"
            "  ON e.entity_type='invoice' AND e.entity_id=i.number"
            " ORDER BY i.number"
        )
        invoices = await cur.fetchall()
    await c.close()
    return render(
        templates, request, "invoices.html",
        { "invoices": invoices,
         "me": request.query_params.get("as", "u1"),
         "roles": roles_for(request),
         "err": request.query_params.get("err")})


@app.get("/invoices/{number}", response_class=HTMLResponse)
async def invoice_detail(request: Request, number: str):
    c = await conn()
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT * FROM demo_invoices WHERE number=%s", (number,))
        inv = await cur.fetchone()
        if inv is None:
            await c.close()
            return HTMLResponse(f"invoice {number} not found", status_code=404)
        await cur.execute(
            "SELECT state, version FROM keelflow_entity_state"
            " WHERE entity_type='invoice' AND entity_id=%s",
            (number,))
        ent = await cur.fetchone()
        await cur.execute(
            "SELECT id, workflow, status, terminal_outcome, callback_state,"
            " created_at FROM keelflow_workflow_runs"
            " WHERE triggered_by_entity_type='invoice'"
            " AND triggered_by_entity_id=%s"
            " ORDER BY created_at", (number,))
        runs = await cur.fetchall()
        await cur.execute(
            "SELECT journal_ref FROM demo_gl_records WHERE invoice_no=%s",
            (number,))
        gl = await cur.fetchone()
        await cur.execute(
            "SELECT amount, posted_by, posted_at FROM demo_ledger_postings"
            " WHERE invoice_no=%s", (number,))
        posting = await cur.fetchone()
    trace = await app.state.audit.trace(
        c, EntityRef(type="invoice", id=number))
    actions = await app.state.sm.allowed_actions(
        EntityRef(type="invoice", id=number), actor_for(request), c,
        permissions=ctx_for(request).permissions)
    # diagrams: SM state view + latest workflow run live overlay
    reg = app.state.registry
    sm_def = reg.state_machine("invoice@v1")
    sm_diagram = state_machine_svg(
        sm_def, current=(ent["state"] if ent else "draft"),
        allowed_actions=set(actions))
    wf_diagram = None
    blocked = []
    if runs:
        detail = await app.state.wf.get_run(c, str(runs[-1]["id"]))
        blocked = [st for st in detail["steps"]
                   if st["status"] in ("blocked", "claimed")
                   and st["type"].startswith("human.")]
        meta = {}
        for st in detail["steps"]:
            if st["type"].startswith("human."):
                spec = reg.job_type(st["type"])
                meta[st["type"]] = {
                    "assignee": getattr(spec.human_task, "assignee_role", None)
                    if spec.human_task else None}
        wf_diagram = workflow_svg(
            reg.workflow(runs[-1]["workflow"]),
            step_status={st["step_id"]: st["status"]
                         for st in detail["steps"]},
            job_type_meta=meta)
    await c.close()
    return render(
        templates, request, "invoice.html",
        { "inv": inv,
         "state": (ent["state"] if ent else "draft"),
         "actions": actions, "runs": runs, "gl": gl,
         "posting": posting, "trace": trace,
         "sm_diagram": sm_diagram, "wf_diagram": wf_diagram,
         "blocked": blocked,
         "me": request.query_params.get("as", "u1"),
         "err": request.query_params.get("err")})


@app.post("/invoices")
async def create_invoice(request: Request,
                         number: str = Form(...),
                         vendor: str = Form("Unknown Vendor"),
                         amount: float = Form(...)):
    c = await conn()
    await c.execute(
        "INSERT INTO demo_invoices (number, vendor, amount)"
        " VALUES (%s, %s, %s) ON CONFLICT (number) DO NOTHING",
        (number, vendor, amount))
    await c.execute(
        "INSERT INTO keelflow_entity_state (entity_type, entity_id, state)"
        " VALUES ('invoice', %s, 'draft')"
        " ON CONFLICT (entity_type, entity_id) DO NOTHING",
        (number,))
    await c.close()
    return back(request, "/")


@app.post("/invoices/{number}/fail_toggle")
async def fail_toggle(request: Request, number: str):
    c = await conn()
    await c.execute(
        "UPDATE demo_invoices SET fail_post = NOT fail_post WHERE number=%s",
        (number,))
    await c.close()
    return back(request, f"/invoices/{number}")


@app.post("/invoices/{number}/action/{action}")
async def sm_action(request: Request, number: str, action: str):
    """State-machine actions the user performs directly:
    submit, resubmit, approve (which triggers post.wf@v1)."""
    c = await conn()
    try:
        async with c.transaction(), c.cursor(row_factory=dict_row) as cur:
            await app.state.sm.transition(
                EntityRef(type="invoice", id=number), action,
                ctx_for(request), cur)
        err = None
    except (ValueError, PermissionError) as e:
        err = str(e)
    await c.close()
    return back(request, f"/invoices/{number}" + (f"?err={err}" if err else ""))


@app.post("/invoices/{number}/resume")
async def resume_run(request: Request, number: str):
    """Repost / retry_post — resume the failed or rejected workflow run."""
    c = await conn()
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id FROM keelflow_workflow_runs"
            " WHERE triggered_by_entity_type='invoice'"
            " AND triggered_by_entity_id=%s AND status IN ('done','failed')"
            " AND terminal_outcome IN ('rejected','failed')"
            " ORDER BY created_at DESC LIMIT 1", (number,))
        run = await cur.fetchone()
    try:
        if run is None:
            raise ValueError("no resumable run for this invoice")
        await app.state.wf.resume_run(c, str(run["id"]), ctx_for(request))
        err = None
    except (ValueError, KeyError) as e:
        err = str(e)
    await c.close()
    return back(request, f"/invoices/{number}" + (f"?err={err}" if err else ""))


# ── inbox ────────────────────────────────────────────────────────────────

@app.get("/inbox", response_class=HTMLResponse)
async def inbox(request: Request):
    c = await conn()
    tasks = await app.state.human.my_tasks(
        c, actor_for(request), roles=roles_for(request))
    from .i18n import resolve_locale, translator
    t = translator(resolve_locale(request))
    for t_ in tasks:
        t_["decisions"] = sorted(
            ((t_["payload"] or {}).get("sm_actions") or {}).keys())
        t_["kind"] = t(f"task.{t_['type']}")
    await c.close()
    return render(
        templates, request, "inbox.html",
        { "tasks": tasks,
         "me": request.query_params.get("as", "u1"),
         "err": request.query_params.get("err")})


@app.post("/tasks/{tid}/claim")
async def claim(request: Request, tid: str):
    c = await conn()
    try:
        await app.state.human.claim(
            c, tid, actor_for(request),
            roles=roles_for(request))
        err = None
    except (PermissionError, KeyError) as e:
        err = str(e)
    await c.close()
    return back(request, "/inbox" + (f"?err={err}" if err else ""))


@app.post("/tasks/{tid}/decision/{decision}")
async def decide(request: Request, tid: str, decision: str,
                 note: str = Form("")):
    """Complete a human task.

    Three task kinds:
    - SM-created (payload.sm_actions) → complete_and_transition
    - posting fix-up (human.posting_fix) → mark done + resume the run
    - workflow step (gl_check) → human.complete → unblocks the run
    """
    c = await conn()
    try:
        async with c.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT type, payload FROM keelflow_jobs WHERE id=%s", (tid,))
            row = await cur.fetchone()
        if row is None:
            raise KeyError(f"task {tid} not found")
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        roles = roles_for(request)
        await app.state.human.claim(
            c, tid, actor_for(request), roles=roles)
        sm_actions = (payload or {}).get("sm_actions") or {}

        if sm_actions:
            await app.state.human.complete_and_transition(
                c, tid, actor_for(request), decision, ctx_for(request), note,
                roles=roles)
        elif row["type"] == "human.posting_fix@v1":
            # fix-up: mark task done + resume the run in ONE transaction
            # (resume_run's inner transaction becomes a savepoint).
            async with c.transaction(), c.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "UPDATE keelflow_jobs SET status='done',"
                    " result=%s, updated_at=now() WHERE id=%s",
                    (json.dumps({"decision": decision, "note": note}), tid))
                await cur.execute(
                    "SELECT id FROM keelflow_workflow_runs"
                    " WHERE triggered_by_entity_type='invoice'"
                    " AND triggered_by_entity_id=%s AND terminal_outcome='failed'"
                    " ORDER BY created_at DESC LIMIT 1",
                    (payload["entity_id"],))
                run = await cur.fetchone()
                if run:
                    await app.state.wf.resume_run(
                        c, str(run["id"]), ctx_for(request))
        elif row["type"].startswith("human."):
            await app.state.human.complete(
                c, tid, actor_for(request),
                {"decision": decision, "note": note}, roles=roles)
        else:
            raise ValueError(f"task type {row['type']} cannot be completed here")
        err = None
    except (PermissionError, KeyError, ValueError) as e:
        err = str(e)
    await c.close()
    return back(request, "/inbox" + (f"?err={err}" if err else ""))


# ── monitoring ───────────────────────────────────────────────────────────

@app.get("/runs/{rid}", response_class=HTMLResponse)
async def run_detail(request: Request, rid: str):
    c = await conn()
    run = await app.state.monitoring.get_run(c, rid)
    await c.close()
    if run is None:
        return HTMLResponse(f"run {rid} not found", status_code=404)
    return render(
        templates, request, "run.html",
        { "run": run,
         "me": request.query_params.get("as", "u1")})


@app.get("/jobs/{jid}", response_class=HTMLResponse)
async def job_detail(request: Request, jid: str):
    c = await conn()
    job = await app.state.monitoring.get_job(c, jid)
    await c.close()
    if job is None:
        return HTMLResponse(f"job {jid} not found", status_code=404)
    return render(
        templates, request, "job.html",
        { "job": job,
         "me": request.query_params.get("as", "u1")})


@app.post("/jobs/{jid}/retry")
async def job_retry(request: Request, jid: str):
    c = await conn()
    await app.state.monitoring.retry_job(c, jid)
    await c.close()
    return back(request, f"/jobs/{jid}")
