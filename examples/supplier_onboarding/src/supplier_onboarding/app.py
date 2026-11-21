"""Demo 2 UI — FastAPI app. Supplier onboarding DAG with parallel checks
and conditional human review.

    uv run --package keelflow-demo-supplier \
        uvicorn supplier_onboarding.app:app --port 8001

Demo only: no auth, binds localhost. Refresh manually (no live updates).
"""
from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager

import chancy
import psycopg
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from psycopg.rows import dict_row

from keelflow.engine.diagram import workflow_svg
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.workflow import WorkflowService
from keelflow.sdk import Actor, JobContext

from . import i18n
from .registry import PERMISSIONS, build_registry

DSN = os.environ.get("KEELFLOW_DSN",
                     "postgresql://postgres:postgres@localhost:5544/keelflow")
TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")

# actor ids seen in this demo
_ACTORS = ("u-proc1", "u-comp1", "u-fin1", "u-procadm", "u-mgr")
_ACTOR_ROLES = {
    "u-proc1": ["procurement"],
    "u-comp1": ["compliance"],
    "u-fin1": ["finance"],
    "u-procadm": ["provisioning"],
    "u-mgr": ["procurement", "compliance", "finance", "provisioning", "manager"],
}
_ACTOR_NAMES = {
    "u-proc1":   "Alice — Procurement",
    "u-comp1":   "Carol — Compliance",
    "u-fin1":    "Frank — Finance",
    "u-procadm": "Pat — Procurement Admin",
    "u-mgr":     "Maya — Manager (all)",
}


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
    app.state.registry = registry
    app.state.jobs = jobs
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
            "names": _ACTOR_NAMES,
            "perms": sorted(PERMISSIONS.get(uid, set())),
            "perms_of": {a: sorted(PERMISSIONS.get(a, set()))
                         for a in _ACTORS}}


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
    return i18n.render(templates, request, "suppliers.html", {
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
            "SELECT id, status, terminal_outcome, workflow, created_at"
            " FROM keelflow_workflow_runs"
            " WHERE inputs->>'supplier_id'=%s ORDER BY created_at DESC",
            (str(sid),))).fetchall()
        vendor = await (await conn.execute(
            "SELECT * FROM demo_vendor_accounts WHERE supplier_id=%s",
            (sid,))).fetchone()
        wf_diagram = None
        blocked = []
        waiting = []
        latest = runs[0] if runs else None
        if latest:
            detail = await app.state.wf.get_run(conn, str(latest["id"]))
            blocked = [st for st in detail["steps"]
                       if st["status"] in ("blocked", "claimed")
                       and st["type"].startswith("human.")]
            waiting = [st for st in detail["steps"]
                       if st["status"] == "blocked"
                       and st["type"].startswith("wait.")]
            meta = {}
            for st in detail["steps"]:
                if st["type"].startswith("human."):
                    spec = app.state.registry.job_type(st["type"])
                    meta[st["type"]] = {
                        "assignee": getattr(spec.ui, "assignee_role", None)
                        if spec.ui else None}
            wf_diagram = workflow_svg(
                app.state.registry.workflow(latest["workflow"]),
                step_status={st["step_id"]: st["status"]
                             for st in detail["steps"]},
                job_type_meta=meta)
        # app-owned mapping for the pending provider request (if any)
        bank_req = await (await conn.execute(
            "SELECT * FROM demo_bank_requests WHERE supplier_id=%s"
            " AND status='sent'", (sid,))).fetchone()
        return i18n.render(templates, request, "supplier.html", {
            "nav": _nav(request), "s": s, "checks": checks,
            "runs": runs, "vendor": vendor, "wf_diagram": wf_diagram,
            "blocked": blocked, "waiting": waiting, "bank_req": bank_req,
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


# ── mock bank provider integration (app-owned boundary) ─────────────────
# The external provider talks ONLY to these app routes; the app does the
# correlation + translation and calls jobs.signal().


@app.post("/suppliers/{sid}/send-bank-request")
async def send_bank_request(request: Request, sid: int):
    """App action: read the open wait job, register the provider request
    in the app's own correlation table, and 'call' the mock provider."""
    ctx = _ctx(request)
    async with await _conn() as conn:
        row = await (await conn.execute(
            """
            SELECT j.id, j.type FROM keelflow_jobs j
            JOIN keelflow_step_runs sr ON sr.job_id = j.id
            JOIN keelflow_workflow_runs r ON r.id = sr.run_id
            WHERE r.status='running'
              AND j.type = 'wait.bank_verify@v1'
              AND j.status IN ('pending','claimed')
              AND (sr.job_id) IS NOT NULL
              AND j.payload->>'supplier_id' = %s
            ORDER BY j.created_at DESC LIMIT 1
            """, (str(sid),))).fetchone()
        if row is None:
            return RedirectResponse(
                f"/suppliers/{sid}?as={ctx.actor.id}"
                "&bank_err=no+open+bank+wait", status_code=303)
        provider_ref = f"bk-{row['id'].hex[:12]}"
        await conn.execute(
            "INSERT INTO demo_bank_requests (provider_ref, supplier_id,"
            " job_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (provider_ref, sid, str(row["id"])))
        await conn.commit()
    # A real integration would POST to the provider API here; the
    # provider answers later via /integrations/bank/callback — never
    # into keelflow directly. The demo simulates that callback with the
    # buttons rendered on the supplier page.
    return RedirectResponse(f"/suppliers/{sid}?as={ctx.actor.id}",
                            status_code=303)


async def handle_bank_callback(conn, jobs, provider_ref: str,
                               outcome: str) -> int:
    """App-owned provider callback, shared by the route and tests.

    Correlates provider_ref → wait job via the app's own table, writes
    the check result row, and resolves the wait in ONE transaction
    (caller commits). Returns the supplier id."""
    req = await (await conn.execute(
        "SELECT supplier_id, job_id, status FROM demo_bank_requests"
        " WHERE provider_ref=%s", (provider_ref,))).fetchone()
    if req is None:
        raise KeyError(f"unknown provider_ref {provider_ref!r}")
    sid = req["supplier_id"]
    ctx = JobContext(
        actor=Actor.service("bank-webhook"),
        permissions=frozenset({"job.signal"}))
    verified = outcome == "verified"
    result = ({"verified": True,
               "method": "penny-deposit (provider callback)"}
              if verified else
              {"verified": False, "reason": "account mismatch"})
    await conn.execute(
        "INSERT INTO demo_check_results"
        " (supplier_id, check_type, result)"
        " VALUES (%s, 'bank_verify', %s)"
        " ON CONFLICT (supplier_id, check_type) DO UPDATE"
        " SET result=EXCLUDED.result",
        (sid, json.dumps(result)),
    )
    await conn.execute(
        "UPDATE demo_bank_requests SET status=%s WHERE provider_ref=%s",
        ("verified" if verified else "failed", provider_ref))
    await jobs.signal(conn, str(req["job_id"]), result, ctx)
    return sid


@app.post("/integrations/bank/callback")
async def bank_callback(request: Request,
                        provider_ref: str = Form(...),
                        outcome: str = Form("verified")):
    """Provider webhook endpoint (app-owned). The mock provider posts
    here; the app correlates the ref to the wait job, writes the check
    result row, and resolves the wait in ONE transaction."""
    sid = "suppliers"
    try:
        async with await _conn() as conn:
            sid = await handle_bank_callback(
                conn, app.state.jobs, provider_ref, outcome)
            await conn.commit()
    except KeyError:
        return RedirectResponse(
            "/suppliers?as=u-proc1&bank_err=unknown+provider_ref",
            status_code=303)
    except (ValueError, PermissionError) as e:
        return RedirectResponse(
            f"/suppliers?as=u-proc1&bank_err={e}", status_code=303)
    return RedirectResponse(
        f"/suppliers/{sid}?as=u-proc1", status_code=303)


# ── inbox ───────────────────────────────────────────────────────────────────

@app.get("/inbox", response_class=HTMLResponse)
async def inbox(request: Request):
    uid = request.query_params.get("as") or "u-proc1"
    async with await _conn() as conn:
        tasks = await app.state.human.my_tasks(
            conn, Actor.user(uid), roles=_ACTOR_ROLES.get(uid, []))
        # attach supplier names for readability
        for t in tasks:
            sid = (t.get("payload") or {}).get("supplier_id")
            if sid:
                r = await (await conn.execute(
                    "SELECT name FROM demo_suppliers WHERE id=%s",
                    (int(sid),))).fetchone()
                t["supplier_name"] = r["name"] if r else f"#{sid}"
    return i18n.render(templates, request, "inbox.html", {
        "nav": _nav(request), "tasks": tasks, "uid": uid})


@app.post("/inbox/{job_id}/complete")
async def complete_task(request: Request, job_id: str,
                        decision: str = Form(...),
                        note: str = Form("")):
    ctx = _ctx(request)
    actor = Actor.user(ctx.actor.id)
    async with await _conn() as conn:
        try:
            await app.state.human.claim(
                conn, job_id, actor, roles=_ACTOR_ROLES.get(ctx.actor.id, []))
            await app.state.human.complete(
                conn, job_id, actor,
                {"decision": decision, "note": note},
                roles=_ACTOR_ROLES.get(ctx.actor.id, []))
            await conn.commit()
        except PermissionError as e:
            return RedirectResponse(
                f"/inbox?as={ctx.actor.id}&err={e}", status_code=303)
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
    return i18n.render(templates, request, "run.html", {
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
    return i18n.render(templates, request, "job.html", {
        "nav": _nav(request), "job": r, "audit": audit})
