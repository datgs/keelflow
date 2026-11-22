"""Demo 5 UI — FastAPI app. Financial payment-run approval DAG with a
parallel risk fan-out, a conditional multi-role approval chain, a bulk
`process_batch` line step, and three app-owned external wait steps.

    uv run --package keelflow-demo-financial \
        uvicorn financial_approval.app:app --port 8002

Demo only: no auth, synthetic data, binds localhost. Refresh manually.
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

from .registry import PERMISSIONS, build_registry

DSN = os.environ.get("KEELFLOW_DSN",
                     "postgresql://postgres:postgres@localhost:5544/keelflow")
TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")

_ACTORS = ("u-clerk", "u-manager", "u-controller", "u-cfo", "u-comp", "u-admin")
_ACTOR_ROLES = {
    "u-clerk":      ["clerk"],
    "u-manager":    ["manager"],
    "u-controller": ["controller"],
    "u-cfo":        ["cfo"],
    "u-comp":       ["compliance"],
    "u-admin":      ["clerk", "manager", "controller", "cfo",
                     "compliance", "treasury"],
}
_ACTOR_NAMES = {
    "u-clerk":      "Cathy — AP Clerk",
    "u-manager":    "Mia — Manager",
    "u-controller": "Cole — Controller",
    "u-cfo":        "Faye — CFO",
    "u-comp":       "Omar — Compliance",
    "u-admin":      "Ava — Admin (all)",
}

# provider wait steps the app can dispatch + resolve
_WAIT_TYPES = {
    "fx":       ("wait.fx_quote@v1",     "fx_quote"),
    "bank_pre": ("wait.bank_preverify@v1", "bank_preverify"),
    "bank_exec": ("wait.bank_execute@v1",  "bank_execute"),
}


def _ctx(request: Request) -> JobContext:
    uid = request.query_params.get("as") or "u-clerk"
    if uid not in _ACTORS:
        uid = "u-clerk"
    return JobContext(actor=Actor.user(uid), tenant_id="demo",
                      permissions=PERMISSIONS.get(uid, set()))


@asynccontextmanager
async def _lifespan(app: FastAPI):
    registry = build_registry()
    kernel = chancy.Chancy(DSN, prefix="keelflow_kernel_fin_")
    await kernel.migrate()
    await kernel.declare(
        chancy.Queue("default", concurrency=8,
                     executor=chancy.Chancy.Executor.Async,
                     eager_polling=True),
        upsert=True)
    app.state.registry = registry
    app.state.jobs = JobService(registry, kernel)
    app.state.wf = WorkflowService(registry, app.state.jobs)
    app.state.human = HumanTaskService(registry, kernel)
    async with kernel:
        yield

app = FastAPI(title="keelflow demo · financial approval", lifespan=_lifespan)
templates = Jinja2Templates(directory=TEMPLATES)


async def _conn():
    return await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row)


def _nav(request: Request) -> dict:
    uid = request.query_params.get("as") or "u-clerk"
    return {"as": uid, "actors": sorted(_ACTORS), "names": _ACTOR_NAMES,
            "perms": sorted(PERMISSIONS.get(uid, set()))}


def _render(request: Request, name: str, ctx: dict):
    return templates.TemplateResponse(
        request, name, {"nav": _nav(request), **ctx})


# ── requests ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def requests(request: Request):
    async with await _conn() as conn:
        rows = await (await conn.execute(
            "SELECT r.*, v.name AS vendor_name FROM demo_pay_requests r"
            " JOIN demo_vendors v ON v.id=r.vendor_id ORDER BY r.id")).fetchall()
        runs = await (await conn.execute(
            "SELECT DISTINCT ON ((inputs->>'request_id')::int)"
            "   id, inputs->>'request_id' AS rid, status, terminal_outcome"
            " FROM keelflow_workflow_runs WHERE workflow='fin.pay_run@v1'"
            " ORDER BY (inputs->>'request_id')::int, created_at DESC"
        )).fetchall()
        open_tasks = await (await conn.execute(
            "SELECT COUNT(*) AS n FROM keelflow_jobs"
            " WHERE type LIKE 'human.%' AND status IN ('pending','claimed')"
        )).fetchone()
    return _render(request, "requests.html", {
        "requests": rows, "runs": {int(r["rid"]): r for r in runs},
        "open_tasks": open_tasks["n"]})


@app.get("/requests/{rid}", response_class=HTMLResponse)
async def request_detail(request: Request, rid: int):
    async with await _conn() as conn:
        p = await (await conn.execute(
            "SELECT r.*, v.name AS vendor_name, v.bank_ref, v.country"
            " FROM demo_pay_requests r JOIN demo_vendors v"
            "   ON v.id=r.vendor_id WHERE r.id=%s", (rid,))).fetchone()
        lines = await (await conn.execute(
            "SELECT * FROM demo_pay_lines WHERE request_id=%s"
            " ORDER BY line_no", (rid,))).fetchall()
        checks = await (await conn.execute(
            "SELECT * FROM demo_fin_checks WHERE request_id=%s"
            " ORDER BY check_type", (rid,))).fetchall()
        runs = await (await conn.execute(
            "SELECT id, status, terminal_outcome, created_at"
            " FROM keelflow_workflow_runs"
            " WHERE inputs->>'request_id'=%s ORDER BY created_at DESC",
            (str(rid),))).fetchall()
        wf_diagram = None
        blocked = []
        waiting = []
        open_waits: set[str] = set()
        latest = runs[0] if runs else None
        if latest:
            detail = await app.state.wf.get_run(conn, str(latest["id"]))
            blocked = [s for s in detail["steps"]
                       if s["status"] in ("blocked", "claimed")
                       and s["type"].startswith("human.")]
            waiting = [s for s in detail["steps"]
                       if s["status"] == "blocked"
                       and s["type"].startswith("wait.")]
            open_waits = {s["type"] for s in waiting}
            meta = {}
            for s in detail["steps"]:
                if s["type"].startswith("human."):
                    spec = app.state.registry.job_type(s["type"])
                    meta[s["type"]] = {
                        "assignee": getattr(spec.ui, "assignee_role", None)
                        if spec.ui else None}
            wf_diagram = workflow_svg(
                app.state.registry.workflow("fin.pay_run@v1"),
                step_status={s["step_id"]: s["status"]
                             for s in detail["steps"]},
                job_type_meta=meta)
        # provider requests already sent for open waits
        prov = await (await conn.execute(
            "SELECT wait_type, provider_ref, status"
            " FROM demo_fin_provider_requests WHERE request_id=%s",
            (rid,))).fetchall()
        prov_by_type = {r["wait_type"]: r for r in prov}
        return _render(request, "request.html", {
            "p": p, "lines": lines, "checks": checks, "runs": runs,
            "wf_diagram": wf_diagram, "blocked": blocked,
            "waiting": waiting, "open_waits": open_waits,
            "prov_by_type": prov_by_type,
            "latest": latest})


@app.post("/requests/{rid}/submit")
async def submit(request: Request, rid: int):
    ctx = _ctx(request)
    ctx.require("pay.submit")
    async with await _conn() as conn, conn.transaction(), \
            conn.cursor() as cur:
        await app.state.wf.start(
            "fin.pay_run@v1", {"request_id": rid}, ctx, cur)
    return RedirectResponse(f"/requests/{rid}?as={ctx.actor.id}",
                            status_code=303)


@app.post("/requests/{rid}/toggle-fail")
async def toggle_fail(request: Request, rid: int):
    ctx = _ctx(request)
    async with await _conn() as conn:
        await conn.execute(
            "UPDATE demo_pay_requests SET fail_sanction = NOT fail_sanction"
            " WHERE id=%s", (rid,))
        await conn.commit()
    return RedirectResponse(f"/requests/{rid}?as={ctx.actor.id}",
                            status_code=303)


@app.post("/requests/{rid}/resume/{run_id}")
async def resume(request: Request, rid: int, run_id: str):
    ctx = _ctx(request)
    async with await _conn() as conn:
        await app.state.wf.resume_run(conn, run_id, ctx)
        await conn.commit()
    return RedirectResponse(f"/requests/{rid}?as={ctx.actor.id}",
                            status_code=303)


@app.post("/requests/{rid}/cancel/{run_id}")
async def cancel_run(request: Request, rid: int, run_id: str):
    """Cancel the run's active job — records a durable cancel request."""
    ctx = _ctx(request)
    async with await _conn() as conn:
        job = await (await conn.execute(
            "SELECT j.id FROM keelflow_jobs j"
            " JOIN keelflow_step_runs sr ON sr.job_id=j.id"
            " WHERE sr.run_id=%s AND j.status IN"
            "   ('pending','claimed','running')"
            " ORDER BY j.created_at DESC LIMIT 1", (run_id,))).fetchone()
        if job:
            await app.state.jobs.cancel(str(job["id"]), ctx)
    return RedirectResponse(f"/requests/{rid}?as={ctx.actor.id}",
                            status_code=303)


# ── external provider boundary (app-owned) ────────────────────────────────
# The mock providers talk ONLY to these app routes. The app registers a
# provider_ref ↔ wait-job correlation, then resolves it via jobs.signal().

@app.post("/requests/{rid}/send/{prov}")
async def send_provider(request: Request, rid: int, prov: str):
    """Dispatch a request to the mock provider for the open wait step."""
    if prov not in _WAIT_TYPES:
        return RedirectResponse(f"/requests/{rid}", status_code=303)
    ctx = _ctx(request)
    wait_type, check_type = _WAIT_TYPES[prov]
    async with await _conn() as conn:
        row = await (await conn.execute(
            """
            SELECT j.id FROM keelflow_jobs j
            JOIN keelflow_step_runs sr ON sr.job_id = j.id
            JOIN keelflow_workflow_runs r ON r.id = sr.run_id
            WHERE r.status='running' AND j.type=%s
              AND j.status IN ('pending','claimed')
              AND j.payload->>'request_id' = %s
            ORDER BY j.created_at DESC LIMIT 1
            """, (wait_type, str(rid)))).fetchone()
        if row is None:
            return RedirectResponse(
                f"/requests/{rid}?as={ctx.actor.id}&prov_err=no+open+{prov}",
                status_code=303)
        provider_ref = f"{prov}-{row['id'].hex[:12]}"
        await conn.execute(
            "INSERT INTO demo_fin_provider_requests"
            " (provider_ref, request_id, wait_type, job_id)"
            " VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (provider_ref, rid, check_type, str(row["id"])))
        await conn.commit()
    # A real integration would POST to the provider here; the provider
    # calls back via /integrations/{prov}/callback — never into keelflow.
    return RedirectResponse(f"/requests/{rid}?as={ctx.actor.id}",
                            status_code=303)


async def handle_provider_callback(conn, jobs, provider_ref: str,
                                   outcome: str) -> int:
    """Correlate provider_ref → wait job, write the check row, and resolve
    the wait in ONE transaction (caller commits). Returns request_id."""
    req = await (await conn.execute(
        "SELECT request_id, wait_type, job_id FROM demo_fin_provider_requests"
        " WHERE provider_ref=%s", (provider_ref,))).fetchone()
    if req is None:
        raise KeyError(f"unknown provider_ref {provider_ref!r}")
    rid = req["request_id"]
    check_type = req["wait_type"]
    ctx = JobContext(actor=Actor.service("fin-provider-webhook"),
                     permissions=frozenset({"job.signal"}))
    ok = outcome in ("verified", "ok")
    result = _provider_result(check_type, ok)
    await conn.execute(
        "INSERT INTO demo_fin_checks (request_id, check_type, result)"
        " VALUES (%s,%s,%s)"
        " ON CONFLICT (request_id, check_type) DO UPDATE"
        " SET result=EXCLUDED.result",
        (rid, check_type, json.dumps(result)))
    await conn.execute(
        "UPDATE demo_fin_provider_requests SET status=%s"
        " WHERE provider_ref=%s",
        ("verified" if ok else "failed", provider_ref))
    if check_type == "bank_execute" and ok:
        await conn.execute(
            "UPDATE demo_pay_requests SET status='executing'"
            " WHERE id=%s", (rid,))
    await jobs.signal(conn, str(req["job_id"]), result, ctx)
    return rid


def _provider_result(check_type: str, ok: bool) -> dict:
    if check_type == "fx_quote":
        return ({"rate": 25400.0, "ccy": "VND→USD",
                 "method": "fx-desk (provider callback)"} if ok else
                {"error": "no rate available"})
    if check_type == "bank_preverify":
        return ({"verified": True, "method": "micro-deposit (callback)"}
                if ok else {"verified": False,
                            "reason": "account mismatch"})
    return ({"settled": True, "swift": "MT103-sent (callback)"}
            if ok else {"settled": False, "reason": "bank rejection"})


@app.post("/integrations/{prov}/callback")
async def provider_callback(request: Request, prov: str,
                            provider_ref: str = Form(...),
                            outcome: str = Form("verified")):
    """Provider webhook endpoint (app-owned)."""
    rid = None
    try:
        async with await _conn() as conn:
            rid = await handle_provider_callback(
                conn, app.state.jobs, provider_ref, outcome)
            await conn.commit()
    except KeyError:
        return RedirectResponse(
            "/?as=u-clerk&prov_err=unknown+provider_ref", status_code=303)
    except (ValueError, PermissionError) as e:
        return RedirectResponse(
            f"/?as=u-clerk&prov_err={e}", status_code=303)
    return RedirectResponse(f"/requests/{rid}?as=u-clerk", status_code=303)


# ── inbox ───────────────────────────────────────────────────────────────────

@app.get("/inbox", response_class=HTMLResponse)
async def inbox(request: Request):
    uid = request.query_params.get("as") or "u-clerk"
    async with await _conn() as conn:
        tasks = await app.state.human.my_tasks(
            conn, Actor.user(uid), roles=_ACTOR_ROLES.get(uid, []))
        for t in tasks:
            rid = (t.get("payload") or {}).get("request_id")
            if rid:
                r = await (await conn.execute(
                    "SELECT ref FROM demo_pay_requests WHERE id=%s",
                    (int(rid),))).fetchone()
                t["request_ref"] = r["ref"] if r else f"#{rid}"
    return _render(request, "inbox.html", {"tasks": tasks, "uid": uid})


@app.post("/inbox/{job_id}/complete")
async def complete_task(request: Request, job_id: str,
                        decision: str = Form(...),
                        note: str = Form("")):
    ctx = _ctx(request)
    actor = Actor.user(ctx.actor.id)
    async with await _conn() as conn:
        try:
            await app.state.human.claim(
                conn, job_id, actor,
                roles=_ACTOR_ROLES.get(ctx.actor.id, []))
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
    return _render(request, "run.html", {
        "run": detail, "steps": detail["steps"],
        "layers": _layers(detail["steps"])})


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job(request: Request, job_id: str):
    async with await _conn() as conn:
        r = await (await conn.execute(
            "SELECT * FROM keelflow_jobs WHERE id=%s", (job_id,))).fetchone()
        audit = await (await conn.execute(
            "SELECT * FROM keelflow_audit WHERE job_id=%s"
            " ORDER BY created_at", (job_id,))).fetchall()
    return _render(request, "job.html", {
        "job": r, "audit": audit,
        "can_cancel": r and r["status"] in ("pending", "claimed", "running")})


@app.post("/jobs/{job_id}/cancel")
async def job_cancel(request: Request, job_id: str):
    """Cooperative cancel — pending jobs cancel outright, running ones
    stop at the next checkpoint."""
    ctx = _ctx(request)
    try:
        await request.app.state.jobs.cancel(job_id, ctx)
    except PermissionError as e:
        return RedirectResponse(
            f"/jobs/{job_id}?as={ctx.actor.id}&err={e}", status_code=303)
    return RedirectResponse(f"/jobs/{job_id}?as={ctx.actor.id}",
                            status_code=303)
