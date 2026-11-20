"""Demo 4 UI — employee helpdesk RAG. Index docs → ask questions →
retrieve → generate → score → human review gate → publish. Chaos knobs
(flaky/slow LLM, flaky embed, weak retrieval) demo retries, timeouts,
DLQ.

    uv run --package keelflow-demo-rag \
        uvicorn rag_demo.app:app --port 8003

Demo only: no auth, binds localhost, synthetic data. Refresh manually.
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
from keelflow.engine.monitoring import MonitoringService
from keelflow.engine.workflow import WorkflowService
from keelflow.sdk import Actor, JobContext

from .registry import PERMISSIONS, REVIEW_THRESHOLD, ROLES, build_registry

DSN = os.environ.get("KEELFLOW_DSN",
                     "postgresql://postgres:postgres@localhost:5544/keelflow")
TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")

_ACTORS = ("u-ask", "u-ops", "u-mgr")
_ACTOR_NAMES = {
    "u-ask": "Ava — Requester",
    "u-ops": "Oscar — Ops/Reviewer",
    "u-mgr": "Maya — Manager (all)",
}
_SETTINGS = ("flaky_embed", "flaky_llm", "slow_llm", "weak_retrieval")


def _ctx(request: Request) -> JobContext:
    uid = request.query_params.get("as") or "u-ask"
    if uid not in _ACTORS:
        uid = "u-ask"
    return JobContext(actor=Actor.user(uid), tenant_id="demo",
                      permissions=PERMISSIONS.get(uid, set()))


@asynccontextmanager
async def _lifespan(app: FastAPI):
    registry = build_registry()
    kernel = chancy.Chancy(DSN, prefix="keelflow_kernel_rag_")
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
    app.state.mon = MonitoringService(jobs, registry=registry)
    async with kernel:
        yield


app = FastAPI(title="keelflow demo · employee helpdesk", lifespan=_lifespan)
templates = Jinja2Templates(directory=TEMPLATES)


async def _conn():
    return await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row)


def _nav(request: Request) -> dict:
    uid = request.query_params.get("as") or "u-ask"
    return {"as": uid, "actors": sorted(_ACTORS),
            "names": _ACTOR_NAMES,
            "perms": sorted(PERMISSIONS.get(uid, set()))}


async def _settings(conn) -> dict[str, bool]:
    rows = await (await conn.execute(
        "SELECT key, value FROM demo_rag_settings")).fetchall()
    return {r["key"]: bool(r["value"]) for r in rows}


def _back(request: Request, path: str) -> RedirectResponse:
    me = request.query_params.get("as") or "u-ask"
    sep = "&" if "?" in path else "?"
    return RedirectResponse(f"{path}{sep}as={me}", status_code=303)


# ── home: questions ─────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    async with await _conn() as conn:
        questions = await (await conn.execute(
            "SELECT * FROM demo_rag_questions ORDER BY id DESC LIMIT 50"
        )).fetchall()
        docs = await (await conn.execute(
            "SELECT id, title, status FROM demo_rag_docs ORDER BY id"
        )).fetchall()
        settings = await _settings(conn)
        tasks = await (await conn.execute(
            "SELECT count(*) AS n FROM keelflow_jobs"
            " WHERE type LIKE 'human.%'"
            " AND status IN ('pending','claimed')")).fetchone()
        runs = await (await conn.execute(
            "SELECT DISTINCT ON ((inputs->>'question_id')::int)"
            "   inputs->>'question_id' AS qid, id, status, terminal_outcome"
            " FROM keelflow_workflow_runs WHERE workflow='rag.answer@v1'"
            " ORDER BY (inputs->>'question_id')::int, created_at DESC"
        )).fetchall()
    return templates.TemplateResponse(request, "home.html", {
        "nav": _nav(request), "questions": questions, "docs": docs,
        "settings": settings, "open_tasks": tasks["n"],
        "runs": {int(r["qid"]): r for r in runs},
        "threshold": REVIEW_THRESHOLD})


@app.post("/ask")
async def ask(request: Request, q: str = Form(...)):
    async with (
        await _conn() as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        await cur.execute(
            "INSERT INTO demo_rag_questions (q) VALUES (%s) RETURNING id",
            (q,))
        qid = (await cur.fetchone())["id"]
        await app.state.wf.start(
            "rag.answer@v1", {"question_id": qid}, _ctx(request), cur)
    return _back(request, "/")


@app.post("/index")
async def index(request: Request):
    async with await _conn() as conn, conn.transaction(), conn.cursor() as cur:
        await app.state.wf.start(
            "rag.index@v1", {"doc_id": None}, _ctx(request), cur)
    return _back(request, "/docs")


@app.post("/knob/{key}")
async def knob(request: Request, key: str):
    if key not in _SETTINGS:
        return RedirectResponse("/?as=u-ask", status_code=303)
    async with await _conn() as conn:
        await conn.execute(
            "UPDATE demo_rag_settings"
            " SET value = to_jsonb(NOT (value::boolean)) WHERE key=%s",
            (key,))
    return _back(request, "/")


# ── docs ────────────────────────────────────────────────────────────────────

@app.get("/docs", response_class=HTMLResponse)
async def docs(request: Request):
    async with await _conn() as conn:
        docs_ = await (await conn.execute(
            "SELECT * FROM demo_rag_docs ORDER BY id")).fetchall()
        n_chunks = await (await conn.execute(
            "SELECT count(*) AS n FROM demo_rag_chunks WHERE status='done'"
        )).fetchone()
        runs = await (await conn.execute(
            "SELECT id, status, terminal_outcome, created_at"
            " FROM keelflow_workflow_runs WHERE workflow='rag.index@v1'"
            " ORDER BY created_at DESC LIMIT 5")).fetchall()
        settings = await _settings(conn)
    return templates.TemplateResponse(request, "docs.html", {
        "nav": _nav(request), "docs": docs_, "runs": runs,
        "n_chunks": n_chunks["n"], "settings": settings})


@app.post("/docs/add")
async def add_doc(request: Request, title: str = Form(...),
                  body: str = Form(...)):
    async with await _conn() as conn:
        await conn.execute(
            "INSERT INTO demo_rag_docs (title, body) VALUES (%s, %s)",
            (title, body))
    return _back(request, "/docs")


# ── question detail ─────────────────────────────────────────────────────────

@app.get("/q/{qid}", response_class=HTMLResponse)
async def question(request: Request, qid: int):
    async with await _conn() as conn:
        q = await (await conn.execute(
            "SELECT * FROM demo_rag_questions WHERE id=%s", (qid,))).fetchone()
        if q is None:
            return RedirectResponse("/?as=u-ask", status_code=303)
        run = await (await conn.execute(
            "SELECT id FROM keelflow_workflow_runs"
            " WHERE workflow='rag.answer@v1'"
            " AND inputs->>'question_id'=%s"
            " ORDER BY created_at DESC LIMIT 1", (str(qid),))).fetchone()
        detail = (await app.state.wf.get_run(conn, str(run["id"]))
                  if run else None)
        chunks = []
        if detail:
            for s in detail["steps"]:
                if s["step_id"] == "retrieve":
                    chunks = (s["outputs"] or {}).get("chunks", [])
        docs_map = await (await conn.execute(
            "SELECT id, title FROM demo_rag_docs")).fetchall()
    wf_diagram = None
    blocked = []
    if detail:
        blocked = [st for st in detail["steps"]
                   if st["status"] in ("blocked", "claimed")
                   and st["type"].startswith("human.")]
        meta = {}
        for st in detail["steps"]:
            if st["type"].startswith("human."):
                spec = app.state.registry.job_type(st["type"])
                meta[st["type"]] = {
                    "assignee": getattr(spec.ui, "assignee_role", None)
                    if spec.ui else None}
        from keelflow.engine.diagram import workflow_svg
        wf_diagram = workflow_svg(
            app.state.registry.workflow(detail["workflow"]),
            step_status={st["step_id"]: st["status"]
                         for st in detail["steps"]},
            job_type_meta=meta)
    return templates.TemplateResponse(request, "question.html", {
        "nav": _nav(request), "q": q, "run": detail, "chunks": chunks,
        "doc_titles": {d["id"]: d["title"] for d in docs_map},
        "wf_diagram": wf_diagram, "blocked": blocked,
        "threshold": REVIEW_THRESHOLD})


# ── inbox (human.review tasks) ──────────────────────────────────────────────

@app.get("/inbox", response_class=HTMLResponse)
async def inbox(request: Request):
    uid = request.query_params.get("as") or "u-ask"
    async with await _conn() as conn:
        tasks = await app.state.human.my_tasks(
            conn, Actor.user(uid), roles=ROLES.get(uid, []))
        # attach question text for display
        for t_ in tasks:
            qid = (t_["payload"] or {}).get("question_id")
            row = await (await conn.execute(
                "SELECT q, answer, confidence FROM demo_rag_questions"
                " WHERE id=%s", (qid or -1,))).fetchone()
            t_["question"] = row["q"] if row else "?"
            t_["draft"] = row["answer"] if row else ""
            t_["confidence"] = row["confidence"] if row else None
    return templates.TemplateResponse(request, "inbox.html", {
        "nav": _nav(request), "tasks": tasks})


@app.post("/inbox/{job_id}/complete")
async def complete(request: Request, job_id: str,
                   decision: str = Form(...),
                   answer: str = Form("")):
    uid = request.query_params.get("as") or "u-ask"
    err = None
    async with await _conn() as conn:
        try:
            if answer.strip():
                qid_row = await (await conn.execute(
                    "SELECT payload->>'question_id' AS qid FROM keelflow_jobs"
                    " WHERE id=%s", (job_id,))).fetchone()
                if qid_row and qid_row["qid"]:
                    await conn.execute(
                        "UPDATE demo_rag_questions SET answer=%s"
                        " WHERE id=%s", (answer, int(qid_row["qid"])))
            await app.state.human.complete(
                conn, job_id, Actor.user(uid),
                {"decision": decision, "answer": answer, "by": uid},
                roles=ROLES.get(uid, []))
        except (PermissionError, KeyError, ValueError) as e:
            err = str(e)
    return _back(request, "/inbox" + (f"?err={err}" if err else ""))


# ── dlq ─────────────────────────────────────────────────────────────────────

@app.get("/dlq", response_class=HTMLResponse)
async def dlq(request: Request):
    async with await _conn() as conn:
        entries = await app.state.mon.dlq_list(conn)
    return templates.TemplateResponse(request, "dlq.html", {
        "nav": _nav(request), "entries": entries})


@app.post("/dlq/{dlq_id}/replay")
async def dlq_replay(request: Request, dlq_id: str):
    async with await _conn() as conn:
        try:
            await app.state.mon.dlq_replay(conn, dlq_id, _ctx(request))
        except (KeyError, RuntimeError) as e:
            return _back(request, f"/dlq?err={e}")
    return _back(request, "/dlq")


# ── run / job detail ────────────────────────────────────────────────────────

@app.get("/runs/{run_id}", response_class=HTMLResponse)
async def run(request: Request, run_id: str):
    async with await _conn() as conn:
        detail = await app.state.wf.get_run(conn, run_id)
    if detail is None:
        return RedirectResponse("/?as=u-ask", status_code=303)
    return templates.TemplateResponse(request, "run.html", {
        "nav": _nav(request), "run": detail})


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job(request: Request, job_id: str):
    async with await _conn() as conn:
        r = await (await conn.execute(
            "SELECT * FROM keelflow_jobs WHERE id=%s", (job_id,))).fetchone()
    if r is None:
        return RedirectResponse("/?as=u-ask", status_code=303)
    return templates.TemplateResponse(request, "job.html", {
        "nav": _nav(request), "job": r})
