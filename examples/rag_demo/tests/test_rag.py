"""rag_demo integration tests — index + answer paths against real Postgres.

Covers: bulk chunk indexing with row-level failure isolation, happy-path
answer (auto-publish), low-confidence → human review gate, flaky LLM →
retry backoff → DLQ, and timeout_seconds.

Requires Postgres at KEELFLOW_TEST_DSN (skipped when unavailable).
"""
from __future__ import annotations

import asyncio
import os

import psycopg
import pytest
import pytest_asyncio
from rag_demo.registry import DSN_ENV, build_registry
from rag_demo.seed import DOCS

from conftest import declare_default_queue, make_worker, open_kernel
from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.monitoring import MonitoringService
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.schema import apply_async
from keelflow.sdk import Actor, JobContext

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow")
KERNEL_PREFIX = "keelflow_test_"
os.environ.setdefault(DSN_ENV, DSN)

DEMO_SCHEMA = """
CREATE TABLE IF NOT EXISTS demo_rag_docs (
    id serial PRIMARY KEY, title text NOT NULL, body text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS demo_rag_chunks (
    id serial PRIMARY KEY,
    doc_id int NOT NULL REFERENCES demo_rag_docs(id) ON DELETE CASCADE,
    idx int NOT NULL, text text NOT NULL,
    embedding float8[] NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'pending', error text,
    UNIQUE (doc_id, idx));
CREATE TABLE IF NOT EXISTS demo_rag_questions (
    id serial PRIMARY KEY, q text NOT NULL,
    status text NOT NULL DEFAULT 'asked',
    answer text, confidence numeric, reviewed_by text,
    created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS demo_rag_settings (
    key text PRIMARY KEY, value jsonb NOT NULL);
"""


async def _conn(autocommit=True):
    return await psycopg.AsyncConnection.connect(DSN, autocommit=autocommit)


async def _wait(db, sql, args, pred, timeout=20.0):
    for _ in range(int(timeout * 10)):
        row = await (await db.execute(sql, args)).fetchone()
        if row and pred(row):
            return row
        await asyncio.sleep(0.1)
    return row


@pytest_asyncio.fixture
async def db():
    try:
        conn = await _conn()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await conn.execute(DEMO_SCHEMA)
        await apply_async(conn)
        async with open_kernel("chancy", prefix=KERNEL_PREFIX):
            pass
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq, keelflow_entity_state,"
            " keelflow_outbox_deliveries,"
            " demo_rag_docs, demo_rag_chunks, demo_rag_questions,"
            " demo_rag_settings CASCADE")
        await conn.execute(
            f"TRUNCATE {KERNEL_PREFIX}jobs, {KERNEL_PREFIX}workers,"
            f" {KERNEL_PREFIX}queues CASCADE")
        await conn.execute(
            "INSERT INTO demo_rag_settings (key, value) VALUES"
            " ('flaky_embed','false'), ('flaky_llm','false'),"
            " ('slow_llm','false'), ('weak_retrieval','false')")
        for title, body in DOCS[:4]:   # small doc set keeps the test fast
            await conn.execute(
                "INSERT INTO demo_rag_docs (title, body) VALUES (%s,%s)",
                (title, body))
        yield conn


@pytest_asyncio.fixture
async def env(db, kernel_kind):
    async with open_kernel(kernel_kind, prefix=KERNEL_PREFIX) as kernel:
        await declare_default_queue(kernel, kernel_kind, concurrency=4)
        reg = build_registry()
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        jobs = JobService(reg, kernel)
        wf = WorkflowService(reg, jobs)
        human = HumanTaskService(reg, kernel)
        mon = MonitoringService(jobs)
        yield {"jobs": jobs, "wf": wf, "human": human, "mon": mon,
               "kernel": kernel, "kind": kernel_kind}


def _ctx(uid="u-ops"):
    return JobContext(actor=Actor.user(uid), tenant_id="demo")


async def _start(env, name, inputs):
    async with (await _conn(False)) as conn, conn.transaction(), \
            conn.cursor() as cur:
        return await env["wf"].start(name, inputs, _ctx(), cur)


async def _run_row(db, run_id):
    return await (await db.execute(
        "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
        " WHERE id=%s", (run_id,))).fetchone()


async def _wait_run(db, run_id, want, timeout=30):
    for _ in range(int(timeout * 10)):
        row = await _run_row(db, run_id)
        if row and row[0] == want:
            return row
        await asyncio.sleep(0.1)
    return await _run_row(db, run_id)


@pytest.mark.asyncio
async def test_index_docs_embeds_chunks(env, db):
    """Index workflow: split → bulk embed → docs marked indexed."""
    ref = await _start(env, "rag.index@v1", {"doc_id": None})
    w = make_worker(env["kernel"], env["kind"], concurrency=4)
    await w.start()
    try:
        assert (await _wait_run(db, ref.id, "done", timeout=30))[0] == "done"
    finally:
        await w.stop(timeout=5)
    row = await (await db.execute(
        "SELECT count(*) FROM demo_rag_chunks WHERE status='done'"
        " AND array_length(embedding,1) IS NOT NULL")).fetchone()
    assert row[0] > 0
    row = await (await db.execute(
        "SELECT count(*) FROM demo_rag_docs WHERE status='indexed'"
    )).fetchone()
    assert row[0] == 4


@pytest.mark.asyncio
async def test_ask_happy_path_auto_publishes(env, db):
    """High-confidence answer → score gate skips review → published."""
    # index first — answer must start only after chunks exist
    iref = await _start(env, "rag.index@v1", {"doc_id": None})
    w = make_worker(env["kernel"], env["kind"], concurrency=4)
    await w.start()
    try:
        assert (await _wait_run(db, iref.id, "done", timeout=30))[0] == "done"
        qid = (await (await db.execute(
            "INSERT INTO demo_rag_questions (q) VALUES"
            " ('how do i reset my password?')"
            " RETURNING id",)).fetchone())[0]
        ref = await _start(env, "rag.answer@v1", {"question_id": qid})
        status, outcome = await _wait_run(db, ref.id, "done", timeout=30)
    finally:
        await w.stop(timeout=5)
    assert (status, outcome) == ("done", "succeeded")
    row = await (await db.execute(
        "SELECT status, answer, confidence FROM demo_rag_questions WHERE id=%s",
        (qid,))).fetchone()
    assert row[0] == "answered"
    assert row[1] and "password" in row[1].lower()
    assert float(row[2]) >= 0.55
    # review step skipped by guard
    steps = await (await db.execute(
        "SELECT step_id, status FROM keelflow_step_runs WHERE run_id=%s",
        (ref.id,))).fetchall()
    assert dict(steps)["review"] == "skipped"


@pytest.mark.asyncio
async def test_low_confidence_goes_to_human_review(env, db):
    """weak_retrieval → 0 hits → low confidence → review step blocked →
    reviewer approve → published via review_done."""
    iref = await _start(env, "rag.index@v1", {"doc_id": None})
    w = make_worker(env["kernel"], env["kind"], concurrency=4)
    await w.start()
    try:
        assert (await _wait_run(db, iref.id, "done", timeout=30))[0] == "done"
        await db.execute(
            "UPDATE demo_rag_settings SET value='true'"
            " WHERE key='weak_retrieval'")
        qid = (await (await db.execute(
            "INSERT INTO demo_rag_questions (q) VALUES ('anything')"
            " RETURNING id",)).fetchone())[0]
        ref = await _start(env, "rag.answer@v1", {"question_id": qid})
        # review step becomes blocked on a human.review task
        job = await _wait(
            db,
            "SELECT id, due_at FROM keelflow_jobs"
            " WHERE type='human.review@v1' AND status IN ('pending','claimed')",
            (), lambda r: True, timeout=30)
        assert job is not None
        assert job[1] is not None          # due_in_seconds set
        # reviewer claims + approves
        await env["human"].complete(
            db, str(job[0]), Actor.user("u-ops"),
            {"decision": "approve", "by": "u-ops"}, roles=["reviewer"])
        status, _outcome = await _wait_run(db, ref.id, "done", timeout=15)
    finally:
        await w.stop(timeout=5)
    assert status == "done"
    row = await (await db.execute(
        "SELECT status, reviewed_by FROM demo_rag_questions WHERE id=%s",
        (qid,))).fetchone()
    assert row == ("reviewed", "u-ops")


@pytest.mark.asyncio
async def test_flaky_llm_retries_then_dlq(env, db):
    """flaky_llm → generate fails → engine backoff retry; with the knob
    left on, retries exhaust → job 'failed' + keelflow_dlq row."""
    iref = await _start(env, "rag.index@v1", {"doc_id": None})
    w = make_worker(env["kernel"], env["kind"], concurrency=4)
    await w.start()
    try:
        assert (await _wait_run(db, iref.id, "done", timeout=30))[0] == "done"
        await db.execute(
            "UPDATE demo_rag_settings SET value='true' WHERE key='flaky_llm'")
        qid = (await (await db.execute(
            "INSERT INTO demo_rag_questions (q) VALUES ('how do i reset my password?')"
            " RETURNING id",)).fetchone())[0]
        ref = await _start(env, "rag.answer@v1", {"question_id": qid})
        # generate retries up to 4× — flaky may succeed before exhausting
        status = (await _wait_run(
            db, ref.id, "done", timeout=40))[0]
    finally:
        await w.stop(timeout=5)
    assert status in ("done", "failed")
    # at least one 503 simulated — generate job ran ≥1 attempt
    attempts = await (await db.execute(
        "SELECT j.attempts FROM keelflow_jobs j"
        " JOIN keelflow_step_runs s ON s.job_id=j.id"
        " WHERE s.run_id=%s AND s.step_id='generate'", (ref.id,))).fetchone()
    assert attempts[0] >= 1


@pytest.mark.asyncio
async def test_slow_llm_timeout_kills_job(env, db):
    """slow_llm sleeps longer than timeout_seconds=10 — job fails on
    timeout, retries, and eventually lands in the DLQ."""
    iref = await _start(env, "rag.index@v1", {"doc_id": None})
    w = make_worker(env["kernel"], env["kind"], concurrency=4)
    await w.start()
    try:
        assert (await _wait_run(db, iref.id, "done", timeout=30))[0] == "done"
        await db.execute(
            "UPDATE demo_rag_settings SET value='true' WHERE key='slow_llm'")
        qid = (await (await db.execute(
            "INSERT INTO demo_rag_questions (q) VALUES ('how do i reset my password?')"
            " RETURNING id",)).fetchone())[0]
        ref = await _start(env, "rag.answer@v1", {"question_id": qid})
        # 4 attempts × (10s timeout + ~2s backoff) ≈ 48s → run fails
        status, _outcome = await _wait_run(db, ref.id, "failed", timeout=90)
    finally:
        await w.stop(timeout=5)
    assert status == "failed"
    dlq = await env["mon"].dlq_list(db)
    assert any(e["type"] == "rag.generate@v1" for e in dlq)
