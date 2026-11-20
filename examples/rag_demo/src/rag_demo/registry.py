"""keelflow demo 4 — employee helpdesk RAG.

Two workflows:

- `rag.index@v1` — split pending docs → bulk-embed chunks (bulk.py
  chunked processing; flaky embedder chaos knob).
- `rag.answer@v1` — retrieve → generate (flaky/slow LLM chaos knobs →
  retry backoff, timeout_seconds, DLQ) → score → human.review gate when
  confidence < THRESHOLD → publish.

Everything is deterministic and local — no real embeddings/LLM. The
"flakiness" knobs live in demo_rag_settings and model the real-world
behaviour durable workflows exist for.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import random
import re

import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel

from keelflow.engine.bulk import BulkJobHandler, BulkJobPayload
from keelflow.engine.bulk import register as register_bulk
from keelflow.engine.registry import InMemoryRegistry
from keelflow.sdk import (
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
    StepSpec,
    TaskUiSpec,
    WorkflowDefinition,
)

DSN_ENV = "KEELFLOW_DSN"
REVIEW_THRESHOLD = 0.55
_EMBED_DIM = 16

PERMISSIONS = {
    "u-ask": {"question:ask"},
    "u-ops": {"question:ask", "doc:index", "task:review", "dlq:replay"},
    "u-mgr": {"question:ask", "doc:index", "task:review", "dlq:replay"},
}
ROLES = {
    "u-ask": ["requester"],
    "u-ops": ["requester", "reviewer"],
    "u-mgr": ["requester", "reviewer", "manager"],
}


# ── deterministic toy "embedder" / "llm" ──────────────────────────────────

_WORD_RE = re.compile(r"[a-z0-9]+")


def embed(text: str) -> list[float]:
    """Bag-of-words hash embedding — deterministic, no model."""
    vec = [0.0] * _EMBED_DIM
    for w in _WORD_RE.findall(text.lower()):
        h = int(hashlib.md5(w.encode()).hexdigest(), 16) % _EMBED_DIM
        vec[h] += 1.0
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [round(v / norm, 4) for v in vec]


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


async def _setting(conn, key: str) -> bool:
    async with conn.cursor(row_factory=dict_row) as cur:
        row = await (await cur.execute(
            "SELECT value FROM demo_rag_settings WHERE key=%s", (key,))
        ).fetchone()
    return bool(row and row["value"])


async def _llm_generate(conn, question: str, ctxs: list[dict]) -> dict:
    """Fake 'LLM': flaky/slow knobs from demo_rag_settings, templated
    answer built from retrieved chunks. Raises to model API errors."""
    if await _setting(conn, "flaky_llm") and random.random() < 0.5:
        # model transient API failure ~half the time → engine retry
        # backoff, without dead-locking the demo
        raise RuntimeError("LLM API: 503 overloaded (simulated)")
    if await _setting(conn, "slow_llm"):
        await asyncio.sleep(30)          # longer than timeout_seconds
    if not ctxs or await _setting(conn, "weak_retrieval"):
        return {"answer": "I don't have enough context to answer that.",
                "confidence": 0.2}
    parts = [c["text"].strip()[:180] for c in ctxs]
    sources = ", ".join(str(c["doc_id"]) for c in ctxs)
    answer = (parts[0] + (f" …also: {parts[1]}" if len(parts) > 1 else "")
              + f" [sources: doc {sources}]")
    score = min(0.95, 0.5 + 0.15 * len(ctxs)
                + 0.1 * (len(set(sources.split(', '))) > 1))
    return {"answer": answer, "confidence": round(score, 2)}


def split_chunks(text: str, size: int = 60) -> list[str]:
    words = text.split()
    return [" ".join(words[i:i + size]) for i in range(0, len(words), size)]


# ── payloads ────────────────────────────────────────────────────────────────

class DocId(BaseModel):
    doc_id: int | None = None          # None → index all pending


class IndexBulk(BulkJobPayload):
    pass


class QId(BaseModel):
    question_id: int


class GenOut(BaseModel):
    question_id: int


class ScoreIn(BaseModel):
    question_id: int
    confidence: float = 0.0


class PubIn(BaseModel):
    question_id: int


def _dict_cur(tx):
    return (tx.connection.cursor(row_factory=dict_row)
            if hasattr(tx, "connection") else tx)


# ── indexing handlers ───────────────────────────────────────────────────────

class SplitH(JobHandler):
    """Stage doc chunks into demo_rag_chunks (pending)."""

    async def run(self, ctx: JobContext, payload: DocId) -> JobResult:
        cur = _dict_cur(ctx.tx)
        where = "status='pending'" if payload.doc_id is None else "id=%s"
        args = () if payload.doc_id is None else (payload.doc_id,)
        await cur.execute(
            f"SELECT id, title, body FROM demo_rag_docs WHERE {where}"
            " ORDER BY id", args)
        docs = await cur.fetchall()
        n = 0
        for d in docs:
            for i, chunk in enumerate(split_chunks(d["body"])):
                await cur.execute(
                    "INSERT INTO demo_rag_chunks (doc_id, idx, text)"
                    " VALUES (%s,%s,%s)"
                    " ON CONFLICT (doc_id, idx) DO UPDATE SET"
                    " text=EXCLUDED.text, status='pending'",
                    (d["id"], i, chunk))
                n += 1
        return JobResult(status="done",
                         outputs={"chunks": n, "docs": len(docs)})


class EmbedRowsH(BulkJobHandler):
    """Bulk pass — embed staged chunks. atomic='row': a bad chunk lands in
    the row error queue without blocking the rest."""

    async def run(self, ctx: JobContext, payload: IndexBulk) -> JobResult:
        cur = _dict_cur(ctx.tx)
        job_id = ctx.extra["job_id"]
        await cur.execute(
            "SELECT id, doc_id, text FROM demo_rag_chunks"
            " WHERE status='pending' ORDER BY id")
        rows = await cur.fetchall()
        batch = [
            (job_id, f"chunk-{r['id']}", json.dumps(
                {"chunk_id": r["id"], "doc_id": r["doc_id"],
                 "text": r["text"]}))
            for r in rows]
        await cur.executemany(
            "INSERT INTO keelflow_row_status (job_id, row_key, status, data)"
            " VALUES (%s, %s, 'pending', %s)", batch)
        return await super().run(ctx, payload)

    async def process_row(self, ctx, row, tx):
        d = row["data"]
        dsn = os.environ[DSN_ENV]
        async with await psycopg.AsyncConnection.connect(
                dsn, row_factory=dict_row) as conn:
            if await _setting(conn, "flaky_embed") and random.random() < 0.25:
                raise RuntimeError("embed API: 429 rate limited (simulated)")
        await tx.execute(
            "UPDATE demo_rag_chunks SET embedding=%s, status='done',"
            " error=NULL WHERE id=%s",
            (embed(d["text"]), d["chunk_id"]))
        return {"chunk_id": d["chunk_id"]}


class MarkDocsH(JobHandler):
    """Mark docs indexed after the embed pass."""

    async def run(self, ctx: JobContext, payload: DocId) -> JobResult:
        cur = _dict_cur(ctx.tx)
        await cur.execute(
            "UPDATE demo_rag_docs SET status='indexed'"
            " WHERE status='pending'")
        return JobResult(status="done")


# ── answering handlers ──────────────────────────────────────────────────────

class RetrieveH(JobHandler):
    """Cosine top-3 over demo_rag_chunks.embedding (toy ANN)."""

    async def run(self, ctx: JobContext, payload: QId) -> JobResult:
        cur = _dict_cur(ctx.tx)
        await cur.execute(
            "SELECT q FROM demo_rag_questions WHERE id=%s",
            (payload.question_id,))
        q = (await cur.fetchone())["q"]
        await cur.execute(
            "SELECT id, doc_id, text, embedding FROM demo_rag_chunks"
            " WHERE status='done'")
        chunks = await cur.fetchall()
        qv = embed(q)
        if await _setting(cur.connection, "weak_retrieval"):
            qv = [0.0] * _EMBED_DIM
        ranked = sorted(
            ((cosine(qv, c["embedding"]), c) for c in chunks),
            key=lambda t: t[0], reverse=True)[:3]
        top = [{"chunk_id": c["id"], "doc_id": c["doc_id"],
                "text": c["text"], "score": round(s, 3)}
               for s, c in ranked if s > 0.0]
        return JobResult(status="done",
                         outputs={"question_id": payload.question_id,
                                  "question": q, "chunks": top})


class GenerateH(JobHandler):
    """'LLM call' — flaky/slow knobs; wrapped by timeout_seconds and the
    RetryPolicy backoff in the job spec."""

    async def run(self, ctx: JobContext, payload: QId) -> JobResult:
        cur = _dict_cur(ctx.tx)
        # chunks come via run inputs — fetch the run's retrieve outputs
        await cur.execute(
            "SELECT sr.outputs FROM keelflow_step_runs sr"
            " JOIN keelflow_workflow_runs r ON r.id=sr.run_id"
            " WHERE sr.step_id='retrieve'"
            " AND r.inputs->>'question_id'=%s"
            " ORDER BY sr.created_at DESC LIMIT 1",
            (str(payload.question_id),))
        row = await cur.fetchone()
        chunks = (row["outputs"] or {}).get("chunks", []) if row else []
        q_row = await (await cur.execute(
            "SELECT q FROM demo_rag_questions WHERE id=%s",
            (payload.question_id,))).fetchone()
        out = await _llm_generate(cur.connection, q_row["q"], chunks)
        await cur.execute(
            "UPDATE demo_rag_questions SET answer=%s, confidence=%s"
            " WHERE id=%s",
            (out["answer"], out["confidence"], payload.question_id))
        return JobResult(status="done",
                         outputs={"question_id": payload.question_id,
                                  "confidence": out["confidence"],
                                  "answer": out["answer"]})


class ScoreH(JobHandler):
    """Decide: confidence ≥ threshold → publish; else human.review."""

    async def run(self, ctx: JobContext, payload: ScoreIn) -> JobResult:
        ok = payload.confidence >= REVIEW_THRESHOLD
        return JobResult(status="done", outputs={"needs_review": not ok,
                                                 "confidence": payload.confidence})


class ReviewCompleteH(JobHandler):
    """Apply the human.review result to demo_rag_questions. Runs inside
    the human-complete TX via on_job_finished outputs."""

    async def run(self, ctx: JobContext, payload: PubIn) -> JobResult:
        cur = _dict_cur(ctx.tx)
        await cur.execute(
            "SELECT outputs FROM keelflow_step_runs sr"
            " JOIN keelflow_workflow_runs r ON r.id=sr.run_id"
            " WHERE sr.step_id='review' AND r.inputs->>'question_id'=%s"
            " ORDER BY sr.created_at DESC LIMIT 1",
            (str(payload.question_id),))
        row = await cur.fetchone()
        outputs = (row["outputs"] or {}) if row else {}
        decision = outputs.get("decision", "approve")
        reviewer = outputs.get("by", "?")   # caller passes the human's id
        await cur.execute(
            "UPDATE demo_rag_questions SET status=%s, reviewed_by=%s"
            " WHERE id=%s",
            ("reviewed" if decision != "reject" else "rejected",
             reviewer, payload.question_id))
        return JobResult(status="done", outputs={"decision": decision})


class PublishH(JobHandler):
    async def run(self, ctx: JobContext, payload: PubIn) -> JobResult:
        cur = _dict_cur(ctx.tx)
        await cur.execute(
            "UPDATE demo_rag_questions SET status='answered' WHERE id=%s",
            (payload.question_id,))
        return JobResult(status="done")


class _NoopH(JobHandler):
    async def run(self, ctx, payload) -> JobResult:  # pragma: no cover
        return JobResult(status="done")


# ── registry ────────────────────────────────────────────────────────────────
def build_registry() -> InMemoryRegistry:
    r = InMemoryRegistry()
    register_bulk(r)

    r.register_job_type(JobTypeSpec(
        type="rag.split@v1", input_schema=DocId, handler=SplitH))
    r.register_job_type(JobTypeSpec(
        type="rag.embed@v1", input_schema=IndexBulk, handler=EmbedRowsH,
        retry=RetryPolicy(max_attempts=3, base_seconds=2)))
    r.register_job_type(JobTypeSpec(
        type="rag.mark_docs@v1", input_schema=DocId, handler=MarkDocsH))
    r.register_job_type(JobTypeSpec(
        type="rag.retrieve@v1", input_schema=QId, handler=RetrieveH))
    r.register_job_type(JobTypeSpec(
        type="rag.generate@v1", input_schema=QId, handler=GenerateH,
        retry=RetryPolicy(max_attempts=4, base_seconds=2),
        timeout_seconds=10))
    r.register_job_type(JobTypeSpec(
        type="rag.score@v1", input_schema=ScoreIn, handler=ScoreH))
    r.register_job_type(JobTypeSpec(
        type="rag.publish@v1", input_schema=PubIn, handler=PublishH))
    r.register_job_type(JobTypeSpec(
        type="rag.review_done@v1", input_schema=PubIn,
        handler=ReviewCompleteH))
    r.register_job_type(JobTypeSpec(
        type="human.review@v1", input_schema=QId, handler=_NoopH,
        is_human=True,
        ui=TaskUiSpec(assignee_role="reviewer", due_in_seconds=300)))

    r.register_workflow(WorkflowDefinition(
        name="rag.index@v1",
        steps=(
            StepSpec("split", "rag.split@v1",
                     input={"doc_id": "${{ inputs.doc_id }}"}),
            StepSpec("embed", "rag.embed@v1", needs=("split",),
                     input={"atomic": "row", "chunk_size": 10}),
            StepSpec("mark_docs", "rag.mark_docs@v1", needs=("embed",),
                     input={"doc_id": "${{ inputs.doc_id }}"}),
        ),
    ))
    q = {"question_id": "${{ inputs.question_id }}"}
    r.register_workflow(WorkflowDefinition(
        name="rag.answer@v1",
        steps=(
            StepSpec("retrieve", "rag.retrieve@v1", input=q),
            StepSpec("generate", "rag.generate@v1", needs=("retrieve",),
                     input=q),
            StepSpec("score", "rag.score@v1", needs=("generate",),
                     input={"question_id": "${{ inputs.question_id }}",
                            "confidence": "${{ steps.generate.outputs.confidence }}"}),
            StepSpec("review", "human.review@v1", needs=("score",),
                     if_expr="${{ steps.score.outputs.needs_review }} == True",
                     input=q),
            StepSpec("publish", "rag.publish@v1", needs=("score",),
                     if_expr="not ${{ steps.score.outputs.needs_review }}",
                     input=q),
            StepSpec("review_done", "rag.review_done@v1", needs=("review",),
                     if_expr="${{ steps.score.outputs.needs_review }} == True",
                     input=q),
        ),
    ))
    return r
