"""T5 contract tests: worker dispatch + end-to-end claim.

Requires Postgres at PYERP_TEST_DSN.
"""
from __future__ import annotations

import asyncio
import os

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy, Queue, Worker
from pydantic import BaseModel

from pyerp.engine import worker as worker_mod
from pyerp.engine.enqueue import JobService
from pyerp.engine.registry import InMemoryRegistry
from pyerp.postgres.schema import apply_async
from pyerp.sdk import (
    Actor,
    JobContext,
    JobHandler,
    JobResult,
    JobStatus,
    JobTypeSpec,
)

DSN = os.environ.get(
    "PYERP_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/pyerp"
)
CHANCY_PREFIX = "pyerp_test_"

CALLS: list[tuple[str, dict]] = []


class Pay(BaseModel):
    invoice_id: str


class OkHandler(JobHandler):
    async def run(self, ctx, payload: Pay):
        CALLS.append(("ok", {"actor": ctx.actor.id, "corr": ctx.correlation_id}))
        return JobResult(status="done", outputs={"matched": 7})


class FailHandler(JobHandler):
    async def run(self, ctx, payload: Pay):
        raise RuntimeError("boom")


async def _conn(autocommit: bool = True) -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(DSN, autocommit=autocommit)


@pytest_asyncio.fixture
async def db():
    try:
        conn = await _conn()
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE hd_flow_jobs, hd_flow_outbox, hd_flow_audit,"
            " hd_flow_step_runs, hd_flow_workflow_runs, hd_flow_chunks,"
            " hd_flow_row_status, hd_flow_dlq CASCADE"
        )
        await conn.execute(
            f"TRUNCATE {CHANCY_PREFIX}jobs, {CHANCY_PREFIX}workers,"
            f" {CHANCY_PREFIX}queues CASCADE"
        )
        yield conn


@pytest_asyncio.fixture
async def kernel(db):
    async with Chancy(DSN, prefix=CHANCY_PREFIX) as client:
        await client.migrate()
        await client.declare(
            Queue(
                "default",
                concurrency=4,
                executor=Chancy.Executor.Async,
                eager_polling=True,
            ),
            upsert=True,
        )
        yield client


@pytest_asyncio.fixture
async def service(kernel):
    reg = InMemoryRegistry()
    reg.register_job_type(
        JobTypeSpec(type="test.ok@v1", input_schema=Pay, handler=OkHandler)
    )
    reg.register_job_type(
        JobTypeSpec(type="test.fail@v1", input_schema=Pay, handler=FailHandler)
    )
    worker_mod.configure(registry=reg, dsn=DSN)
    return JobService(reg, kernel)


async def _enqueue(service, type_name: str, payload: dict) -> str:
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            ref = await service.builder(type_name).context(
                JobContext(actor=Actor.user("u1"), correlation_id="corr-x")
            ).payload(payload).enqueue(cur)
    return ref.id


async def _wait_status(db, job_id: str, want: str, timeout: float = 15.0) -> str:
    for _ in range(int(timeout / 0.1)):
        row = await (
            await db.execute("SELECT status FROM hd_flow_jobs WHERE id=%s", (job_id,))
        ).fetchone()
        if row and row[0] == want:
            return row[0]
        await asyncio.sleep(0.1)
    return row[0] if row else "missing"


@pytest.mark.asyncio
async def test_worker_claims_and_completes(service, db, kernel):
    job_id = await _enqueue(service, "test.ok@v1", {"invoice_id": "i-1"})
    w = Worker(kernel, shutdown_timeout=5)
    await w.start()
    try:
        assert await _wait_status(db, job_id, "done") == "done"
    finally:
        await w.stop(timeout=5)
    assert CALLS == [("ok", {"actor": "u1", "corr": "corr-x"})]

    row = await (
        await db.execute(
            "SELECT status, result, attempts FROM hd_flow_jobs WHERE id=%s", (job_id,)
        )
    ).fetchone()
    assert row[0] == JobStatus.DONE.value
    assert row[1]["matched"] == 7
    assert row[2] == 1


@pytest.mark.asyncio
async def test_handler_exception_marks_failed(service, db, kernel):
    job_id = await _enqueue(service, "test.fail@v1", {"invoice_id": "i-2"})
    w = Worker(kernel, shutdown_timeout=5)
    await w.start()
    try:
        status = await _wait_status(db, job_id, "failed", timeout=20)
    finally:
        await w.stop(timeout=5)
    # kernel retries up to max_attempts=3 → eventually 'failed'
    assert status == JobStatus.FAILED.value
    row = await (
        await db.execute("SELECT error FROM hd_flow_jobs WHERE id=%s", (job_id,))
    ).fetchone()
    assert "boom" in row[0]


@pytest.mark.asyncio
async def test_dispatch_unknown_type(service, db, kernel):
    # craft a job row whose type is unregistered, then kernel-dispatch it
    job_id = await _enqueue(service, "test.ok@v1", {"invoice_id": "i-3"})
    await db.execute(
        "UPDATE hd_flow_jobs SET type='test.unknown@v1' WHERE id=%s", (job_id,)
    )
    w = Worker(kernel, shutdown_timeout=5)
    await w.start()
    try:
        status = await _wait_status(db, job_id, "failed", timeout=15)
    finally:
        await w.stop(timeout=5)
    assert status == JobStatus.FAILED.value
