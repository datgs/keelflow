"""T4 contract tests: transactional enqueue.

Requires Postgres at KEELFLOW_TEST_DSN (default: spike container DSN).
Run: `docker start keelflow-postgres` then `uv run pytest tests/test_enqueue.py`.
"""
from __future__ import annotations

import os
import uuid

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy
from pydantic import BaseModel

from keelflow.engine.enqueue import EnqueueError, JobService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import (
    Actor,
    JobContext,
    JobHandler,
    JobResult,
    JobStatus,
    JobTypeSpec,
)

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
CHANCY_PREFIX = "keelflow_test_"


class Payload(BaseModel):
    invoice_id: str


class _Noop(JobHandler):
    async def run(self, ctx, payload):
        return JobResult(status="done")


def _ctx() -> JobContext:
    return JobContext(
        actor=Actor.user("u_test"),
        correlation_id="corr-1",
        permissions=frozenset({"invoices.match"}),
        extra={"cost_center": "cc1"},
    )


async def _connect(autocommit: bool = False) -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(DSN, autocommit=autocommit)


@pytest_asyncio.fixture
async def db():
    """Fresh keelflow_* + chancy schema per test (autocommit admin conn)."""
    try:
        conn = await _connect(autocommit=True)
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq CASCADE"
        )
        yield conn


@pytest_asyncio.fixture
async def kernel():
    try:
        async with Chancy(DSN, prefix=CHANCY_PREFIX) as client:
            await client.migrate()
            yield client
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"kernel unavailable: {exc}")


@pytest_asyncio.fixture
async def service(kernel):
    reg = InMemoryRegistry()
    reg.register_job_type(
        JobTypeSpec(type="invoice.match@v1", input_schema=Payload, handler=_Noop)
    )
    return JobService(reg, kernel, PostgresStorage(DSN))


async def _count(conn, table: str, job_id: str | None = None) -> int:
    if table == "keelflow_jobs" and job_id:
        cur = await conn.execute(
            f"SELECT count(*) FROM {table} WHERE id = %s", (job_id,)
        )
    else:
        cur = await conn.execute(f"SELECT count(*) FROM {table}")
    return (await cur.fetchone())[0]


@pytest.mark.asyncio
async def test_commit_makes_job_visible(service, db, kernel):
    async with await _connect() as conn, conn.transaction(), conn.cursor() as cur:
        ref = await service.builder("invoice.match@v1").context(_ctx()).payload(
            Payload(invoice_id="inv-1")
        ).enqueue(cur)
        # committed on exiting conn.transaction()

    assert await _count(db, "keelflow_jobs", ref.id) == 1
    assert await _count(db, "keelflow_outbox") == 1

    row = await (await db.execute(
        "SELECT type, status, context, max_attempts FROM keelflow_jobs WHERE id=%s",
        (ref.id,),
    )).fetchone()
    assert row[0] == "invoice.match@v1"
    assert row[1] == JobStatus.PENDING.value
    assert row[2]["actor"]["id"] == "u_test"
    assert row[2]["correlation_id"] == "corr-1"
    assert row[3] == 3

    # kernel row committed in the same tx
    async with kernel.pool.connection() as kconn, kconn.cursor() as cur:
        r = await cur.execute(
            f"SELECT count(*) FROM {CHANCY_PREFIX}jobs WHERE meta->>'job_id' = %s",
            (ref.id,),
        )
        assert (await r.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_rollback_leaves_no_trace(service, db, kernel):
    ref_id = str(uuid.uuid4())
    async with await _connect() as conn:
        with pytest.raises(RuntimeError):
            async with conn.transaction(), conn.cursor() as cur:
                ref = await service.builder("invoice.match@v1").context(_ctx()).payload(
                    Payload(invoice_id="inv-2")
                ).enqueue(cur)
                ref_id = ref.id
                raise RuntimeError("abort")

    assert await _count(db, "keelflow_jobs", ref_id) == 0
    assert await _count(db, "keelflow_outbox") == 0
    async with kernel.pool.connection() as kconn, kconn.cursor() as cur:
        r = await cur.execute(
            f"SELECT count(*) FROM {CHANCY_PREFIX}jobs WHERE meta->>'job_id' = %s",
            (ref_id,),
        )
        assert (await r.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_missing_context_refused(service, db):
    async with await _connect() as conn, conn.transaction(), conn.cursor() as cur:
        with pytest.raises(EnqueueError, match="context"):
            await service.builder("invoice.match@v1").payload(
                Payload(invoice_id="x")
            ).enqueue(cur)


@pytest.mark.asyncio
async def test_bad_payload_rejected_at_enqueue(service, db):
    async with await _connect() as conn, conn.transaction(), conn.cursor() as cur:
        with pytest.raises(EnqueueError, match="schema validation"):
            await service.builder("invoice.match@v1").context(_ctx()).payload(
                {"wrong": "shape"}
            ).enqueue(cur)


@pytest.mark.asyncio
async def test_unknown_type_rejected(service, db):
    async with await _connect() as conn, conn.transaction(), conn.cursor() as cur:
        with pytest.raises(KeyError):
            await service.builder("nope@v1").context(_ctx()).enqueue(cur)


@pytest.mark.asyncio
async def test_oversize_payload_rejected(service, db, kernel):
    big = Payload(invoice_id="x" * 9000)
    async with await _connect() as conn, conn.transaction(), conn.cursor() as cur:
        with pytest.raises(EnqueueError, match="exceeds"):
            await service.builder("invoice.match@v1").context(_ctx()).payload(
                big
            ).enqueue(cur)


@pytest.mark.asyncio
async def test_unique_key_idempotent_enqueue(service, db, kernel):
    """Same (type, unique_key) while non-terminal -> same JobRef, one
    kernel row, one outbox row; different key -> new job."""
    async with await _connect() as conn, conn.transaction(), conn.cursor() as cur:
        first = await service.builder("invoice.match@v1") \
            .context(_ctx()).payload(Payload(invoice_id="inv-1")) \
            .deduplication_key("k-1").enqueue(cur)
        second = await service.builder("invoice.match@v1") \
            .context(_ctx()).payload(Payload(invoice_id="inv-1")) \
            .deduplication_key("k-1").enqueue(cur)
        third = await service.builder("invoice.match@v1") \
            .context(_ctx()).payload(Payload(invoice_id="inv-2")) \
            .deduplication_key("k-2").enqueue(cur)
    assert second.id == first.id                 # dedup — same job
    assert third.id != first.id                  # different key -> new job
    assert await _count(db, "keelflow_jobs") == 2

    async with kernel.pool.connection() as kconn, kconn.cursor() as cur:
        kc = await (await cur.execute(
            f"SELECT count(*) FROM {CHANCY_PREFIX}jobs"
            " WHERE meta->>'job_id' = %s", (first.id,))).fetchone()
        assert kc[0] == 1                        # one kernel row only
    out = await (await db.execute(
        "SELECT count(*) FROM keelflow_outbox"
        " WHERE payload->>'job_id' = %s", (first.id,))).fetchone()
    assert out[0] == 1                           # one outbox row only


@pytest.mark.asyncio
async def test_unique_key_released_when_terminal(service, db):
    """Once the job is terminal, the key frees — a new enqueue succeeds."""
    async with await _connect() as conn, conn.transaction(), conn.cursor() as cur:
        first = await service.builder("invoice.match@v1") \
            .context(_ctx()).payload(Payload(invoice_id="inv-1")) \
            .deduplication_key("k-reuse").enqueue(cur)
    await db.execute(
        "UPDATE keelflow_jobs SET status='done' WHERE id=%s", (first.id,))
    async with await _connect() as conn, conn.transaction(), conn.cursor() as cur:
        second = await service.builder("invoice.match@v1") \
            .context(_ctx()).payload(Payload(invoice_id="inv-1")) \
            .deduplication_key("k-reuse").enqueue(cur)
    assert second.id != first.id
    assert await _count(db, "keelflow_jobs") == 2
