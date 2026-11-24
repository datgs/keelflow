"""Storage-port conformance suite (T3).

Behavior-level proofs every Storage adapter must satisfy, run against
each registered implementation. Today Postgres is the sole adapter; a
future backend subclasses `StorageContract` with its own
`storage`/`session` fixtures to inherit the same proofs.

The proofs pin the port's semantics, not Postgres specifics:
- ops never commit — the caller's unit of work is the only boundary;
- nested scopes are savepoints (child failure keeps parent writes);
- job lifecycle: insert (dedup) → load → mark_running → finish;
- runs/steps bookkeeping round-trips;
- outbox claim/settle + per-subscriber delivery dedup;
- entity state ensure/transition/version;
- bulk partition + row/chunk bookkeeping.
"""
from __future__ import annotations

import os
import uuid

import psycopg
import pytest
import pytest_asyncio

from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import Actor, AuditEvent, EntityRef, JobContext
from keelflow.sdk.services import JobStatus
from keelflow.sdk.storage import DlqInsert, JobFinish, JobInsert

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)


def _ctx() -> JobContext:
    return JobContext(actor=Actor.user("contract-tester"))


def _job(job_id: str | None = None, **kw) -> JobInsert:
    from keelflow.engine.context import ctx_to_dict

    return JobInsert(
        id=job_id or str(uuid.uuid4()),
        type=kw.pop("type", "contract@v1"),
        payload=kw.pop("payload", {}),
        context=kw.pop("context", ctx_to_dict(_ctx())),
        **kw,
    )


@pytest_asyncio.fixture
async def storage():
    yield PostgresStorage(DSN)


@pytest_asyncio.fixture
async def session(storage):
    try:
        conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox,"
            " keelflow_outbox_deliveries, keelflow_audit, keelflow_step_runs,"
            " keelflow_workflow_runs, keelflow_chunks, keelflow_row_status,"
            " keelflow_row_archive, keelflow_dlq, keelflow_entity_state"
            " CASCADE"
        )
        await conn.close()
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"postgres unavailable: {exc}")
    sess = await PostgresStorage(DSN).connect()
    try:
        yield sess
    finally:
        await sess.close()


# ── unit-of-work discipline ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ops_never_commit(storage, session):
    """A write inside a rolled-back unit leaves no trace — proof the op
    honored the caller's boundary instead of committing itself."""
    job_id = str(uuid.uuid4())
    sess = await storage.connect()
    try:
        try:
            async with sess.begin() as uow:
                await storage.job_insert(uow, _job(job_id))
                raise RuntimeError("abort")
        except RuntimeError:
            pass
        async with sess.begin() as uow:
            assert await storage.job_load(uow, job_id) is None
    finally:
        await sess.close()


@pytest.mark.asyncio
async def test_commit_is_visible_across_sessions(storage, session):
    """A committed unit is readable on a different session — the op
    wrote into the shared store, not a private buffer."""
    job_id = str(uuid.uuid4())
    async with session.begin() as uow:
        await storage.job_insert(uow, _job(job_id))
    sess2 = await storage.connect()
    try:
        async with sess2.begin() as uow:
            row = await storage.job_load(uow, job_id)
            assert row is not None and row["type"] == "contract@v1"
    finally:
        await sess2.close()


@pytest.mark.asyncio
async def test_nested_scope_is_savepoint(storage, session):
    """Child-scope failure rolls back child writes only."""
    parent_id, child_id = str(uuid.uuid4()), str(uuid.uuid4())
    async with session.begin() as uow:
        await storage.job_insert(uow, _job(parent_id))
        try:
            async with uow.nested() as child:
                await storage.job_insert(child, _job(child_id))
                raise RuntimeError("child boom")
        except RuntimeError:
            pass
        # child write gone inside the still-open unit
        assert await storage.job_load(uow, child_id) is None
    sess2 = await storage.connect()
    try:
        async with sess2.begin() as uow:
            assert await storage.job_load(uow, parent_id) is not None
            assert await storage.job_load(uow, child_id) is None
    finally:
        await sess2.close()


@pytest.mark.asyncio
async def test_begin_rejects_non_idle_session(storage, session):
    """One open unit at a time — a second begin() raises rather than
    silently nesting or discarding the first."""
    async with session.begin():
        with pytest.raises(RuntimeError):
            async with session.begin():
                pass


# ── job lifecycle ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_job_dedup_returns_existing_id(storage, session):
    """Second insert with the same (type, unique_key) lands nothing and
    hands back the live job's id."""
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    async with session.begin() as uow:
        first = await storage.job_insert(
            uow, _job(a, unique_key="dup-1"))
        second = await storage.job_insert(
            uow, _job(b, unique_key="dup-1"))
        assert first == a
        assert second == a
        assert await storage.job_load(uow, b) is None


@pytest.mark.asyncio
async def test_job_mark_running_fenced_by_cancel(storage, session):
    """pending→running increments attempts, but never when
    cancel_requested is set — the claim-time race the port must fence."""
    job_id = str(uuid.uuid4())
    async with session.begin() as uow:
        await storage.job_insert(uow, _job(job_id))
        assert await storage.job_mark_running(uow, job_id) is True
        assert (await storage.job_attempts(uow, job_id)) == (1, 3)
        assert await storage.job_cancel_request(uow, job_id) == "running"
        assert await storage.job_mark_running(uow, job_id) is False


@pytest.mark.asyncio
async def test_job_finish_writes_terminal_state(storage, session):
    job_id = str(uuid.uuid4())
    async with session.begin() as uow:
        await storage.job_insert(uow, _job(job_id))
        await storage.job_finish(uow, job_id, JobFinish(
            status=JobStatus.DONE, result={"ok": 1}, result_bytes=7))
        row = await storage.job_load(uow, job_id)
        assert row["status"] == "done"
        assert row["result"] == {"ok": 1}
        assert row["result_bytes"] == 7


# ── workflow runs/steps ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_and_steps_round_trip(storage, session):
    run_id = str(uuid.uuid4())
    async with session.begin() as uow:
        await storage.run_insert(uow, run_id, "wf@v1", {"x": 1})
        await storage.step_runs_insert(
            uow, run_id, [("s1", "job.a@v1"), ("s2", "job.b@v1")])
        steps = await storage.steps_get(uow, run_id)
        assert [s["step_id"] for s in steps] == ["s1", "s2"]
        sr = await storage.step_run_for_update(uow, run_id, "s1")
        assert sr["status"] == "pending"
        job_id = str(uuid.uuid4())
        await storage.job_insert(uow, _job(job_id))
        await storage.step_attach_job(
            uow, str(sr["id"]), job_id, "claimed")
        await storage.step_settle(uow, str(sr["id"]), "done", {"o": 2})
        await storage.run_finish(uow, run_id, "done", "succeeded")
        run = await storage.run_get(uow, run_id)
        assert run["status"] == "done"
        assert run["terminal_outcome"] == "succeeded"


@pytest.mark.asyncio
async def test_trigger_callback_lock_reads_pending(storage, session):
    run_id = str(uuid.uuid4())
    async with session.begin() as uow:
        await storage.run_insert(
            uow, run_id, "wf@v1", {},
            triggered_by_entity_type="order", triggered_by_entity_id="o-1",
            trigger_config={"on_succeeded": "posted"},
            trigger_ctx={})
        run = await storage.trigger_callback_lock(uow, run_id)
        assert run["callback_state"] == "pending"
        await storage.run_mark_callback_done(uow, run_id)
        run = await storage.trigger_callback_lock(uow, run_id)
        assert run["callback_state"] == "done"


# ── entity state + audit ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_entity_ensure_and_transition(storage, session):
    async with session.begin() as uow:
        state = await storage.entity_ensure_locked(
            uow, "order", "o-9", "draft")
        assert state == "draft"
        await storage.entity_transition(uow, "order", "o-9", "approved")
        assert await storage.entity_state(uow, "order", "o-9") == "approved"


@pytest.mark.asyncio
async def test_audit_record_and_trace(storage, session):
    async with session.begin() as uow:
        await storage.audit_record(uow, AuditEvent(
            actor=Actor.user("u1"), action="order.approve",
            entity=EntityRef(type="order", id="o-9"),
            changes={"state": ("draft", "approved")}))
        rows = await storage.audit_trace(uow, "order", "o-9")
        assert len(rows) == 1 and rows[0]["action"] == "order.approve"


# ── outbox ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_outbox_claim_settle_delivery_dedup(storage, session):
    async with session.begin() as uow:
        await storage.outbox_insert(uow, "e@v1", {"a": 1})
        row = await storage.outbox_claim_next(uow)
        assert row["event_type"] == "e@v1"
        assert await storage.delivery_seen(uow, row["id"], "sub1") is False
        await storage.delivery_mark(uow, row["id"], "sub1")
        await storage.outbox_settle(uow, row["id"], "published")
    async with session.begin() as uow:
        assert await storage.delivery_seen(uow, row["id"], "sub1") is True
        assert await storage.outbox_claim_next(uow) is None


# ── dlq ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dlq_insert_and_lookup(storage, session):
    job_id = str(uuid.uuid4())
    async with session.begin() as uow:
        await storage.dlq_insert(uow, DlqInsert(
            id=job_id, type="contract@v1", payload={}, context={},
            error="boom", attempts=3))
        row = await storage.dlq_get(uow, job_id)
        assert row["error"] == "boom"
        listed = await storage.dlq_list(uow)
        assert str(listed[0]["id"]) == job_id


# ── bulk bookkeeping ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_bulk_partition_and_counts(storage, session):
    job_id = str(uuid.uuid4())
    conn = session.native
    async with session.begin() as uow:
        await storage.job_insert(uow, _job(job_id))
        for i in range(5):
            await conn.execute(
                "INSERT INTO keelflow_row_status (job_id, row_key, data)"
                " VALUES (%s, %s, '{}')",
                (job_id, f"r{i}"))
        assert await storage.bulk_pending_count(uow, job_id) == 5
        n_rows, n_chunks = await storage.bulk_partition(
            uow, job_id, 2, [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()])
        assert (n_rows, n_chunks) == (5, 3)
        assert await storage.bulk_pending_count(uow, job_id) == 0
        counts = await storage.chunk_counts(uow, job_id)
        assert counts == {"pending": 3}


@pytest.mark.asyncio
async def test_rows_archive_done_is_idempotent(storage, session):
    job_id = str(uuid.uuid4())
    conn = session.native
    async with session.begin() as uow:
        await storage.job_insert(uow, _job(job_id))
        for i in range(3):
            await conn.execute(
                "INSERT INTO keelflow_row_status"
                " (job_id, row_key, status, data)"
                " VALUES (%s, %s, 'done', '{}')",
                (job_id, f"r{i}"))
        assert await storage.rows_archive_done(uow, job_id) == 3
        # second pass: nothing left to move
        assert await storage.rows_archive_done(uow, job_id) == 0
        assert await storage.row_counts(uow, job_id) == {}
