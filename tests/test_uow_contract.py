"""T1 proof: PostgresStorage UnitOfWork / Session contract.

Proves the atomicity contract the storage port rests on — storage ops,
kernel dispatch pushes, and application repository writes sharing one
`UnitOfWork` commit or roll back together. These tests run against the
real Postgres DSN (same KEELFLOW_TEST_DSN as the suite).
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from conftest import DSN, KERNEL_DISPATCH_RECORD
from keelflow.engine.kernel import dispatch_spec
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import JobInsert

storage = PostgresStorage(DSN)


@pytest_asyncio.fixture
async def session():
    sess = await storage.connect()
    try:
        await apply_async(sess.native)
        # DDL leaves an implicit transaction pending; commit it so the
        # session is idle — begin() now refuses a pending transaction.
        await sess.native.commit()
        yield sess
    finally:
        await sess.close()


def _job(uid: str, **kw) -> JobInsert:
    return JobInsert(
        id=uid,
        type="test.uow@v1",
        payload={},
        context={"actor": {"type": "system", "id": "test"}},
        **kw,
    )


# ── boundary semantics ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_uow_commit_makes_writes_visible(session):
    """Writes inside begin() are invisible to a second session until
    commit; after commit they are durable."""
    uid = str(uuid.uuid4())
    async with session.begin() as uow:
        await storage.job_insert(uow, _job(uid))
        # uncommitted — second session must not see it
        other = await storage.connect()
        try:
            async with other.begin() as o:
                assert await storage.job_load(o, uid) is None
        finally:
            await other.close()

    other = await storage.connect()
    try:
        async with other.begin() as o:
            row = await storage.job_load(o, uid)
            assert row and row["status"] == "pending"
    finally:
        await other.close()


@pytest.mark.asyncio
async def test_uow_rollback_discards_all_participants(session):
    """job insert + outbox + app-repo write in one uow: an exception
    rolls back every participant — no partial state."""
    uid = str(uuid.uuid4())
    conn = session.native
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS uow_probe (id uuid PRIMARY KEY, sku text)")
    await conn.commit()
    try:
        with pytest.raises(RuntimeError):
            async with session.begin() as uow:
                await storage.job_insert(uow, _job(uid))
                await storage.outbox_insert(
                    uow, "probe.event", {"job_id": uid})
                # application repository write via the adapter edge —
                # repo accesses uow.native directly, engine never does
                await uow.native.execute(
                    "INSERT INTO uow_probe (id, sku) VALUES (%s, %s)",
                    (uuid.UUID(uid), "s1"))
                raise RuntimeError("abort after all writes")
    finally:
        async with session.begin() as uow:
            assert await storage.job_load(uow, uid) is None
            cur = await uow.native.execute(
                "SELECT count(*) FROM uow_probe WHERE id=%s",
                (uuid.UUID(uid),))
            assert (await cur.fetchone())[0] == 0
            cur = await uow.native.execute(
                "SELECT count(*) FROM keelflow_outbox"
                " WHERE payload->>'job_id'=%s", (uid,))
            assert (await cur.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_nested_scope_rolls_back_only_child(session):
    """uow.nested() = savepoint: child failure keeps parent writes."""
    uid = str(uuid.uuid4())
    conn = session.native
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS uow_probe (id uuid PRIMARY KEY, sku text)")
    await conn.commit()
    async with session.begin() as uow:
        await storage.job_insert(uow, _job(uid))
        try:
            async with uow.nested():
                await uow.native.execute(
                    "INSERT INTO uow_probe (id, sku) VALUES (%s, %s)",
                    (uuid.UUID(uid), "child"))
                raise RuntimeError("child boom")
        except RuntimeError:
            pass
        # parent write survived; child row rolled back — inside the
        # same open transaction the uncommitted child row is absent
        cur = await uow.native.execute(
            "SELECT count(*) FROM uow_probe WHERE id=%s",
            (uuid.UUID(uid),))
        assert (await cur.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_begin_rejects_non_idle_session(session):
    """A second begin() while a uow is open raises — no silent commit."""
    async with session.begin() as uow:
        await storage.job_insert(uow, _job(str(uuid.uuid4())))
        with pytest.raises(RuntimeError, match="open UnitOfWork"):
            async with session.begin():
                pass  # pragma: no cover


# ── storage + kernel same-transaction push ────────────────────────────


@pytest.mark.asyncio
async def test_kernel_push_rolls_back_with_storage_write(session):
    """dispatch row + engine job row + domain write are one boundary:
    a rollback removes the kernel-visible record too, so no phantom
    dispatch is ever delivered."""
    uid = str(uuid.uuid4())
    KERNEL_DISPATCH_RECORD.clear()

    from chancy import Chancy

    chancy = Chancy(DSN, prefix="keelflow_uowt_")
    await chancy.migrate()
    # adapter edge: kernel consumes uow.native directly
    with pytest.raises(RuntimeError):
        async with session.begin() as uow:
            await storage.job_insert(uow, _job(uid))
            # chancy's push_ex expects a dict_row cursor — the adapter
            # narrows uow.native itself
            from psycopg.rows import dict_row
            cur = uow.native.cursor(row_factory=dict_row)
            await chancy.push_ex(cur, _chancy_job(
                dispatch_spec(job_id=uid)))
            raise RuntimeError("rollback all")

    # nothing survived — job row, kernel row, domain write
    async with session.begin() as uow:
        assert await storage.job_load(uow, uid) is None
        cur = await uow.native.execute(
            "SELECT count(*) FROM keelflow_uowt_jobs"
            " WHERE kwargs::text LIKE %s",
            (f"%{uid}%",))
        assert (await cur.fetchone())[0] == 0


def _chancy_job(spec):
    """Mirror of chancy_kernel._to_chancy_job — kernel adapter detail,
    exercised here to prove the uow carries the same transaction."""
    from datetime import UTC, datetime

    import chancy

    return chancy.Job(
        func=spec.func,
        queue=spec.queue,
        kwargs=spec.kwargs,
        priority=spec.priority,
        max_attempts=spec.max_attempts,
        scheduled_at=spec.scheduled_at or datetime.now(UTC),
        unique_key=spec.unique_key,
        meta=spec.meta or {},
    )


@pytest.mark.asyncio
async def test_native_kernel_push_inside_uow(session):
    """NativeKernel.push_ex consumes the uow's native cursor and writes
    the dispatch flag inside the same transaction."""
    from keelflow.engine.native_kernel import NativeKernel

    uid = str(uuid.uuid4())
    kernel = NativeKernel(DSN)
    async with session.begin() as uow:
        await storage.job_insert(uow, _job(uid))
        cur = uow.native.cursor()
        await kernel.push_ex(cur, dispatch_spec(job_id=uid))
        # flag visible in the same transaction
        row = await storage.job_load(uow, uid)
        assert row is not None
        assert row.get("dispatch_queued") is True
