"""T0.2 recovery proofs (readiness gate).

Executable evidence for the guarantee matrix — complements existing
coverage: worker death mid-job (test_e2e §4), concurrent claims
(test_concurrency_scheduler), outbox redelivery (test_events),
cancel races (test_engine_hardening).

Here: DB disconnect mid-handler, scheduler process restart, and the
database snapshot/restore path operators use for backup/restore.
"""
from __future__ import annotations

import asyncio

import psycopg
import pytest
import pytest_asyncio
from pydantic import BaseModel

from conftest import DSN, declare_default_queue, make_worker, open_kernel
from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.engine.scheduler import tick
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import (
    Actor,
    CronSpec,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
)
from keelflow.testing import temp_test_db

CALLS: dict[str, int] = {}
EFFECTS: list[str] = []


class P(BaseModel):
    pass


class KillerH(JobHandler):
    """First attempt terminates its own database connection mid-write —
    simulates a network drop / DB restart during the handler. The write
    rolls back with the transaction; the kernel redelivers and the
    second attempt completes. Idempotent write proves exactly-once
    effect despite redelivery."""

    async def run(self, ctx, payload):
        CALLS["killer"] = CALLS.get("killer", 0) + 1
        await ctx.uow.execute(
            "INSERT INTO recovery_probe (tag) VALUES ('wrote')"
            " ON CONFLICT (tag) DO NOTHING")
        EFFECTS.append("wrote")
        if CALLS["killer"] == 1:
            conn = ctx.uow.native
            cur = await conn.execute("SELECT pg_backend_pid()")
            pid = (await cur.fetchone())["pg_backend_pid"]
            # sever the session itself — psycopg raises OperationalError
            await conn.execute(
                "SELECT pg_terminate_backend(%s)", (pid,))
        return JobResult(status="done")


class OkH(JobHandler):
    async def run(self, ctx, payload):
        EFFECTS.append("ok")
        return JobResult(status="done")


@pytest_asyncio.fixture
async def env(kernel_kind):
    if kernel_kind == "sqlite":
        pytest.skip("DB-disconnect proof is Postgres-specific")
    try:
        conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS recovery_probe"
            " (tag text PRIMARY KEY)")
        await conn.execute("DELETE FROM recovery_probe")
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox,"
            " keelflow_outbox_deliveries CASCADE")
    conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    async with conn, open_kernel(kernel_kind) as kernel:
        await declare_default_queue(kernel, kernel_kind, concurrency=2)
        reg = InMemoryRegistry()
        reg.register_job_type(JobTypeSpec(
            type="kill.conn@v1", input_schema=P, handler=KillerH,
            retry=RetryPolicy(max_attempts=3, base_seconds=0.5)))
        reg.register_job_type(JobTypeSpec(
            type="sched.t@v1", input_schema=P, handler=OkH,
            schedule=CronSpec(cron="* * * * *")))
        storage = PostgresStorage(DSN)
        jobs = JobService(reg, kernel, storage)
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel,
                             storage=storage)
        CALLS.clear()
        EFFECTS.clear()
        yield {"reg": reg, "jobs": jobs, "kernel": kernel,
               "storage": storage, "db": conn, "kind": kernel_kind}


@pytest.mark.asyncio
async def test_db_disconnect_mid_handler_rolls_back_and_redelivers(
        env):
    """Kill the handler's own connection mid-UoW: the partial write
    rolls back, the kernel redelivers, and the retry completes — the
    idempotent domain write lands exactly once."""
    ctx = JobContext(actor=Actor.user("u"))
    sess = await env["storage"].connect()
    async with sess.begin() as uow:
        ref = await env["jobs"].builder("kill.conn@v1") \
            .payload({}).context(ctx).enqueue(uow)
    await sess.close()

    w = make_worker(env["kernel"], env["kind"])
    await w.start()
    try:
        for _ in range(400):
            row = await (await env["db"].execute(
                "SELECT status, attempts FROM keelflow_jobs WHERE id=%s",
                (ref.id,))).fetchone()
            if row and row[0] == "done":
                break
            await asyncio.sleep(0.1)
    finally:
        await w.stop(timeout=10)

    assert row[0] == "done"
    assert CALLS["killer"] >= 2          # redelivered after the drop
    assert len(EFFECTS) >= 2
    rows = await (await env["db"].execute(
        "SELECT count(*) FROM recovery_probe")).fetchone()
    assert rows[0] == 1                  # exactly-once effect


@pytest.mark.asyncio
async def test_scheduler_restart_never_double_enqueues(env):
    """A crashed/restarted scheduler replays ticks; unique-keyed cron
    slots make replays no-ops — two 'processes' tick the same slot."""
    session = await env["storage"].connect()
    try:
        async with session.begin() as uow:
            n1 = await tick(uow, env["storage"], env["reg"],
                            env["kernel"])
        # simulate restart: a second session ticks the same slot
        session2 = await env["storage"].connect()
        try:
            async with session2.begin() as uow2:
                n2 = await tick(uow2, env["storage"], env["reg"],
                                env["kernel"])
        finally:
            await session2.close()
        assert n1 + n2 <= 1
        rows = await (await env["db"].execute(
            "SELECT count(*) FROM keelflow_jobs WHERE type='sched.t@v1'"
        )).fetchone()
        assert rows[0] == 1
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_database_snapshot_restore_path():
    """Backup/restore rehearsal: `CREATE DATABASE … TEMPLATE` is the
    operator snapshot primitive. Prove a snapshot taken from a live
    keelflow schema restores job state intact."""
    async with temp_test_db() as src_dsn:
        conn = await psycopg.AsyncConnection.connect(
            src_dsn, autocommit=True)
        try:
            await apply_async(conn)
            await conn.execute(
                "INSERT INTO keelflow_jobs"
                " (id, type, status, payload, context) VALUES"
                " (gen_random_uuid(), 'bak.t@v1', 'running', '{}',"
                "  '{\"actor\":{\"type\":\"system\",\"id\":\"s\"}}')")
        finally:
            await conn.close()

        # snapshot: copy-on-write database clone (what pg_dump + restore
        # proves at the same level for logical backups)
        admin = src_dsn.rsplit("/", 1)[0] + "/postgres"
        src_name = src_dsn.rsplit("/", 1)[1]
        bak_name = f"{src_name}_bak"
        conn = await psycopg.AsyncConnection.connect(
            admin, autocommit=True)
        try:
            await conn.execute(
                f'CREATE DATABASE "{bak_name}" TEMPLATE "{src_name}"')
        finally:
            await conn.close()
        try:
            bak_dsn = src_dsn.rsplit("/", 1)[0] + f"/{bak_name}"
            conn = await psycopg.AsyncConnection.connect(
                bak_dsn, autocommit=True)
            try:
                row = await (await conn.execute(
                    "SELECT type, status FROM keelflow_jobs")).fetchone()
                assert row[0] == "bak.t@v1" and row[1] == "running"
            finally:
                await conn.close()
        finally:
            conn = await psycopg.AsyncConnection.connect(
                admin, autocommit=True)
            try:
                await conn.execute(
                    f'DROP DATABASE IF EXISTS "{bak_name}" WITH (FORCE)')
            finally:
                await conn.close()
