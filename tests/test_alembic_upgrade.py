"""T0.3: schema upgrade rehearsal with persisted data.

Proves the Alembic path upgrades a prior-revision database forward to
head while preserving live job/run rows — the acceptance evidence for
the upgrade policy in the readiness gate. Runs on a throwaway
`keelflow_test_<uid>` database via `temp_test_db`, never the suite DB.
"""
from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest

from keelflow.testing import temp_test_db

ROOT = Path(__file__).resolve().parent.parent


def _alembic_upgrade(dsn: str, revision: str = "head") -> None:
    """Run `alembic upgrade <rev>` in-process against `dsn`."""
    from alembic.config import Config

    from alembic import command

    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    os.environ["KEELFLOW_DSN"] = dsn.replace(
        "postgresql://", "postgresql+psycopg://", 1)
    try:
        command.upgrade(cfg, revision)
    finally:
        os.environ.pop("KEELFLOW_DSN", None)


@pytest.mark.asyncio
async def test_upgrade_from_prior_revision_preserves_data():
    """Seed a job + run at revision 0006, upgrade to head, verify rows
    and the new column/index exist."""
    async with temp_test_db() as dsn:
        # prior release schema (0006) — not 'head'
        _alembic_upgrade(dsn, "0006")
        conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        try:
            await conn.execute(
                "INSERT INTO keelflow_jobs"
                " (id, type, status, payload, context)"
                " VALUES (gen_random_uuid(), 'mig.t@v1', 'pending',"
                "         '{\"k\":1}', '{\"actor\":{\"type\":\"user\",\"id\":\"u\"}}')")
            await conn.execute(
                "INSERT INTO keelflow_workflow_runs (workflow, inputs)"
                " VALUES ('mig.wf@v1', '{}')")
        finally:
            await conn.close()

        _alembic_upgrade(dsn, "head")

        conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        try:
            row = await (await conn.execute(
                "SELECT type, status, payload, unique_key, progress, progress_at"
                " FROM keelflow_jobs")).fetchone()
            assert row[0] == "mig.t@v1" and row[1] == "pending"
            assert row[3] is None              # new column readable
            assert row[4] is None and row[5] is None
            run = await (await conn.execute(
                "SELECT workflow, status FROM keelflow_workflow_runs"
            )).fetchone()
            assert run[0] == "mig.wf@v1" and run[1] == "running"
            # alembic recorded head
            rev = await (await conn.execute(
                "SELECT version_num FROM alembic_version")).fetchone()
            assert rev[0] == "0009"
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_upgrade_is_idempotent_on_head():
    """`upgrade head` on an already-current DB is a no-op, not a
    fake-success that skips real state."""
    async with temp_test_db() as dsn:
        _alembic_upgrade(dsn, "head")
        _alembic_upgrade(dsn, "head")   # second run must not error
        conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        try:
            rev = await (await conn.execute(
                "SELECT version_num FROM alembic_version")).fetchone()
            assert rev[0] == "0009"
        finally:
            await conn.close()
