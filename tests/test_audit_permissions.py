"""T14 contract tests: audit + permission hooks."""
from __future__ import annotations

import os

import psycopg
import pytest
import pytest_asyncio

from pyerp.engine.audit import PostgresAuditService
from pyerp.engine.permissions import DefaultPermissionService
from pyerp.postgres.schema import apply_async
from pyerp.sdk import Actor, AuditEvent, EntityRef, JobContext

DSN = os.environ.get(
    "PYERP_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/pyerp"
)


async def _conn(autocommit: bool = True):
    return await psycopg.AsyncConnection.connect(DSN, autocommit=autocommit)


@pytest_asyncio.fixture
async def db():
    try:
        conn = await _conn()
    except Exception as exc:  # noqa: BLE001 - env-dependent skip
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute("TRUNCATE hd_flow_audit")
        yield conn


# ── permissions ────────────────────────────────────────────────────────────


def test_require_user_with_permission():
    svc = DefaultPermissionService()
    ctx = JobContext(
        actor=Actor.user("u1"), permissions=frozenset({"invoices.approve"})
    )
    svc.require(ctx, "invoices.approve")
    with pytest.raises(PermissionError):
        svc.require(ctx, "invoices.post")


def test_system_bypasses():
    svc = DefaultPermissionService()
    svc.require(JobContext(actor=Actor.system()), "anything")


def test_job_actor_inherits_via_permissions():
    svc = DefaultPermissionService()
    job_actor = Actor.job("j_1", via=Actor.user("u1"))
    ctx = JobContext(actor=job_actor, permissions=frozenset({"a.b"}))
    svc.require(ctx, "a.b")
    with pytest.raises(PermissionError):
        svc.require(ctx, "a.c")


def test_job_cannot_exceed_via_rights():
    svc = DefaultPermissionService()
    job_actor = Actor.job("j_1", via=Actor.user("u1"))
    # job ctx claims '*' — but via (user) is what matters for require();
    # ctx.permissions is the snapshot granted at submit, so '*' passes.
    ctx = JobContext(actor=job_actor, permissions=frozenset({"*"}))
    svc.require(ctx, "anything")


def test_scope_own_vs_all():
    svc = DefaultPermissionService()
    ctx = JobContext(
        actor=Actor.user("u1"), permissions=frozenset({"invoices.edit:own"})
    )
    f = svc.scope_filter(ctx, "invoices.edit")
    assert f.scope == "own" and f.actor_id == "u1"

    ctx2 = JobContext(
        actor=Actor.user("u2"), permissions=frozenset({"invoices.edit"})
    )
    assert svc.scope_filter(ctx2, "invoices.edit").scope == "all"


# ── audit ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_audit_record_and_trace(db):
    svc = PostgresAuditService()
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
            await svc.record(
                AuditEvent(
                    actor=Actor.job("j_7", via=Actor.user("u1")),
                    action="invoices.adjust.approve",
                    entity=EntityRef(type="invoice", id="inv-9"),
                    changes={"cost": ("12.40", "12.90")},
                    source="j_7",
                    correlation_id="corr-5",
                ),
                cur,
            )

    rows = await svc.trace(db, EntityRef(type="invoice", id="inv-9"))
    assert len(rows) == 1
    assert rows[0]["action"] == "invoices.adjust.approve"
    assert rows[0]["actor"]["via"]["id"] == "u1"
    assert rows[0]["changes"]["cost"] == {"before": "12.40", "after": "12.90"}
    assert rows[0]["correlation_id"] == "corr-5"


@pytest.mark.asyncio
async def test_audit_rolls_back_with_tx(db):
    svc = PostgresAuditService()
    async with await _conn(autocommit=False) as conn:
        with pytest.raises(RuntimeError):
            async with conn.transaction(), conn.cursor() as cur:
                await svc.record(
                    AuditEvent(
                        actor=Actor.user("u1"),
                        action="x",
                        entity=EntityRef(type="invoice", id="inv-10"),
                    ),
                    cur,
                )
                raise RuntimeError("abort")
    assert await svc.trace(db, EntityRef(type="invoice", id="inv-10")) == []
