"""Human-task RBAC cases the demo UI exposes — regression tests.

Every case here maps to something a viewer can click in the demo:

  my_tasks     → which tasks appear in YOUR inbox (role/user/unassigned)
  claim        → who may claim (assignee user, assignee role, wrong role)
  complete     → only the claimer can complete
  claim again  → second claimant gets PermissionError ("claimed by X")

Requires Postgres at KEELFLOW_TEST_DSN (skipped when unavailable).
"""
from __future__ import annotations

import os

import psycopg
import pytest
import pytest_asyncio

from keelflow.engine.human import HumanTaskService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.sdk import Actor

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN",
    "postgresql://postgres:postgres@localhost:5544/keelflow_test")


@pytest_asyncio.fixture
async def db():
    try:
        conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        from keelflow.postgres.schema import apply_async
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq, keelflow_entity_state"
            " CASCADE")
        yield conn


@pytest_asyncio.fixture
async def human(db):
    return HumanTaskService(InMemoryRegistry(), kernel=None)


async def _task(db, assignee, type_="human.x@v1"):
    """Insert a bare human job row; returns its id."""
    row = await (await db.execute(
        "INSERT INTO keelflow_jobs"
        " (id,type,status,payload,context,attempts,max_attempts,"
        "  run_at,assignee,created_at,updated_at)"
        " VALUES (gen_random_uuid(),%s,'pending','{}','{}',0,3,"
        "         now(),%s,now(),now()) RETURNING id",
        (type_, assignee))).fetchone()
    return str(row[0])


async def test_role_task_hidden_from_wrong_role(db, human):
    """A `role:gl` task must not appear in a finance user's inbox."""
    await _task(db, "role:gl")
    assert await human.my_tasks(db, Actor.user("fin1"),
                                roles=["finance"]) == []
    tasks = await human.my_tasks(db, Actor.user("gl1"), roles=["gl"])
    assert len(tasks) == 1


async def test_wrong_role_cannot_claim(db, human):
    """THE bug: gl1 could claim a finance task and then couldn't see it.
    Now claim() enforces the assignee role."""
    tid = await _task(db, "role:gl")
    with pytest.raises(PermissionError, match="requires role"):
        await human.claim(db, tid, Actor.user("fin1"), roles=["finance"])
    # right role claims fine
    await human.claim(db, tid, Actor.user("gl1"), roles=["gl"])


async def test_unclaimed_task_cannot_be_completed_by_wrong_role(db, human):
    tid = await _task(db, "role:gl")
    with pytest.raises(PermissionError, match="requires role"):
        await human.complete(db, tid, Actor.user("fin1"), roles=["finance"])
    state = await (await db.execute(
        "SELECT status, claimed_by FROM keelflow_jobs WHERE id=%s", (tid,)
    )).fetchone()
    assert state[0] == "pending" and state[1] is None


async def test_assigned_user_must_complete_even_if_unclaimed(db, human):
    tid = await _task(db, "user:bob")
    with pytest.raises(PermissionError, match="assigned to user:bob"):
        await human.complete(db, tid, Actor.user("alice"))
    await human.complete(db, tid, Actor.user("bob"))


async def test_completed_task_cannot_be_claimed_again(db, human):
    tid = await _task(db, "role:gl")
    await human.claim(db, tid, Actor.user("gl1"), roles=["gl"])
    await human.complete(db, tid, Actor.user("gl1"), roles=["gl"])
    with pytest.raises(ValueError, match="not open"):
        await human.claim(db, tid, Actor.user("gl1"), roles=["gl"])


async def test_user_task_only_claimable_by_that_user(db, human):
    tid = await _task(db, "user:bob")
    with pytest.raises(PermissionError, match="assigned to user:bob"):
        await human.claim(db, tid, Actor.user("alice"), roles=["admin"])
    await human.claim(db, tid, Actor.user("bob"))  # no roles needed


async def test_unassigned_claimable_by_anyone(db, human):
    tid = await _task(db, None)
    await human.claim(db, tid, Actor.user("anybody"))


async def test_second_claimant_rejected(db, human):
    """First claim wins — the 'claimed by X' UI case."""
    tid = await _task(db, "role:gl")
    await human.claim(db, tid, Actor.user("gl1"), roles=["gl"])
    with pytest.raises(PermissionError, match="already claimed"):
        await human.claim(db, tid, Actor.user("gl2"), roles=["gl"])
    # same actor re-claiming is idempotent (no raise)
    await human.claim(db, tid, Actor.user("gl1"), roles=["gl"])


async def test_complete_only_by_claimer(db, human):
    tid = await _task(db, "role:gl")
    await human.claim(db, tid, Actor.user("gl1"), roles=["gl"])
    with pytest.raises(PermissionError):
        await human.complete(db, tid, Actor.user("gl2"), {"decision": "ok"},
                             roles=["gl"])
    await human.complete(db, tid, Actor.user("gl1"), {"decision": "ok"},
                         roles=["gl"])
    with pytest.raises(ValueError, match="not open"):
        await human.complete(db, tid, Actor.user("gl1"), {"decision": "again"},
                             roles=["gl"])


async def test_claimed_task_visible_to_claimer_regardless_of_role(db, human):
    """The earlier fix: a claimed task stays in YOUR inbox even when its
    assignee role isn't yours — otherwise you'd claim then lose it."""
    tid = await _task(db, "role:gl")
    # bypass claim-role for this edge case via direct row update
    await db.execute(
        "UPDATE keelflow_jobs SET claimed_by='user:weird',"
        " status='claimed' WHERE id=%s", (tid,))
    tasks = await human.my_tasks(db, Actor.user("weird"), roles=[])
    assert [str(t["id"]) for t in tasks] == [tid]


async def test_my_tasks_scopes(db, human):
    """Inbox shows: my user tasks + my role tasks + unassigned."""
    await _task(db, "user:u1")
    await _task(db, "role:gl", type_="human.y@v1")
    await _task(db, "role:finance", type_="human.z@v1")
    await _task(db, None, type_="human.free@v1")
    tasks = await human.my_tasks(db, Actor.user("u1"), roles=["gl"])
    types = sorted(t["type"] for t in tasks)
    assert types == ["human.free@v1", "human.x@v1", "human.y@v1"]
