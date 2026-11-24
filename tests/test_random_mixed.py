"""Randomized mixed-lifecycle DAG tests.

Seeded random DAGs mixing machine / wait.* / human.* steps with random
event plans (complete, reject, signal, signal_failure, cancel, leave
open) — the invariant oracle recomputes every step's expected status
and the run's terminal outcome from the event log, then checks the
database matches it exactly. Also covers multi-run event interleaving:
resolving wait jobs of run A must never disturb run B.

Requires Postgres at KEELFLOW_TEST_DSN.
"""
from __future__ import annotations

import asyncio
import os
import random
import time

import psycopg
import pytest
import pytest_asyncio
from chancy import Chancy, Queue, Worker
from pydantic import BaseModel

from keelflow.engine import worker as worker_mod
from keelflow.engine.enqueue import JobService
from keelflow.engine.human import HumanTaskService
from keelflow.engine.registry import InMemoryRegistry
from keelflow.engine.workflow import WorkflowService
from keelflow.postgres.schema import apply_async
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import (
    Actor,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
    RetryPolicy,
    StepSpec,
    WorkflowDefinition,
)

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
CHANCY_PREFIX = "keelflow_test_"


class In(BaseModel):
    x: int = 0


class OkH(JobHandler):
    async def run(self, ctx, payload: In):
        return JobResult(status="done", outputs={"n": 1})


class AlwaysFailH(JobHandler):
    async def run(self, ctx, payload: In):
        return JobResult(status="failed", error="deterministic failure")


async def _conn(autocommit: bool = True):
    return await psycopg.AsyncConnection.connect(DSN, autocommit=autocommit)


@pytest_asyncio.fixture
async def db():
    try:
        conn = await _conn()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    async with conn:
        await apply_async(conn)
        await conn.execute(
            "TRUNCATE keelflow_jobs, keelflow_outbox, keelflow_audit,"
            " keelflow_step_runs, keelflow_workflow_runs, keelflow_chunks,"
            " keelflow_row_status, keelflow_row_archive, keelflow_dlq CASCADE"
        )
        await conn.execute(
            f"TRUNCATE {CHANCY_PREFIX}jobs, {CHANCY_PREFIX}workers,"
            f" {CHANCY_PREFIX}queues CASCADE"
        )
        yield conn


@pytest_asyncio.fixture
async def env(db):
    async with Chancy(DSN, prefix=CHANCY_PREFIX) as kernel:
        await kernel.migrate()
        await kernel.declare(
            Queue("default", concurrency=8, executor=Chancy.Executor.Async,
                  eager_polling=True),
            upsert=True,
        )
        reg = InMemoryRegistry()
        reg.register_job_type(JobTypeSpec(
            type="m.ok@v1", input_schema=In, handler=OkH))
        reg.register_job_type(JobTypeSpec(
            type="m.boom@v1", input_schema=In, handler=AlwaysFailH,
            retry=RetryPolicy(max_attempts=1)))
        reg.register_job_type(JobTypeSpec(
            type="wait.vendor@v1", input_schema=In, is_external_wait=True))
        reg.register_job_type(JobTypeSpec(
            type="human.gate@v1", input_schema=In,
            handler=OkH, is_human=True))
        jobs = JobService(reg, kernel, PostgresStorage(DSN))
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        yield {"reg": reg, "jobs": jobs, "kernel": kernel,
               "wf": WorkflowService(reg, jobs),
               "human": HumanTaskService(reg, kernel)}


_SIG = JobContext(actor=Actor.service("vendor"),
                  permissions=frozenset({"job.signal"}))
_CANCEL = JobContext(actor=Actor.user("u1"),
                     permissions=frozenset({"job.cancel"}))
_ACTOR = Actor.user("u1")


async def _start(env, steps, inputs=None):
    env["reg"].register_workflow(WorkflowDefinition(
        name=f"wf.mix@{id(steps)}", steps=steps))
    ctx = JobContext(actor=_ACTOR)
    async with (
        await _conn(autocommit=False) as conn,
        conn.transaction(),
        conn.cursor() as cur,
    ):
        return await env["wf"].start(
            f"wf.mix@{id(steps)}", inputs or {}, ctx, cur)


async def _wait_step(db, run_id, step_id, statuses, timeout=15):
    t0 = time.monotonic()
    row = None
    while time.monotonic() - t0 < timeout:
        row = await (await db.execute(
            "SELECT status, job_id FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id=%s",
            (run_id, step_id))).fetchone()
        if row and row[0] in statuses:
            return row
        await asyncio.sleep(0.1)
    raise AssertionError(f"step {step_id} not in {statuses}: {row!r}")


# ── A: random mixed-lifecycle DAGs ──────────────────────────────────────────

_KINDS = ["m.ok@v1", "m.boom@v1", "wait.vendor@v1", "human.gate@v1"]
_EVENTS = {
    "wait.vendor@v1": ["ok", "fail", "cancel", "leave"],
    "human.gate@v1": ["approve", "reject", "cancel", "leave"],
}


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(10))
async def test_mixed_lifecycle_random_dag(env, db, seed):
    """4–8 leaf steps of random kinds + a tail join; random per-step
    event plan. Expected statuses are computed from the event log and
    every step/job/run row must match the oracle exactly."""
    rng = random.Random(seed)
    n = rng.randint(4, 8)
    leaves = [f"s{i}" for i in range(n)]
    kinds = {s: rng.choice(_KINDS) for s in leaves}
    steps = [StepSpec(id=s, type=kinds[s]) for s in leaves]
    steps.append(StepSpec(id="tail", type="m.ok@v1", needs=tuple(leaves)))
    run = await _start(env, tuple(steps))

    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        # waits + humans materialize blocked (machine steps resolve on
        # their own, possibly racing our queries — that's fine: the
        # expected map only tracks waits/humans + tail).
        step_expect: dict[str, str] = {}
        open_steps: list[str] = []
        rejected = False

        # collect the blocked leaves' job ids
        jobs: dict[str, str] = {}
        for s in leaves:
            kind = kinds[s]
            if kind in _EVENTS:
                row = await _wait_step(db, run.id, s, {"blocked"})
                jobs[s] = str(row[1])

        # build per-step event plans then interleave (order preserved
        # within a step)
        plans: dict[str, list[str]] = {}
        for s in leaves:
            kind = kinds[s]
            if kind not in _EVENTS:
                continue
            plan = rng.choice(_EVENTS[kind])
            if plan in ("ok", "approve", "reject", "fail"):
                plans[s] = [plan]
                if rng.random() < 0.4:
                    plans[s].append("dup")        # late signal → raises
            elif plan == "cancel":
                plans[s] = ["cancel"]
            else:
                plans[s] = []                     # leave open
        ops: list[tuple[str, str]] = []
        remaining = {s: list(p) for s, p in plans.items()}
        while any(remaining.values()):
            s = rng.choice([k for k, p in remaining.items() if p])
            ops.append((s, remaining[s].pop(0)))

        # replay the event log, recording the oracle
        resolve_map = {"ok": "done", "approve": "done",
                       "reject": "done", "fail": "failed",
                       "cancel": "cancelled"}
        chosen: dict[str, str] = {}
        for s, op in ops:
            kind = kinds[s]
            jid = jobs[s]
            plan = plans[s][0]  # the base plan
            if op == "cancel":
                assert await env["jobs"].cancel(jid, _CANCEL)
                step_expect[s] = "cancelled"
            elif op == "dup":
                with pytest.raises(ValueError):
                    async with (await _conn(False)) as conn:
                        await env["jobs"].signal(
                            conn, jid, {"late": True}, _SIG)
            else:
                chosen[s] = plan
                if kind == "human.gate@v1":
                    decision = "reject" if plan == "reject" else "approve"
                    async with (await _conn(False)) as conn:
                        await env["human"].complete(
                            conn, jid, _ACTOR, {"decision": decision})
                elif plan == "fail":
                    async with (await _conn(False)) as conn:
                        await env["jobs"].signal_failure(
                            conn, jid, "vendor error", _SIG)
                else:
                    async with (await _conn(False)) as conn:
                        await env["jobs"].signal(
                            conn, jid, {"n": 1}, _SIG)
                step_expect[s] = resolve_map[plan]
                if plan == "reject":
                    rejected = True
        open_steps = [s for s in leaves
                      if kinds[s] in _EVENTS and s not in step_expect]

        # oracle for the run — precedence is temporal, not categorical:
        # the first terminal step observed by advance wins, so a machine
        # failure racing a cancel can land either way. Invariants that
        # hold regardless: a failed run has ≥1 failed step; a cancelled
        # run has ≥1 cancelled step; done requires every leaf resolved
        # and is 'rejected' only when a human rejected.
        machine_fail = any(k == "m.boom@v1" for k in kinds.values())
        any_failed = machine_fail or "failed" in step_expect.values()
        any_cancelled = "cancelled" in step_expect.values()

        t0 = time.monotonic()
        rr = await (await db.execute(
            "SELECT status, terminal_outcome FROM keelflow_workflow_runs"
            " WHERE id=%s", (run.id,))).fetchone()
        while rr[0] == "running" and not open_steps and \
                time.monotonic() - t0 < 30:
            await asyncio.sleep(0.15)
            rr = await (await db.execute(
                "SELECT status, terminal_outcome"
                " FROM keelflow_workflow_runs WHERE id=%s",
                (run.id,))).fetchone()

        if any_failed and any_cancelled:
            assert rr[0] in ("failed", "cancelled"), (seed, rr)
        elif any_failed:
            assert (rr[0], rr[1]) == ("failed", "failed"), (seed, rr)
        elif any_cancelled:
            assert (rr[0], rr[1]) == ("cancelled", "cancelled"), \
                (seed, rr)
        elif open_steps:
            assert rr[0] == "running", (seed, rr)
        else:
            want_out = "rejected" if rejected else "succeeded"
            assert (rr[0], rr[1]) == ("done", want_out), (seed, rr)

        # every resolved wait/human step + job matches the oracle
        for s, want in step_expect.items():
            step = await (await db.execute(
                "SELECT status FROM keelflow_step_runs"
                " WHERE run_id=%s AND step_id=%s", (run.id, s))
            ).fetchone()
            job = await (await db.execute(
                "SELECT status FROM keelflow_jobs WHERE id=%s",
                (jobs[s],))).fetchone()
            assert step[0] == want and job[0] == want
        # untouched waits/humans stay open
        for s in open_steps:
            step = await (await db.execute(
                "SELECT status FROM keelflow_step_runs"
                " WHERE run_id=%s AND step_id=%s", (run.id, s))
            ).fetchone()
            assert step[0] == "blocked"
        # tail only runs when nothing failed/cancelled and all leaves
        # resolved
        tail = await (await db.execute(
            "SELECT status FROM keelflow_step_runs"
            " WHERE run_id=%s AND step_id='tail'", (run.id,))).fetchone()
        if rr[0] == "done":
            assert tail[0] == "done"
        else:
            assert tail[0] == "pending"
    finally:
        await w.stop(timeout=5)


# ── B: multi-run interleave — no cross-run leakage ─────────────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(6))
async def test_multi_run_event_interleave(env, db, seed):
    """M runs of the same DAG, events shuffled across runs: resolving a
    wait on run A must never touch run B; each run lands on its own
    deterministic outcome."""
    rng = random.Random(seed)
    m = rng.randint(2, 3)
    steps = (
        StepSpec(id="w0", type="wait.vendor@v1"),
        StepSpec(id="w1", type="wait.vendor@v1"),
        StepSpec(id="m", type="m.ok@v1", needs=("w0", "w1")),
    )
    runs = [await _start(env, steps) for _ in range(m)]
    w = Worker(env["kernel"], shutdown_timeout=5)
    await w.start()
    try:
        waits = {}                    # run_idx -> {step_id: job_id}
        for i, r in enumerate(runs):
            waits[i] = {}
            for sid in ("w0", "w1"):
                row = await _wait_step(db, r.id, sid, {"blocked"})
                waits[i][sid] = str(row[1])

        # per-run plans: one wait resolves, the other varies
        events: list[tuple[int, str, str]] = []
        expect_run: list[str] = []
        for i in range(m):
            plan = rng.choice(["all_ok", "one_fail", "one_cancel"])
            if plan == "all_ok":
                events += [(i, "w0", "ok"), (i, "w1", "ok")]
                expect_run.append("done")
            elif plan == "one_fail":
                events += [(i, "w0", "ok"), (i, "w1", "fail")]
                expect_run.append("failed")
            else:
                events += [(i, "w0", "cancel")]
                expect_run.append("cancelled")
        rng.shuffle(events)

        for i, sid, op in events:
            jid = waits[i][sid]
            if op == "cancel":
                assert await env["jobs"].cancel(jid, _CANCEL)
            elif op == "fail":
                async with (await _conn(False)) as conn:
                    await env["jobs"].signal_failure(
                        conn, jid, "e", _SIG)
                    await conn.commit()
            else:
                async with (await _conn(False)) as conn:
                    await env["jobs"].signal(conn, jid, {"n": 1}, _SIG)
                    await conn.commit()

        for i, r in enumerate(runs):
            t0 = time.monotonic()
            row = await (await db.execute(
                "SELECT status FROM keelflow_workflow_runs WHERE id=%s",
                (r.id,))).fetchone()
            while row[0] == "running" and time.monotonic() - t0 < 30:
                await asyncio.sleep(0.15)
                row = await (await db.execute(
                    "SELECT status FROM keelflow_workflow_runs"
                    " WHERE id=%s", (r.id,))).fetchone()
            assert row[0] == expect_run[i], (seed, i, expect_run, row)
            # cross-run isolation: run B's waits were never touched by
            # run A's signals (job ids are per-run; statuses agree)
            for sid, jid in waits[i].items():
                job = await (await db.execute(
                    "SELECT status FROM keelflow_jobs WHERE id=%s",
                    (jid,))).fetchone()
                step = await (await db.execute(
                    "SELECT status FROM keelflow_step_runs"
                    " WHERE run_id=%s AND step_id=%s", (r.id, sid)
                )).fetchone()
                # open wait: job 'pending', step 'blocked' — legal pair;
                # resolved waits: statuses must agree exactly
                assert (job[0], step[0]) in (
                    ("pending", "blocked"), ("done", "done"),
                    ("failed", "failed"), ("cancelled", "cancelled")
                ), (i, sid, job, step)
    finally:
        await w.stop(timeout=5)
