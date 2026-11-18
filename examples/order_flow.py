"""Order → review → approve → post: end-to-end demo (same as E2E test).

Run against a local Postgres (docker run -e POSTGRES_PASSWORD=postgres
-p 5544:5432 postgres:16-alpine) then:

    PYERP_DSN=postgresql://postgres:postgres@localhost:5544/pyerp \\
        python examples/order_flow.py
"""
from __future__ import annotations

import asyncio
import os

import psycopg
from chancy import Chancy, Queue, Worker
from pydantic import BaseModel

from pyerp.engine import worker as worker_mod
from pyerp.engine.bulk import BulkJobHandler, BulkJobPayload
from pyerp.engine.bulk import register as register_bulk
from pyerp.engine.enqueue import JobService
from pyerp.engine.human import HumanTaskService
from pyerp.engine.registry import InMemoryRegistry
from pyerp.engine.statemachine import EngineStateMachineService
from pyerp.engine.workflow import WorkflowService
from pyerp.postgres.schema import apply_async
from pyerp.sdk import (
    Actor,
    EntityRef,
    JobContext,
    JobResult,
    JobHandler,
    JobTypeSpec,
    StateMachineDefinition,
    StepSpec,
    TransitionSpec,
    WorkflowDefinition,
)

DSN = os.environ.get(
    "PYERP_DSN", "postgresql://postgres:postgres@localhost:5544/pyerp"
)


class PostStep(JobHandler):
    async def run(self, ctx, payload):
        return JobResult(status="done", outputs={"posted": ctx.actor.id})


class OrderImport(BulkJobHandler):
    async def process_row(self, ctx, row, tx):
        print(f"  import {row['row_key']}")
        return {"imported": row["row_key"]}


class P(BaseModel):
    pass


async def main() -> None:
    conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    await apply_async(conn)

    async with Chancy(DSN, prefix="pyerp_") as kernel:
        await kernel.migrate()
        await kernel.declare(
            Queue("default", concurrency=8, executor=Chancy.Executor.Async,
                  eager_polling=True),
            upsert=True,
        )

        reg = InMemoryRegistry()
        register_bulk(reg)
        reg.register_job_type(JobTypeSpec(
            type="imp.orders@v1", input_schema=BulkJobPayload,
            handler=OrderImport))
        reg.register_job_type(JobTypeSpec(
            type="post.step@v1", input_schema=P, handler=PostStep))
        reg.register_job_type(JobTypeSpec(
            type="human.review@v1", input_schema=P, handler=PostStep,
            is_human=True))
        reg.register_workflow(WorkflowDefinition(
            name="post.wf@v1",
            steps=(StepSpec(id="p1", type="post.step@v1"),
                   StepSpec(id="p2", type="post.step@v1", needs=("p1",))),
        ))
        reg.register_state_machine(StateMachineDefinition(
            name="order@v1", entity="order",
            states=("draft", "in_review", "approved"),
            transitions=(
                TransitionSpec("draft", "in_review", "submit",
                               creates_job="human.review@v1",
                               guard={"assignee_role": "finance"}),
                TransitionSpec("in_review", "approved", "approve",
                               triggers_workflow="post.wf@v1"),
            ),
            initial="draft",
        ))

        jobs = JobService(reg, kernel)
        worker_mod.configure(registry=reg, dsn=DSN, kernel=kernel)
        sm = EngineStateMachineService(reg, kernel)
        human = HumanTaskService(reg, kernel)

        ctx = JobContext(actor=Actor.user("u1"), permissions=frozenset({"*"}))

        # bulk import 20 rows
        async with conn.transaction(), conn.cursor() as cur:
            ref = await jobs.builder("imp.orders@v1").context(ctx) \
                .payload({"chunk_size": 5, "atomic": "row"}).enqueue(cur)
            for i in range(20):
                await cur.execute(
                    "INSERT INTO hd_flow_row_status (job_id, row_key,"
                    " status, data) VALUES (%s, %s, 'pending', %s)",
                    (ref.id, f"SKU-{i}", f'{{"sku":"SKU-{i}"}}'),
                )
        print(f"bulk job {ref.id}")

        w = Worker(kernel, shutdown_timeout=5)
        await w.start()
        try:
            while True:
                row = await (await conn.execute(
                    "SELECT status FROM hd_flow_jobs WHERE id=%s", (ref.id,)
                )).fetchone()
                if row[0] in ("done", "failed"):
                    break
                await asyncio.sleep(0.2)
            print("bulk:", row[0])

            # order lifecycle
            order = EntityRef(type="order", id="SO-1")
            async with conn.transaction(), conn.cursor() as cur:
                await sm.transition(order, "submit", ctx, cur)
            tasks = await human.my_tasks(conn, Actor.user("fin"),
                                         roles=["finance"])
            print("inbox:", [t["type"] for t in tasks])
            tid = str(tasks[0]["id"])
            await human.claim(conn, tid, Actor.user("fin"))
            await human.complete(conn, tid, Actor.user("fin"), {"ok": True})
            async with conn.transaction(), conn.cursor() as cur:
                await sm.transition(order, "approve", ctx, cur)
            print("order state:", await sm.current(order, conn))
            await asyncio.sleep(1.0)
            run = await (await conn.execute(
                "SELECT status FROM hd_flow_workflow_runs"
                " ORDER BY created_at DESC LIMIT 1"
            )).fetchone()
            print("workflow:", run[0])
        finally:
            await w.stop(timeout=5)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
