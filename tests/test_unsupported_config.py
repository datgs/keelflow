"""T0 guard: declared-but-unsupported spec fields fail fast."""

import pytest
from pydantic import BaseModel

from conftest import DSN
from keelflow.engine.enqueue import EnqueueError, JobService, _resolve_path
from keelflow.engine.registry import InMemoryRegistry
from keelflow.postgres.storage import PostgresStorage
from keelflow.sdk import (
    Actor,
    ConcurrencySpec,
    JobContext,
    JobHandler,
    JobResult,
    JobTypeSpec,
)


class P(BaseModel):
    sku: str | None = None


class OkH(JobHandler):
    async def run(self, ctx, payload):
        return JobResult(status="done")


def _spec(**kw) -> JobTypeSpec:
    return JobTypeSpec(type="cfg.t@v1", input_schema=P, handler=OkH, **kw)


def test_max_workers_rejected_at_registration():
    """A silent no-op limit is worse than no field — fail loudly."""
    spec = _spec(concurrency=ConcurrencySpec(max_workers=2))
    with pytest.raises(ValueError, match="max_workers"):
        InMemoryRegistry().register_job_type(spec)


def test_concurrency_key_defaults_accepted():
    reg = InMemoryRegistry()
    reg.register_job_type(_spec(concurrency=ConcurrencySpec()))
    reg.register_job_type(_spec())  # default-constructed spec


def test_resolve_path():
    scope = {"payload": {"sku": "S1", "deep": {"x": 5}}}
    assert _resolve_path("payload.sku", scope) == "S1"
    assert _resolve_path("payload.deep.x", scope) == 5
    assert _resolve_path("payload.missing", scope) is None


@pytest.mark.asyncio
async def test_spec_concurrency_key_resolves_to_job_row():
    """spec.concurrency.key is enforced: it lands on the job's
    concurrency_key column (advisory-lock serialization)."""

    from conftest import open_kernel

    reg = InMemoryRegistry()
    reg.register_job_type(_spec(
        concurrency=ConcurrencySpec(key="payload.sku")))
    ctx = JobContext(actor=Actor.user("u"))
    async with open_kernel("native") as kernel:
        jobs = JobService(reg, kernel, PostgresStorage(DSN))
        storage = PostgresStorage(DSN)
        session = await storage.connect()
        try:
            async with session.begin() as uow:
                ref = await jobs.builder("cfg.t@v1") \
                    .payload({"sku": "SKU-9"}) \
                    .context(ctx) \
                    .enqueue(uow)
                row = await storage.job_load(uow, ref.id)
                assert row["concurrency_key"] == "SKU-9"
            # missing path → EnqueueError, no job row
            reg2 = InMemoryRegistry()
            reg2.register_job_type(JobTypeSpec(
                type="cfg.nokey@v1", input_schema=P, handler=OkH,
                concurrency=ConcurrencySpec(key="payload.absent")))
            jobs2 = JobService(reg2, kernel, PostgresStorage(DSN))
            async with session.begin() as uow:
                with pytest.raises(EnqueueError, match="concurrency.key"):
                    await jobs2.builder("cfg.nokey@v1") \
                        .payload({"sku": "x"}) \
                        .context(ctx) \
                        .enqueue(uow)
        finally:
            await session.close()
