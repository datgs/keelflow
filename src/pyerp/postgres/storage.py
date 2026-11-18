"""Postgres implementation of the Storage protocol."""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from ..sdk.services import Job, JobResult, JobStatus, Storage, Transaction


class PostgresStorage(Storage):
    """Postgres-backed storage. Uses caller's transaction for `defer`."""

    async def defer(self, job: Job, tx: Transaction) -> None:
        await tx.execute(
            """
            INSERT INTO pyerp_jobs
                (id, type, status, payload, context, attempts, max_attempts,
                 run_at, claimed_by, lease_until, concurrency_key, assignee,
                 parent_job_id, result, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                job.id, job.type, job.status.value,
                json.dumps(job.payload), _ctx_to_json(job.context),
                job.attempts, 3, job.run_at or _now(), job.claimed_by,
                job.lease_until, job.concurrency_key, None,
                job.parent_job_id, json.dumps(job.result) if job.result else None,
                job.created_at or _now(), _now(),
            ),
        )

    async def claim(self, worker_id: str, types: list[str], limit: int) -> list[Job]:
        raise NotImplementedError("claim is implemented in worker via psycopg")

    async def heartbeat(self, job_id: str, lease_until: datetime) -> None:
        raise NotImplementedError

    async def finish(self, job_id: str, result: JobResult) -> None:
        raise NotImplementedError

    async def reclaim_expired(self) -> int:
        raise NotImplementedError


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ctx_to_json(ctx: Any) -> str:
    d = asdict(ctx) if hasattr(ctx, "__dataclass_fields__") else dict(ctx)
    return json.dumps(d, default=str)
