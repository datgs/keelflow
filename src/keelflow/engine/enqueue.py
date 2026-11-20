"""Transactional enqueue pipeline (T4).

`JobService.builder(type)` returns a fluent `JobBuilder`; `.context()` is
mandatory, payload is validated at enqueue time (fail fast), and the write
happens inside the caller's transaction:

    INSERT keelflow_jobs          — engine-visible job record
    chancy.push_ex(cursor, ...)  — kernel job claimed by workers
    INSERT keelflow_outbox        — 'job.enqueued' event intent

All three share the caller's transaction: commit → job becomes visible and
a `pg_notify` wakes workers; rollback → nothing exists.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import chancy
from pydantic import BaseModel, ValidationError

from ..sdk import (
    Actor,
    JobContext,
    JobRef,
    JobStatus,
    Registry,
    Transaction,
)
from .context import ctx_to_dict

#: Kernel job func that dispatches into our handler registry. The worker (T5)
#: registers this name; kwargs carry the keelflow_jobs id.
DISPATCH_FUNC = "keelflow.engine.worker.dispatch"

DEFAULT_QUEUE = "default"


class EnqueueError(ValueError):
    """Raised for un-enqueueable jobs (missing context, bad payload, …)."""


class JobBuilder:
    """Fluent builder; one instance per enqueue."""

    def __init__(self, registry: Registry, type_name: str) -> None:
        self._registry = registry
        self._type = type_name
        self._payload: dict[str, Any] | None = None
        self._ctx: JobContext | None = None
        self._run_at: datetime | None = None
        self._concurrency_key: str | None = None
        self._unique_key: str | None = None
        self._assignee: str | None = None

    # ── fluent setters ───────────────────────────────────────────────

    def payload(self, p: BaseModel | dict[str, Any]) -> JobBuilder:
        if isinstance(p, BaseModel):
            self._payload = p.model_dump(mode="json")
        elif isinstance(p, dict):
            self._payload = p
        else:
            raise EnqueueError(f"payload must be a BaseModel or dict, got {type(p)}")
        return self

    def context(self, ctx: JobContext) -> JobBuilder:
        self._ctx = ctx
        return self

    def run_at(self, ts: datetime) -> JobBuilder:
        self._run_at = ts
        return self

    def concurrency_key(self, key: str) -> JobBuilder:
        self._concurrency_key = key
        return self

    def unique_key(self, key: str) -> JobBuilder:
        """Idempotent enqueue: at most one non-terminal job per
        (type, unique_key). A duplicate returns the existing job's
        JobRef — no second kernel push, no second outbox row."""
        self._unique_key = key
        return self

    def assignee(self, actor: Actor | str) -> JobBuilder:
        if isinstance(actor, Actor):
            self._assignee = f"{actor.type.value}:{actor.id}"
        else:
            self._assignee = actor
        return self

    # ── enqueue ──────────────────────────────────────────────────────

    async def enqueue(self, tx: Transaction, *, chancy_client: chancy.Chancy) -> JobRef:
        spec = self._registry.job_type(self._type)  # KeyError → unknown type

        if self._ctx is None:
            raise EnqueueError(".context(ctx) is required before enqueue()")

        # Enqueue-time validation: fail fast on badly-typed payloads.
        payload_dict = self._payload or {}
        try:
            spec.input_schema.model_validate(payload_dict)  # type: ignore[attr-defined]
        except ValidationError as exc:
            raise EnqueueError(
                f"payload for {self._type} failed schema validation: {exc}"
            ) from exc

        body = json.dumps(payload_dict, default=_json_default)
        if len(body.encode()) > spec.payload_max_bytes:
            raise EnqueueError(
                f"payload for {self._type} exceeds {spec.payload_max_bytes} bytes"
            )

        job_id = uuid.uuid4()
        now = datetime.now(UTC)
        cursor = _as_cursor(tx)

        # 1. engine-visible record — ON CONFLICT on the partial unique
        # index makes enqueue idempotent per (type, unique_key).
        await cursor.execute(
            """
            INSERT INTO keelflow_jobs
                (id, type, status, payload, context, attempts, max_attempts,
                 run_at, claimed_by, lease_until, concurrency_key, assignee,
                 parent_job_id, result, created_at, updated_at, unique_key)
            VALUES (%s,%s,%s,%s,%s,0,%s,%s,NULL,NULL,%s,%s,NULL,NULL,%s,%s,%s)
            ON CONFLICT (type, unique_key)
                WHERE unique_key IS NOT NULL
                  AND status IN ('pending','claimed','running','blocked')
            DO NOTHING
            RETURNING id
            """,
            (
                job_id,
                self._type,
                JobStatus.PENDING.value,
                body,
                json.dumps(ctx_to_dict(self._ctx)),
                spec.retry.max_attempts,
                self._run_at or now,
                self._concurrency_key,
                self._assignee,
                now,
                now,
                self._unique_key,
            ),
        )
        inserted = await cursor.fetchone()
        if inserted is None:
            # Duplicate non-terminal (type, unique_key): return the live
            # job's ref without pushing a second kernel job or outbox row.
            existing = await (await cursor.execute(
                "SELECT id FROM keelflow_jobs WHERE type=%s AND unique_key=%s"
                " AND status IN ('pending','claimed','running','blocked')"
                " ORDER BY created_at LIMIT 1",
                (self._type, self._unique_key),
            )).fetchone()
            return JobRef(id=str(existing["id"]))

        # 2. kernel job (claimed by workers). Same transaction → notify
        #    fires only on commit.
        await chancy_client.push_ex(
            cursor,
            chancy.Job(
                func=DISPATCH_FUNC,
                queue=DEFAULT_QUEUE,
                kwargs={"job_id": str(job_id)},
                max_attempts=spec.retry.max_attempts,
                scheduled_at=self._run_at or now,
                # NOTE: concurrency_key is NOT mapped to unique_key — it
                # serializes *execution* (advisory lock in dispatch), not
                # enqueue-time dedup.
                meta={"job_id": str(job_id), "type": self._type},
            ),
        )

        # 3. outbox intent — event dispatcher (T13) publishes on commit.
        await cursor.execute(
            """
            INSERT INTO keelflow_outbox (event_type, payload)
            VALUES ('job.enqueued', %s)
            """,
            (
                json.dumps(
                    {
                        "job_id": str(job_id),
                        "type": self._type,
                        "correlation_id": self._ctx.correlation_id,
                    }
                ),
            ),
        )

        return JobRef(id=str(job_id))


class JobService:
    """SDK-facing service; construct with the shared registry + chancy app."""

    def __init__(self, registry: Registry, chancy_client: chancy.Chancy) -> None:
        self._registry = registry
        self._chancy = chancy_client

    @property
    def registry(self) -> Registry:
        return self._registry

    @property
    def kernel(self) -> chancy.Chancy:
        return self._chancy

    def builder(self, type: str) -> JobBuilder:
        return _BoundJobBuilder(self._registry, type, self._chancy)


class _BoundJobBuilder(JobBuilder):
    """JobBuilder bound to a chancy client so enqueue(tx) matches the SDK."""

    def __init__(self, registry: Registry, type_name: str, client: chancy.Chancy) -> None:
        super().__init__(registry, type_name)
        self._client = client

    async def enqueue(self, tx: Transaction) -> JobRef:  # type: ignore[override]
        return await super().enqueue(tx, chancy_client=self._client)


# ── helpers ─────────────────────────────────────────────────────────────


def _as_cursor(tx: Transaction):
    """Return a dict_row cursor for the caller's transaction.

    chancy.push_ex requires dict_row records, so we always open a cursor
    (even if the caller passed one — its row factory may differ).
    """
    from psycopg.rows import dict_row

    if hasattr(tx, "connection"):  # AsyncCursor
        return tx.connection.cursor(row_factory=dict_row)
    if hasattr(tx, "cursor"):  # AsyncConnection
        return tx.cursor(row_factory=dict_row)
    raise EnqueueError(f"tx must be a psycopg connection or cursor, got {type(tx)}")


def _json_default(o: Any) -> Any:
    if isinstance(o, Enum):
        return o.value
    if is_dataclass(o) and not isinstance(o, type):
        return asdict(o)
    if isinstance(o, frozenset | set):
        return list(o)
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)



