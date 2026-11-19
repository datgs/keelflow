"""Service contracts exposed to applications and workers."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Generic, Protocol, TypeVar

from pydantic import BaseModel

from .actor import Actor
from .context import JobContext
from .specs import JobResult, JobStatus

# ── Common reference / record types ────────────────────────────────────


@dataclass(frozen=True)
class JobRef:
    id: str


@dataclass(frozen=True)
class RunRef:
    id: str


@dataclass(frozen=True)
class EntityRef:
    type: str
    id: str


@dataclass(frozen=True)
class DomainEvent:
    type: str                    # "invoice.matched@v1"
    payload: dict[str, Any]
    occurred_at: datetime | None = None


@dataclass(frozen=True)
class AuditEvent:
    actor: Actor
    action: str                  # same namespace as permissions
    entity: EntityRef
    changes: dict[str, tuple[Any, Any]] = field(default_factory=dict)  # before→after
    source: str | None = None    # job_id | request_id
    correlation_id: str = ""


@dataclass(frozen=True)
class ScopeFilter:
    """Result of scope resolution for a permission.

    When `own`, callers must add `created_by == actor_id` to queries.
    """

    scope: str  # "all" | "own"
    actor_id: str


@dataclass(frozen=True)
class Job:
    id: str
    type: str
    status: JobStatus
    payload: dict[str, Any]
    context: JobContext
    attempts: int = 0
    max_attempts: int = 3
    run_at: datetime | None = None
    claimed_by: str | None = None
    lease_until: datetime | None = None
    concurrency_key: str | None = None
    assignee: str | None = None
    parent_job_id: str | None = None
    result: dict[str, Any] | None = None
    created_at: datetime | None = None


@dataclass(frozen=True)
class StepRun:
    id: str
    step_id: str
    status: JobStatus
    job_id: str | None
    outputs: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkflowRun:
    id: str
    workflow: str
    status: JobStatus
    inputs: dict[str, Any]
    steps: tuple[StepRun, ...] = ()


T = TypeVar("T")


@dataclass(frozen=True)
class Page(Generic[T]):
    items: list[T]
    total: int
    next_cursor: str | None = None


@dataclass(frozen=True)
class JobFilter:
    types: tuple[str, ...] = ()
    statuses: tuple[JobStatus, ...] = ()
    assignee: str | None = None
    limit: int = 50
    cursor: str | None = None


@dataclass(frozen=True)
class RunFilter:
    workflows: tuple[str, ...] = ()
    statuses: tuple[JobStatus, ...] = ()
    limit: int = 50
    cursor: str | None = None


# ── TX abstraction ─────────────────────────────────────────────────────


class Transaction(Protocol):
    """A caller-owned transaction/connection passed to engine writes.

    Concrete: a psycopg3 ``AsyncConnection`` or ``AsyncCursor``. Engine
    writes participate in the caller's transaction — commit makes intent
    visible, rollback discards it.
    """

    async def execute(self, query: str, params: tuple | None = None): ...

    def cursor(self, *args: Any, **kwargs: Any): ...


# ── Service contracts ──────────────────────────────────────────────────


class JobBuilder(Protocol):
    """Fluent factory. `.context()` is REQUIRED — no enqueue without it."""

    def payload(self, p: BaseModel) -> JobBuilder: ...
    def context(self, ctx: JobContext) -> JobBuilder: ...
    def run_at(self, ts: datetime) -> JobBuilder: ...
    def concurrency_key(self, key: str) -> JobBuilder: ...
    def assignee(self, actor: Actor | str) -> JobBuilder: ...
    async def enqueue(self, tx: Transaction) -> JobRef: ...


class JobService(Protocol):
    def builder(self, type: str) -> JobBuilder: ...

    # lifecycle
    async def retry(self, job_id: str, ctx: JobContext) -> None: ...
    async def cancel(self, job_id: str, ctx: JobContext) -> None: ...

    # read (monitoring / Inbox)
    async def list(self, f: JobFilter) -> Page[Job]: ...  # type: ignore[valid-type]
    async def get(self, job_id: str) -> Job: ...

    # worker protocol
    async def claim_next(self, worker_id: str, types: list[str]) -> Job | None: ...  # type: ignore[valid-type]
    async def heartbeat(self, job_id: str, worker_id: str) -> None: ...
    async def complete(self, job_id: str, result: JobResult) -> None: ...


class WorkflowService(Protocol):
    async def start(
        self, name: str, inputs: dict, ctx: JobContext, tx: Transaction
    ) -> RunRef: ...
    async def get_run(self, run_id: str) -> WorkflowRun: ...
    async def list_runs(self, f: RunFilter) -> Page[WorkflowRun]: ...
    async def resume_step(self, run_id: str, step_id: str, ctx: JobContext) -> None: ...


class EventHandler(Protocol):
    async def __call__(self, event: DomainEvent, ctx: JobContext) -> None: ...


class EventBus(Protocol):
    """Transactional outbox — publish inside the same TX as the data change."""

    async def publish(self, event: DomainEvent, tx: Transaction) -> None: ...
    def subscribe(self, event_type: str, handler: EventHandler) -> None: ...


class StateMachineService(Protocol):
    async def transition(
        self, entity: EntityRef, action: str, ctx: JobContext, tx: Transaction
    ) -> None: ...
    async def current(self, entity: EntityRef) -> str: ...
    async def allowed_actions(
        self, entity: EntityRef, actor: Actor, conn: Any, *,
        permissions: frozenset[str] | None = None,
    ) -> list[str]: ...


class PermissionService(Protocol):
    def require(
        self, ctx: JobContext, permission: str, entity: EntityRef | None = None
    ) -> None: ...
    def scope_filter(self, ctx: JobContext, permission: str) -> ScopeFilter: ...


class AuditService(Protocol):
    async def record(self, event: AuditEvent, tx: Transaction) -> None: ...
    async def trace(self, entity: EntityRef) -> list[AuditEvent]: ...


class Storage(Protocol):
    """DB boundary — only flow/postgres implements this."""

    async def defer(self, job: Job, tx: Transaction) -> None: ...
    async def claim(self, worker_id: str, types: list[str], limit: int) -> list[Job]: ...
    async def heartbeat(self, job_id: str, lease_until: datetime) -> None: ...
    async def finish(self, job_id: str, result: JobResult) -> None: ...
    async def reclaim_expired(self) -> int: ...
