"""Semantic storage port — Keelflow-owned durable state.

One canonical contract for everything the engine persists: jobs,
workflow runs/steps, bulk chunks/rows, entity state, outbox, audit,
scheduler/ops reads, and DLQ entries. Operations receive an open
`UnitOfWork` and never commit — the caller owns the boundary via
`session.begin()`. Backend-specific queries (SQL, Mongo filters)
live only inside adapter implementations.

Signatures are deliberately semantic, not query-shaped: no SQL text,
no cursors, no driver types cross this interface. Read methods take
`uow` too — session-bound backends (Mongo) need the session context
even for reads.

Supersedes `sdk.services.Storage` (defer/claim stubs), which is removed
as part of the storage-port migration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from .services import AuditEvent, JobStatus
from .uow import Session, UnitOfWork


def json_default(o: Any) -> Any:
    """JSON fallback for engine payloads (Enum, dataclass, set, datetime).

    Adapters use this when serializing `payload`/`context`/`outputs`/
    `result` so values written through the port survive round-trips.
    """
    from dataclasses import asdict, is_dataclass
    from enum import Enum

    if isinstance(o, Enum):
        return o.value
    if is_dataclass(o) and not isinstance(o, type):
        return asdict(o)
    if isinstance(o, frozenset | set):
        return list(o)
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


class ExecutionLostError(Exception):
    """The current dispatch no longer owns this job's execution.

    Raised by fenced storage writes when the row's `execution_token` no
    longer matches the token handed to this dispatch — a newer claim,
    reschedule, or ops reset took over. Workers treat it as
    "abandon the attempt", never as a retryable handler failure.
    """


# ── write payloads ────────────────────────────────────────────────────


@dataclass(frozen=True)
class JobInsert:
    """New engine job row. Mirrors `keelflow_jobs` semantics without
    exposing the schema."""

    id: str
    type: str
    payload: dict[str, Any]
    context: dict[str, Any]
    status: str = "pending"
    max_attempts: int = 3
    run_at: datetime | None = None
    concurrency_key: str | None = None
    assignee: str | None = None
    due_at: datetime | None = None
    parent_job_id: str | None = None
    unique_key: str | None = None
    cancel_requested: bool = False


@dataclass(frozen=True)
class JobFinish:
    """Terminal/intermediate finish write on a job row."""

    status: JobStatus
    result: dict[str, Any] | None = None
    error: str | None = None
    result_bytes: int | None = None


@dataclass(frozen=True)
class DlqInsert:
    id: str
    type: str
    payload: dict[str, Any]
    context: dict[str, Any]
    error: str
    attempts: int
    run_id: str | None = None
    step_run_id: str | None = None


@dataclass(frozen=True)
class RowArchiveResult:
    mode: str  # archive | delete | keep
    moved: int = 0
    summary: dict[str, Any] = field(default_factory=dict)


# ── the port ──────────────────────────────────────────────────────────


@runtime_checkable
class Storage(Protocol):
    """Semantic persistence port for engine-owned state."""

    dsn: str

    async def connect(self) -> Session:
        """Open a backend session (one connection/client). The caller
        owns its lifecycle — `session.close()`."""
        ...

    # ── job lifecycle ────────────────────────────────────────────

    async def job_insert(self, uow: UnitOfWork, job: JobInsert) -> str | None:
        """Insert a job. Returns the new id, or the id of a live job
        with the same (type, unique_key) — the dedup contract."""
        ...

    async def job_insert_many(self, uow: UnitOfWork, jobs: list[JobInsert]) -> int: ...

    async def job_load(self, uow: UnitOfWork, job_id: str) -> dict[str, Any] | None:
        """Full row for dispatch: type, status, payload, context,
        concurrency_key, cancel_requested, attempts, max_attempts."""
        ...

    async def job_mark_running(
        self,
        uow: UnitOfWork,
        job_id: str,
        *,
        progress_timeout_seconds: float | None = None,
        claim_token: str | None = None,
    ) -> str | None:
        """Atomic transition to `running`, only when cancel_requested
        is false. None = not startable.

        Two claim models share this contract:

        - `claim_token=None` (engine-owned claim — chancy/dispatch):
          pending or running → running, mints a fresh non-reusable
          `execution_token` and increments `attempts` (the attempt is
          counted when the dispatch starts, not per kernel delivery).
          A `claimed` row carrying a token belongs to a kernel claim
          and is not hijacked.
        - `claim_token` supplied (kernel-owned claim — native): the
          claim already minted the token and consumed the attempt;
          this only validates the claim still owns the row
          (`status='claimed' AND execution_token=claim_token`) and
          flips to running. A dispatch whose claim was reclaimed
          (token cleared/replaced) gets None and must not run.

        When `progress_timeout_seconds` is set, the attempt's progress
        clock (`progress_at`/`progress_timeout_seconds`, `progress`
        cleared) is initialized in the same unit so monitoring and
        fenced reports observe it after the running transition commits.
        Returns the owning token on success."""
        ...

    async def job_cancel_requested(self, uow: UnitOfWork, job_id: str) -> bool: ...

    async def job_progress_report(
        self, job_id: str, execution_token: str, progress: dict[str, Any]
    ) -> bool:
        """Persist progress for the current running attempt.

        Commits on its own connection/transaction — never joins or
        commits the handler's business UoW. Returns False when the
        execution token no longer owns the job (a newer claim, a
        reschedule, or an ops reset took over) or the job left
        `running`; callers should treat False as a lost execution,
        not a successful report. Deadline enforcement is the in-process
        watchdog, not this write."""
        ...

    async def job_finish(
        self,
        uow: UnitOfWork,
        job_id: str,
        finish: JobFinish,
        *,
        execution_token: str | None = None,
    ) -> None:
        """Terminal/intermediate finish write.

        When `execution_token` is supplied the update is fenced to the
        owning claim — zero matching rows raises `ExecutionLostError`
        so the caller's UoW rolls back its business writes instead of
        committing a stale result."""
        ...

    async def job_cancel_request(self, uow: UnitOfWork, job_id: str) -> str | None:
        """Flag cooperative cancel; pending → cancelled immediately.
        Returns the post-update status, or None when the job was not
        live (terminal or missing)."""
        ...

    async def job_cancel_request_descendants(
        self, uow: UnitOfWork, job_id: str
    ) -> None:
        """Cascade the cancel request to non-terminal children."""
        ...

    async def job_error_and_reschedule(
        self,
        uow: UnitOfWork,
        job_id: str,
        error: str,
        delay_s: float,
        *,
        execution_token: str | None = None,
    ) -> None:
        """Record a retryable failure, defer run_at, and release the
        claim token. Token-fenced like `job_finish` — a stale dispatch
        must not reschedule a job a newer claim owns."""
        ...

    async def job_retry_reset(self, uow: UnitOfWork, job_id: str) -> int:
        """Ops action: failed → pending, attempts=0. Rows changed."""
        ...

    async def job_attempts(
        self, uow: UnitOfWork, job_id: str
    ) -> tuple[int, int] | None:
        """(attempts, max_attempts) for retry-exhaustion decisions."""
        ...

    async def job_lock(self, uow: UnitOfWork, job_id: str) -> dict[str, Any] | None:
        """Row-lock a job for a read-modify-write in this UoW."""
        ...

    async def job_signal_wait(
        self,
        uow: UnitOfWork,
        job_id: str,
        status: JobStatus,
        result: dict[str, Any],
        error: str | None,
    ) -> None:
        """Resolve an open wait.* job. Caller validated type/status."""
        ...

    async def job_type_of(self, uow: UnitOfWork, job_id: str) -> str | None: ...

    async def job_parent_cancel_flag(self, uow: UnitOfWork, job_id: str) -> bool: ...

    # ── human tasks (jobs subset) ────────────────────────────────

    async def tasks_open_for(
        self, uow: UnitOfWork, actor_id: str, roles: list[str]
    ) -> list[dict[str, Any]]: ...

    async def task_update(
        self,
        uow: UnitOfWork,
        job_id: str,
        *,
        status: str | None = None,
        claimed_by: str | None = None,
        clear_claim: bool = False,
        result: dict[str, Any] | None = None,
        assignee: str | None = None,
    ) -> None: ...

    async def tasks_escalate_overdue(
        self, uow: UnitOfWork, older_than_days: int, to_role: str
    ) -> int: ...

    # ── scheduler ────────────────────────────────────────────────

    async def jobs_live_by_type(
        self, uow: UnitOfWork, type_name: str
    ) -> list[dict[str, Any]]:
        """Non-terminal jobs of a type — scheduler overlap policy."""
        ...

    # ── workflow runs / steps ────────────────────────────────────

    async def run_insert(
        self,
        uow: UnitOfWork,
        run_id: str,
        workflow: str,
        inputs: dict[str, Any],
        *,
        triggered_by_entity_type: str | None = None,
        triggered_by_entity_id: str | None = None,
        trigger_config: dict[str, Any] | None = None,
        trigger_ctx: dict[str, Any] | None = None,
    ) -> None: ...

    async def step_runs_insert(
        self,
        uow: UnitOfWork,
        run_id: str,
        steps: list[tuple[str, str]],
    ) -> None:
        """Insert pending step_runs: [(step_id, type)]."""
        ...

    async def run_get(
        self, uow: UnitOfWork, run_id: str, *, for_update: bool = False
    ) -> dict[str, Any] | None: ...

    async def steps_get(self, uow: UnitOfWork, run_id: str) -> list[dict[str, Any]]: ...

    async def step_set_status(
        self, uow: UnitOfWork, step_run_id: str, status: str
    ) -> None: ...

    async def step_attach_job(
        self,
        uow: UnitOfWork,
        step_run_id: str,
        job_id: str,
        status: str,
    ) -> None: ...

    async def step_settle(
        self,
        uow: UnitOfWork,
        step_run_id: str,
        status: str,
        outputs: dict[str, Any],
    ) -> None:
        """Record the finished job's outputs + status on its step."""
        ...

    async def step_fail(self, uow: UnitOfWork, step_run_id: str, error: str) -> None:
        """Mark a step failed with `{"error": ...}` outputs — the
        advance-time failure isolation path."""
        ...

    async def step_reset(self, uow: UnitOfWork, step_run_id: str) -> None:
        """pending + job_id cleared + attempts+1 — resume path."""
        ...

    async def run_set_status(
        self, uow: UnitOfWork, run_id: str, status: str
    ) -> None: ...

    async def run_finish(
        self,
        uow: UnitOfWork,
        run_id: str,
        status: str,
        terminal_outcome: str,
    ) -> None: ...

    async def run_reopen(self, uow: UnitOfWork, run_id: str) -> None:
        """running + terminal_outcome cleared + callback_state=pending
        — resume path."""
        ...

    async def run_mark_callback_done(self, uow: UnitOfWork, run_id: str) -> None: ...

    async def step_run_by_job(self, uow: UnitOfWork, job_id: str) -> str | None:
        """step_run id attached to a job, if any."""
        ...

    async def step_run_get(
        self, uow: UnitOfWork, step_run_id: str
    ) -> dict[str, Any] | None: ...

    async def step_run_for_update(
        self, uow: UnitOfWork, run_id: str, step_id: str
    ) -> dict[str, Any] | None: ...

    async def steps_resumable(
        self, uow: UnitOfWork, run_id: str
    ) -> list[dict[str, Any]]:
        """Failed steps plus done-steps whose outputs rejected — the
        resume_run target set."""
        ...

    async def trigger_callback_lock(
        self, uow: UnitOfWork, run_id: str
    ) -> dict[str, Any] | None:
        """Row-lock run for the exactly-once trigger callback check."""
        ...

    async def run_parent_ctx(
        self, uow: UnitOfWork, run_id: str
    ) -> dict[str, Any] | None:
        """Serialized JobContext dict of the run's first step job — the
        ctx children inherit on resume/advance."""
        ...

    # ── bulk ─────────────────────────────────────────────────────

    async def bulk_pending_count(self, uow: UnitOfWork, job_id: str) -> int:
        """Unchunked pending rows — partition input."""
        ...

    async def bulk_partition(
        self,
        uow: UnitOfWork,
        job_id: str,
        chunk_size: int,
        chunk_ids: list[Any],
    ) -> tuple[int, int]:
        """Claim pending unchunked rows, create the pre-generated
        chunks, stamp rows with their chunk id. Returns
        (rows_mapped, chunks_created)."""
        ...

    async def chunk_claim(
        self, uow: UnitOfWork, chunk_id: str, claimed_by: str
    ) -> None: ...

    async def rows_reset_processing(self, uow: UnitOfWork, chunk_id: str) -> None: ...

    async def rows_fetch_pending(
        self, uow: UnitOfWork, chunk_id: str
    ) -> list[dict[str, Any]]: ...

    async def rows_mark(
        self,
        uow: UnitOfWork,
        row_ids: list[Any],
        status: str,
        *,
        error_by_id: dict[Any, str] | None = None,
    ) -> None: ...

    async def chunk_mark(
        self,
        uow: UnitOfWork,
        chunk_id: str,
        status: str,
        result: dict[str, Any] | None = None,
    ) -> None: ...

    async def chunk_cancel(
        self, uow: UnitOfWork, chunk_id: str
    ) -> None:
        """Mark cancelled only when not already terminal — the
        settle-cancelled-chunk path is idempotent."""
        ...

    async def chunk_counts(self, uow: UnitOfWork, job_id: str) -> dict[str, int]: ...

    async def row_counts(self, uow: UnitOfWork, job_id: str) -> dict[str, int]: ...

    async def row_disposition(self, uow: UnitOfWork, job_id: str) -> str: ...

    async def rows_archive_done(self, uow: UnitOfWork, job_id: str) -> int:
        """Move done rows to archive (idempotent). Rows moved."""
        ...

    async def rows_delete_done(self, uow: UnitOfWork, job_id: str) -> int: ...

    async def job_result_merge(
        self,
        uow: UnitOfWork,
        job_id: str,
        status: str,
        patch: dict[str, Any],
    ) -> dict[str, Any]:
        """Terminalize parent and merge the fan-in summary into its
        result. Returns the merged result."""
        ...

    # ── state machine / audit ────────────────────────────────────

    async def entity_ensure_locked(
        self,
        uow: UnitOfWork,
        entity_type: str,
        entity_id: str,
        initial: str,
    ) -> str:
        """Create the entity row if absent, then row-lock it and
        return the current state — first-transition race safety."""
        ...

    async def entity_transition(
        self,
        uow: UnitOfWork,
        entity_type: str,
        entity_id: str,
        to_state: str,
    ) -> None:
        """Set state + bump version (conflict-safe upsert)."""
        ...

    async def entity_state(
        self, uow: UnitOfWork, entity_type: str, entity_id: str
    ) -> str | None: ...

    async def audit_record(self, uow: UnitOfWork, event: AuditEvent) -> None: ...

    async def audit_trace(
        self, uow: UnitOfWork, entity_type: str, entity_id: str
    ) -> list[dict[str, Any]]: ...

    # ── events / outbox ──────────────────────────────────────────

    async def outbox_insert(
        self, uow: UnitOfWork, event_type: str, payload: dict[str, Any]
    ) -> None: ...

    async def outbox_claim_next(self, uow: UnitOfWork) -> dict[str, Any] | None:
        """Claim one pending row (skip-locked semantics)."""
        ...

    async def delivery_seen(
        self, uow: UnitOfWork, outbox_id: Any, subscriber: str
    ) -> bool: ...

    async def delivery_mark(
        self, uow: UnitOfWork, outbox_id: Any, subscriber: str
    ) -> None: ...

    async def outbox_settle(self, uow: UnitOfWork, outbox_id: Any, status: str) -> None:
        """published → terminal; failed → attempts+1 (caller chose)."""
        ...

    # ── monitoring / ops ─────────────────────────────────────────

    async def ops_runs_list(
        self, uow: UnitOfWork, *, limit: int = 50
    ) -> list[dict[str, Any]]: ...

    async def ops_jobs_list(
        self,
        uow: UnitOfWork,
        *,
        types: list[str] | None = None,
        statuses: list[str] | None = None,
        limit: int = 50,
        cursor: Any | None = None,
    ) -> dict[str, Any]: ...

    async def ops_rows_failed(
        self, uow: UnitOfWork, job_id: str
    ) -> list[dict[str, Any]]: ...

    async def ops_rows_archived(
        self,
        uow: UnitOfWork,
        job_id: str,
        *,
        limit: int = 50,
        after_id: Any | None = None,
    ) -> list[dict[str, Any]]: ...

    async def ops_results_oversized(
        self, uow: UnitOfWork, *, threshold: int = 32768, limit: int = 50
    ) -> list[dict[str, Any]]: ...

    async def ops_stale_progress(self, uow: UnitOfWork) -> list[dict[str, Any]]: ...

    async def ops_metrics_snapshot(self, uow: UnitOfWork) -> dict[str, Any]:
        """Queue depth / running / failed24h + optional
        backend-specific worker-count metric (may be None)."""
        ...

    async def ops_sweep(
        self,
        uow: UnitOfWork,
        *,
        jobs_days: float | None,
        outbox_days: float | None,
        archive_days: float | None,
        dlq_days: float | None,
        batch: int,
    ) -> dict[str, int]: ...

    # ── dlq ──────────────────────────────────────────────────────

    async def dlq_insert(self, uow: UnitOfWork, entry: DlqInsert) -> None:
        """Conflict-safe upsert keyed by entry id."""
        ...

    async def dlq_list(
        self, uow: UnitOfWork, *, limit: int = 50
    ) -> list[dict[str, Any]]: ...

    async def dlq_get(self, uow: UnitOfWork, dlq_id: str) -> dict[str, Any] | None: ...

    # ── process liveness (observability) ─────────────────────────

    async def heartbeat_upsert(
        self,
        uow: UnitOfWork,
        name: str,
        kind: str,
        *,
        stale_after_s: float = 30.0,
    ) -> None:
        """Record a worker/scheduler liveness tick inside `uow`.

        `stale_after_s` declares the freshness window monitoring uses
        for this process — pick it with headroom over the heartbeat
        cadence so a healthy process is not reported stale by delay."""
        ...
