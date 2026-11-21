"""Definition specs — job types, workflows, state machines."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel


class JobStatus(str, Enum):
    """Strict job-state enum. No custom states."""

    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"  # waiting on `needs` steps or a human task


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff: Literal["exponential", "fixed"] = "exponential"
    base_seconds: float = 30.0
    jitter: bool = True


@dataclass(frozen=True)
class ConcurrencySpec:
    max_workers: int | None = None
    # Path inside payload used to serialize: "payload.sku_id"
    key: str | None = None


@dataclass(frozen=True)
class CronSpec:
    cron: str                       # "0 2 * * *"
    timezone: str = "UTC"
    window: str | None = None       # "22:00-06:00" allowed hours
    overlap: Literal["skip", "queue", "kill_previous"] = "skip"


@dataclass(frozen=True)
class TaskUiSpec:
    """UI hints for human.* task types (level 1/2/3)."""
    route: str | None = None                    # level 1 deep link
    quick_actions: tuple[dict[str, Any], ...] = ()  # level 2 buttons
    form_schema: dict[str, Any] | None = None       # level 3 embedded form
    display_fields: tuple[str, ...] = ()            # summary card fields
    assignee_role: str | None = None                # role:X inbox assignment
    due_in_seconds: float | None = None             # sets keelflow_jobs.due_at


@dataclass(frozen=True)
class JobResult:
    status: Literal["done", "failed"]
    outputs: dict[str, Any] = field(default_factory=dict)  # refs only
    error: str | None = None


@dataclass(frozen=True)
class BatchRowResult:
    """Outcome for one row inside a `process_batch` callback.

    `row_id` is the engine-owned `keelflow_row_status.id` handed to the
    callback — NOT the application `row_key` (which carries business
    meaning but is not unique). A result list must contain exactly one
    entry per input row; missing/duplicate/unknown row_ids are a
    malformed batch and roll the whole chunk back for retry.

    `error=None` → row done; non-empty error → row failed into the
    error queue while sibling rows still commit.
    """

    row_id: int
    outputs: dict[str, Any] | None = None   # not persisted; for handler use
    error: str | None = None


@dataclass(frozen=True)
class JobTypeSpec:
    """Registered contract for one job type."""

    type: str                                    # "domain.action@vN"
    input_schema: type[BaseModel]  # type: ignore[valid-type]
    output_schema: type[BaseModel] | None = None  # type: ignore[valid-type]
    handler: type[Any] = None  # type: ignore[valid-type]  # subclass of JobHandler
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    concurrency: ConcurrencySpec = field(default_factory=ConcurrencySpec)
    schedule: CronSpec | None = None
    ui: TaskUiSpec | None = None
    is_human: bool = False                       # human.* vs machine
    # External-wait job: workflow materialization creates a durable
    # blocked job row with NO kernel claim; the application resolves it
    # later via `signal`/`signal_failure` (e.g. a vendor callback after
    # its own auth + correlation). Requires a `wait.*` type name and is
    # mutually exclusive with `is_human`.
    is_wait: bool = False
    payload_max_bytes: int = 8192
    # Size visibility on handler outputs (two-tier):
    #   result_max_bytes — hard cap; None = unlimited (default, opt-in).
    #   result_warning_bytes — soft threshold; exceeded → jlog warning +
    #   the size is stored in result_bytes/outputs_bytes for ops queries.
    #   Never fails the job. None disables the warning.
    result_max_bytes: int | None = None
    result_warning_bytes: int | None = 32768
    # kill the handler coroutine after N seconds; counts as a failed
    # attempt (retryable until max_attempts).
    timeout_seconds: float | None = None
    # Opt in to cooperative mid-run cancellation: the handler may call
    # `await ctx.execution.checkpoint()` at safe points; a committed
    # `cancel_requested` flag observed there raises JobCancelled → the
    # handler transaction rolls back and the job terminalizes as
    # 'cancelled' (no retry, no DLQ). Without opt-in, checkpoints raise
    # RuntimeError and running jobs drain to done/failed.
    cooperative_cancel: bool = False


@dataclass(frozen=True)
class StepSpec:
    id: str
    type: str
    input: dict[str, Any] = field(default_factory=dict)
    needs: tuple[str, ...] = ()
    if_expr: str | None = None


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str                        # "domain.name@vN"
    steps: tuple[StepSpec, ...]
    inputs_schema: type[BaseModel] | None = None


@dataclass(frozen=True)
class WorkflowTriggerSpec:
    """Typed trigger contract for a state-machine transition.

    When the transition runs, the named workflow starts with the entity
    recorded on the run (`triggered_by_entity_*`). When the run reaches a
    terminal outcome the engine applies the matching callback action to
    the entity — in the same transaction that finalizes the run.

    Outcomes: `on_done` (run finished), `on_rejected` (a human step
    completed with `_rejected` marker — business rejection, not a
    technical failure), `on_failed` (retry exhaustion). `failure_job`
    is an optional human task type spawned on `on_failed`.
    """

    workflow: str                            # "name@vN"
    on_done: str | None = None               # SM action on run success
    on_rejected: str | None = None           # SM action on business reject
    on_failed: str | None = None             # SM action on tech failure
    failure_job: str | None = None           # human.* type on on_failed


@dataclass(frozen=True)
class TransitionSpec:
    from_state: str
    to_state: str
    action: str
    guard: dict[str, Any] = field(default_factory=dict)
    creates_job: str | None = None           # job type to spawn
    triggers_workflow: str | WorkflowTriggerSpec | None = None
    notify: str | None = None


@dataclass(frozen=True)
class StateMachineDefinition:
    name: str                        # "entity@vN"
    entity: str
    states: tuple[str, ...]
    transitions: tuple[TransitionSpec, ...]
    initial: str
