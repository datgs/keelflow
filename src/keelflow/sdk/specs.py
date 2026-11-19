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


@dataclass(frozen=True)
class JobResult:
    status: Literal["done", "failed"]
    outputs: dict[str, Any] = field(default_factory=dict)  # refs only
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
    payload_max_bytes: int = 8192


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
class TransitionSpec:
    from_state: str
    to_state: str
    action: str
    guard: dict[str, Any] = field(default_factory=dict)
    creates_job: str | None = None            # job type to spawn
    triggers_workflow: str | None = None     # workflow to start
    notify: str | None = None


@dataclass(frozen=True)
class StateMachineDefinition:
    name: str                        # "entity@vN"
    entity: str
    states: tuple[str, ...]
    transitions: tuple[TransitionSpec, ...]
    initial: str
