"""In-memory Registry implementation.

Full schema validation at enqueue/execution time lives with the builder
(validate on `enqueue`) and worker (validate before `run`).
"""
from __future__ import annotations

from ..sdk import (
    JobTypeSpec,
    Registry,
    StateMachineDefinition,
    WorkflowDefinition,
)


class InMemoryRegistry(Registry):
    def __init__(self) -> None:
        self._job_types: dict[str, JobTypeSpec] = {}
        self._workflows: dict[str, WorkflowDefinition] = {}
        self._state_machines: dict[str, StateMachineDefinition] = {}

    def register_job_type(self, spec: JobTypeSpec) -> None:
        if spec.is_wait and not spec.type.startswith("wait."):
            raise ValueError(
                f"is_wait type {spec.type!r} must use the 'wait.' namespace")
        if spec.is_wait and spec.is_human:
            raise ValueError(
                f"type {spec.type!r} cannot be both is_wait and is_human")
        if spec.is_wait and spec.handler is not None:
            raise ValueError(
                f"is_wait type {spec.type!r} must not declare a handler"
                " — it is resolved by signal, not dispatched")
        self._job_types[spec.type] = spec

    def register_workflow(self, defn: WorkflowDefinition) -> None:
        self._workflows[defn.name] = defn

    def register_state_machine(self, defn: StateMachineDefinition) -> None:
        self._state_machines[defn.name] = defn

    def job_type(self, name: str) -> JobTypeSpec:
        return self._job_types[name]

    def workflow(self, name: str) -> WorkflowDefinition:
        return self._workflows[name]

    def state_machine(self, name: str) -> StateMachineDefinition:
        return self._state_machines[name]
