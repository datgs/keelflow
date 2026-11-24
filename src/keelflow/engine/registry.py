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
    #: Permissions the engine itself requires at runtime; pre-registered
    #: so applications don't have to declare them.
    ENGINE_PERMISSIONS: frozenset[str] = frozenset(
        {"job.cancel", "job.signal"})

    def __init__(self) -> None:
        self._job_types: dict[str, JobTypeSpec] = {}
        self._workflows: dict[str, WorkflowDefinition] = {}
        self._state_machines: dict[str, StateMachineDefinition] = {}
        self._permissions: set[str] = set(self.ENGINE_PERMISSIONS)

    def register_permission(self, permission: str) -> None:
        """Declare a permission string that specs may reference.

        Call before registering definitions that reference it — a
        `guard.permission` naming an unregistered permission fails at
        registration, not later as a silent 403.
        """
        if not permission or not isinstance(permission, str):
            raise ValueError("permission must be a non-empty string")
        self._permissions.add(permission)

    def permissions(self) -> frozenset[str]:
        """All registered permission strings (engine + application)."""
        return frozenset(self._permissions)

    def register_job_type(self, spec: JobTypeSpec) -> None:
        if spec.concurrency.max_workers is not None:
            raise ValueError(
                f"{spec.type!r}: concurrency.max_workers is declared "
                "but not enforced by any kernel — refusing to register "
                "a silent no-op limit. Remove the field or wait for "
                "per-type concurrency support.")
        if spec.is_external_wait and not spec.type.startswith("wait."):
            raise ValueError(
                f"is_external_wait type {spec.type!r} must use the 'wait.' namespace")
        if spec.is_external_wait and spec.is_human:
            raise ValueError(
                f"type {spec.type!r} cannot be both is_external_wait and is_human")
        if spec.is_external_wait and spec.handler is not None:
            raise ValueError(
                f"is_external_wait type {spec.type!r} must not declare a handler"
                " — it is resolved by signal, not dispatched")
        self._job_types[spec.type] = spec

    def register_workflow(self, defn: WorkflowDefinition) -> None:
        self._workflows[defn.name] = defn

    def register_state_machine(self, defn: StateMachineDefinition) -> None:
        for t in defn.transitions:
            perm = (t.guard or {}).get("permission")
            if not perm:
                continue
            base = perm.split(":", 1)[0]
            if base not in self._permissions:
                raise ValueError(
                    f"state machine {defn.name!r} transition"
                    f" {t.action!r} references unregistered permission"
                    f" {perm!r} — call register_permission({base!r})"
                    " first")
        self._state_machines[defn.name] = defn

    def job_type(self, name: str) -> JobTypeSpec:
        return self._job_types[name]

    def workflow(self, name: str) -> WorkflowDefinition:
        return self._workflows[name]

    def state_machine(self, name: str) -> StateMachineDefinition:
        return self._state_machines[name]
