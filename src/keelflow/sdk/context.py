"""JobContext — ambient execution context for every job."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .actor import Actor


@dataclass(frozen=True)
class JobContext:
    """Ambient data carried with every job, serialized into jobs.context.

    Payload carries business arguments (schema-validated); context carries
    who/where/trace. They are never mixed. Context is read-only in handlers —
    handlers may not mutate it; downstream data goes through step outputs.
    """

    actor: Actor
    tenant_id: str | None = None
    correlation_id: str = ""
    permissions: frozenset[str] = frozenset()
    request_id: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    # In-run cursor set by the dispatcher when the handler executes inside
    # the finish TX. Never serialized (excluded from ctx_to_dict).
    tx: Any = field(default=None, repr=False, compare=False)

    def require(self, permission: str) -> None:
        """Raise if the actor lacks a permission (service-layer check)."""
        if "*" not in self.permissions and permission not in self.permissions:
            raise PermissionError(
                f"actor {self.actor.type.value}:{self.actor.id} lacks {permission}"
            )
