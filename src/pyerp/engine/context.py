"""JobContext (de)serialization between sdk types and jobs.context JSONB."""
from __future__ import annotations

from typing import Any

from ..sdk import Actor, ActorType, JobContext


def ctx_to_dict(ctx: JobContext) -> dict[str, Any]:
    return {
        "actor": _actor_to_dict(ctx.actor),
        "tenant_id": ctx.tenant_id,
        "correlation_id": ctx.correlation_id,
        "permissions": sorted(ctx.permissions),
        "request_id": ctx.request_id,
        "extra": ctx.extra,
    }


def ctx_from_dict(d: dict[str, Any]) -> JobContext:
    return JobContext(
        actor=_actor_from_dict(d["actor"]),
        tenant_id=d.get("tenant_id"),
        correlation_id=d.get("correlation_id", ""),
        permissions=frozenset(d.get("permissions", ())),
        request_id=d.get("request_id"),
        extra=d.get("extra") or {},
    )


def _actor_to_dict(a: Actor) -> dict[str, Any]:
    return {
        "type": a.type.value,
        "id": a.id,
        "via": _actor_to_dict(a.via) if a.via else None,
    }


def _actor_from_dict(d: dict[str, Any]) -> Actor:
    via = d.get("via")
    return Actor(
        type=ActorType(d["type"]),
        id=d["id"],
        via=_actor_from_dict(via) if via else None,
    )
