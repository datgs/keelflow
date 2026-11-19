"""PermissionService (T14) — actor-aware RBAC hooks.

`require(ctx, permission)` resolves the actor from `ctx` (never global
state). Actors:

- `system` bypasses (the engine must run).
- `job` inherits the permissions of its `via` actor — a job can never
  exceed the rights of whoever triggered it.
- `user`/`service` must hold the permission (or the `*` wildcard).

`scope_filter` maps a permission to its scope: `all` or `own` (the caller
adds `created_by == actor.id` to queries). Grants come from
`ctx.permissions` entries shaped `"perm"`, `"perm:all"`, or `"perm:own"`;
a bare `"perm"` or `"perm:*"` means `all`.
"""
from __future__ import annotations

from ..sdk import (
    Actor,
    ActorType,
    EntityRef,
    JobContext,
    PermissionService,
    ScopeFilter,
)


class DefaultPermissionService(PermissionService):
    """Convention-based service; swap for project-specific grant models."""

    def require(
        self, ctx: JobContext, permission: str, entity: EntityRef | None = None
    ) -> None:
        actor = _effective_actor(ctx.actor)
        if actor.type == ActorType.SYSTEM:
            return
        perms = ctx.permissions
        if (
            "*" in perms
            or permission in perms
            or f"{permission}:all" in perms
            or f"{permission}:own" in perms
        ):
            return
        raise PermissionError(
            f"actor {actor.type.value}:{actor.id} lacks {permission}"
        )

    def scope_filter(self, ctx: JobContext, permission: str) -> ScopeFilter:
        actor = _effective_actor(ctx.actor)
        if actor.type == ActorType.SYSTEM or "*" in ctx.permissions:
            return ScopeFilter(scope="all", actor_id=actor.id)
        if f"{permission}:own" in ctx.permissions and (
            permission not in ctx.permissions
            and f"{permission}:all" not in ctx.permissions
        ):
            return ScopeFilter(scope="own", actor_id=actor.id)
        return ScopeFilter(scope="all", actor_id=actor.id)


def _effective_actor(actor: Actor) -> Actor:
    """A job actor acts with its via actor's identity for permission checks."""
    while actor.type == ActorType.JOB and actor.via is not None:
        actor = actor.via
    return actor
