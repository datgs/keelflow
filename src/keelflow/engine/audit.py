"""AuditService (T14) — append-only audit writer in the caller's UoW.

`record` inserts an audit row inside the caller's unit of work, so an
unaudited change is impossible (the insert rolls back with the data).
`trace` reads the correlation chain for an entity.

Thin adapter over the semantic Storage port — kept as a named service so
callers declare an audit dependency without holding Storage.
"""
from __future__ import annotations

from typing import Any

from ..sdk import AuditEvent, AuditService, EntityRef, Storage


class PostgresAuditService(AuditService):
    def __init__(self, storage: Storage | None = None) -> None:
        self._storage = storage

    def _store(self) -> Storage:
        if self._storage is None:
            from .worker import _get_storage

            self._storage = _get_storage()
        return self._storage

    async def record(self, event: AuditEvent, uow) -> None:
        await self._store().audit_record(uow, event)

    async def trace(self, uow, entity: EntityRef) -> list[dict[str, Any]]:  # type: ignore[override]
        return await self._store().audit_trace(
            uow, entity.type, entity.id)
