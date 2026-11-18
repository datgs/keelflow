"""AuditService (T14) — append-only audit writer in the caller's TX.

`record` inserts into `hd_flow_audit` inside the caller's transaction, so
an unaudited change is impossible (the insert rolls back with the data).
`trace` reads the correlation chain for an entity.
"""
from __future__ import annotations

import json
from typing import Any

from ..sdk import AuditEvent, AuditService, EntityRef, Transaction
from .enqueue import _as_cursor


class PostgresAuditService(AuditService):
    async def record(self, event: AuditEvent, tx: Transaction) -> None:
        cursor = _as_cursor(tx)
        await cursor.execute(
            """
            INSERT INTO hd_flow_audit
                (actor, action, entity_type, entity_id, changes, source,
                 correlation_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                json.dumps(_actor(event.actor)),
                event.action,
                event.entity.type,
                event.entity.id,
                json.dumps(_changes(event.changes)),
                event.source,
                event.correlation_id or None,
            ),
        )

    async def trace(self, conn, entity: EntityRef) -> list[dict[str, Any]]:  # type: ignore[override]
        cur = await conn.execute(
            "SELECT occurred_at, actor, action, changes, source, correlation_id"
            " FROM hd_flow_audit"
            " WHERE entity_type=%s AND entity_id=%s ORDER BY id",
            (entity.type, entity.id),
        )
        return [
            {
                "occurred_at": r[0],
                "actor": r[1],
                "action": r[2],
                "changes": r[3],
                "source": r[4],
                "correlation_id": r[5],
            }
            for r in await cur.fetchall()
        ]


def _actor(actor) -> dict[str, Any]:
    d = {"type": actor.type.value, "id": actor.id}
    if actor.via:
        d["via"] = _actor(actor.via)
    return d


def _changes(changes: dict[str, tuple[Any, Any]]) -> dict[str, Any]:
    return {k: {"before": b, "after": a} for k, (b, a) in changes.items()}
