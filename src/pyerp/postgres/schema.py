"""Apply the engine schema (`schema.sql`) to a psycopg connection.

Used by tests and by the Alembic migration in `alembic/versions/`.
All statements are idempotent (`IF NOT EXISTS`).
"""
from __future__ import annotations

from pathlib import Path

SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def schema_sql() -> str:
    return SCHEMA_PATH.read_text()


def apply_sync(conn) -> None:
    """Apply schema on a psycopg (sync) connection; commits."""
    with conn.cursor() as cur:
        cur.execute(schema_sql())
    conn.commit()


async def apply_async(conn) -> None:
    """Apply schema on a psycopg AsyncConnection; commits."""
    async with conn.cursor() as cur:
        await cur.execute(schema_sql())
    await conn.commit()
