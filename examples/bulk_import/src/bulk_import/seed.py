"""Seed — engine + demo schema only. Imports are created via the UI
(upload a CSV or generate a synthetic one).

    uv run --package keelflow-demo-import python -m bulk_import.seed
"""
from __future__ import annotations

import asyncio
import os

import psycopg

from keelflow.postgres.schema import apply_async

from .registry import DSN_ENV

with open(os.path.join(os.path.dirname(__file__), "schema.sql")) as _fh:
    _DEMO_SCHEMA = _fh.read()


async def main() -> None:
    dsn = os.environ[DSN_ENV]
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        await apply_async(conn)
        await conn.execute(_DEMO_SCHEMA)
        await conn.commit()
    print("schema ready — upload a CSV in the UI")


if __name__ == "__main__":
    asyncio.run(main())
