"""Seed demo data: domain schema, keelflow schema, a few invoices.

    uv run python -m invoice_flow.seed
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

import psycopg

from keelflow.postgres.schema import apply_async

DSN = os.environ.get(
    "KEELFLOW_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow")

DEMO_SCHEMA = (Path(__file__).parent / "schema.sql").read_text()

SEED_INVOICES = [
    ("INV-1001", "Acme Office Supplies", 1240.00, "USD"),
    ("INV-1002", "Globex Logistics", 860.50, "USD"),
    ("INV-1003", "Initech Catering", 312.75, "USD"),
]


async def main() -> None:
    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        await conn.execute(DEMO_SCHEMA)
        await apply_async(conn)
        for num, vendor, amount, cur in SEED_INVOICES:
            await conn.execute(
                "INSERT INTO demo_invoices (number, vendor, amount, currency)"
                " VALUES (%s, %s, %s, %s)"
                " ON CONFLICT (number) DO NOTHING",
                (num, vendor, amount, cur),
            )
            await conn.execute(
                "INSERT INTO keelflow_entity_state"
                " (entity_type, entity_id, state)"
                " VALUES ('invoice', %s, 'draft')"
                " ON CONFLICT (entity_type, entity_id) DO NOTHING",
                (num,),
            )
        await conn.commit()
    print(f"seeded {len(SEED_INVOICES)} invoices into {DSN}")


if __name__ == "__main__":
    asyncio.run(main())
