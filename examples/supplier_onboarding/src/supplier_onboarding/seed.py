"""Seed demo data (synthetic). Run once before the app/worker.

    uv run --package keelflow-demo-supplier \
        python -m supplier_onboarding.seed
"""
from __future__ import annotations

import asyncio
import os

import psycopg

from keelflow.postgres.schema import apply_async

from .registry import DSN_ENV

_SQL_DIR = os.path.dirname(__file__)
with open(os.path.join(_SQL_DIR, "schema.sql")) as _fh:
    _DEMO_SCHEMA = _fh.read()

SUPPLIERS = [
    # name, country, tax_id, email, bank_ref — risk is derived (mock hash)
    ("acme parts gmbh", "DE", "DE-88120031", "ops@acme-parts.example", "DE89370400440532013000"),
    ("golden lotus trading co", "VN", "VN-0301458821", "sales@golden-lotus.example", "VN070400001234"),
    ("blackstone surplus llc", "US", "US-81-4209971", "desk@blackstone.example", "US021000021"),
    ("meridian foods s.a.", "AR", "AR-30-71556677-9", "export@meridian.example", "AR015005000123"),
    ("quantum alloy labs", "UK", "GB-998877665", "ap@quantum-alloy.example", "GB29NWBK601613"),
    ("red kite logistics", "PL", "PL-5261038829", "office@redkite.example", "XX0000"),  # bad bank → medium/high
]


async def main() -> None:
    dsn = os.environ[DSN_ENV]
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        await apply_async(conn)                     # engine schema
        await conn.execute(_DEMO_SCHEMA)            # demo schema
        for name, country, tax, email, bank in SUPPLIERS:
            await conn.execute(
                "INSERT INTO demo_suppliers"
                " (name, country, tax_id, contact_email, bank_ref)"
                " VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (name, country, tax, email, bank))
        await conn.commit()
    print(f"seeded {len(SUPPLIERS)} suppliers + engine schema")


if __name__ == "__main__":
    asyncio.run(main())
