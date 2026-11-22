"""Seed demo data (synthetic). Run once before the app/worker.

    uv run --package keelflow-demo-financial \
        python -m financial_approval.seed
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

# (name, country, bank_ref)
VENDORS = [
    ("acme parts gmbh",        "DE", "DE89370400440532013000"),
    ("golden lotus trading",   "VN", "VN070400001234"),
    ("meridian foods s.a.",    "AR", "AR015005000123"),
    ("quantum alloy labs",     "UK", "GB29NWBK601613"),
]

# (ref, vendor_idx, ccy, amount, urgent, [line amounts…])
# 1 blank invoice_no is injected to fail one line in pay_lines.
REQUESTS = [
    ("PAY-0001", 0, "USD",  18_400.00, False, [7_000.00, 6_400.00, 5_000.00]),
    ("PAY-0002", 1, "VND",  42_000.00, True,  [20_000.00, 22_000.00]),
    ("PAY-0003", 2, "USD", 120_500.00, False,
     [60_500.00, 40_000.00, 20_000.00]),       # high → cfo+comp chain
    ("PAY-0004", 3, "USD",   9_750.00, False, [9_750.00]),
]


async def main() -> None:
    dsn = os.environ[DSN_ENV]
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        await apply_async(conn)
        await conn.execute(_DEMO_SCHEMA)
        # vendors
        for i, (name, country, bank) in enumerate(VENDORS, start=1):
            await conn.execute(
                "INSERT INTO demo_vendors (name, country, bank_ref)"
                " VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (name, country, bank))
        # requests + lines (idempotent on ref)
        for ref, vidx, ccy, amount, urgent, line_amts in REQUESTS:
            row = await (await conn.execute(
                "INSERT INTO demo_pay_requests"
                " (ref, vendor_id, currency, amount, urgent)"
                " VALUES (%s,%s,%s,%s,%s)"
                " ON CONFLICT (ref) DO NOTHING RETURNING id",
                (ref, vidx, ccy, amount, urgent))).fetchone()
            rid = row[0] if row else (await (await conn.execute(
                "SELECT id FROM demo_pay_requests WHERE ref=%s",
                (ref,))).fetchone())[0]
            for ln, amt in enumerate(line_amts, start=1):
                # blank invoice_no on the last line of PAY-0001 → one
                # per-row business error inside the bulk step
                inv = None if (ref == "PAY-0001" and ln == len(line_amts)) \
                    else f"INV-{ref}-{ln}"
                await conn.execute(
                    "INSERT INTO demo_pay_lines"
                    " (request_id, line_no, invoice_no, gl_account, amount)"
                    " VALUES (%s,%s,%s,%s,%s)"
                    " ON CONFLICT (request_id, line_no) DO NOTHING",
                    (rid, ln, inv, "6100", amt))
        await conn.commit()
    print("seeded financial_approval demo data.")


if __name__ == "__main__":
    asyncio.run(main())
