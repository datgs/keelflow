"""invoice_flow UI locale contract test — TestClient, real Postgres.

Covers: ?lang= beats the kf_lang cookie, unsupported locales fall back to
English, the cookie persists the chosen locale, identifiers/values are
never translated, and all five locales render.

Requires Postgres at KEELFLOW_TEST_DSN (skipped when unavailable).
"""
from __future__ import annotations

import asyncio
import os

import psycopg
import pytest
from fastapi.testclient import TestClient
from invoice_flow.registry import DSN_ENV

DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow")
KERNEL_PREFIX = "keelflow_test_"
os.environ.setdefault(DSN_ENV, DSN)

LOCALES = ("en", "vi", "es", "zh-Hans", "hi")


async def _migrate_kernel() -> None:
    from chancy import Chancy
    async with Chancy(DSN, prefix=KERNEL_PREFIX) as k:
        await k.migrate()


@pytest.fixture(scope="module")
def client():
    try:
        conn = psycopg.connect(DSN, autocommit=True)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unavailable: {exc}")
    with conn:
        from keelflow.postgres.schema import apply_sync
        apply_sync(conn)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS demo_invoices ("
            " number TEXT PRIMARY KEY, vendor TEXT NOT NULL,"
            " amount NUMERIC(12,2) NOT NULL,"
            " currency TEXT NOT NULL DEFAULT 'USD',"
            " fail_post BOOLEAN NOT NULL DEFAULT FALSE,"
            " created_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS demo_ledger_postings ("
            " id BIGSERIAL PRIMARY KEY,"
            " invoice_no TEXT NOT NULL UNIQUE,"
            " amount NUMERIC(12,2) NOT NULL, posted_by TEXT NOT NULL,"
            " posted_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS demo_gl_records ("
            " invoice_no TEXT PRIMARY KEY, journal_ref TEXT NOT NULL,"
            " recorded_by TEXT NOT NULL,"
            " recorded_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        conn.execute(
            "INSERT INTO demo_invoices (number, vendor, amount)"
            " VALUES ('INV-9001','Test Vendor', 100.00)"
            " ON CONFLICT (number) DO NOTHING")
    asyncio.run(_migrate_kernel())

    from invoice_flow.app import app
    with TestClient(app, base_url="http://test") as c:
        yield c


def test_default_locale_is_english(client):
    r = client.get("/?as=u1")
    assert r.status_code == 200
    assert '<html lang="en">' in r.text
    assert "New invoice" in r.text
    # data ids are not translated
    assert "INV-9001" in r.text


def test_query_param_selects_locale_and_sets_cookie(client):
    client.cookies.delete("kf_lang")
    r = client.get("/?as=u1&lang=vi")
    assert r.status_code == 200
    assert '<html lang="vi">' in r.text
    assert "Hóa đơn mới" in r.text
    assert r.cookies.get("kf_lang") == "vi"
    # identifiers and fixture values stay untranslated
    assert "INV-9001" in r.text
    assert "?as=u1" in r.text


def test_cookie_persists_locale(client):
    client.cookies.clear()
    client.cookies.set("kf_lang", "es")
    r = client.get("/?as=u1")  # no ?lang= — cookie wins
    assert '<html lang="es">' in r.text
    assert "Nueva factura" in r.text
    client.cookies.delete("kf_lang")


def test_unsupported_locale_falls_back_to_english(client):
    r = client.get("/?as=u1&lang=ar")
    assert r.status_code == 200
    assert '<html lang="en">' in r.text
    assert "New invoice" in r.text


def test_all_locales_render(client):
    for lang in LOCALES:
        r = client.get(f"/?as=u1&lang={lang}")
        assert r.status_code == 200
        assert f'<html lang="{lang}">' in r.text
        assert f'value="{lang}"' in r.text  # selector offers every locale


def test_inbox_renders_in_locale(client):
    r = client.get("/inbox?as=fin1&lang=zh-Hans")
    assert r.status_code == 200
    assert '<html lang="zh-Hans">' in r.text
    assert "收件箱" in r.text
