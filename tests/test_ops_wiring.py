"""Ops tooling explicit-DSN wiring tests (review finding 4).

`make_server(dsn)` / `make_app(dsn)` must use the storage built from
*that* DSN — never the worker-global `_get_storage()` which reads
`$KEELFLOW_DSN`. Proven by injecting a fake storage factory and calling
tools/routes with `KEELFLOW_DSN` unset; any lazy fallback raises.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from keelflow.mcp import server as mcp_server


class FakeSession:
    def __init__(self):
        self.native = None

    def begin(self):
        @asynccontextmanager
        async def _uow():
            yield object()  # opaque uow — the fake service accepts any

        return _uow()

    async def close(self):
        pass


class FakeStorage:
    """Duck-typed stand-in recording how ops services call storage."""

    def __init__(self, dsn):
        self.dsn = dsn
        self.connect_calls = 0
        self.seen_uows = []

    async def connect(self):
        self.connect_calls += 1
        return FakeSession()

    async def ops_metrics_snapshot(self, uow):
        self.seen_uows.append(uow)
        return {"queue_depth": 0, "running": 0, "failed_24h": 0,
                "oldest_pending_s": None, "dsn": self.dsn}

    async def audit_trace(self, uow, entity_type, entity_id):
        self.seen_uows.append(uow)
        return [{"action": "x", "entity_type": entity_type}]


@pytest.fixture
def fake_storage(monkeypatch):
    """Replace PostgresStorage in mcp + admin with recording fakes and
    remove KEELFLOW_DSN so any lazy env-based fallback would KeyError."""
    monkeypatch.delenv("KEELFLOW_DSN", raising=False)
    stores: list[FakeStorage] = []

    def factory(dsn):
        s = FakeStorage(dsn)
        stores.append(s)
        return s

    monkeypatch.setattr(mcp_server, "PostgresStorage", factory)
    return stores


@pytest.mark.asyncio
async def test_mcp_tools_use_explicit_dsn(fake_storage):
    import json

    app = mcp_server.make_server("postgresql://x")
    res = await app.call_tool("metrics_snapshot", {})
    metrics = json.loads(res.content[0].text)
    assert metrics["dsn"] == "postgresql://x"

    res = await app.call_tool(
        "audit_trace", {"entity_type": "order", "entity_id": "o1"})
    events = json.loads(res.content[0].text)
    assert events[0]["entity_type"] == "order"
    # session-per-tool path ran on our fake, not env storage
    assert fake_storage[0].connect_calls == 2


def test_admin_q_routes_through_explicit_storage(monkeypatch):
    """The admin request loop's `q` helper opens a storage session and
    runs the coroutine inside `session.begin()` — same for writes."""
    from keelflow.admin import app as admin_app

    monkeypatch.delenv("KEELFLOW_DSN", raising=False)
    stores = []
    monkeypatch.setattr(
        admin_app, "PostgresStorage",
        lambda dsn: stores.append(FakeStorage(dsn)) or stores[-1])

    captured = {}

    class FakeServer:
        def __init__(self, addr, handler):
            captured["handler"] = handler

        def serve_forever(self):
            pass

    monkeypatch.setattr(admin_app, "ThreadingHTTPServer", FakeServer)
    admin_app.make_app("postgresql://y")
    assert len(stores) == 1 and stores[0].dsn == "postgresql://y"
    handler_cls = captured["handler"]
    h = handler_cls.__new__(handler_cls)  # skip socket wiring in __init__
    out = _call_view(h._home)
    assert "queue depth" in out
    assert stores[0].connect_calls >= 1


def _call_view(view):
    """Run a sync view method that internally uses asyncio.run(q(coro))
    — needs a clean event loop, so run it on a fresh thread."""
    import concurrent.futures

    def run():
        return view()

    with concurrent.futures.ThreadPoolExecutor() as pool:
        return pool.submit(run).result()


def test_sdk_builder_surface_matches_protocol():
    """Public JobBuilder protocol covers every fluent setter the engine
    builder implements (review finding 5 — deduplication_key)."""
    from keelflow.engine.enqueue import JobBuilder as EngineBuilder
    from keelflow.sdk.services import JobBuilder as SdkBuilder

    engine_setters = {
        name for name in vars(EngineBuilder)
        if not name.startswith("_") and name != "enqueue"
    }
    for name in engine_setters:
        assert name in dir(SdkBuilder), (
            f"{name} implemented on engine.JobBuilder but missing "
            "from the public sdk.services.JobBuilder protocol")
