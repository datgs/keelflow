"""Static guards for the progress-heartbeat demo (no Postgres needed).

The demo runs a worker that claims jobs, so it must never default into
a shared application database or the application's Chancy prefix — an
application's pending dispatch rows would be claimed by a worker that
only registers demo job types and fail 'unregistered type'.
"""
from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

import pytest

DEMO_PATH = Path(__file__).with_name("progress_heartbeat.py")


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "keelflow_progress_heartbeat_demo", DEMO_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_demo_requires_isolated_dsn(monkeypatch):
    """No default database: running without KEELFLOW_DEMO_DSN exits
    before touching any connection."""
    monkeypatch.delenv("KEELFLOW_DEMO_DSN", raising=False)
    mod = _load_module()
    with pytest.raises(SystemExit, match="KEELFLOW_DEMO_DSN"):
        asyncio.run(mod.main())


def test_demo_uses_dedicated_kernel_prefix():
    mod = _load_module()
    assert mod.DEMO_KERNEL_PREFIX == "keelflow_demo_kernel_"
    # Never the application prefix — kernel tables are prefix-namespaced.
    assert mod.DEMO_KERNEL_PREFIX != "keelflow_kernel_"
