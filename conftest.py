"""Shared kernel-backend plumbing for the adapter-parameterized suite.

Run the kernel-dependent tests against a specific backend:

    uv run pytest -q                      # chancy (default, unchanged)
    uv run pytest -q --kernel=native      # native Postgres backend only
    uv run pytest -q --kernel=both        # every kernel fixture ×2

Files that use the helpers below run under both backends. Tests that
exercise chancy-only internals (plugins, {prefix}jobs tables) stay on
the default path or call `require_chancy`.
"""
from __future__ import annotations

import os
import warnings
from contextlib import asynccontextmanager

import pytest

_KINDS = {"chancy": ["chancy"], "native": ["native"],
          "sqlite": ["sqlite"], "both": ["chancy", "native"],
          "all": ["chancy", "native", "sqlite"]}


def pytest_addoption(parser):
    parser.addoption(
        "--kernel", choices=list(_KINDS), default="chancy",
        help="kernel backend for adapter-parameterized tests")


def pytest_generate_tests(metafunc):
    if "kernel_kind" in metafunc.fixturenames:
        metafunc.parametrize(
            "kernel_kind", _KINDS[metafunc.config.getoption("--kernel")])


DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow_test"
)
CHANCY_PREFIX = "keelflow_test_"


def pytest_sessionstart(session):
    """Fail-closed guard: fixtures truncate keelflow_* tables, so the
    suite must run against a clearly disposable database. A custom
    `KEELFLOW_TEST_DSN` without 'test' in the database name requires
    `KEELFLOW_TEST_ALLOW_DESTRUCTIVE=1`. The default `keelflow_test`
    database is created if missing."""
    import asyncio

    from keelflow.testing import assert_safe_test_dsn, ensure_test_database

    try:
        assert_safe_test_dsn(DSN)
    except RuntimeError as exc:
        pytest.exit(str(exc), returncode=3)
    try:
        asyncio.run(ensure_test_database(DSN))
    except Exception as exc:  # noqa: BLE001 - DB may be down; tests skip
        warnings.warn(f"could not ensure test database: {exc}")


@asynccontextmanager
async def open_kernel(kind: str, *, dsn: str = DSN,
                      prefix: str = CHANCY_PREFIX):
    """Async context manager yielding a Kernel (or client) for `kind`.

    For 'chancy' the raw `chancy.Chancy` client is yielded (fixtures
    still declare queues / build `chancy.Worker` themselves); for
    'native' a `NativeKernel`; for 'sqlite' a `SQLiteKernel` on the
    given path/URI (`dsn` should be a temp file path — :memory: does
    not share across connections).
    """
    if kind == "native":
        from keelflow.engine.native_kernel import NativeKernel

        k = NativeKernel(dsn)
        await k.migrate()
        yield k
    elif kind == "sqlite":
        from keelflow.engine.sqlite_kernel import SQLiteKernel

        k = SQLiteKernel(dsn)
        await k.migrate()
        yield k
    else:
        from chancy import Chancy

        async with Chancy(dsn, prefix=prefix) as client:
            await client.migrate()
            yield client


async def declare_default_queue(kernel, kind: str, *, concurrency: int):
    """Per-kind default queue declaration (no-op for native)."""
    if kind == "native":
        return
    from chancy import Chancy, Queue

    await kernel.declare(
        Queue("default", concurrency=concurrency,
              executor=Chancy.Executor.Async, eager_polling=True),
        upsert=True)


def make_worker(kernel, kind: str, *, concurrency: int = 4,
                shutdown_timeout: float = 5, dispatch=None, **kwargs):
    """Backend-agnostic worker creation.

    chancy → `chancy.Worker(kernel, shutdown_timeout=…)` — dispatch is
    selected per job by `spec.func`, so a `dispatch` override is a
    no-op here (use CONTRACT_FUNC in the spec).
    native/sqlite → worker with fast polling for tests; `dispatch`
    overrides the engine entry point (contract tests inject a stub).
    """
    if kind in ("native", "sqlite"):
        return kernel.create_worker(
            concurrency=concurrency,
            poll_interval=kwargs.pop("poll_interval", 0.05),
            lease_seconds=kwargs.pop("lease_seconds", 30.0),
            heartbeat_interval=kwargs.pop("heartbeat_interval", 1.0),
            dispatch=dispatch,
            **kwargs)
    from chancy import Worker

    return Worker(kernel, shutdown_timeout=shutdown_timeout, **kwargs)


# ── kernel-port contract stubs ──────────────────────────────────────────

#: Dotted path resolvable by chancy's in-process executor.
CONTRACT_FUNC = "conftest.kernel_contract_dispatch"

#: (job_id, step_run_id) invocations recorded by the stub.
KERNEL_DISPATCH_RECORD: list[tuple] = []
#: set by the stub when it observes cancellation mid-flight.
KERNEL_DISPATCH_CANCELLED: list[str] = []


async def kernel_contract_dispatch(job_id, step_run_id=None):
    """Stub dispatch used by the kernel-port contract suite.

    Importable by dotted path (chancy) and injectable as a callable
    (native/sqlite `create_worker(dispatch=…)`).
    """
    KERNEL_DISPATCH_RECORD.append((job_id, step_run_id))


def require_chancy(kernel_kind: str):
    if kernel_kind != "chancy":
        pytest.skip("chancy-only test (kernel internals)")
