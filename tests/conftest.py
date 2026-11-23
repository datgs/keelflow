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
from contextlib import asynccontextmanager

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--kernel", choices=["chancy", "native", "both"], default="chancy",
        help="kernel backend for adapter-parameterized tests")


def pytest_generate_tests(metafunc):
    if "kernel_kind" in metafunc.fixturenames:
        mode = metafunc.config.getoption("--kernel")
        kinds = ["chancy", "native"] if mode == "both" else [mode]
        metafunc.parametrize("kernel_kind", kinds)


DSN = os.environ.get(
    "KEELFLOW_TEST_DSN", "postgresql://postgres:postgres@localhost:5544/keelflow"
)
CHANCY_PREFIX = "keelflow_test_"


@asynccontextmanager
async def open_kernel(kind: str, *, dsn: str = DSN,
                      prefix: str = CHANCY_PREFIX):
    """Async context manager yielding a Kernel (or client) for `kind`.

    For 'chancy' the raw `chancy.Chancy` client is yielded (fixtures
    still declare queues / build `chancy.Worker` themselves); for
    'native' a `NativeKernel` is yielded.
    """
    if kind == "native":
        from keelflow.engine.native_kernel import NativeKernel

        k = NativeKernel(dsn)
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
                shutdown_timeout: float = 5, **kwargs):
    """Backend-agnostic worker creation.

    chancy → `chancy.Worker(kernel, shutdown_timeout=…)`
    native → `NativeWorker` with fast polling for tests.
    """
    if kind == "native":
        return kernel.create_worker(
            concurrency=concurrency,
            poll_interval=kwargs.pop("poll_interval", 0.05),
            lease_seconds=kwargs.pop("lease_seconds", 30.0),
            heartbeat_interval=kwargs.pop("heartbeat_interval", 1.0),
            **kwargs)
    from chancy import Worker

    return Worker(kernel, shutdown_timeout=shutdown_timeout, **kwargs)


def require_chancy(kernel_kind: str):
    if kernel_kind != "chancy":
        pytest.skip("chancy-only test (kernel internals)")
