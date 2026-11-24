"""Disposable-Postgres helpers for integration tests.

Two layers:

- `assert_safe_test_dsn(dsn)` — fail-closed guard for fixtures that
  truncate engine tables. Passes only when the DSN's database name
  contains `test` (`keelflow_test`, `keelflow_test_mig`) OR the caller
  sets `KEELFLOW_TEST_ALLOW_DESTRUCTIVE=1` as an explicit opt-in.
- `postgres_env()` — async context manager wiring registry, storage,
  kernel, and services against a safe test DSN so app tests get a
  working `JobService`/`WorkflowService` without a shared dev database.
"""
from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from urllib.parse import urlparse

ALLOW_ENV = "KEELFLOW_TEST_ALLOW_DESTRUCTIVE"

#: Default suite target — a dedicated throwaway database, never the dev
#: `keelflow` database on the same server.
DEFAULT_TEST_DSN = (
    "postgresql://postgres:postgres@localhost:5544/keelflow_test"
)


def _db_name(dsn: str) -> str:
    name = urlparse(dsn).path.lstrip("/")
    if not name:
        raise ValueError(f"test DSN must name a database: {dsn!r}")
    return name


def _assert_test_named(dsn: str) -> str:
    name = _db_name(dsn)
    if "test" not in name.lower():
        raise RuntimeError(
            f"refusing destructive tests on database {name!r}: test "
            "fixtures truncate keelflow_* tables. Use a database whose "
            "name contains 'test' (e.g. keelflow_test) or set "
            f"{ALLOW_ENV}=1 to acknowledge the destruction."
        )
    return name


def assert_safe_test_dsn(dsn: str) -> str:
    """Guard destructive fixtures. Returns the database name when safe."""
    if os.environ.get(ALLOW_ENV) == "1":
        return _db_name(dsn)
    return _assert_test_named(dsn)


async def ensure_test_database(dsn: str) -> None:
    """Create the DSN's database when absent (same server, postgres db).

    Requires a test-named database — this helper exists to create
    disposable databases, not arbitrary ones.
    """
    import psycopg

    name = _assert_test_named(dsn)
    admin = urlparse(dsn)._replace(path="/postgres").geturl()
    conn = await psycopg.AsyncConnection.connect(admin, autocommit=True)
    try:
        cur = await conn.execute(
            "SELECT 1 FROM pg_database WHERE datname=%s", (name,))
        if await cur.fetchone() is None:
            # name is trusted (test-named), still quote it safely
            await conn.execute(
                f'CREATE DATABASE "{name.replace(chr(34), "")}"')
    finally:
        await conn.close()


@asynccontextmanager
async def temp_test_db(base_dsn: str | None = None) -> AsyncIterator[str]:
    """Yield a DSN for a fresh `keelflow_test_<uid>` database, dropped
    on exit. Used by migration/backup rehearsal tests that must not
    touch the suite's database."""
    import psycopg

    base = urlparse(base_dsn or DEFAULT_TEST_DSN)
    server = base._replace(path="/postgres").geturl()
    name = f"keelflow_test_{uuid.uuid4().hex[:10]}"
    conn = await psycopg.AsyncConnection.connect(server, autocommit=True)
    try:
        await conn.execute(f'CREATE DATABASE "{name}"')
    finally:
        await conn.close()
    try:
        yield base._replace(path=f"/{name}").geturl()
    finally:
        conn = await psycopg.AsyncConnection.connect(
            server, autocommit=True)
        try:
            await conn.execute(
                f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        finally:
            await conn.close()


@asynccontextmanager
async def postgres_env(
    dsn: str | None = None,
    *,
    kernel=None,
    prefix: str = "keelflow_test_kernel_",
) -> AsyncIterator[SimpleNamespace]:
    """Wire a full engine environment for tests: registry, storage,
    kernel, and services — mirroring `worker.configure` semantics.

    `kernel=None` creates a Chancy client on `prefix` (owned + closed);
    pass a Kernel adapter to keep caller ownership. The caller registers
    job types on `env.registry`.
    """
    import chancy
    import psycopg

    from ..engine import worker as worker_mod
    from ..engine.audit import PostgresAuditService
    from ..engine.enqueue import JobService
    from ..engine.human import HumanTaskService
    from ..engine.kernel import as_kernel
    from ..engine.registry import InMemoryRegistry
    from ..engine.statemachine import EngineStateMachineService
    from ..engine.workflow import WorkflowService
    from ..postgres.schema import apply_async
    from ..postgres.storage import PostgresStorage

    dsn = dsn or os.environ.get("KEELFLOW_TEST_DSN", DEFAULT_TEST_DSN)
    assert_safe_test_dsn(dsn)
    await ensure_test_database(dsn)

    conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    try:
        await apply_async(conn)
    finally:
        await conn.close()

    storage = PostgresStorage(dsn)
    registry = InMemoryRegistry()
    own_client = kernel is None
    if own_client:
        kernel = chancy.Chancy(dsn, prefix=prefix)
        await kernel.migrate()
        await kernel.declare(
            chancy.Queue("default", concurrency=8,
                         executor=chancy.Executor.Async,
                         eager_polling=True),
            upsert=True)
    port = as_kernel(kernel)
    await port.migrate()

    audit = PostgresAuditService(storage)
    jobs = JobService(registry, port, storage)
    wf = WorkflowService(registry, jobs)
    sm = EngineStateMachineService(registry, port, audit=audit,
                                   storage=storage)
    human = HumanTaskService(registry, port, sm=sm, storage=storage)
    worker_mod.configure(registry=registry, dsn=dsn, kernel=port,
                         storage=storage)
    try:
        yield SimpleNamespace(
            dsn=dsn, registry=registry, storage=storage, kernel=port,
            jobs=jobs, wf=wf, sm=sm, human=human, audit=audit)
    finally:
        if own_client:
            await kernel.close()
