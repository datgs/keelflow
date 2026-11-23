"""Backend-neutral unit-of-work contracts.

A `Session` owns one backend connection/client and is the factory for
`UnitOfWork` scopes. A `UnitOfWork` is one atomic boundary: storage
operations, kernel dispatch pushes, and application repository calls that
share a `UnitOfWork` commit or roll back together — provided they all
belong to the same backend. There is no cross-backend atomicity.

Neither contract exposes SQL, cursors, or driver APIs. Backend adapters
and application repositories reach the underlying driver object through
`native` (a `psycopg.AsyncConnection`, an `aiosqlite.Connection`, a
Motor session, …) — `native` is the sanctioned adapter edge, never used
inside engine or SDK core.

Lifetimes:

    session = await storage.connect()           # owns the connection
    async with session.begin() as uow:          # opens TX; commits on clean
        await storage.job_insert(uow, ...)      #   exit, rolls back on raise
        await repo.write(uow, ...)              # app repository same TX
        await kernel.push_ex(uow, spec)         # dispatch row same TX
    await session.close()

`uow.nested()` creates a child scope inside the same transaction (a
Postgres savepoint): a raised exception rolls back only the child scope.
Backend adapters that cannot nest transactions must still preserve the
"child failure aborts the child's writes, not the parent's" contract or
document the fallback in their capability notes.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol


class Tx(Protocol):
    """Shared handle carried by Session and UnitOfWork.

    `native` is the backend driver's connection/session object. Adapters
    type-narrow it (Postgres → `psycopg.AsyncConnection`); engine and SDK
    core never touch it.
    """

    @property
    def native(self) -> Any: ...


class Session(Tx, Protocol):
    """A backend connection/session owner. Opened via `storage.connect()`."""

    def begin(self) -> AbstractAsyncContextManager[UnitOfWork]:
        """Open a new transaction unit. Commits on clean exit, rolls back
        on exception. Requires the session to be idle — implementations
        raise when uncommitted work is already pending."""
        ...

    def key_lock(self, key: str) -> AbstractAsyncContextManager[None]:
        """Serialize execution on a key across processes.

        Postgres maps to a session-scoped advisory lock; other backends
        supply an equivalent capability (lock document, distributed
        mutex). May raise `NotImplementedError` when the backend cannot
        provide the semantic — the engine degrades to the documented
        fallback (claim-level SKIP-LOCKED serialization only).
        """
        ...

    async def close(self) -> None: ...


class UnitOfWork(Tx, Protocol):
    """One atomic boundary shared by storage ops, kernel push, repos."""

    def nested(self) -> AbstractAsyncContextManager[UnitOfWork]:
        """Child scope inside this transaction (savepoint semantics).

        An exception exiting the child rolls back only writes made
        through the child; the parent's writes survive.
        """
        ...
