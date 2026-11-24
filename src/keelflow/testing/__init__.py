"""keelflow.testing — Postgres-first integration test helpers.

The public helper is `postgres_env()`: it wires registry + storage +
kernel + services the same way `worker.configure` does, against an
isolated test database.

Safety model: integration tests are destructive (fixtures truncate
`keelflow_*` tables), so `ensure_safe_test_dsn` / `temp_test_db` refuse
anything that is not clearly a disposable test database. In-memory
fakes are intentionally NOT shipped here — they cannot prove
transaction, lock, isolation, or crash-recovery behavior.
"""
from __future__ import annotations

from .postgres import (
    assert_safe_test_dsn,
    ensure_test_database,
    postgres_env,
    temp_test_db,
)

__all__ = [
    "assert_safe_test_dsn",
    "ensure_test_database",
    "postgres_env",
    "temp_test_db",
]
