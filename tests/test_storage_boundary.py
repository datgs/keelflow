"""Storage-port boundary guard (T2, plan §T2 last step).

Static proof that engine and SDK core never touch driver APIs or embed
engine-table SQL — all storage access flows through the semantic
`Storage` port and the `UnitOfWork` scope. Kernel adapters
(`*_kernel.py`) and the `postgres/` package are sanctioned adapter
positions; `admin/` and `mcp/` are Postgres-specific ops tooling that
may open driver connections for read queries.
"""
from __future__ import annotations

import re
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src" / "keelflow"

# Adapter positions allowed to hold driver handles and SQL.
_KERNEL_ADAPTERS = {"chancy_kernel.py", "native_kernel.py", "sqlite_kernel.py"}
_ALLOW_DRIVER = {"postgres"}            # package dirs
_ALLOW_DRIVER_FILES = _KERNEL_ADAPTERS  # files inside engine/

_SQL_RE = re.compile(
    r"""(?x)\b(?:SELECT|INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+
        ["'`]?\s*keelflow_"""
)
_DRIVER_RE = re.compile(
    r"^\s*(?:import|from)\s+(psycopg|aiosqlite|asyncpg|motor|pymongo)\b",
    re.MULTILINE,
)


def _engine_sdk_files():
    for base in (SRC / "engine", SRC / "sdk"):
        yield from sorted(base.glob("*.py"))


def test_no_driver_imports_in_engine_or_sdk():
    offenders = [
        f for f in _engine_sdk_files()
        if f.name not in _ALLOW_DRIVER_FILES and _DRIVER_RE.search(f.read_text())
    ]
    assert offenders == [], (
        "driver imports outside adapters: "
        + ", ".join(f.name for f in offenders)
    )


def test_no_engine_table_sql_in_engine_or_sdk():
    """SQL naming engine tables may live only in postgres/ + kernel
    adapters (docstrings/comments excluded via the statement regex)."""
    offenders = []
    for f in _engine_sdk_files():
        if f.name in _ALLOW_DRIVER_FILES:
            continue
        text = f.read_text()
        # strip docstrings + comments to avoid flagging narrative SQL
        text = re.sub(r'"""(?:[^"\\]|\\.|"(?!""))*"""', "", text, flags=re.DOTALL)
        text = re.sub(r"#[^\n]*", "", text)
        if _SQL_RE.search(text):
            offenders.append(f.name)
    assert offenders == [], (
        "engine-table SQL outside adapters: " + ", ".join(offenders)
    )


def test_sdk_has_no_driver_imports_at_all():
    """The SDK is the application contract — kernels included."""
    offenders = [
        f for f in (SRC / "sdk").glob("*.py")
        if _DRIVER_RE.search(f.read_text())
    ]
    assert offenders == [], (
        "driver imports in SDK: " + ", ".join(f.name for f in offenders)
    )
