"""T-A4 CLI proofs."""

from unittest.mock import patch

import pytest

from keelflow.cli import _load_registry, main


def _run_main(argv):
    """CLI main() spins its own asyncio.run — call it on a thread with
    no running loop so nested asyncio.run works."""
    import threading

    done = threading.Event()
    box = {}

    def go():
        with patch("sys.argv", argv):
            try:
                main()
            except BaseException as e:  # noqa: BLE001
                box["exc"] = e
        done.set()

    t = threading.Thread(target=go)
    t.start()
    assert done.wait(timeout=60), "CLI call timed out"
    if "exc" in box:
        raise box["exc"]


def test_help_lists_subcommands(capsys):
    with pytest.raises(SystemExit) as e:
        main(["--help"])
    assert e.value.code == 0
    out = capsys.readouterr().out
    for sub in ("worker", "scheduler", "admin", "mcp",
                "init-db", "upgrade-db"):
        assert sub in out


def test_registry_loader_good_factory():
    reg = _load_registry("tests.test_cli:_factory")
    assert hasattr(reg, "register_job_type")


def _factory():
    from keelflow.engine.registry import InMemoryRegistry
    return InMemoryRegistry()


def test_registry_loader_missing_attr():
    with pytest.raises(SystemExit, match="no attribute"):
        _load_registry("os:nonexistent_attr_xyz")


def test_registry_loader_bad_module():
    with pytest.raises(SystemExit, match="cannot import"):
        _load_registry("no.such.module.xyz:thing")


def test_registry_loader_requires_colon():
    with pytest.raises(SystemExit, match="module:attr"):
        _load_registry("just_a_name")


def test_init_db_then_upgrade_db():
    """init-db on a fresh temp DB + upgrade-db refuses the
    unversioned state until baselined."""
    import asyncio

    from keelflow.testing import temp_test_db

    async def _run():
        async with temp_test_db() as dsn:
            # init-db: creates tables on empty db
            _run_main(["keelflow", "init-db", "--dsn", dsn])
            # upgrade-db on table-full but unversioned db → refuse
            with pytest.raises(SystemExit, match="baseline|stamp"):
                _run_main(["keelflow", "upgrade-db", "--dsn", dsn])

    asyncio.run(_run())


def test_upgrade_db_advances_versioned_database():
    """DB at alembic 0006 upgrades to head via upgrade-db."""
    import asyncio
    import os
    from pathlib import Path

    import psycopg
    from alembic.config import Config

    from alembic import command
    from keelflow.testing import temp_test_db

    ROOT = Path(__file__).resolve().parent.parent

    async def _run():
        async with temp_test_db() as dsn:
            cfg = Config(str(ROOT / "alembic.ini"))
            cfg.set_main_option("script_location", str(ROOT / "alembic"))
            os.environ["KEELFLOW_DSN"] = dsn.replace(
                "postgresql://", "postgresql+psycopg://", 1)
            try:
                command.upgrade(cfg, "0006")
            finally:
                os.environ.pop("KEELFLOW_DSN", None)
            _run_main(["keelflow", "upgrade-db", "--dsn", dsn])
            conn = await psycopg.AsyncConnection.connect(
                dsn, autocommit=True)
            try:
                rev = await (await conn.execute(
                    "SELECT version_num FROM alembic_version")).fetchone()
                assert rev[0] == "0010"
            finally:
                await conn.close()

    asyncio.run(_run())


def test_cli_defaults_to_native(capsys):
    with pytest.raises(SystemExit):
        main(["worker", "--help"])
    out = capsys.readouterr().out
    assert "default: native" in out
    with pytest.raises(SystemExit):
        main(["scheduler", "--help"])
    assert "default: native" in capsys.readouterr().out


def test_cli_warns_for_explicit_chancy(monkeypatch, caplog):
    import logging
    from argparse import Namespace

    from keelflow.cli import cmd_scheduler
    from keelflow.engine.native_kernel import NativeKernel

    monkeypatch.setattr("keelflow.cli._load_registry", lambda _: _factory())
    monkeypatch.setattr("keelflow.cli._dsn", lambda _: "postgresql://unused")
    from keelflow import cli

    monkeypatch.setattr(cli, "asyncio", type("FakeAsyncio", (), {
        "run": staticmethod(lambda coroutine: coroutine.close())})())
    monkeypatch.setattr(
        "keelflow.postgres.storage.PostgresStorage",
        lambda dsn: object(),
    )
    monkeypatch.setattr(
        "keelflow.cli._kernel",
        lambda *_: NativeKernel("postgresql://unused"),
    )
    monkeypatch.setattr(
        "keelflow.engine.chancy_kernel.warn_best_effort",
        lambda: logging.getLogger("test").warning("Chancy best effort"),
    )
    caplog.set_level(logging.WARNING)
    args = Namespace(registry="ignored", dsn="unused", kernel="chancy")
    cmd_scheduler(args)
    assert "Chancy best effort" in caplog.text


def test_cli_rejects_queue_for_native():
    from argparse import Namespace

    from keelflow.cli import cmd_worker

    args = Namespace(queue=["default@2"], kernel="native")
    with pytest.raises(SystemExit, match="only with --kernel chancy"):
        cmd_worker(args)


def test_sqlite_help_labels_proof_only(capsys):
    with pytest.raises(SystemExit):
        main(["worker", "--help"])
    assert "proof" in capsys.readouterr().out
