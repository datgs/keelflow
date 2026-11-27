"""keelflow CLI — operational entry points.

Subcommands:

    keelflow worker    --registry pkg.mod:build_registry [--kernel K]
    keelflow scheduler --registry pkg.mod:build_registry [--kernel K]
    keelflow admin     --dsn DSN [--host --port]
    keelflow mcp       --dsn DSN [--transport stdio|sse]
    keelflow init-db   --dsn DSN          # fresh schema only
    keelflow upgrade-db --dsn DSN         # alembic versioned upgrade

Schema init and upgrade are deliberately different commands:
`init-db` creates tables on an empty database; `upgrade-db` runs Alembic
revisions and refuses a database whose revision state is unknown — a
`CREATE IF NOT EXISTS` apply is never reported as a migration.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent


def _load_registry(spec: str):
    """`module:attr` — attr is a Registry or a zero-arg factory."""
    if ":" not in spec:
        raise SystemExit(
            f"--registry expects 'module:attr', got {spec!r}")
    mod_name, attr = spec.split(":", 1)
    try:
        mod = importlib.import_module(mod_name)
    except ImportError as exc:
        raise SystemExit(
            f"cannot import registry module {mod_name!r}: {exc}")
    try:
        target = getattr(mod, attr)
    except AttributeError:
        raise SystemExit(
            f"module {mod_name!r} has no attribute {attr!r}")
    if callable(target) and not hasattr(target, "job_type"):
        target = target()
    return target


def _dsn(args) -> str:
    dsn = args.dsn or os.environ.get("KEELFLOW_DSN")
    if not dsn:
        raise SystemExit("--dsn or $KEELFLOW_DSN required")
    return dsn


def _kernel(args, dsn: str):
    kind = args.kernel
    if kind == "chancy":
        import chancy
        return chancy.Chancy(dsn, prefix=args.prefix)
    if kind == "native":
        from .engine.native_kernel import NativeKernel
        return NativeKernel(dsn)
    if kind == "sqlite":
        from .engine.sqlite_kernel import SQLiteKernel
        return SQLiteKernel(dsn)
    raise SystemExit(f"unknown kernel {kind!r}")


def cmd_worker(args) -> None:
    from .engine.worker import run_worker

    if args.queue and args.kernel != "chancy":
        raise SystemExit("--queue is supported only with --kernel chancy")
    registry = _load_registry(args.registry)
    dsn = _dsn(args)
    kernel = _kernel(args, dsn)
    if args.kernel == "chancy":
        from .engine.chancy_kernel import warn_best_effort
        warn_best_effort()
    queues = None
    if args.kernel == "chancy" and args.queue:
        import chancy
        queues = [chancy.Queue(
            name, concurrency=c,
            executor=chancy.Executor.Async, eager_polling=True)
            for name, c in (q.split("@") for q in args.queue)]
    run_worker(registry=registry, dsn=dsn, kernel=kernel,
               queues=queues, prefix=args.prefix)


def cmd_scheduler(args) -> None:
    from .engine.scheduler import run_scheduler

    registry = _load_registry(args.registry)
    dsn = _dsn(args)
    kernel = _kernel(args, dsn)
    if args.kernel == "chancy":
        from .engine.chancy_kernel import warn_best_effort
        warn_best_effort()

    from .postgres.storage import PostgresStorage
    storage = PostgresStorage(dsn)

    async def _run():
        await kernel.migrate()
        await run_scheduler(storage, registry, kernel)

    asyncio.run(_run())


def cmd_admin(args) -> None:
    from .admin.app import make_app
    make_app(_dsn(args), host=args.host, port=args.port)


def cmd_mcp(args) -> None:
    from .mcp.server import make_server
    make_server(_dsn(args)).run(args.transport)


def cmd_init_db(args) -> None:
    """Fresh schema only — refuses a database that already has
    engine tables (use upgrade-db there)."""
    dsn = _dsn(args)

    async def _run():
        import psycopg

        conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        try:
            cur = await conn.execute(
                "SELECT to_regclass('public.keelflow_jobs')")
            if (await cur.fetchone())[0] is not None:
                raise SystemExit(
                    "keelflow_jobs already exists — use `keelflow "
                    "upgrade-db` for a versioned migration instead")
            from .postgres.schema import apply_async
            await apply_async(conn)
            print("schema initialized (fresh database)")
        finally:
            await conn.close()

    asyncio.run(_run())


def cmd_upgrade_db(args) -> None:
    """Versioned upgrade via Alembic — never a silent CREATE IF NOT
    EXISTS: unknown/unversioned databases fail closed."""
    dsn = _dsn(args)
    sa_dsn = dsn.replace("postgresql://", "postgresql+psycopg://", 1)

    # Preflight: a database with tables but no alembic_version is
    # unversioned — applying migrations could silently clash.
    async def _preflight():
        import psycopg
        conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        try:
            cur = await conn.execute(
                "SELECT to_regclass('public.keelflow_jobs'),"
                "       to_regclass('public.alembic_version')")
            jobs_tbl, ver_tbl = await cur.fetchone()
            if jobs_tbl is not None and ver_tbl is None:
                raise SystemExit(
                    "database has keelflow tables but no alembic_version "
                    "— it was initialized without migrations. Baseline it "
                    "first: `alembic stamp 0001` (or the revision matching "
                    "its schema), then run upgrade-db again.")
            if jobs_tbl is None:
                raise SystemExit(
                    "no keelflow tables — use `keelflow init-db` on a "
                    "fresh database, or `alembic stamp` + upgrade-db if "
                    "you manage versions externally.")
        finally:
            await conn.close()

    asyncio.run(_preflight())
    os.environ["KEELFLOW_DSN"] = sa_dsn
    try:
        from alembic.config import Config

        from alembic import command
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("script_location", str(ROOT / "alembic"))
        command.upgrade(cfg, "head")
        print("upgraded to head")
    finally:
        os.environ.pop("KEELFLOW_DSN", None)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        "keelflow", description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def _common(sp, registry=False):
        sp.add_argument("--dsn", default=None,
                        help="Postgres DSN (or $KEELFLOW_DSN)")
        if registry:
            sp.add_argument("--registry", required=True,
                            help="module:attr — Registry or factory")
            sp.add_argument("--kernel",
                            choices=["chancy", "native", "sqlite"],
                            default="native",
                            help="dispatch backend (default: native; chancy is "
                                 "opt-in; sqlite is local/proof only, not production)")
            sp.add_argument("--prefix", default="keelflow_kernel_",
                            help="kernel table prefix (chancy)")

    w = sub.add_parser("worker", help="run a job worker")
    _common(w, registry=True)
    w.add_argument("--queue", action="append", metavar="NAME@N",
                   help="chancy queue declaration (repeatable)")
    w.set_defaults(fn=cmd_worker)

    s = sub.add_parser("scheduler", help="run the cron scheduler")
    _common(s, registry=True)
    s.set_defaults(fn=cmd_scheduler)

    a = sub.add_parser("admin", help="SuperAdmin HTTP UI (no auth — "
                                     "loopback or protected proxy only)")
    _common(a)
    a.add_argument("--host", default="127.0.0.1")
    a.add_argument("--port", type=int, default=8410)
    a.set_defaults(fn=cmd_admin)

    m = sub.add_parser("mcp", help="read-only MCP tools server")
    _common(m)
    m.add_argument("--transport", choices=["stdio", "sse"],
                   default="stdio")
    m.set_defaults(fn=cmd_mcp)

    i = sub.add_parser("init-db", help="fresh schema on an empty "
                                       "database only")
    _common(i)
    i.set_defaults(fn=cmd_init_db)

    u = sub.add_parser("upgrade-db", help="versioned Alembic upgrade "
                                          "(existing databases)")
    _common(u)
    u.set_defaults(fn=cmd_upgrade_db)

    args = p.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":  # pragma: no cover
    main()
