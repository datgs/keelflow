"""SuperAdmin — deliberately rough server-rendered ops UI (T15).

`make_app(dsn)` returns a small WSGI/HTTP handler for `python -m
pyerp.admin`. Read-only views: queue overview, job list/detail, workflow
runs/detail, failed rows, metrics. Mutations limited to `POST /retry`.
"""
from .app import make_app

__all__ = ["make_app"]
