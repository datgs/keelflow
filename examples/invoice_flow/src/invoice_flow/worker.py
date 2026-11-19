"""Kernel worker for the demo — claims keelflow jobs and dispatches handlers.

    uv run python -m invoice_flow.worker

Demo only: no auth, localhost infrastructure assumed.
"""
from __future__ import annotations

import os

from keelflow.engine.worker import run_worker

from .registry import DSN_ENV, build_registry

if __name__ == "__main__":
    if DSN_ENV not in os.environ:
        os.environ[DSN_ENV] = (
            "postgresql://postgres:postgres@localhost:5544/keelflow")
    run_worker(registry=build_registry())
