"""Worker process — claims machine jobs incl. bulk chunks.

    uv run --package keelflow-demo-import python -m bulk_import.worker
"""
from __future__ import annotations

from keelflow.engine.worker import run_worker

from .registry import build_registry


def main() -> None:
    run_worker(registry=build_registry(), prefix="keelflow_kernel_imp_")


if __name__ == "__main__":
    main()
