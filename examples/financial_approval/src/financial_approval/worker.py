"""Worker process — claims and runs machine jobs (human steps are UI-side).

    uv run --package keelflow-demo-financial \
        python -m financial_approval.worker
"""
from __future__ import annotations

from keelflow.engine.worker import run_worker

from .registry import build_registry


def main() -> None:
    run_worker(registry=build_registry(), prefix="keelflow_kernel_fin_")


if __name__ == "__main__":
    main()
