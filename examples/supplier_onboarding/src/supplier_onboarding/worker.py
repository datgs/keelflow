"""Worker process — claims and runs machine jobs (human steps are UI-side).

    uv run --package keelflow-demo-supplier \
        python -m supplier_onboarding.worker
"""
from __future__ import annotations

from keelflow.engine.worker import run_worker

from .registry import build_registry


def main() -> None:
    run_worker(registry=build_registry(), prefix="keelflow_kernel_sup_")


if __name__ == "__main__":
    main()
