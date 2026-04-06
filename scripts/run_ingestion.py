"""Run the full MarketLens ingestion pipeline."""
from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data.ingest import (
    build_metrics,
    seed_instruments,
    sync_daily_bars,
    sync_fundamentals,
    sync_macro,
)

StepFn = Callable[[], None]

PIPELINE_STEPS: dict[str, tuple[str, StepFn]] = {
    "seed": ("Seed instruments", seed_instruments.main),
    "daily_bars": ("Sync daily bars", sync_daily_bars.main),
    "macro": ("Sync macro data", sync_macro.main),
    "fundamentals": ("Sync fundamentals", sync_fundamentals.sync_fundamentals),
    "metrics": ("Build derived metrics", build_metrics.main),
}


def _run_step(name: str, step_label: str, step_fn: StepFn) -> None:
    started_at = time.perf_counter()
    print(f"==> {step_label} [{name}]")

    try:
        step_fn()
    except Exception:
        elapsed = time.perf_counter() - started_at
        print(f"!! {step_label} failed after {elapsed:.1f}s")
        raise

    elapsed = time.perf_counter() - started_at
    print(f"<== {step_label} completed in {elapsed:.1f}s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full MarketLens ingestion pipeline.")
    parser.add_argument(
        "--skip",
        nargs="*",
        choices=tuple(PIPELINE_STEPS.keys()),
        default=(),
        help="Optional pipeline steps to skip.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    skipped_steps = set(args.skip)

    selected_steps = [
        (name, label, step_fn)
        for name, (label, step_fn) in PIPELINE_STEPS.items()
        if name not in skipped_steps
    ]

    if not selected_steps:
        print("No ingestion steps selected.")
        return

    print("Starting ingestion pipeline...")
    for name, label, step_fn in selected_steps:
        _run_step(name, label, step_fn)
    print("Ingestion pipeline completed successfully.")


if __name__ == "__main__":
    main()
