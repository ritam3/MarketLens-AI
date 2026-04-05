from __future__ import annotations

from argparse import Namespace

from scripts import run_ingestion


def test_run_ingestion_runs_steps_in_order_and_respects_skip(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(run_ingestion, "parse_args", lambda: Namespace(skip=["macro"]))
    monkeypatch.setattr(
        run_ingestion,
        "PIPELINE_STEPS",
        {
            "seed": ("Seed instruments", lambda: calls.append("seed")),
            "daily_bars": ("Sync daily bars", lambda: calls.append("daily_bars")),
            "macro": ("Sync macro data", lambda: calls.append("macro")),
            "fundamentals": ("Sync fundamentals", lambda: calls.append("fundamentals")),
        },
    )

    run_ingestion.main()

    assert calls == ["seed", "daily_bars", "fundamentals"]
