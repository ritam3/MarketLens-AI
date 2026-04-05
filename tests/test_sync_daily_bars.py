from datetime import date

import pytest

from app.data.ingest.sync_daily_bars import _build_daily_bar_rows


def test_build_daily_bar_rows_maps_finnhub_candles() -> None:
    candles = {
        "s": "ok",
        "c": [101.5, 102.25],
        "h": [103.0, 104.0],
        "l": [99.5, 100.5],
        "o": [100.0, 101.0],
        "t": [1704067200, 1704153600],
        "v": [1_000_000, 1_200_000],
    }

    rows = _build_daily_bar_rows(instrument_id=42, source="finnhub", candles=candles)

    assert rows == [
        {
            "instrument_id": 42,
            "price_date": date(2024, 1, 1),
            "open": 100.0,
            "high": 103.0,
            "low": 99.5,
            "close": 101.5,
            "adjusted_close": None,
            "volume": 1_000_000,
            "vwap": None,
            "source": "finnhub",
        },
        {
            "instrument_id": 42,
            "price_date": date(2024, 1, 2),
            "open": 101.0,
            "high": 104.0,
            "low": 100.5,
            "close": 102.25,
            "adjusted_close": None,
            "volume": 1_200_000,
            "vwap": None,
            "source": "finnhub",
        },
    ]


def test_build_daily_bar_rows_handles_no_data() -> None:
    assert _build_daily_bar_rows(instrument_id=42, source="finnhub", candles={"s": "no_data"}) == []


def test_build_daily_bar_rows_rejects_mismatched_candle_arrays() -> None:
    candles = {
        "s": "ok",
        "c": [101.5],
        "h": [103.0],
        "l": [99.5],
        "o": [100.0],
        "t": [1704067200, 1704153600],
        "v": [1_000_000],
    }

    with pytest.raises(ValueError, match="mismatched lengths"):
        _build_daily_bar_rows(instrument_id=42, source="finnhub", candles=candles)
