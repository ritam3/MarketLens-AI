from app.data.ingest.sync_daily_bars import parse_rows


def test_parse_rows_maps_fmp_daily_payload() -> None:
    rows = parse_rows(
        [
            {
                "date": "2024-01-01",
                "open": 100.0,
                "high": 103.0,
                "low": 99.5,
                "close": 101.5,
                "adjClose": 101.0,
                "volume": 1_000_000,
                "vwap": 100.75,
            }
        ]
    )

    assert rows == [
        {
            "price_date": "2024-01-01",
            "open": 100.0,
            "high": 103.0,
            "low": 99.5,
            "close": 101.5,
            "adjusted_close": 101.0,
            "volume": 1_000_000,
            "vwap": 100.75,
            "source": "fmp",
        }
    ]


def test_parse_rows_skips_items_without_date() -> None:
    assert parse_rows([{"open": 100.0}]) == []
