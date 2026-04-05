from app.data.ingest.seed_instruments import build_instrument_payload, classify_market_cap


def test_classify_market_cap_uses_expected_buckets() -> None:
    assert classify_market_cap(None) is None
    assert classify_market_cap(250_000) == "mega"
    assert classify_market_cap(50_000) == "large"
    assert classify_market_cap(5_000) == "mid"
    assert classify_market_cap(500) == "small"
    assert classify_market_cap(100) == "micro"


def test_build_instrument_payload_maps_fmp_profile_fields() -> None:
    payload = build_instrument_payload(
        symbol="AAPL",
        asset_type="equity",
        profile={
            "companyName": "Apple Inc.",
            "exchangeShortName": "NASDAQ",
            "currency": "USD",
            "country": "US",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "mktCap": 2_500_000,
            "isActivelyTrading": True,
        },
    )

    assert payload == {
        "symbol": "AAPL",
        "name": "Apple Inc.",
        "asset_type": "equity",
        "exchange": "NASDAQ",
        "currency": "USD",
        "country": "US",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "market_cap_class": "mega",
        "is_active": True,
    }
