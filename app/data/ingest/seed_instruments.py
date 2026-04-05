"""Seed instrument metadata into the database."""
from __future__ import annotations

from typing import Any

from sqlalchemy import text

from app.data.clients.fmp_client import FMPClient

SEED_SYMBOLS = [
    {"symbol": "AAPL", "asset_type": "equity"},
    {"symbol": "MSFT", "asset_type": "equity"},
    {"symbol": "NVDA", "asset_type": "equity"},
    {"symbol": "AMD", "asset_type": "equity"},
    {"symbol": "AVGO", "asset_type": "equity"},
    {"symbol": "GOOGL", "asset_type": "equity"},
    {"symbol": "AMZN", "asset_type": "equity"},
    {"symbol": "META", "asset_type": "equity"},
    {"symbol": "JPM", "asset_type": "equity"},
    {"symbol": "GS", "asset_type": "equity"},
    {"symbol": "XOM", "asset_type": "equity"},
    {"symbol": "CVX", "asset_type": "equity"},
    {"symbol": "LLY", "asset_type": "equity"},
    {"symbol": "UNH", "asset_type": "equity"},
    {"symbol": "SPY", "asset_type": "etf"},
    {"symbol": "QQQ", "asset_type": "etf"},
]


def classify_market_cap(market_cap: float | None) -> str | None:
    if market_cap is None:
        return None
    if market_cap >= 200_000:
        return "mega"
    if market_cap >= 10_000:
        return "large"
    if market_cap >= 2_000:
        return "mid"
    if market_cap >= 300:
        return "small"
    return "micro"


def build_instrument_payload(
    *,
    symbol: str,
    asset_type: str,
    profile: dict[str, Any],
) -> dict[str, Any]:
    market_cap = profile.get("mktCap") or profile.get("marketCap") or profile.get("marketCapitalization")

    return {
        "symbol": symbol,
        "name": profile.get("companyName") or profile.get("name"),
        "asset_type": asset_type,
        "exchange": profile.get("exchangeShortName") or profile.get("exchange"),
        "currency": profile.get("currency"),
        "country": profile.get("country"),
        "sector": profile.get("sector"),
        "industry": profile.get("industry"),
        "market_cap_class": classify_market_cap(market_cap),
        "is_active": profile.get("isActivelyTrading", True),
    }


def main() -> None:
    from app.data.db.session import SessionLocal

    client = FMPClient()
    db = SessionLocal()

    try:
        for item in SEED_SYMBOLS:
            symbol = item["symbol"]
            asset_type = item["asset_type"]

            profile = client.company_profile(symbol)
            payload = build_instrument_payload(
                symbol=symbol,
                asset_type=asset_type,
                profile=profile,
            )

            stmt = text(
                """
                insert into instruments (
                    symbol,
                    name,
                    asset_type,
                    exchange,
                    currency,
                    country,
                    sector,
                    industry,
                    market_cap_class,
                    is_active
                )
                values (
                    :symbol,
                    :name,
                    :asset_type,
                    :exchange,
                    :currency,
                    :country,
                    :sector,
                    :industry,
                    :market_cap_class,
                    true
                )
                on conflict (symbol)
                do update set
                    name = excluded.name,
                    asset_type = excluded.asset_type,
                    exchange = excluded.exchange,
                    currency = excluded.currency,
                    country = excluded.country,
                    sector = excluded.sector,
                    industry = excluded.industry,
                    market_cap_class = excluded.market_cap_class,
                    is_active = excluded.is_active,
                    updated_at = now()
                """
            )

            db.execute(stmt, payload)
            print(f"Seeded {symbol}")

        db.commit()
        print("Done seeding instruments.")

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
