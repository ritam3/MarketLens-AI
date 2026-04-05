from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import text

from app.data.clients.fmp_client import FMPClient
from app.data.db.session import SessionLocal


def fetch_instruments(db) -> list[dict]:
    stmt = text(
        """
        select instrument_id, symbol
        from instruments
        where is_active = true
        order by symbol
        """
    )
    result = db.execute(stmt)
    return [{"instrument_id": row.instrument_id, "symbol": row.symbol} for row in result]


def parse_rows(payload: list[dict]) -> list[dict]:
    rows: list[dict] = []

    for item in payload:
        price_date = item.get("date")
        if not price_date:
            continue

        rows.append(
            {
                "price_date": price_date,
                "open": item.get("open"),
                "high": item.get("high"),
                "low": item.get("low"),
                "close": item.get("close"),
                "adjusted_close": item.get("adjClose") or item.get("adjustedClose"),
                "volume": item.get("volume"),
                "vwap": item.get("vwap"),
                "source": "fmp",
            }
        )

    return rows


def upsert_daily_bar(db, instrument_id: int, row: dict) -> None:
    stmt = text(
        """
        insert into market_bars_daily (
            instrument_id,
            price_date,
            open,
            high,
            low,
            close,
            adjusted_close,
            volume,
            vwap,
            source
        )
        values (
            :instrument_id,
            :price_date,
            :open,
            :high,
            :low,
            :close,
            :adjusted_close,
            :volume,
            :vwap,
            :source
        )
        on conflict (instrument_id, price_date)
        do update set
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            adjusted_close = excluded.adjusted_close,
            volume = excluded.volume,
            vwap = excluded.vwap,
            source = excluded.source
        """
    )

    db.execute(
        stmt,
        {
            "instrument_id": instrument_id,
            "price_date": row["price_date"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "adjusted_close": row["adjusted_close"],
            "volume": row["volume"],
            "vwap": row["vwap"],
            "source": row["source"],
        },
    )


def main() -> None:
    client = FMPClient()
    db = SessionLocal()

    end_date = date.today()
    start_date = end_date - timedelta(days=365 * 2)

    try:
        instruments = fetch_instruments(db)
        print(f"Found {len(instruments)} instruments to sync.")

        for idx, instrument in enumerate(instruments, start=1):
            instrument_id = instrument["instrument_id"]
            symbol = instrument["symbol"]

            print(f"[{idx}/{len(instruments)}] Syncing {symbol}...")

            try:
                payload = client.historical_price_eod_full(
                    symbol=symbol,
                    from_date=start_date.isoformat(),
                    to_date=end_date.isoformat(),
                )
                rows = parse_rows(payload)

                if not rows:
                    print(f"[{idx}/{len(instruments)}] No rows returned for {symbol}")
                    continue

                for row in rows:
                    upsert_daily_bar(db, instrument_id, row)

                db.commit()
                print(f"[{idx}/{len(instruments)}] Upserted {len(rows)} rows for {symbol}")

            except Exception as exc:
                db.rollback()
                print(f"[{idx}/{len(instruments)}] Failed to sync {symbol}: {exc}")

        print("Done syncing daily bars.")

    finally:
        db.close()


if __name__ == "__main__":
    main()