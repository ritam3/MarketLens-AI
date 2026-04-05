"""Sync macroeconomic data."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

from sqlalchemy import text

from app.data.clients.fred_client import FREDClient
from app.data.db.session import SessionLocal

SERIES_IDS = [
    "CPIAUCSL",   # CPI
    "FEDFUNDS",   # Fed Funds
    "DGS10",      # 10Y Treasury
]


def to_decimal(value: str | None) -> Decimal | None:
    if value is None or value == "" or value == ".":
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError):
        return None


def upsert_macro_series(db, row: dict) -> None:
    stmt = text(
        """
        insert into macro_series (
            series_id,
            title,
            units,
            frequency,
            seasonal_adjustment,
            notes,
            source,
            last_updated
        )
        values (
            :series_id,
            :title,
            :units,
            :frequency,
            :seasonal_adjustment,
            :notes,
            :source,
            :last_updated
        )
        on conflict (series_id)
        do update set
            title = excluded.title,
            units = excluded.units,
            frequency = excluded.frequency,
            seasonal_adjustment = excluded.seasonal_adjustment,
            notes = excluded.notes,
            source = excluded.source,
            last_updated = excluded.last_updated
        """
    )

    db.execute(
        stmt,
        {
            "series_id": row["id"],
            "title": row.get("title"),
            "units": row.get("units"),
            "frequency": row.get("frequency"),
            "seasonal_adjustment": row.get("seasonal_adjustment"),
            "notes": row.get("notes"),
            "source": "fred",
            "last_updated": row.get("last_updated"),
        },
    )


def upsert_macro_observation(db, series_id: str, row: dict) -> None:
    raw_value = row.get("value")
    value = to_decimal(raw_value)

    stmt = text(
        """
        insert into macro_observations (
            series_id,
            obs_date,
            value,
            raw_value
        )
        values (
            :series_id,
            :obs_date,
            :value,
            :raw_value
        )
        on conflict (series_id, obs_date)
        do update set
            value = excluded.value,
            raw_value = excluded.raw_value
        """
    )

    db.execute(
        stmt,
        {
            "series_id": series_id,
            "obs_date": row["date"],
            "value": value,
            "raw_value": raw_value,
        },
    )


def rebuild_cpi_events(db) -> None:
    delete_stmt = text(
        """
        delete from market_events
        where event_type = 'cpi_release'
          and source = 'fred'
        """
    )
    db.execute(delete_stmt)

    insert_stmt = text(
        """
        insert into market_events (
            event_type,
            event_date,
            instrument_id,
            event_name,
            actual_value,
            expected_value,
            surprise_value,
            payload,
            source
        )
        select
            'cpi_release' as event_type,
            mo.obs_date as event_date,
            null as instrument_id,
            'CPI Release' as event_name,
            mo.value as actual_value,
            null as expected_value,
            null as surprise_value,
            jsonb_build_object(
                'series_id', mo.series_id,
                'title', ms.title,
                'raw_value', mo.raw_value
            ) as payload,
            'fred' as source
        from macro_observations mo
        join macro_series ms on ms.series_id = mo.series_id
        where mo.series_id = 'CPIAUCSL'
        """
    )
    db.execute(insert_stmt)


def main() -> None:
    client = FREDClient()
    db = SessionLocal()

    try:
        for series_id in SERIES_IDS:
            print(f"Syncing macro series {series_id}...")

            series_resp = client.series(series_id)
            series_rows = series_resp.get("seriess", [])
            if not series_rows:
                print(f"No metadata returned for {series_id}")
                continue

            upsert_macro_series(db, series_rows[0])

            obs_resp = client.observations(series_id, observation_start="2020-01-01")
            observations = obs_resp.get("observations", [])

            for obs in observations:
                upsert_macro_observation(db, series_id, obs)

            db.commit()
            print(f"Upserted {len(observations)} observations for {series_id}")

        print("Rebuilding CPI events...")
        rebuild_cpi_events(db)
        db.commit()

        print("Done syncing macro data.")

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()