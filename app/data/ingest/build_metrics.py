"""Build derived metrics."""
from __future__ import annotations

from decimal import Decimal

from sqlalchemy import text

from app.data.db.session import SessionLocal


def rebuild_metrics_for_instrument(db, instrument_id: int) -> int:
    delete_stmt = text(
        """
        delete from market_metrics_daily
        where instrument_id = :instrument_id
        """
    )
    db.execute(delete_stmt, {"instrument_id": instrument_id})

    insert_stmt = text(
        """
        with base as (
            select
                d.instrument_id,
                d.price_date,
                coalesce(d.adjusted_close, d.close) as px,
                d.volume,
                lag(coalesce(d.adjusted_close, d.close)) over (
                    partition by d.instrument_id
                    order by d.price_date
                ) as prev_px,
                lag(coalesce(d.adjusted_close, d.close), 30) over (
                    partition by d.instrument_id
                    order by d.price_date
                ) as px_30d_ago,
                lag(coalesce(d.adjusted_close, d.close), 90) over (
                    partition by d.instrument_id
                    order by d.price_date
                ) as px_90d_ago,
                avg(d.volume) over (
                    partition by d.instrument_id
                    order by d.price_date
                    rows between 19 preceding and current row
                ) as avg_20d_volume
            from market_bars_daily d
            where d.instrument_id = :instrument_id
        ),
        shares as (
            select distinct on (fq.instrument_id)
                fq.instrument_id,
                fq.shares_outstanding
            from fundamentals_quarterly fq
            where fq.instrument_id = :instrument_id
              and fq.shares_outstanding is not null
            order by fq.instrument_id, fq.fiscal_period_end desc
        ),
        returns_base as (
            select
                b.instrument_id,
                b.price_date,
                b.px,
                b.volume,
                s.shares_outstanding,
                case
                    when b.prev_px is null or b.prev_px = 0 then null
                    else ((b.px - b.prev_px) / b.prev_px) * 100
                end as daily_return_pct,
                case
                    when b.px_30d_ago is null or b.px_30d_ago = 0 then null
                    else ((b.px - b.px_30d_ago) / b.px_30d_ago) * 100
                end as return_30d_pct,
                case
                    when b.px_90d_ago is null or b.px_90d_ago = 0 then null
                    else ((b.px - b.px_90d_ago) / b.px_90d_ago) * 100
                end as return_90d_pct,
                b.avg_20d_volume,
                case
                    when b.avg_20d_volume is null or b.avg_20d_volume = 0 then null
                    else b.volume / b.avg_20d_volume
                end as abnormal_volume_ratio
            from base b
            left join shares s on s.instrument_id = b.instrument_id
        )
        insert into market_metrics_daily (
            instrument_id,
            metric_date,
            market_cap,
            shares_outstanding,
            daily_return_pct,
            return_30d_pct,
            return_90d_pct,
            volatility_20d_pct,
            avg_20d_volume,
            abnormal_volume_ratio,
            source
        )
        select
            rb.instrument_id,
            rb.price_date as metric_date,
            case
                when rb.shares_outstanding is null or rb.px is null then null
                else rb.px * rb.shares_outstanding
            end as market_cap,
            rb.shares_outstanding,
            rb.daily_return_pct,
            rb.return_30d_pct,
            rb.return_90d_pct,
            stddev_samp(rb.daily_return_pct) over (
                partition by rb.instrument_id
                order by rb.price_date
                rows between 19 preceding and current row
            ) as volatility_20d_pct,
            rb.avg_20d_volume,
            rb.abnormal_volume_ratio,
            'derived' as source
        from returns_base rb
        order by rb.price_date
        """
    )

    db.execute(insert_stmt, {"instrument_id": instrument_id})

    count_stmt = text(
        """
        select count(*)
        from market_metrics_daily
        where instrument_id = :instrument_id
        """
    )
    return db.execute(count_stmt, {"instrument_id": instrument_id}).scalar_one()


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


def backfill_shares_from_instruments(db) -> None:
    """
    Optional fallback:
    if fundamentals_quarterly is empty, leave shares_outstanding null.
    This function is kept here as a placeholder for future logic.
    """
    return None


def main() -> None:
    db = SessionLocal()

    try:
        backfill_shares_from_instruments(db)

        instruments = fetch_instruments(db)
        print(f"Building metrics for {len(instruments)} instruments...")

        for idx, instrument in enumerate(instruments, start=1):
            instrument_id = instrument["instrument_id"]
            symbol = instrument["symbol"]

            try:
                count = rebuild_metrics_for_instrument(db, instrument_id)
                db.commit()
                print(f"[{idx}/{len(instruments)}] Built {count} metric rows for {symbol}")
            except Exception as exc:
                db.rollback()
                print(f"[{idx}/{len(instruments)}] Failed for {symbol}: {exc}")

        print("Done building metrics.")

    finally:
        db.close()


if __name__ == "__main__":
    main()