"""Sync quarterly fundamentals from SEC EDGAR."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.data.clients.sec_edgar_client import SECEdgarClient

INSTRUMENTS_QUERY = text(
    """
    select instrument_id, symbol
    from instruments
    where is_active = true
      and asset_type = 'equity'
    order by symbol
    """
)

UPSERT_FUNDAMENTALS_QUERY = text(
    """
    insert into fundamentals_quarterly (
        instrument_id,
        fiscal_period_end,
        fiscal_year,
        fiscal_quarter,
        revenue,
        gross_profit,
        operating_income,
        net_income,
        eps,
        ebitda,
        free_cash_flow,
        total_assets,
        total_liabilities,
        shareholders_equity,
        shares_outstanding,
        source
    )
    values (
        :instrument_id,
        :fiscal_period_end,
        :fiscal_year,
        :fiscal_quarter,
        :revenue,
        :gross_profit,
        :operating_income,
        :net_income,
        :eps,
        :ebitda,
        :free_cash_flow,
        :total_assets,
        :total_liabilities,
        :shareholders_equity,
        :shares_outstanding,
        :source
    )
    on conflict (instrument_id, fiscal_period_end) do update
    set
        fiscal_year = excluded.fiscal_year,
        fiscal_quarter = excluded.fiscal_quarter,
        revenue = excluded.revenue,
        gross_profit = excluded.gross_profit,
        operating_income = excluded.operating_income,
        net_income = excluded.net_income,
        eps = excluded.eps,
        ebitda = excluded.ebitda,
        free_cash_flow = excluded.free_cash_flow,
        total_assets = excluded.total_assets,
        total_liabilities = excluded.total_liabilities,
        shareholders_equity = excluded.shareholders_equity,
        shares_outstanding = excluded.shares_outstanding,
        source = excluded.source
    """
)

DURATION_FACTS = {
    "revenue": {
        "taxonomy": "us-gaap",
        "concepts": (
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "SalesRevenueNet",
            "Revenues",
        ),
        "units": ("USD",),
        "quarterly": True,
    },
    "gross_profit": {
        "taxonomy": "us-gaap",
        "concepts": ("GrossProfit",),
        "units": ("USD",),
        "quarterly": True,
    },
    "operating_income": {
        "taxonomy": "us-gaap",
        "concepts": ("OperatingIncomeLoss",),
        "units": ("USD",),
        "quarterly": True,
    },
    "net_income": {
        "taxonomy": "us-gaap",
        "concepts": ("NetIncomeLoss",),
        "units": ("USD",),
        "quarterly": True,
    },
    "eps": {
        "taxonomy": "us-gaap",
        "concepts": (
            "EarningsPerShareDiluted",
            "EarningsPerShareBasicAndDiluted",
            "EarningsPerShareBasic",
        ),
        "units": ("USD/shares", "USD"),
        "quarterly": True,
    },
    "operating_cash_flow": {
        "taxonomy": "us-gaap",
        "concepts": (
            "NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        ),
        "units": ("USD",),
        "quarterly": True,
    },
    "capital_expenditures": {
        "taxonomy": "us-gaap",
        "concepts": (
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "PropertyPlantAndEquipmentAdditions",
        ),
        "units": ("USD",),
        "quarterly": True,
    },
}

INSTANT_FACTS = {
    "total_assets": {
        "taxonomy": "us-gaap",
        "concepts": ("Assets",),
        "units": ("USD",),
    },
    "total_liabilities": {
        "taxonomy": "us-gaap",
        "concepts": ("Liabilities",),
        "units": ("USD",),
    },
    "shareholders_equity": {
        "taxonomy": "us-gaap",
        "concepts": (
            "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        ),
        "units": ("USD",),
    },
    "shares_outstanding": {
        "taxonomy": "dei",
        "concepts": ("EntityCommonStockSharesOutstanding", "EntityCommonStockSharesOutstanding"),
        "units": ("shares",),
    },
}


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.fromisoformat(value[:10]).date()


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, "", "None", "null", "-"):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _quarter_from_date(value: date) -> int:
    return ((value.month - 1) // 3) + 1


def _quarter_from_fp(value: Any, fallback_date: date) -> int | None:
    if isinstance(value, str) and value.upper().startswith("Q") and value[1:].isdigit():
        quarter = int(value[1:])
        if 1 <= quarter <= 4:
            return quarter
    return _quarter_from_date(fallback_date)


def _extract_fact_entries(
    company_facts: dict[str, Any],
    *,
    taxonomy: str,
    concepts: tuple[str, ...],
    units: tuple[str, ...],
) -> list[dict[str, Any]]:
    taxonomy_facts = company_facts.get("facts", {}).get(taxonomy, {})
    entries: list[dict[str, Any]] = []

    for concept in concepts:
        concept_data = taxonomy_facts.get(concept, {})
        unit_map = concept_data.get("units", {})
        if not isinstance(unit_map, dict):
            continue
        for unit in units:
            unit_entries = unit_map.get(unit, [])
            if isinstance(unit_entries, list):
                entries.extend(unit_entries)
        if entries:
            break

    return entries


def _choose_latest(existing: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    if existing is None:
        return candidate
    existing_filed = existing.get("filed_date") or date.min
    candidate_filed = candidate.get("filed_date") or date.min
    if candidate_filed >= existing_filed:
        return candidate
    return existing


def _duration_days(entry: dict[str, Any]) -> int | None:
    start_date = _parse_date(entry.get("start"))
    end_date = _parse_date(entry.get("end"))
    if start_date is None or end_date is None:
        return None
    return (end_date - start_date).days


def _extract_period_map(
    company_facts: dict[str, Any],
    *,
    taxonomy: str,
    concepts: tuple[str, ...],
    units: tuple[str, ...],
    quarterly_only: bool,
) -> dict[date, dict[str, Any]]:
    period_map: dict[date, dict[str, Any]] = {}

    for entry in _extract_fact_entries(
        company_facts,
        taxonomy=taxonomy,
        concepts=concepts,
        units=units,
    ):
        end_date = _parse_date(entry.get("end"))
        if end_date is None:
            continue

        value = _to_decimal(entry.get("val"))
        if value is None:
            continue

        if quarterly_only:
            days = _duration_days(entry)
            if days is None or days < 60 or days > 120:
                continue

        candidate = {
            "value": value,
            "fy": entry.get("fy"),
            "fp": entry.get("fp"),
            "form": entry.get("form"),
            "filed_date": _parse_date(entry.get("filed")),
        }
        period_map[end_date] = _choose_latest(period_map.get(end_date), candidate)

    return period_map


def _compute_free_cash_flow(period_row: dict[str, Any]) -> Decimal | None:
    operating_cash_flow = period_row.get("operating_cash_flow")
    capital_expenditures = period_row.get("capital_expenditures")
    if operating_cash_flow is None or capital_expenditures is None:
        return None
    if capital_expenditures < 0:
        return operating_cash_flow + capital_expenditures
    return operating_cash_flow - capital_expenditures


def merge_company_facts(company_facts: dict[str, Any]) -> list[dict[str, Any]]:
    period_maps = {
        name: _extract_period_map(
            company_facts,
            taxonomy=config["taxonomy"],
            concepts=config["concepts"],
            units=config["units"],
            quarterly_only=config.get("quarterly", False),
        )
        for name, config in {**DURATION_FACTS, **INSTANT_FACTS}.items()
    }

    period_dates = sorted(
        set().union(*(period_map.keys() for period_map in period_maps.values())),
        reverse=True,
    )

    rows: list[dict[str, Any]] = []

    for period_end in period_dates:
        metadata = None
        for name in ("revenue", "net_income", "eps", "total_assets", "shares_outstanding"):
            metadata = period_maps.get(name, {}).get(period_end)
            if metadata is not None:
                break

        fiscal_year = int(metadata["fy"]) if metadata and metadata.get("fy") not in (None, "") else period_end.year
        fiscal_quarter = _quarter_from_fp(metadata.get("fp") if metadata else None, period_end)

        row = {
            "fiscal_period_end": period_end,
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "revenue": period_maps["revenue"].get(period_end, {}).get("value"),
            "gross_profit": period_maps["gross_profit"].get(period_end, {}).get("value"),
            "operating_income": period_maps["operating_income"].get(period_end, {}).get("value"),
            "net_income": period_maps["net_income"].get(period_end, {}).get("value"),
            "eps": period_maps["eps"].get(period_end, {}).get("value"),
            "ebitda": None,
            "operating_cash_flow": period_maps["operating_cash_flow"].get(period_end, {}).get("value"),
            "capital_expenditures": period_maps["capital_expenditures"].get(period_end, {}).get("value"),
            "total_assets": period_maps["total_assets"].get(period_end, {}).get("value"),
            "total_liabilities": period_maps["total_liabilities"].get(period_end, {}).get("value"),
            "shareholders_equity": period_maps["shareholders_equity"].get(period_end, {}).get("value"),
            "shares_outstanding": period_maps["shares_outstanding"].get(period_end, {}).get("value"),
            "source": "sec_edgar",
        }
        row["free_cash_flow"] = _compute_free_cash_flow(row)
        row.pop("operating_cash_flow")
        row.pop("capital_expenditures")

        if any(
            row[field] is not None
            for field in (
                "revenue",
                "gross_profit",
                "operating_income",
                "net_income",
                "eps",
                "free_cash_flow",
                "total_assets",
                "total_liabilities",
                "shareholders_equity",
                "shares_outstanding",
            )
        ):
            rows.append(row)

    return rows


def fetch_instruments(db: Session) -> list[dict[str, Any]]:
    return [dict(row._mapping) for row in db.execute(INSTRUMENTS_QUERY)]


def sync_fundamentals() -> None:
    from app.data.db.session import SessionLocal

    client = SECEdgarClient()

    with SessionLocal() as db:
        instruments = fetch_instruments(db)
        total = len(instruments)
        print(f"Found {total} instruments to sync fundamentals for.")

        for index, instrument in enumerate(instruments, start=1):
            instrument_id = instrument["instrument_id"]
            symbol = instrument["symbol"]
            print(f"[{index}/{total}] Syncing fundamentals for {symbol}...")

            try:
                company_facts = client.company_facts(symbol)
                rows = merge_company_facts(company_facts)

                if not rows:
                    print(f"[{index}/{total}] No SEC fundamentals returned for {symbol}.")
                    continue

                db.execute(
                    UPSERT_FUNDAMENTALS_QUERY,
                    [{"instrument_id": instrument_id, **row} for row in rows],
                )
                db.commit()
                print(f"[{index}/{total}] Upserted {len(rows)} fundamentals rows for {symbol}.")
            except Exception as exc:
                db.rollback()
                print(f"[{index}/{total}] Failed to sync fundamentals for {symbol}: {exc}")


if __name__ == "__main__":
    sync_fundamentals()
