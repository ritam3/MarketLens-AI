from __future__ import annotations

import os
import time
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_REQUEST_DELAY_SECONDS = float(os.getenv("FMP_REQUEST_DELAY_SECONDS", "0.25"))
BASE_URL = "https://financialmodelingprep.com/stable"


class FMPClient:
    def __init__(
        self,
        api_key: str | None = None,
        request_delay_seconds: float | None = None,
    ) -> None:
        self.api_key = api_key or FMP_API_KEY
        if not self.api_key:
            raise ValueError("FMP_API_KEY is not set in .env")
        self.request_delay_seconds = (
            FMP_REQUEST_DELAY_SECONDS if request_delay_seconds is None else request_delay_seconds
        )

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        if self.request_delay_seconds > 0:
            time.sleep(self.request_delay_seconds)

        response = requests.get(
            f"{BASE_URL}{path}",
            params={**params, "apikey": self.api_key},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def company_profile(self, symbol: str) -> dict[str, Any]:
        data = self._get("/profile", {"symbol": symbol})
        if isinstance(data, list):
            return data[0] if data else {}
        if isinstance(data, dict):
            return data
        return {}

    def _get_list(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        data = self._get(path, params)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "historical" in data and isinstance(data["historical"], list):
            return data["historical"]
        return []

    def historical_price_eod_full(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        return self._get_list("/historical-price-eod/full", params)

    def historical_chart(
        self,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        return self._get_list(f"/historical-chart/{interval}", params)

    def income_statements(
        self,
        symbol: str,
        period: str = "quarter",
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "period": period}
        if limit is not None:
            params["limit"] = limit
        return self._get_list("/income-statement", params)

    def balance_sheet_statements(
        self,
        symbol: str,
        period: str = "quarter",
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "period": period}
        if limit is not None:
            params["limit"] = limit
        return self._get_list("/balance-sheet-statement", params)

    def cash_flow_statements(
        self,
        symbol: str,
        period: str = "quarter",
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "period": period}
        if limit is not None:
            params["limit"] = limit
        return self._get_list("/cash-flow-statement", params)

    def earnings_calendar(
        self,
        symbol: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        if limit is not None:
            params["limit"] = limit
        return self._get_list("/earnings-calendar", params)
