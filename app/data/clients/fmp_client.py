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
        data = self._get("/historical-price-eod/full", params)

        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "historical" in data:
            return data["historical"]
        return []
