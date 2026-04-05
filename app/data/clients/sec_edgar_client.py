"""SEC EDGAR API client."""
from __future__ import annotations

import os
import time
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

SEC_EDGAR_USER_AGENT = os.getenv("SEC_EDGAR_USER_AGENT", "MarketLensAI/0.1")
SEC_EDGAR_REQUEST_DELAY_SECONDS = float(os.getenv("SEC_EDGAR_REQUEST_DELAY_SECONDS", "0.25"))
SEC_EDGAR_BASE_URL = "https://data.sec.gov"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


class SECEdgarClient:
    """Small SEC EDGAR client for ticker lookup and company facts."""

    def __init__(
        self,
        user_agent: str | None = None,
        request_delay_seconds: float | None = None,
    ) -> None:
        self.user_agent = user_agent or SEC_EDGAR_USER_AGENT
        self.request_delay_seconds = (
            SEC_EDGAR_REQUEST_DELAY_SECONDS
            if request_delay_seconds is None
            else request_delay_seconds
        )
        self._ticker_map: dict[str, str] | None = None

    def _get_json(self, url: str) -> dict[str, Any]:
        if self.request_delay_seconds > 0:
            time.sleep(self.request_delay_seconds)

        response = requests.get(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected SEC response type: {type(data)!r}")
        return data

    def ticker_map(self) -> dict[str, str]:
        if self._ticker_map is not None:
            return self._ticker_map

        data = self._get_json(SEC_TICKERS_URL)
        ticker_map: dict[str, str] = {}

        for item in data.values():
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("ticker", "")).upper()
            cik = item.get("cik_str")
            if not ticker or cik in (None, ""):
                continue
            ticker_map[ticker] = str(int(cik)).zfill(10)

        self._ticker_map = ticker_map
        return ticker_map

    def cik_for_ticker(self, symbol: str) -> str:
        ticker = symbol.upper()
        cik = self.ticker_map().get(ticker)
        if cik is None:
            raise KeyError(f"No SEC CIK found for symbol {symbol!r}")
        return cik

    def company_facts(self, symbol: str) -> dict[str, Any]:
        cik = self.cik_for_ticker(symbol)
        return self._get_json(f"{SEC_EDGAR_BASE_URL}/api/xbrl/companyfacts/CIK{cik}.json")
