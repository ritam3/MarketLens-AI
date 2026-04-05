"""FRED API client."""
from __future__ import annotations

import os
import time
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_REQUEST_DELAY_SECONDS = float(os.getenv("FRED_REQUEST_DELAY_SECONDS", "0.25"))
BASE_URL = "https://api.stlouisfed.org/fred"


class FREDClient:
    """Small FRED client for metadata and observation pulls."""

    def __init__(
        self,
        api_key: str | None = None,
        request_delay_seconds: float | None = None,
    ) -> None:
        self.api_key = api_key or FRED_API_KEY
        if not self.api_key:
            raise ValueError("FRED_API_KEY is not set in .env")
        self.request_delay_seconds = (
            FRED_REQUEST_DELAY_SECONDS if request_delay_seconds is None else request_delay_seconds
        )

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        if self.request_delay_seconds > 0:
            time.sleep(self.request_delay_seconds)

        response = httpx.get(
            f"{BASE_URL}{path}",
            params={**params, "api_key": self.api_key, "file_type": "json"},
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()

    def series(self, series_id: str) -> dict[str, Any]:
        return self._get("/series", {"series_id": series_id})

    def series_observations(
        self,
        series_id: str,
        observation_start: str | None = None,
        observation_end: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"series_id": series_id}
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end
        return self._get("/series/observations", params)

    def observations(
        self,
        series_id: str,
        observation_start: str | None = None,
        observation_end: str | None = None,
    ) -> dict[str, Any]:
        return self.series_observations(
            series_id=series_id,
            observation_start=observation_start,
            observation_end=observation_end,
        )
