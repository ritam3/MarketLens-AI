from __future__ import annotations

from types import SimpleNamespace

from app.data.clients import fmp_client, fred_client, sec_edgar_client


def test_fmp_client_applies_request_delay(monkeypatch) -> None:
    calls: list[float] = []

    def fake_sleep(seconds: float) -> None:
        calls.append(seconds)

    def fake_get(*args, **kwargs):
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: [],
        )

    monkeypatch.setattr(fmp_client.time, "sleep", fake_sleep)
    monkeypatch.setattr(fmp_client.requests, "get", fake_get)

    client = fmp_client.FMPClient(api_key="test-key", request_delay_seconds=0.1)
    client.company_profile("AAPL")

    assert calls == [0.1]


def test_fred_client_applies_request_delay(monkeypatch) -> None:
    calls: list[float] = []

    def fake_sleep(seconds: float) -> None:
        calls.append(seconds)

    def fake_get(*args, **kwargs):
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {},
        )

    monkeypatch.setattr(fred_client.time, "sleep", fake_sleep)
    monkeypatch.setattr(fred_client.httpx, "get", fake_get)

    client = fred_client.FREDClient(api_key="test-key", request_delay_seconds=0.2)
    client.series("GDP")

    assert calls == [0.2]


def test_fred_client_observations_alias_calls_series_observations(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_series_observations(self, series_id: str, observation_start=None, observation_end=None):
        captured["series_id"] = series_id
        captured["observation_start"] = observation_start
        captured["observation_end"] = observation_end
        return {"observations": []}

    monkeypatch.setattr(fred_client.FREDClient, "series_observations", fake_series_observations)

    client = fred_client.FREDClient(api_key="test-key", request_delay_seconds=0)
    response = client.observations("CPIAUCSL", observation_start="2020-01-01")

    assert response == {"observations": []}
    assert captured == {
        "series_id": "CPIAUCSL",
        "observation_start": "2020-01-01",
        "observation_end": None,
    }


def test_sec_edgar_client_applies_request_delay(monkeypatch) -> None:
    calls: list[float] = []

    def fake_sleep(seconds: float) -> None:
        calls.append(seconds)

    def fake_get(*args, **kwargs):
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."},
            },
        )

    monkeypatch.setattr(sec_edgar_client.time, "sleep", fake_sleep)
    monkeypatch.setattr(sec_edgar_client.requests, "get", fake_get)

    client = sec_edgar_client.SECEdgarClient(
        user_agent="test-agent",
        request_delay_seconds=0.3,
    )
    assert client.cik_for_ticker("AAPL") == "0000320193"

    assert calls == [0.3]
