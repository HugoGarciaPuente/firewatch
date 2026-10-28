"""Tests for the bulk daily weather client (mocked HTTP, no live calls)."""

import csv

import httpx

from firewatch.clients.daily_weather import (
    DailyWeatherClient,
    WEATHER_FIELDS,
    write_weather_csv,
)


def _daily_payload():
    return {
        "daily": {
            "time": ["2024-01-01", "2024-01-02"],
            "temperature_2m_max": [10.0, 12.5],
            "temperature_2m_min": [1.0, 2.0],
            "relative_humidity_2m_min": [40, 35],
            "wind_speed_10m_max": [15.0, 20.0],
            "precipitation_sum": [0.0, 3.2],
        }
    }


def test_fetch_daily_parses_all_variables():
    seen = {}

    def handler(request):
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json=_daily_payload())

    client = DailyWeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        archive_url="https://archive.test/v1/archive",
        forecast_url="https://forecast.test/v1/forecast",
    )

    rows = client.fetch_daily(40.4, -3.7, "2024-01-01", "2024-01-02")

    assert seen["params"]["timezone"] == "GMT"
    assert "temperature_2m_max" in seen["params"]["daily"]
    assert rows == [
        {
            "day": "2024-01-01",
            "tmax_c": 10.0,
            "tmin_c": 1.0,
            "rh_min": 40.0,
            "wind_max_kmh": 15.0,
            "precip_mm": 0.0,
        },
        {
            "day": "2024-01-02",
            "tmax_c": 12.5,
            "tmin_c": 2.0,
            "rh_min": 35.0,
            "wind_max_kmh": 20.0,
            "precip_mm": 3.2,
        },
    ]


def test_fetch_daily_falls_back_to_the_forecast_url():
    calls = []

    def handler(request):
        calls.append(request.url.host)
        if request.url.host == "archive.test":
            return httpx.Response(200, json={"daily": {"time": []}})
        return httpx.Response(200, json=_daily_payload())

    client = DailyWeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        archive_url="https://archive.test/v1/archive",
        forecast_url="https://forecast.test/v1/forecast",
        forecast_window_days=100_000,
    )

    rows = client.fetch_daily(40.4, -3.7, "2024-01-01", "2024-01-02")

    assert calls == ["archive.test", "forecast.test"]
    assert len(rows) == 2


def test_fetch_forecast_never_calls_the_archive():
    """The target day is future: asking ERA5 first would be a pointless call."""
    calls = []

    def handler(request):
        calls.append(request.url.host)
        return httpx.Response(200, json=_daily_payload())

    client = DailyWeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        archive_url="https://archive.test/v1/archive",
        forecast_url="https://forecast.test/v1/forecast",
    )

    rows = client.fetch_forecast(40.4, -3.7, "2026-10-29", "2026-10-29")

    assert calls == ["forecast.test"]
    assert len(rows) == 2


def test_a_long_historical_range_does_not_try_the_forecast_api():
    """The forecast endpoint rejects a 2017-2026 range; retrying it only adds noise."""
    calls = []

    def handler(request):
        calls.append(request.url.host)
        return httpx.Response(200, json={"daily": {"time": []}})

    client = DailyWeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        archive_url="https://archive.test/v1/archive",
        forecast_url="https://forecast.test/v1/forecast",
    )

    rows = client.fetch_daily(40.4, -3.7, "2017-01-01", "2026-10-25")

    assert calls == ["archive.test"]
    assert rows == []


def test_429_retries_using_retry_after(monkeypatch):
    from firewatch.clients import daily_weather

    sleeps = []
    monkeypatch.setattr(daily_weather.time, "sleep", sleeps.append)
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, json=_daily_payload())

    client = DailyWeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        archive_url="https://archive.test/v1/archive",
        retries=1,
    )

    rows = client.fetch_daily(40.4, -3.7, "2024-01-01", "2024-01-02")

    assert len(rows) == 2
    assert sleeps == [7.0]


def test_days_with_every_variable_null_are_omitted():
    payload = {
        "daily": {
            "time": ["2024-01-01", "2024-01-02"],
            "temperature_2m_max": [None, 12.0],
            "temperature_2m_min": [None, 2.0],
            "relative_humidity_2m_min": [None, 35],
            "wind_speed_10m_max": [None, 20.0],
            "precipitation_sum": [None, 0.0],
        }
    }

    def handler(request):
        return httpx.Response(200, json=payload)

    client = DailyWeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        archive_url="https://archive.test/v1/archive",
    )

    rows = client.fetch_daily(40.4, -3.7, "2024-01-01", "2024-01-02")

    assert [row["day"] for row in rows] == ["2024-01-02"]


def test_partial_null_keeps_the_day_with_none():
    payload = {
        "daily": {
            "time": ["2024-01-01"],
            "temperature_2m_max": [None],
            "temperature_2m_min": [2.0],
            "relative_humidity_2m_min": [35],
            "wind_speed_10m_max": [20.0],
            "precipitation_sum": [0.0],
        }
    }

    def handler(request):
        return httpx.Response(200, json=payload)

    client = DailyWeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        archive_url="https://archive.test/v1/archive",
    )

    rows = client.fetch_daily(40.4, -3.7, "2024-01-01", "2024-01-01")

    assert len(rows) == 1
    assert rows[0]["tmax_c"] is None
    assert rows[0]["tmin_c"] == 2.0


def test_write_weather_csv_has_a_stable_header(tmp_path):
    path = tmp_path / "weather.csv"
    rows = [
        {
            "province_code": "28",
            "day": "2024-01-01",
            "tmax_c": 10.0,
            "tmin_c": 1.0,
            "rh_min": 40.0,
            "wind_max_kmh": 15.0,
            "precip_mm": 0.0,
        }
    ]

    written = write_weather_csv(path, rows)

    assert written == 1
    with path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert tuple(reader.fieldnames) == WEATHER_FIELDS
        assert list(reader)[0]["province_code"] == "28"
