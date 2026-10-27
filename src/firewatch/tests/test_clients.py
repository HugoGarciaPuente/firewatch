"""Tests for the NASA FIRMS client adapter (mocked HTTP, no live calls)."""

import httpx
import pytest

from firewatch.clients.firms import FirmsClient


def _client_with_handler(handler, api_key: str = "mock-key") -> FirmsClient:
    """Build a FirmsClient whose transport is a MockTransport."""
    return FirmsClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        api_key=api_key,
    )


def test_source_defaults_to_the_configured_product(monkeypatch):
    """The product must be switchable by env, and a caller can still override it.

    NRT only reaches ~4 months back and SP ends where the standard product has
    been processed, so a summer backfill needs to pick the source per period.
    """
    from firewatch.config import settings

    monkeypatch.setattr(settings, "NASA_FIRMS_SOURCE", "VIIRS_SNPP_SP")
    assert FirmsClient(api_key="mock-key").source == "VIIRS_SNPP_SP"
    assert (
        FirmsClient(api_key="mock-key", source="VIIRS_NOAA21_NRT").source
        == "VIIRS_NOAA21_NRT"
    )


def test_fetch_by_bbox_returns_list_of_records(firms_sample):
    def handler(request: httpx.Request) -> httpx.Response:
        assert "/area/csv" in request.url.path
        return httpx.Response(200, json=firms_sample)

    client = _client_with_handler(handler)
    result = client.fetch_by_bbox("10,20,30,40", date="2024-08-15")

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["latitude"] == 19.5
    assert result[0]["acq_date"] == "2024-08-15"


def test_fetch_sends_bbox_and_date_params(firms_sample):
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = request.url
        return httpx.Response(200, json=firms_sample)

    client = _client_with_handler(handler, api_key="test-api-key")
    client.fetch_by_bbox("10,20,30,40", date="2024-08-15")

    # NASA FIRMS path format: /area/csv/{MAP_KEY}/{SOURCE}/{WEST,SOUTH,EAST,NORTH}/{DAY_RANGE}/{DATE}
    path = captured["url"].path
    assert "test-api-key" in path
    assert "20,10,40,30" in path  # west,south,east,north
    assert "10,20,30,40" not in path  # the caller's order must never reach FIRMS
    assert path.endswith("/1/2024-08-15")
    # Everything travels in the path; a query string would mean a second code path.
    assert captured["url"].query == b""


def test_fetch_omits_date_when_not_provided(firms_sample):
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = request.url
        return httpx.Response(200, json=firms_sample)

    client = _client_with_handler(handler, api_key="test-api-key")
    client.fetch_by_bbox("0,0,1,1")

    path = captured["url"].path
    assert path.endswith("/1")
    assert "2024" not in path


def test_fetch_parses_csv_response():
    csv_data = (
        "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time\n"
        "36.96669,-1.90653,315.82,0.39,0.36,2026-10-07,206\n"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=csv_data, headers={"content-type": "text/csv"})

    client = _client_with_handler(handler)
    result = client.fetch_by_bbox("36,-6,44,2")

    assert len(result) == 1
    assert result[0]["latitude"] == "36.96669"
    assert result[0]["bright_ti4"] == "315.82"
    assert result[0]["acq_date"] == "2026-10-07"


def test_fetch_raises_on_http_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    client = _client_with_handler(handler)
    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_by_bbox("0,0,1,1", date="2024-01-01")


def test_missing_api_key_is_rejected_at_construction():
    """FIRMS has no anonymous area endpoint, so there is no fallback URL to build.

    The branch this replaced was also broken (it forwarded the bbox in
    south,west,north,east order) and no test ever executed it.
    """
    with pytest.raises(ValueError, match="NASA_FIRMS_API_KEY"):
        FirmsClient(api_key="")


def test_weather_client_returns_metrics():
    from firewatch.clients.weather import WeatherClient

    payload = {
        "current": {
            "temperature_2m": 28.5,
            "relative_humidity_2m": 32.0,
            "wind_speed_10m": 22.4,
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    client = WeatherClient(client=httpx.Client(transport=httpx.MockTransport(handler)))
    data = client.get_weather(40.4168, -3.7038)

    assert data is not None
    assert data["temperature_c"] == 28.5
    assert data["relative_humidity"] == 32.0
    assert data["wind_speed_kmh"] == 22.4


def test_weather_client_handles_error_gracefully():
    from firewatch.clients.weather import WeatherClient

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="Service Unavailable")

    client = WeatherClient(client=httpx.Client(transport=httpx.MockTransport(handler)))
    data = client.get_weather(40.4168, -3.7038)

    assert data is None


def test_weather_client_caches_per_cell():
    """Nearby points in the same cell must not trigger a second HTTP request."""
    from firewatch.clients.weather import WeatherClient

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            200,
            json={
                "current": {
                    "temperature_2m": 28.5,
                    "relative_humidity_2m": 32.0,
                    "wind_speed_10m": 22.4,
                }
            },
        )

    client = WeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        cell_degrees=0.25,
    )

    # Three points well inside the same 0.25° cell.
    client.get_weather(40.4168, -3.7038)
    client.get_weather(40.4200, -3.7000)
    client.get_weather(40.4100, -3.6900)

    assert calls["n"] == 1


def test_weather_client_caches_failures():
    """A failing upstream must not be retried once per record."""
    from firewatch.clients.weather import WeatherClient

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    client = WeatherClient(client=httpx.Client(transport=httpx.MockTransport(handler)))
    assert client.get_weather(40.4168, -3.7038) is None
    assert client.get_weather(40.4170, -3.7040) is None

    assert calls["n"] == 1


def test_weather_client_uses_the_archive_for_old_dates():
    """The forecast endpoint only reaches ~90 days back; summer history needs ERA5.

    Without this, Open-Meteo answers 400 for June and every point is scored on
    brightness alone -- silently, because the fallback is a valid risk value.
    """
    from datetime import date as date_cls, timedelta

    from firewatch.clients.weather import WeatherClient

    hosts = []

    def handler(request: httpx.Request) -> httpx.Response:
        hosts.append(request.url.host)
        return httpx.Response(
            200,
            json={
                "hourly": {
                    "temperature_2m": [12.0, 28.5],
                    "relative_humidity_2m": [80.0, 32.0],
                    "wind_speed_10m": [5.0, 22.4],
                }
            },
        )

    client = WeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        base_url="https://forecast.test/v1/forecast",
        archive_url="https://archive.test/v1/archive",
        forecast_days=30,
    )
    old = (date_cls.today() - timedelta(days=120)).isoformat()
    recent = (date_cls.today() - timedelta(days=2)).isoformat()

    client.get_weather(40.4168, -3.7038, date=old)
    client.get_weather(40.4168, -3.7038, date=recent)

    assert hosts == ["archive.test", "forecast.test"]


def test_weather_client_falls_back_when_the_forecast_series_is_empty():
    """The forecast window edge returns 200 with 24 nulls, not an error."""
    from datetime import date as date_cls, timedelta

    from firewatch.clients.weather import WeatherClient

    hosts = []
    empty = {
        "hourly": {
            "temperature_2m": [None] * 24,
            "relative_humidity_2m": [None] * 24,
            "wind_speed_10m": [None] * 24,
        }
    }
    filled = {
        "hourly": {
            "temperature_2m": [12.0] * 24,
            "relative_humidity_2m": [80.0] * 24,
            "wind_speed_10m": [5.0] * 24,
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        hosts.append(request.url.host)
        payload = empty if request.url.host == "forecast.test" else filled
        return httpx.Response(200, json=payload)

    client = WeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        base_url="https://forecast.test/v1/forecast",
        archive_url="https://archive.test/v1/archive",
        forecast_days=30,
    )
    # Two days old: the heuristic picks the forecast, whose series is empty.
    recent = (date_cls.today() - timedelta(days=2)).isoformat()
    data = client.get_weather(40.4168, -3.7038, date=recent)

    assert hosts == ["forecast.test", "archive.test"]
    assert data is not None
    assert data["temperature_c"] == 12.0


def test_weather_client_uses_the_requested_date():
    """A past date must query hourly data, not current conditions."""
    from firewatch.clients.weather import WeatherClient

    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["params"] = request.url.params
        return httpx.Response(
            200,
            json={
                "hourly": {
                    "temperature_2m": [12.0, None, 28.5, 21.0],
                    "relative_humidity_2m": [80.0, 65.0, 32.0, 45.0],
                    "wind_speed_10m": [5.0, 12.0, 22.4, 9.0],
                }
            },
        )

    client = WeatherClient(client=httpx.Client(transport=httpx.MockTransport(handler)))
    data = client.get_weather(40.4168, -3.7038, date="2026-10-07")

    assert seen["params"]["start_date"] == "2026-10-07"
    assert seen["params"]["end_date"] == "2026-10-07"
    assert seen["params"]["timezone"] == "GMT"
    assert "hourly" in seen["params"]
    assert "current" not in seen["params"]
    assert data == {
        "temperature_c": 28.5,
        "relative_humidity": 32.0,
        "wind_speed_kmh": 22.4,
    }


def test_weather_client_uses_the_overpass_hour():
    """With acq_time, pick the hour nearest the overpass, not the daily maximum."""
    from firewatch.clients.weather import WeatherClient

    payload = {
        "hourly": {
            "time": [f"2026-10-07T{h:02d}:00" for h in range(24)],
            "temperature_2m": [10.0 + h for h in range(24)],
            "relative_humidity_2m": [90.0 - h for h in range(24)],
            "wind_speed_10m": [1.0 + h * 0.5 for h in range(24)],
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    client = WeatherClient(client=httpx.Client(transport=httpx.MockTransport(handler)))
    data = client.get_weather(40.4168, -3.7038, date="2026-10-07", acq_time="1345")

    # 13:45 is 15 minutes from 14:00 and 45 from 13:00.
    assert data == {
        "temperature_c": 24.0,
        "relative_humidity": 76.0,
        "wind_speed_kmh": 8.0,
    }


def test_weather_client_shares_the_hourly_series_across_overpass_times():
    """One request per cell/day, but each fire keeps the weather of its own hour."""
    from firewatch.clients.weather import WeatherClient

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            200,
            json={
                "hourly": {
                    "time": [f"2026-10-07T{h:02d}:00" for h in range(24)],
                    "temperature_2m": [10.0 + h for h in range(24)],
                    "relative_humidity_2m": [90.0 - h for h in range(24)],
                    "wind_speed_10m": [1.0 + h * 0.5 for h in range(24)],
                }
            },
        )

    client = WeatherClient(client=httpx.Client(transport=httpx.MockTransport(handler)))

    night = client.get_weather(40.4168, -3.7038, date="2026-10-07", acq_time="0200")
    day = client.get_weather(40.4170, -3.7040, date="2026-10-07", acq_time="1400")

    assert calls["n"] == 1
    assert night["temperature_c"] == 12.0
    assert day["temperature_c"] == 24.0


def test_weather_client_cache_includes_the_date():
    """The same cell on two different dates needs two lookups."""
    from firewatch.clients.weather import WeatherClient

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            200,
            json={
                "hourly": {
                    "temperature_2m": [20.0],
                    "relative_humidity_2m": [50.0],
                    "wind_speed_10m": [10.0],
                }
            },
        )

    client = WeatherClient(client=httpx.Client(transport=httpx.MockTransport(handler)))
    client.get_weather(40.4168, -3.7038, date="2026-10-07")
    client.get_weather(40.4170, -3.7040, date="2026-10-08")

    assert calls["n"] == 2


def test_weather_client_does_not_invent_missing_metrics():
    """A partial payload must yield None, not plausible defaults."""
    from firewatch.clients.weather import WeatherClient

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"current": {"temperature_2m": 28.5}})

    client = WeatherClient(client=httpx.Client(transport=httpx.MockTransport(handler)))

    assert client.get_weather(40.4168, -3.7038) is None


def test_weather_client_does_not_invent_missing_hourly_metrics():
    """An empty or incomplete hourly block must not produce a fabricated day."""
    from firewatch.clients.weather import WeatherClient

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "hourly": {
                    "temperature_2m": [],
                    "relative_humidity_2m": [],
                    "wind_speed_10m": [],
                }
            },
        )

    client = WeatherClient(client=httpx.Client(transport=httpx.MockTransport(handler)))

    assert client.get_weather(40.4168, -3.7038, date="2026-10-07") is None


def test_weather_client_retries_a_failure_after_the_ttl():
    """A cached failure must expire instead of poisoning the whole run."""
    from firewatch.clients.weather import WeatherClient

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    client = WeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        failure_ttl=0.0,
    )

    assert client.get_weather(40.4168, -3.7038) is None
    assert client.get_weather(40.4168, -3.7038) is None

    assert calls["n"] == 2
