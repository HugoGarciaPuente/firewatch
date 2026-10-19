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
    assert path.endswith("/1/2024-08-15")


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
    assert client.cache_stats() == {"hits": 2, "misses": 1, "size": 1}


def test_weather_client_cache_can_be_disabled():
    from firewatch.clients.weather import WeatherClient

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={"current": {}})

    client = WeatherClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        cache_enabled=False,
    )
    client.get_weather(40.4168, -3.7038)
    client.get_weather(40.4168, -3.7038)

    assert calls["n"] == 2


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
