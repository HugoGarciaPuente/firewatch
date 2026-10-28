"""Tests for the EFFIS burnt-area client (mocked HTTP, no live calls)."""

from datetime import date

import httpx
import pytest

from firewatch.clients.effis import (
    EffisClient,
    EffisError,
    normalize_effis_area,
)


def _client(handler, **kwargs) -> EffisClient:
    """Build an EffisClient whose transport is a MockTransport."""
    kwargs.setdefault("page_size", 2)
    kwargs.setdefault("retries", 0)
    kwargs.setdefault("retry_delay", 0)
    return EffisClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        base_url="https://effis.test/burntareas/current",
        **kwargs,
    )


def test_client_requires_a_two_letter_country():
    """An empty/invalid code would download the whole European catalogue."""
    with pytest.raises(ValueError):
        EffisClient(base_url="https://effis.test", country="ESP")


def _page(rows, count, next_url=None):
    return {
        "count": count,
        "next": next_url,
        "previous": None,
        "results": rows,
    }


def test_fetch_paginates_and_normalizes(effis_sample):
    """EFFIS ignores date filters: the client must walk every page itself."""
    requested = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request)
        offset = int(request.url.params.get("offset", 0))
        rows = effis_sample[offset : offset + 2]
        next_url = "https://effis.test/next" if offset + 2 < len(effis_sample) else None
        return httpx.Response(
            200, json=_page(rows, count=len(effis_sample), next_url=next_url)
        )

    records = _client(handler).fetch_burnt_areas()

    assert [record["source_id"] for record in records] == ["1001", "1002", "1003"]
    assert [request.url.params["offset"] for request in requested] == ["0", "2"]
    assert all(request.url.params["country"] == "ES" for request in requested)
    assert all(request.url.params["limit"] == "2" for request in requested)

    first = records[0]
    assert first["source"] == "effis"
    assert first["country"] == "ES"
    assert first["firedate"] == date(2026, 7, 15)
    assert first["firedate_ts"].isoformat() == "2026-07-15T14:30:00+02:00"
    assert first["province"] == "Madrid"
    assert first["municipality"] == "Madrid"
    assert first["municipality_code"] is None
    assert first["area_ha"] == 12.5
    assert first["latitude"] == pytest.approx(40.4168)
    assert first["longitude"] == pytest.approx(-3.7038)
    assert first["bbox"] == [-3.71, 40.41, -3.69, 40.42]
    assert first["last_update"].isoformat() == "2026-07-16T08:00:00+02:00"


def test_fetch_filters_dates_inclusively_and_by_area(effis_sample):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json=_page(effis_sample, count=len(effis_sample))
        )

    client = _client(handler, page_size=10)

    only_large = client.fetch_burnt_areas(
        start_date="2026-07-15", end_date="2026-07-16", min_area_ha=1.0
    )
    assert [record["source_id"] for record in only_large] == ["1001"]

    including_small = client.fetch_burnt_areas(
        start_date="2026-07-15", end_date="2026-07-16", min_area_ha=0.5
    )
    assert [record["source_id"] for record in including_small] == ["1001", "1002"]

    june = client.fetch_burnt_areas(
        start_date="2026-06-01", end_date="2026-06-01", min_area_ha=1.0
    )
    assert [record["source_id"] for record in june] == ["1003"]


def test_fetch_skips_a_record_from_another_country(effis_sample):
    """The API's country filter is not trusted as the only scope guard."""
    foreign = dict(effis_sample[0], id=2001, country="PT")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_page([foreign], count=1))

    assert _client(handler, page_size=10).fetch_burnt_areas() == []


def test_fetch_limit_stops_after_n_records(effis_sample):
    requested = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request)
        return httpx.Response(
            200,
            json=_page(effis_sample[:2], count=len(effis_sample), next_url="next"),
        )

    records = _client(handler).fetch_burnt_areas(limit=1)

    assert [record["source_id"] for record in records] == ["1001"]
    assert len(requested) == 1, "limit must stop paging, not just slice the result"


def test_fetch_rejects_an_inverted_range():
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("must validate before any request")

    with pytest.raises(ValueError):
        _client(handler).fetch_burnt_areas(
            start_date="2026-07-16", end_date="2026-07-15"
        )


def test_fetch_retries_a_transient_http_error(monkeypatch, effis_sample):
    monkeypatch.setattr("firewatch.clients.effis.time.sleep", lambda seconds: None)
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503, json={"detail": "temporarily down"})
        return httpx.Response(200, json=_page(effis_sample[:1], count=1))

    records = _client(handler, page_size=10, retries=1).fetch_burnt_areas()

    assert [record["source_id"] for record in records] == ["1001"]
    assert calls["n"] == 2


def test_fetch_raises_after_the_last_retry():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500, json={"detail": "boom"})

    with pytest.raises(EffisError):
        _client(handler).fetch_burnt_areas()

    assert calls["n"] == 1


def test_fetch_rejects_an_unexpected_payload():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[1, 2, 3])

    with pytest.raises(EffisError):
        _client(handler).fetch_burnt_areas()


def test_normalize_uses_the_bbox_when_the_centroid_is_missing():
    record = normalize_effis_area(
        {
            "id": 7,
            "bbox": [-1.0, 40.0, 1.0, 42.0],
            "firedate": "2026-08-01",
            "area_ha": 3,
            "province": "Madrid",
            "commune": "Madrid",
            "country": "ES",
        }
    )

    assert record["latitude"] == pytest.approx(41.0)
    assert record["longitude"] == pytest.approx(0.0)
    assert record["firedate"] == date(2026, 8, 1)
    assert record["firedate_ts"].isoformat() == "2026-08-01T00:00:00"


def test_normalize_skips_a_record_without_date_or_position():
    assert normalize_effis_area({"id": 1, "centroid": {"coordinates": [0, 0]}}) is None
    assert normalize_effis_area({"id": 2, "firedate": "2026-08-01"}) is None
