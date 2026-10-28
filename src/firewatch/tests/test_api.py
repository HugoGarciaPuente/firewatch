"""Tests for the FastAPI app (no external services needed)."""

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Secrets must exist before config is imported, otherwise production
# validation would reject the import.
os.environ.setdefault("API_KEY", "test-key-1234567890")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-abcdef")

from firewatch.api.main import app  # noqa: E402

API_KEY = os.environ["API_KEY"]
AUTH = {"X-API-Key": API_KEY}

client = TestClient(app)


@pytest.fixture(autouse=True)
def _isolate_cookies():
    """Stop session cookies leaking between tests via the shared client."""
    client.cookies.clear()
    yield
    client.cookies.clear()


@pytest.fixture
def authed():
    """A client that has exchanged the API key for a session cookie."""
    c = TestClient(app)
    c.post("/auth/login", headers=AUTH)
    return c


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_openapi_document_generated():
    response = client.get("/openapi.json")

    assert response.status_code == 200
    paths = response.json()["paths"]
    assert "/api/v1/fire/events" in paths
    assert "/api/v1/fire/ingest" in paths
    assert "/api/v1/fire/stats" in paths
    # Verify risk query parameters are documented
    parameters = [p["name"] for p in paths["/api/v1/fire/events"]["get"]["parameters"]]
    assert "risk_level" in parameters
    assert "min_risk_score" in parameters
    # Day-granularity filtering
    assert "date" in parameters
    # Per-day history breakdown powers the day selector
    assert "/api/v1/fire/dates" in paths
    # /stats accepts a day so the top KPIs follow the selected day
    stats_params = [p["name"] for p in paths["/api/v1/fire/stats"]["get"]["parameters"]]
    assert "date" in stats_params
    # Statistics section: aggregated by province/municipality, optional day/range/province
    assert "/api/v1/fire/locality-stats" in paths
    locality_params = [
        p["name"] for p in paths["/api/v1/fire/locality-stats"]["get"]["parameters"]
    ]
    assert {"date", "start_date", "end_date", "province", "limit"} <= set(locality_params)
    # The province selector needs the full list, not only the top N ranking
    assert "/api/v1/fire/provinces" in paths
    # The ML forecast is read from fire_predictions, with an optional date/model
    assert "/api/v1/fire/forecast" in paths
    forecast_params = [
        p["name"] for p in paths["/api/v1/fire/forecast"]["get"]["parameters"]
    ]
    assert {"date", "model"} <= set(forecast_params)


def test_dashboard_endpoint_requires_session(authed):
    """The dashboard is browser-only: no session, no dashboard."""
    assert client.get("/dashboard").status_code == 401

    response = authed.get("/dashboard")
    assert response.status_code == 200
    assert "FireWatch" in response.text


def test_stats_page_requires_session(authed):
    """The statistics page is another dashboard shell: it must not be public."""
    assert client.get("/stats").status_code == 401

    response = authed.get("/stats")
    assert response.status_code == 200
    assert "Estadísticas" in response.text


def test_forecast_page_requires_session(authed):
    """The forecast map is a separate dashboard shell and must stay protected."""
    assert client.get("/forecast").status_code == 401

    response = authed.get("/forecast")
    assert response.status_code == 200
    assert "Pronóstico 24 h" in response.text
    assert 'id="forecast-map"' in response.text


def test_sources_page_requires_session(authed):
    """The sources page is part of the protected shell, not a public readme."""
    assert client.get("/sources").status_code == 401

    response = authed.get("/sources")
    assert response.status_code == 200
    assert "Fuentes y método" in response.text
    for source in ("NASA FIRMS", "EFFIS", "Open-Meteo", "Nominatim"):
        assert source in response.text


def test_login_page_is_public():
    """Must stay reachable, otherwise nobody can ever authenticate."""
    response = client.get("/login")
    assert response.status_code == 200
    assert "api-key" in response.text


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "query, expected_filters, expected_scope",
    [
        (
            "date=2026-10-12",
            {
                "date": "2026-10-12",
                "start_date": None,
                "end_date": None,
                "province": None,
            },
            "2026-10-12",
        ),
        (
            "start_date=2026-10-01&end_date=2026-10-12&province=Cáceres",
            {
                "date": None,
                "start_date": "2026-10-01",
                "end_date": "2026-10-12",
                "province": "Cáceres",
            },
            "2026-10-01..2026-10-12",
        ),
    ],
)
def test_locality_stats_shapes_the_aggregations(
    authed, monkeypatch, query, expected_filters, expected_scope
):
    """The endpoint exposes totals, levels, rankings and daily, for a day or a range."""
    from datetime import date

    from firewatch.api import endpoints as ep

    class FakeCursor:
        def __init__(self, calls):
            self.calls = calls
            self._rows = []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            self.calls.append((sql, params))
            if "COUNT(DISTINCT acq_date)" in sql:
                self._rows = [
                    (1017, 1014, 334, 36, 29, date(2026, 9, 28), date(2026, 10, 26), 0.91)
                ]
            elif "GROUP BY risk_level" in sql:
                self._rows = [("high", 400), ("moderate", 350), ("low", 267)]
            elif "GROUP BY 1, 2, 3, 4" in sql:
                self._rows = [
                    ("Cedillo", "Cáceres", "Extremadura", "España", 31, 10, 0.8)
                ]
            elif "GROUP BY 1, 2, 3" in sql:
                self._rows = [
                    ("Cáceres", "Extremadura", "España", 120, 15, 40, 0.82)
                ]
            elif "GROUP BY acq_date" in sql:
                self._rows = [(date(2026, 9, 28), 30, 12, 0.8)]

        def fetchone(self):
            return self._rows[0]

        def fetchall(self):
            return self._rows

    class FakeConn:
        def __init__(self):
            self.calls = []

        def cursor(self, *args, **kwargs):
            return FakeCursor(self.calls)

        def close(self):
            pass

    conn = FakeConn()
    monkeypatch.setattr(ep.psycopg2, "connect", lambda *args, **kwargs: conn)

    response = authed.get(f"/api/v1/fire/locality-stats?{query}&limit=5")

    assert response.status_code == 200
    data = response.json()
    assert data["scope"] == expected_scope
    assert data["filters"] == expected_filters
    assert data["totals"] == {
        "events": 1017,
        "located": 1014,
        "municipalities": 334,
        "provinces": 36,
        "days": 29,
        "first_date": "2026-09-28",
        "last_date": "2026-10-26",
        "max_risk_score": 0.91,
    }
    assert data["by_risk_level"] == {
        "extreme": 0,
        "high": 400,
        "moderate": 350,
        "low": 267,
    }
    assert data["provinces"][0]["province"] == "Cáceres"
    assert data["provinces"][0]["high"] == 40
    assert data["municipalities"][0]["municipality"] == "Cedillo"
    assert data["municipalities"][0]["max_risk_score"] == 0.8
    assert data["daily"] == [
        {"date": "2026-09-28", "events": 30, "high": 12, "max_risk_score": 0.8}
    ]
    # Every aggregate query must receive the whole range and the province filter.
    for _, params in conn.calls:
        params = params or ()
        if expected_filters["date"]:
            assert expected_filters["date"] in params
        else:
            assert expected_filters["start_date"] in params
            assert expected_filters["end_date"] in params
        if expected_filters["province"]:
            assert expected_filters["province"] in params


def test_list_provinces_returns_selector_options(authed, monkeypatch):
    """The selector needs every province, not only the top N of the ranking."""
    from firewatch.api import endpoints as ep

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            assert "GROUP BY 1, 2, 3" in sql
            self._rows = [
                ("Ariège", "Occitanie", "France", 40),
                ("Badajoz", "Extremadura", "España", 148),
            ]

        def fetchall(self):
            return self._rows

    class FakeConn:
        def cursor(self, *args, **kwargs):
            return FakeCursor()

        def close(self):
            pass

    monkeypatch.setattr(ep.psycopg2, "connect", lambda *args, **kwargs: FakeConn())

    response = authed.get("/api/v1/fire/provinces")

    assert response.status_code == 200
    assert response.json() == [
        {
            "province": "Ariège",
            "community": "Occitanie",
            "country": "France",
            "events": 40,
        },
        {
            "province": "Badajoz",
            "community": "Extremadura",
            "country": "España",
            "events": 148,
        },
    ]


def test_forecast_defaults_to_latest_date_and_model(authed, monkeypatch):
    """The dashboard must open on the latest forecast, not on an empty today."""
    from datetime import date, datetime, timezone

    from firewatch.api import endpoints as ep

    class FakeCursor:
        def __init__(self):
            self.calls = []
            self._one = None
            self._rows = []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            self.calls.append((sql, params))
            if "MAX(target_date)" in sql:
                self._one = (date(2026, 10, 29),)
            elif "SELECT model_name" in sql:
                self._one = ("hgb",)
            elif "FROM fire_predictions" in sql:
                self._rows = [
                    (
                        "28",
                        "Madrid",
                        0.1007123,
                        0.1658653,
                        "590c93d9b521",
                        "open-meteo-archive+forecast",
                        datetime(2026, 10, 28, 8, 43, 45, tzinfo=timezone.utc),
                    ),
                    (
                        "33",
                        "Asturias",
                        0.0715243,
                        0.0838815,
                        "590c93d9b521",
                        "open-meteo-archive+forecast",
                        datetime(2026, 10, 28, 8, 43, 45, tzinfo=timezone.utc),
                    ),
                ]

        def fetchone(self):
            return self._one

        def fetchall(self):
            return self._rows

    class FakeConn:
        def __init__(self):
            self.cursor_instance = None

        def cursor(self):
            self.cursor_instance = FakeCursor()
            return self.cursor_instance

        def close(self):
            pass

    conn = FakeConn()
    monkeypatch.setattr(ep.psycopg2, "connect", lambda *args, **kwargs: conn)

    response = authed.get("/api/v1/fire/forecast")

    assert response.status_code == 200
    data = response.json()
    assert data["target_date"] == "2026-10-29"
    assert data["model_name"] == "hgb"
    assert data["model_version"] == "590c93d9b521"
    assert data["weather_source"] == "open-meteo-archive+forecast"
    assert data["generated_at"] == "2026-10-28T08:43:45+00:00"
    assert data["count"] == 2
    assert data["provinces"] == [
        {
            "province_code": "28",
            "province_name": "Madrid",
            "probability": 0.1007,
            "probability_calibrated": 0.1659,
            "latitude": 40.4168,
            "longitude": -3.7038,
        },
        {
            "province_code": "33",
            "province_name": "Asturias",
            "probability": 0.0715,
            "probability_calibrated": 0.0839,
            "latitude": 43.3619,
            "longitude": -5.8494,
        },
    ]

    calls = conn.cursor_instance.calls
    assert any("MAX(target_date)" in sql for sql, _ in calls)
    assert [params for sql, params in calls if "ORDER BY probability DESC" in sql] == [
        ("2026-10-29", "hgb")
    ]


def test_forecast_returns_404_for_an_absent_date(authed, monkeypatch):
    from firewatch.api import endpoints as ep

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            pass

        def fetchone(self):
            return None

        def fetchall(self):
            return []

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    monkeypatch.setattr(ep.psycopg2, "connect", lambda *args, **kwargs: FakeConn())

    response = authed.get("/api/v1/fire/forecast?date=2026-10-30")

    assert response.status_code == 404
    assert "2026-10-30" in response.json()["detail"]


def test_forecast_returns_404_when_the_table_is_empty(authed, monkeypatch):
    from firewatch.api import endpoints as ep

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            pass

        def fetchone(self):
            return (None,)

        def fetchall(self):
            return []

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    monkeypatch.setattr(ep.psycopg2, "connect", lambda *args, **kwargs: FakeConn())

    response = authed.get("/api/v1/fire/forecast")

    assert response.status_code == 404
    assert response.json()["detail"] == "No forecast available"


def test_forecast_rejects_an_invalid_date(authed):
    response = authed.get("/api/v1/fire/forecast?date=2026-13-01")

    assert response.status_code == 422
    assert "YYYY-MM-DD" in response.json()["detail"]


@pytest.mark.parametrize(
    "query",
    [
        "date=2026-10-12&start_date=2026-10-01",
        "start_date=2026-10-12&end_date=2026-10-01",
        "date=2026-13-01",
    ],
)
def test_locality_stats_rejects_invalid_filters(authed, query):
    """Bad dates and contradictory filters are a 4xx, not a Postgres 500."""
    response = authed.get(f"/api/v1/fire/locality-stats?{query}")

    assert response.status_code == 422


def test_fire_endpoints_require_a_session():
    """Unauthenticated reads must be refused, not served."""
    for path in (
        "/api/v1/fire/stats",
        "/api/v1/fire/dates",
        "/api/v1/fire/events",
        "/api/v1/fire/forecast",
        "/api/v1/fire/locality-stats",
        "/api/v1/fire/provinces",
    ):
        assert client.get(path).status_code == 401, path


def test_login_rejects_wrong_key():
    response = client.post("/auth/login", headers={"X-API-Key": "wrong-key"})

    assert response.status_code == 401
    assert "firewatch_session" not in response.cookies


def test_login_rejects_missing_key():
    assert client.post("/auth/login").status_code == 401


def test_login_issues_httponly_session_cookie():
    response = client.post("/auth/login", headers=AUTH)

    assert response.status_code == 200
    raw_cookie = response.headers.get("set-cookie", "")
    assert "firewatch_session" in raw_cookie
    assert "HttpOnly" in raw_cookie
    assert "SameSite=strict" in raw_cookie.replace("samesite", "SameSite")


def test_session_endpoint_reports_state():
    anonymous = TestClient(app)
    assert anonymous.get("/auth/session").json() == {"authenticated": False}

    signed_in = TestClient(app)
    signed_in.post("/auth/login", headers=AUTH)
    assert signed_in.get("/auth/session").json() == {"authenticated": True}


def test_logout_invalidates_the_session(authed):
    assert authed.get("/api/v1/fire/stats").status_code == 200

    assert authed.post("/auth/logout").status_code == 200

    assert authed.get("/api/v1/fire/stats").status_code == 401


def test_session_cookie_cannot_be_forged():
    from firewatch.api import auth

    # Tampered payload, original signature.
    forged = auth.issue_session()[:-4] + "AAAA"
    assert auth.verify_session(forged) is False
    # Signed with the wrong secret.
    import hmac as _hmac
    from hashlib import sha256

    import base64
    import json

    payload = base64.urlsafe_b64encode(
        json.dumps({"iat": 0, "exp": 9999999999}).encode()
    ).rstrip(b"=").decode()
    sig = _hmac.new(b"attacker-secret", payload.encode(), sha256).digest()
    evil = payload + "." + base64.urlsafe_b64encode(sig).rstrip(b"=").decode()
    assert auth.verify_session(evil) is False


def test_expired_session_is_rejected():
    from firewatch.api import auth

    assert auth.verify_session(auth.issue_session(max_age=-1)) is False


def test_empty_key_never_authenticates():
    """A blank configured key must not behave as 'no key required'."""
    from firewatch.api import auth
    from firewatch.config import settings

    original = settings.API_KEY
    try:
        settings.API_KEY = ""
        assert auth.verify_api_key("") is False
        assert auth.verify_api_key(None) is False
        assert auth.verify_api_key("anything") is False
    finally:
        settings.API_KEY = original


def test_missing_session_secret_disables_sessions():
    from firewatch.api import auth
    from firewatch.config import settings

    original = settings.SESSION_SECRET
    try:
        settings.SESSION_SECRET = ""
        assert auth.verify_session(auth.issue_session()) is False
    finally:
        settings.SESSION_SECRET = original


def test_static_dashboard_shell_requires_session():
    """Regression: /static/index.html was publicly reachable.

    Serving the dashboard shell without auth means the only gate is the
    client-side JS redirect, which anyone can bypass by disabling JavaScript.
    """
    for path in (
        "/static/index.html",
        "/static/stats.html",
        "/static/forecast.html",
        "/static/sources.html",
        "/static/app.js",
        "/static/stats.js",
        "/static/forecast.js",
        "/static/style.css",
        "/static/data/spain-provinces.geojson",
    ):
        assert client.get(path).status_code == 401, path


def test_static_assets_served_with_session(authed):
    for path in (
        "/static/index.html",
        "/static/stats.html",
        "/static/forecast.html",
        "/static/sources.html",
        "/static/app.js",
        "/static/stats.js",
        "/static/forecast.js",
        "/static/style.css",
        "/static/data/spain-provinces.geojson",
    ):
        assert authed.get(path).status_code == 200, path


def test_login_page_does_not_depend_on_protected_static():
    """The login page cannot load /static/style.css any more, so it must inline it."""
    body = client.get("/login").text
    assert "/static/style.css" not in body, "login page would render unstyled"
    assert "--bg-main" in body, "login page should carry its own variables"


def test_forecast_is_a_separate_page_with_a_province_map():
    """Forecasts live on /forecast, not inside the FIRMS dashboard."""
    static_dir = Path(__file__).resolve().parents[1] / "static"
    dashboard_html = (static_dir / "index.html").read_text(encoding="utf-8")
    forecast_html = (static_dir / "forecast.html").read_text(encoding="utf-8")
    forecast_js = (static_dir / "forecast.js").read_text(encoding="utf-8")

    assert 'href="/forecast"' in dashboard_html
    assert "forecast-list" not in dashboard_html
    assert 'id="forecast-map"' in forecast_html
    assert "/api/v1/fire/forecast" in forecast_js
    assert "/static/data/spain-provinces.geojson" in forecast_js
    assert "colorForProbability" in forecast_js


def test_sources_page_is_linked_from_every_section():
    """The last section must be reachable from every protected page."""
    static_dir = Path(__file__).resolve().parents[1] / "static"

    for name in ("index.html", "stats.html", "forecast.html"):
        body = (static_dir / name).read_text(encoding="utf-8")
        assert 'href="/sources"' in body, name


def test_stats_js_ignores_stale_filter_responses():
    """Two quick filter changes must not let the older response repaint the page.

    There is no JS runtime in the test image, so this pins the guard in the
    asset; the browser-level check was done manually when the race was fixed.
    """
    source = (Path(__file__).resolve().parents[1] / "static" / "stats.js").read_text(
        encoding="utf-8"
    )

    assert "this.requestId += 1" in source
    assert "requestId !== this.requestId" in source
