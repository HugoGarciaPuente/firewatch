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
        "/static/app.js",
        "/static/stats.js",
        "/static/style.css",
    ):
        assert client.get(path).status_code == 401, path


def test_static_assets_served_with_session(authed):
    for path in (
        "/static/index.html",
        "/static/stats.html",
        "/static/app.js",
        "/static/stats.js",
        "/static/style.css",
    ):
        assert authed.get(path).status_code == 200, path


def test_login_page_does_not_depend_on_protected_static():
    """The login page cannot load /static/style.css any more, so it must inline it."""
    body = client.get("/login").text
    assert "/static/style.css" not in body, "login page would render unstyled"
    assert "--bg-main" in body, "login page should carry its own variables"


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
