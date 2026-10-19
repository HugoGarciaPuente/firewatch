"""Tests for the FastAPI app (no external services needed)."""

import os

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


def test_root_returns_app_info():
    response = client.get("/")

    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "FireWatch"
    assert body["status"] == "running"


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


def test_health_db_endpoint():
    response = client.get("/health/db")
    # Will be 200 when DB is up, or 503 if DB is not reachable
    assert response.status_code in (200, 503)
    if response.status_code == 200:
        assert response.json() == {"status": "healthy", "database": "connected"}


def test_dashboard_endpoint_requires_session(authed):
    """The dashboard is browser-only: no session, no dashboard."""
    assert client.get("/dashboard").status_code == 401

    response = authed.get("/dashboard")
    assert response.status_code == 200
    assert "FireWatch" in response.text


def test_login_page_is_public():
    """Must stay reachable, otherwise nobody can ever authenticate."""
    response = client.get("/login")
    assert response.status_code == 200
    assert "api-key" in response.text


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


def test_fire_endpoints_require_a_session():
    """Unauthenticated reads must be refused, not served."""
    for path in ("/api/v1/fire/stats", "/api/v1/fire/dates", "/api/v1/fire/events"):
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
    for path in ("/static/index.html", "/static/app.js", "/static/style.css"):
        assert client.get(path).status_code == 401, path


def test_static_assets_served_with_session(authed):
    for path in ("/static/index.html", "/static/app.js", "/static/style.css"):
        assert authed.get(path).status_code == 200, path


def test_login_page_does_not_depend_on_protected_static():
    """The login page cannot load /static/style.css any more, so it must inline it."""
    body = client.get("/login").text
    assert "/static/style.css" not in body, "login page would render unstyled"
    assert "--bg-main" in body, "login page should carry its own variables"
