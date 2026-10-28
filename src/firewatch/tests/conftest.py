"""Shared pytest fixtures for FireWatch tests."""

import json
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture
def firms_sample() -> list[dict]:
    """Sample NASA FIRMS response (list of records) from fixtures."""
    return json.loads((FIXTURES_DIR / "firms_sample.json").read_text(encoding="utf-8"))


@pytest.fixture
def effis_sample() -> list[dict]:
    """Sample EFFIS burnt-area records (raw API shape) from fixtures."""
    return json.loads((FIXTURES_DIR / "effis_sample.json").read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def _block_outbound_network(monkeypatch, request):
    """Fail loudly if a test opens a real socket.

    A test that reaches a third party is worse than a slow test: it burns a
    public service's rate limit on every run, produces different results when
    that service is up or down, and hides the fact that the code path was never
    exercised against a stub. `test_processing` once geocoded its fixture
    coordinates against the live Nominatim instance and got back "Manaus,
    Brasil" for a Spanish bounding box.

    Blocking at the socket layer catches any library, including ones that build
    their own client internally. Tests that legitimately need a stubbed HTTP
    exchange use httpx.MockTransport, which never reaches this hook.
    """
    import socket

    class NetworkAccessInTest(RuntimeError):
        pass

    real_connect = socket.socket.connect

    def guarded_connect(self, address, *args, **kwargs):
        host = address[0] if isinstance(address, tuple) else address
        if isinstance(host, str) and host not in ("127.0.0.1", "::1", "localhost"):
            raise NetworkAccessInTest(
                f"test {request.node.name} tried to reach {host!r}. Stub the "
                f"client (httpx.MockTransport) instead of calling a live API."
            )
        return real_connect(self, address, *args, **kwargs)

    def guarded_connect_ex(self, address, *args, **kwargs):
        guarded_connect(self, address, *args, **kwargs)
        return 0

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Clear the login throttle between tests.

    `login_limiter` is a module-level singleton because its counters must
    outlive the request that created them. That same lifetime makes it leak
    across tests: without this, every test that logs in spends budget that
    later tests then hit as an unexpected 429. The symptom is a test that
    passes alone and fails in a full run, which is a miserable thing to debug.
    """
    from firewatch.api.ratelimit import reset_login_limiter

    reset_login_limiter()
    yield
    reset_login_limiter()


@pytest.fixture(scope="session")
def client():
    """Shared TestClient for route-level tests.

    Lives here rather than in test_api.py so several modules can exercise the
    same app instance without each building its own.
    """
    from fastapi.testclient import TestClient

    from firewatch.api.main import app

    return TestClient(app)


@pytest.fixture
def api_key():
    """The configured API key, for tests that need a *valid* credential."""
    from firewatch.config import settings

    return settings.API_KEY
