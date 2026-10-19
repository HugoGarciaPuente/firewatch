"""Tests for the login rate limiter.

`POST /auth/login` is the only gate on the whole dataset: one shared API key,
accepted without limit. Without a throttle, brute force is not just possible
but undetectable, because every rejection logged an identical line with no
source address.

These tests pin the three properties that make the throttle worth having:
failed attempts count, clients are isolated from each other, and the limit is
enforced before the credential is checked.
"""

import pytest

from firewatch.api.ratelimit import RateLimiter

client_address = RateLimiter.client_address


class _FakeRequest:
    """Minimal stand-in for the attributes the limiter reads."""

    def __init__(self, host="203.0.113.9", forwarded=None, user_agent="curl/8"):
        self.client = type("C", (), {"host": host})()
        self.headers = {}
        if forwarded is not None:
            self.headers["x-forwarded-for"] = forwarded
        if user_agent is not None:
            self.headers["user-agent"] = user_agent


# --- the counter ------------------------------------------------------------


def test_allows_up_to_the_limit_then_blocks():
    limiter = RateLimiter(limit=3, window_seconds=60)
    for _ in range(3):
        allowed, remaining, _ = limiter.check("1.1.1.1")
        assert allowed is True
        assert remaining >= 0

    allowed, remaining, retry_after = limiter.check("1.1.1.1")
    assert allowed is False
    assert remaining == 0
    assert retry_after > 0, "a rejected caller must be told how long to wait"


def test_rejected_attempts_still_consume_the_window():
    """Hammering must not keep the window open forever."""
    limiter = RateLimiter(limit=2, window_seconds=60)
    limiter.check("1.1.1.1")
    limiter.check("1.1.1.1")

    for _ in range(10):
        assert limiter.check("1.1.1.1")[0] is False


def test_one_client_cannot_lock_out_another():
    """Shared IP addresses and NAT make cross-talk a real failure mode."""
    limiter = RateLimiter(limit=2, window_seconds=60)
    limiter.check("1.1.1.1")
    limiter.check("1.1.1.1")
    assert limiter.check("1.1.1.1")[0] is False

    allowed, _, _ = limiter.check("2.2.2.2")
    assert allowed is True, "a different client must have its own budget"


def test_window_expires():
    limiter = RateLimiter(limit=1, window_seconds=0.05)
    assert limiter.check("1.1.1.1")[0] is True
    assert limiter.check("1.1.1.1")[0] is False

    import time

    time.sleep(0.08)
    assert limiter.check("1.1.1.1")[0] is True, "the window must reopen"


def test_reset_clears_one_client_or_everyone():
    limiter = RateLimiter(limit=1, window_seconds=60)
    limiter.check("1.1.1.1")
    limiter.check("2.2.2.2")

    limiter.reset("1.1.1.1")
    assert limiter.check("1.1.1.1")[0] is True
    assert limiter.check("2.2.2.2")[0] is False

    limiter.reset()
    assert limiter.check("2.2.2.2")[0] is True


@pytest.mark.parametrize("limit,window", [(0, 60), (-1, 60), (5, 0), (5, -1)])
def test_rejects_a_nonsensical_policy(limit, window):
    """A misconfigured limiter must fail loudly, not silently allow everything."""
    with pytest.raises(ValueError):
        RateLimiter(limit=limit, window_seconds=window)


# --- address resolution -----------------------------------------------------


def test_uses_the_peer_address_when_there_is_no_proxy():
    assert client_address(_FakeRequest(host="198.51.100.5")) == "198.51.100.5"


def test_prefers_the_left_most_forwarded_entry():
    """Behind a proxy every visitor shares one peer address.

    Without this, a single noisy client would exhaust the bucket for everyone
    behind the same reverse proxy.
    """
    request = _FakeRequest(host="10.0.0.1", forwarded="203.0.113.7, 10.0.0.1")
    assert client_address(request) == "203.0.113.7"


def test_tolerates_a_missing_client():
    """Never raises: an unidentified caller must still get a bucket."""
    request = _FakeRequest()
    request.client = None
    assert client_address(request) == "unknown"


# --- integration with the route --------------------------------------------


def test_login_is_rate_limited_after_repeated_failures(client):
    """End-to-end: bad keys get a 429 once the window fills."""
    statuses = []
    for _ in range(12):
        r = client.post("/auth/login", headers={"X-API-Key": "wrong"})
        statuses.append(r.status_code)

    assert statuses[0] == 401, "an invalid key is still a 401, not a 429"
    assert 429 in statuses, f"the limiter never engaged: {statuses}"

    blocked = client.post("/auth/login", headers={"X-API-Key": "wrong"})
    assert blocked.status_code == 429
    assert "Retry-After" in blocked.headers


def test_successful_login_after_limit_is_also_blocked(client, api_key):
    """Throttling only failures would leave the endpoint brute-forceable."""
    for _ in range(12):
        response = client.post("/auth/login", headers={"X-API-Key": "wrong"})
        if response.status_code == 429:
            break

    r = client.post("/auth/login", headers={"X-API-Key": api_key})
    assert r.status_code == 429, "the limiter must apply to valid keys as well"


def test_429_carries_retry_after(client):
    r = None
    for _ in range(12):
        r = client.post("/auth/login", headers={"X-API-Key": "wrong"})
        if r.status_code == 429:
            break
    assert r is not None and r.status_code == 429
    assert int(r.headers["Retry-After"]) >= 1
