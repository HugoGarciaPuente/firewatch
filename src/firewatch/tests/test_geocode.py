"""Tests for the reverse-geocoding client and the locality pipeline.

FIRMS returns coordinates and nothing else, so locality has to be resolved
against Nominatim. Two properties matter here that a naive implementation gets
wrong:

1. the cache is keyed on the *exact* coordinate, not a coarse cell. Weather can
   be cached per 0.25 degree cell because temperature does not vary across 25 km.
   A locality does: two points 500 m apart can sit in different municipalities,
   and returning the cell centre'"'"'s name would mislabel a real fire.
2. failures are cached too, so a dead upstream costs one request per point
   rather than one per record in a backfill.

The persistence tests at the bottom pin the behaviour that keeps the dashboard
stable: a re-ingested point whose geocoding fails must not lose the locality it
already had.
"""

import httpx
import pytest

from firewatch.clients.geocode import GeoCodeClient

SPANISH_PAYLOAD = {
    "display_name": "Alarcon, Cuenca, Castilla-La Mancha, Espana",
    "address": {
        "city": "Alarcon",
        "county": "Cuenca",
        "state": "Castilla-La Mancha",
        "country": "Espana",
        "country_code": "es",
    },
}

VILLAGE_PAYLOAD = {
    "display_name": "Dima, Bizkaia, Euskadi, 48141, Espana",
    "address": {
        "village": "Dima",
        "state": "Bizkaia",
        "country": "Espana",
        "country_code": "es",
    },
}

NO_RESULT_PAYLOAD = {"error": "Unable to geocode"}


def _client(handler, **kwargs):
    """Build a client over a MockTransport so no network call happens."""
    transport = httpx.MockTransport(handler)
    kwargs.setdefault("min_interval", 0)
    return GeoCodeClient(client=httpx.Client(transport=transport), **kwargs)


def test_parses_municipality_and_province():
    client = _client(lambda request: httpx.Response(200, json=SPANISH_PAYLOAD))
    loc = client.get_location(39.590, -2.100)
    assert loc is not None
    assert loc["locality"] == "Alarcon"
    assert loc["province"] == "Castilla-La Mancha"
    assert loc["country"] == "Espana"
    assert loc["country_code"] == "es"


def test_falls_back_through_the_settlement_vocabulary():
    """Nominatim labels a small Spanish municipality `village`, not `city`."""
    client = _client(lambda request: httpx.Response(200, json=VILLAGE_PAYLOAD))
    assert client.get_location(43.101, -2.681)["locality"] == "Dima"


def test_uses_county_when_state_is_absent():
    payload = {"display_name": "x", "address": {"city": "Y", "county": "Cuenca"}}
    client = _client(lambda request: httpx.Response(200, json=payload))
    assert client.get_location(40.0, -2.0)["province"] == "Cuenca"


def test_points_with_no_mapped_locality_return_none():
    """Open sea answers with an `error` key and no display_name."""
    client = _client(lambda request: httpx.Response(200, json=NO_RESULT_PAYLOAD))
    assert client.get_location(35.978, -9.165) is None


def test_caches_on_exact_coordinate_not_a_cell():
    """Two nearby points must not share a locality.

    Fails if someone copies WeatherClient'"'"'s cell snapping here: both points
    land inside the same 0.25 degree cell (floor(39.551/0.25) ==
    floor(39.549/0.25) == 158), so a cell-keyed cache would return one
    municipality for both and mislabel a fire.
    """
    calls = []

    def handler(request):
        lat = float(request.url.params["lat"])
        calls.append(lat)
        name = "Alarcon" if lat > 39.55 else "Tarancón"
        return httpx.Response(
            200,
            json={"display_name": name, "address": {"city": name, "country_code": "es"}},
        )

    client = _client(handler)

    first = client.get_location(39.551, -2.101)
    second = client.get_location(39.549, -2.099)

    assert len(calls) == 2, "nearby points must each trigger their own lookup"
    assert first is not None and second is not None
    assert first["locality"] != second["locality"]


def test_repeated_coordinate_hits_the_cache():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(200, json=SPANISH_PAYLOAD)

    client = _client(handler)
    for _ in range(5):
        client.get_location(39.590, -2.100)

    assert len(calls) == 1
    stats = client.cache_stats()
    assert stats["hits"] == 4
    assert stats["misses"] == 1


def test_failures_are_cached_too():
    """A dead upstream must not cause one retry per record."""
    calls = []

    def handler(request):
        calls.append(1)
        raise httpx.ConnectError("upstream down")

    client = _client(handler)
    assert client.get_location(39.590, -2.100) is None
    assert client.get_location(39.590, -2.100) is None
    assert client.get_location(39.590, -2.100) is None
    assert len(calls) == 1, "a failed lookup must be cached like a successful one"


def test_http_error_is_not_fatal():
    client = _client(lambda request: httpx.Response(503, text="unavailable"))
    assert client.get_location(40.0, -3.0) is None


def test_sends_an_identifying_user_agent():
    """The OSMF usage policy requires a real User-Agent. Not optional."""
    seen = {}

    def handler(request):
        seen["ua"] = request.headers.get("user-agent")
        return httpx.Response(200, json=SPANISH_PAYLOAD)

    client = _client(handler)
    client.get_location(39.590, -2.100)
    assert seen["ua"]
    assert "python-requests" not in seen["ua"].lower()


def test_requests_municipality_zoom_not_street_level():
    """zoom=10 resolves to municipality; higher zooms return street noise."""
    seen = {}

    def handler(request):
        seen["zoom"] = request.url.params.get("zoom")
        return httpx.Response(200, json=SPANISH_PAYLOAD)

    client = _client(handler)
    client.get_location(39.590, -2.100)
    assert seen["zoom"] == "10"


@pytest.mark.parametrize("lat,lon", [(None, None), ("x", "y")])
def test_bad_coordinates_do_not_raise(lat, lon):
    """Garbage input must degrade to None, never crash the consumer loop."""
    client = _client(lambda request: httpx.Response(200, json=SPANISH_PAYLOAD))
    try:
        result = client.get_location(lat, lon)
    except (TypeError, ValueError):
        pytest.fail("get_location raised on invalid coordinates")
    assert result is None or isinstance(result, dict)


# --- integration with the persistence layer -----------------------------------


class _FakeCursor:
    def __init__(self, log):
        self._log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._log.append(("execute", sql, params))

    def fetchall(self):
        return []


class _FakeConn:
    """Minimal psycopg2 stand-in capturing the SQL the writer emits."""

    def __init__(self):
        self.log = []
        self.commits = 0

    def cursor(self, *a, **kw):
        return _FakeCursor(self.log)

    def commit(self):
        self.commits += 1


def _insert_sql(conn):
    return next(s for kind, s, _ in conn.log if kind == "execute" and "INSERT INTO fire_events" in s)


def _insert_params(conn):
    return next(p for kind, _, p in conn.log if kind == "execute" and "INSERT INTO fire_events" in _)


def test_upsert_writes_the_locality_column():
    """The locality must reach SQL, not stay in memory."""
    from firewatch.services.processing import upsert_record

    conn = _FakeConn()
    upsert_record(
        conn,
        {"acq_date": "2026-10-13", "latitude": 39.590, "longitude": -2.100, "brightness": 340.0},
        location={"locality": "Alarcon", "province": "Cuenca", "country_code": "es"},
    )
    assert "locality" in _insert_sql(conn)


def test_upsert_never_blanks_an_existing_locality():
    """Re-ingesting a point while geocoding is down must keep the old name.

    FIRMS reissues recent days, so the same coordinate is ingested repeatedly.
    If the ON CONFLICT clause assigned locality unconditionally, a single
    Nominatim outage would blank every locality in the database.
    """
    from firewatch.services.processing import upsert_record

    conn = _FakeConn()
    upsert_record(
        conn,
        {"acq_date": "2026-10-13", "latitude": 39.590, "longitude": -2.100},
        location=None,
    )
    assert "COALESCE(EXCLUDED.locality, fire_events.locality)" in _insert_sql(conn)
    assert _insert_params(conn)[-1] is None


def test_upsert_preserves_non_ascii_locality():
    """Place names carry accents; they must not be escaped into \\uXXXX."""
    from firewatch.services.processing import upsert_record

    conn = _FakeConn()
    upsert_record(
        conn,
        {"acq_date": "2026-10-13", "latitude": 39.590, "longitude": -2.100},
        location={"locality": "Alarcón", "province": "Cuenca"},
    )
    assert "Alarcón" in _insert_params(conn)[-1]


def test_consume_passes_the_location_through():
    """The consumer must wire the geocode result into the upsert."""
    from firewatch.services import processing

    class _Msg:
        def __init__(self, payload):
            self._payload = payload
            self.error = lambda: None
            self.value = lambda: __import__("json").dumps(payload).encode()

    class _Consumer:
        def poll(self, timeout_ms=0):
            return {0: [_Msg({"acq_date": "2026-10-13", "latitude": 39.59,
                              "longitude": -2.10, "brightness": 340.0})]}

        def close(self):
            pass

    class _Geo:
        def get_location(self, lat, lon):
            return {"locality": "Alarcon", "country_code": "es"}

    conn = _FakeConn()
    processing.consume_and_process(
        consumer=_Consumer(),
        conn=conn,
        batch_limit=1,
        weather_client=None,
        geocode_client=_Geo(),
    )
    assert "Alarcon" in _insert_params(conn)[-1]
