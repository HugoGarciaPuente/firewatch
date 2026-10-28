"""Tests for the confirmed-fire label storage (no Postgres required)."""

from datetime import date, datetime, timezone

from firewatch.services.labels import (
    count_confirmed_fires,
    ensure_confirmed_fires_table,
    map_province_codes,
    upsert_confirmed_fires,
)


class FakeCursor:
    def __init__(self, log, count=0):
        self._log = log
        self._count = count

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self._log.append((sql, params))

    def fetchone(self):
        return (self._count,)


class FakeConn:
    """Captures SQL executed against it (no real database)."""

    def __init__(self, count=0):
        self.log = []
        self.commits = 0
        self._count = count

    def cursor(self):
        return FakeCursor(self.log, self._count)

    def commit(self):
        self.commits += 1


def _record(**overrides):
    record = {
        "source": "effis",
        "source_id": "1001",
        "country": "ES",
        "firedate": date(2026, 7, 15),
        "firedate_ts": datetime(2026, 7, 15, 14, 30, tzinfo=timezone.utc),
        "province": "Madrid",
        "municipality": "Madrid",
        "municipality_code": None,
        "area_ha": 12.5,
        "latitude": 40.4168,
        "longitude": -3.7038,
        "bbox": [-3.71, 40.41, -3.69, 40.42],
        "last_update": datetime(2026, 7, 16, 8, 0, tzinfo=timezone.utc),
    }
    record.update(overrides)
    return record


def test_ensure_table_creates_the_confirmed_fires_schema():
    conn = FakeConn()

    ensure_confirmed_fires_table(conn)

    statements = [sql for sql, _ in conn.log]
    assert any("CREATE TABLE IF NOT EXISTS confirmed_fires" in sql for sql in statements)
    assert any("PRIMARY KEY (source, source_id)" in sql for sql in statements)
    assert any("confirmed_fires_firedate_idx" in sql for sql in statements)
    assert any("USING GIST (geom)" in sql for sql in statements)
    assert any("province_code" in sql for sql in statements)
    assert conn.commits == 1


def test_upsert_is_idempotent_on_source_and_source_id():
    conn = FakeConn()

    written = upsert_confirmed_fires(conn, [_record(), _record(source_id="1002")])

    assert written == 2
    assert conn.commits == 1
    inserts = [sql for sql, _ in conn.log if "INSERT INTO confirmed_fires" in sql]
    assert len(inserts) == 2
    assert "ON CONFLICT (source, source_id) DO UPDATE" in inserts[0]


def test_upsert_writes_a_longitude_latitude_point():
    """ST_MakePoint(x, y) is (lon, lat); swapping them puts Spain in the ocean."""
    conn = FakeConn()

    upsert_confirmed_fires(conn, [_record()])

    params = conn.log[0][1]
    assert params[0] == "effis"
    assert params[1] == "1001"
    assert params[3] == date(2026, 7, 15)
    assert params[-2] == -3.7038
    assert params[-1] == 40.4168
    assert "ST_SetSRID(ST_MakePoint(%s, %s), 4326)" in conn.log[0][0]


def test_upsert_never_erases_a_previously_resolved_ine_code():
    """EFFIS has no INE code; a later re-import must not blank the mapping."""
    conn = FakeConn()

    upsert_confirmed_fires(conn, [_record()])

    sql = conn.log[0][0]
    assert "COALESCE(" in sql
    assert "confirmed_fires.municipality_code" in sql


def test_count_confirmed_fires_reads_the_table():
    conn = FakeConn(count=12_745)

    assert count_confirmed_fires(conn) == 12_745
    assert "SELECT COUNT(*) FROM confirmed_fires" in conn.log[0][0]


def test_upsert_sets_the_province_code_from_the_effis_name():
    """The panel groups by province_code, so the insert must not leave it NULL."""
    conn = FakeConn()

    upsert_confirmed_fires(conn, [_record(province="Tenerife")])

    # Param order: source, source_id, country, firedate, firedate_ts,
    # province, province_code, ...
    assert conn.log[0][1][5] == "Tenerife"
    assert conn.log[0][1][6] == "38"


class MapCursor:
    def __init__(self, log, names, count=1):
        self._log = log
        self._names = names
        self._count = count
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self._log.append((sql, params))
        if "UPDATE" in sql:
            self.rowcount = self._count

    def fetchall(self):
        return [(name,) for name in self._names]

    def fetchone(self):
        return (self._count,)


class MapConn:
    def __init__(self, names, count=1):
        self.log = []
        self.commits = 0
        self._names = names
        self._count = count

    def cursor(self):
        return MapCursor(self.log, self._names, self._count)

    def commit(self):
        self.commits += 1


def test_map_province_codes_updates_known_names_and_reports_unknown():
    conn = MapConn(["Tenerife", "Valencia/València", "Nueva Provincia"])

    affected, unmatched = map_province_codes(conn)

    assert affected == 2
    assert unmatched == ["Nueva Provincia"]
    updates = [params for sql, params in conn.log if "UPDATE" in sql]
    assert ("38", "Tenerife", "38") in updates
    assert ("46", "Valencia/València", "46") in updates
    assert conn.commits == 1


def test_map_province_codes_dry_run_counts_without_writing():
    conn = MapConn(["Tenerife"], count=3)

    affected, unmatched = map_province_codes(conn, dry_run=True)

    assert affected == 3
    assert unmatched == []
    assert not any("UPDATE" in sql for sql, _ in conn.log)
    assert conn.commits == 0
