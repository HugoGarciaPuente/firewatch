"""Tests for the processing service (no Kafka/Postgres required)."""

import pytest

from firewatch.config import resolve_regions, settings
from firewatch.services.processing import (
    InvalidRecord,
    _ensure_table,
    consume_and_process,
    record_to_row,
)


def test_record_to_row_maps_firms_fields():
    record = {
        "acq_date": "2024-08-15",
        "latitude": 19.5,
        "longitude": -155.3,
        "brightness": 320.5,
        "scan": 5,
        "track": 124,
    }

    row = record_to_row(record)

    acq_date, lat, lon, brightness, scan, track, acq = row
    assert acq_date == "2024-08-15"
    assert lat == 19.5
    assert lon == -155.3
    assert brightness == 320.5
    assert scan == 5
    assert track == 124
    assert acq == "2024-08-15"


def test_record_to_row_handles_missing_optionals():
    row = record_to_row({"date": "2024-01-01", "lat": "1.0", "lon": "2.0"})

    acq_date, lat, lon, brightness, scan, track, acq = row
    assert acq_date == "2024-01-01"
    assert lat == 1.0
    assert lon == 2.0
    assert brightness is None
    assert scan is None
    assert track is None
    assert acq == "2024-01-01"


class FakeCursor:
    def __init__(self, log):
        self._log = log

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self._log.append(("execute", sql, params))


class FakeConn:
    """Captures SQL executed against it (no real database)."""

    def __init__(self):
        self.log: list = []
        self.commits = 0

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        self.commits += 1

    def close(self):
        pass


class FakeMessage:
    def __init__(self, value):
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value


class FakeConsumer:
    def __init__(self, batches):
        # batches: list of {("topic", 0): [messages]} returned per poll
        self._batches = list(batches)

    def poll(self, timeout_ms=None):
        if self._batches:
            return self._batches.pop(0)
        return {}


def test_consume_and_process_upserts_message(firms_sample):
    conn = FakeConn()
    consumer = FakeConsumer(
        [{("firms-ingest", 0): [FakeMessage(firms_sample[0])]}]
    )

    processed = consume_and_process(consumer=consumer, conn=conn, batch_limit=1)

    assert processed == 1
    # First statement: CREATE EXTENSION, then CREATE TABLE / INDEX
    sqls = [sql for kind, sql, _ in conn.log if kind == "execute"]
    assert any("postgis" in s.lower() for s in sqls)
    # The upsert must be idempotent
    upsert_sql = next(s for s in sqls if "INSERT INTO fire_events" in s)
    assert "ON CONFLICT (acq_date, latitude, longitude) DO UPDATE" in upsert_sql
    assert "ST_MakePoint" in upsert_sql  # PostGIS geometry is populated
    assert conn.commits >= 1


def test_consume_and_process_accepts_bytes_payload(firms_sample):
    import json

    conn = FakeConn()
    consumer = FakeConsumer(
        [{("firms-ingest", 0): [FakeMessage(json.dumps(firms_sample[1]).encode())]}]
    )

    # The geocode client is stubbed explicitly. Without this the consumer builds
    # a real GeoCodeClient and hits Nominatim over the network: the suite would
    # depend on a third party, burn the 1 req/s rate limit on every run, and
    # return whatever that service felt like (this test once resolved the
    # fixture's coordinates to "Manaus, Brasil").
    class NoGeocode:
        calls = 0

        def get_location(self, lat, lon):
            NoGeocode.calls += 1
            return None

    processed = consume_and_process(
        consumer=consumer,
        conn=conn,
        batch_limit=1,
        geocode_client=NoGeocode(),
    )

    assert processed == 1
    assert NoGeocode.calls == 1
    upserts = [
        params
        for kind, sql, params in conn.log
        if kind == "execute" and "INSERT INTO fire_events" in sql
    ]
    assert len(upserts) == 1
    # params = (*row, longitude, latitude, locality_json)
    # The locality column was appended after geom, so it is now the last value.
    assert upserts[0][-3] == -60.0  # longitude (x of ST_MakePoint)
    assert upserts[0][-2] == -3.1  # latitude (y of ST_MakePoint)
    assert upserts[0][-1] is None


def test_consume_and_process_handles_empty_poll():
    conn = FakeConn()
    consumer = FakeConsumer([{}])

    processed = consume_and_process(
        consumer=consumer, conn=conn, batch_limit=1, max_empty_polls=1
    )

    assert processed == 0

# ---------------------------------------------------------------------------
# Audit fixes: input validation, poison-pill resilience, alert wiring
# ---------------------------------------------------------------------------


def test_record_to_row_rejects_missing_date():
    with pytest.raises(InvalidRecord):
        record_to_row({"latitude": 40.0, "longitude": -3.0, "brightness": 350.0})


def test_record_to_row_rejects_missing_coordinates():
    """Regression: absent lat/lon used to become 0,0 (Gulf of Guinea)."""
    with pytest.raises(InvalidRecord):
        record_to_row({"acq_date": "2026-10-08", "brightness": 350.0})


def test_record_to_row_rejects_out_of_range_coordinates():
    with pytest.raises(InvalidRecord):
        record_to_row({"acq_date": "2026-10-08", "latitude": 999.0, "longitude": 5.0})


def test_record_to_row_keeps_zero_coordinates():
    """A legitimate 0.0 must not be treated as 'absent' by the or-chaining."""
    row = record_to_row({"acq_date": "2026-10-08", "latitude": 0.0, "longitude": 0.0})
    assert row[1] == 0.0
    assert row[2] == 0.0


def test_record_to_row_rejects_algerian_coordinates():
    """Coordinates in North Africa/Algeria must be rejected."""
    with pytest.raises(InvalidRecord):
        record_to_row({"acq_date": "2026-10-08", "latitude": 36.5, "longitude": 1.2})


def test_record_to_row_preserves_fractional_scan_track():
    """Regression: int() truncated FIRMS scan/track angles to 0."""
    row = record_to_row(
        {
            "acq_date": "2026-10-08",
            "latitude": 40.1,
            "longitude": -3.1,
            "scan": "0.39",
            "track": "0.72",
        }
    )
    assert row[4] == pytest.approx(0.39)
    assert row[5] == pytest.approx(0.72)


def test_ensure_table_migrates_scan_track_to_double():
    conn = FakeConn()
    _ensure_table(conn)
    sqls = " ".join(s for kind, s, _ in conn.log if kind == "execute")
    assert "ALTER COLUMN scan TYPE DOUBLE PRECISION" in sqls
    assert "ALTER COLUMN track TYPE DOUBLE PRECISION" in sqls


def test_invalid_record_does_not_kill_the_consumer():
    """A poison message must be skipped, not raise, or the partition blocks forever."""
    good = {"acq_date": "2026-10-08", "latitude": 40.0, "longitude": -3.0, "brightness": 360.0}
    bad = {"brightness": 360.0}  # no date, no coords

    conn = FakeConn()
    consumer = FakeConsumer([{("firms-ingest", 0): [FakeMessage(bad), FakeMessage(good)]}])

    processed = consume_and_process(
        consumer=consumer, conn=conn, batch_limit=1, max_empty_polls=1
    )

    assert processed == 1  # the good record still made it through
    upserts = [
        p for kind, sql, p in conn.log if kind == "execute" and "INSERT INTO fire_events" in sql
    ]
    assert len(upserts) == 1


def test_high_risk_event_publishes_alert():
    """Regression: alert_producer was never wired, so the topic stayed empty."""
    hot = {"acq_date": "2026-10-08", "latitude": 40.0, "longitude": -3.0, "brightness": 395.0}
    conn = FakeConn()
    consumer = FakeConsumer([{("firms-ingest", 0): [FakeMessage(hot)]}])

    sent = []

    class FakeAlertProducer:
        def send(self, topic, payload):
            sent.append((topic, payload))

    processed = consume_and_process(
        consumer=consumer,
        conn=conn,
        batch_limit=1,
        max_empty_polls=1,
        alert_producer=FakeAlertProducer(),
    )

    assert processed == 1
    assert len(sent) == 1
    topic, payload = sent[0]
    assert topic == settings.KAFKA_RISK_TOPIC
    assert payload["risk_level"] in ("high", "extreme")
    assert payload["latitude"] == 40.0


def test_low_risk_event_does_not_publish_alert():
    cool = {"acq_date": "2026-10-08", "latitude": 40.0, "longitude": -3.0, "brightness": 301.0}
    conn = FakeConn()
    consumer = FakeConsumer([{("firms-ingest", 0): [FakeMessage(cool)]}])

    sent = []

    class FakeAlertProducer:
        def send(self, topic, payload):
            sent.append((topic, payload))

    consume_and_process(
        consumer=consumer,
        conn=conn,
        batch_limit=1,
        max_empty_polls=1,
        alert_producer=FakeAlertProducer(),
    )

    assert sent == []


def test_trigger_ingest_defaults_to_regions_not_the_whole_planet():
    """Regression: /fire/ingest once fell back to a planet-wide default bbox.

    The handler no longer fetches anything itself -- it schedules a background
    task -- so the assertion is on the regions it *schedules*. If someone
    reintroduces a planet-wide default, this fails without touching the network.
    """
    from fastapi import BackgroundTasks

    from firewatch.api import endpoints

    background = BackgroundTasks()
    body = endpoints.trigger_ingest(background)

    scheduled_regions = background.tasks[0].args[1]
    scheduled_bboxes = [bbox for _, bbox in scheduled_regions]

    assert scheduled_bboxes == [b for _, b in resolve_regions()]
    assert "-90,-180,90,180" not in scheduled_bboxes
    assert {label for label, _ in scheduled_regions} == {label for label, _ in resolve_regions()}
    assert body["status"] == "accepted"
    assert body["job_id"]


def test_trigger_ingest_returns_before_doing_any_work():
    """The whole point of the change: no blocking call in the request path.

    Scheduling is only worth it if the request does not touch FIRMS or Kafka.
    This asserts the handler added a task and did not execute it -- FastAPI
    runs BackgroundTasks after the response is sent, not during the call.
    """
    from fastapi import BackgroundTasks

    from firewatch.api import endpoints

    background = BackgroundTasks()
    endpoints.trigger_ingest(background)

    assert len(background.tasks) == 1
    assert background.tasks[0].func is endpoints._run_ingest


def test_ingest_response_bbox_is_not_a_concatenated_string():
    """Regression: the old response joined every region bbox into one string.

    `",".join(...)` over three bboxes produced "35.9,-9.6,43.9,3.6,27.4,...",
    which is not a parseable bbox -- and it is exactly the value the dashboard
    would have sent straight back.
    """
    from fastapi import BackgroundTasks

    from firewatch.api import endpoints

    body = endpoints.trigger_ingest(BackgroundTasks())

    assert isinstance(body["regions"], list)
    assert body["regions"], "at least one region must be reported"
    for entry in body["regions"]:
        assert set(entry) == {"label", "bbox"}
        assert entry["bbox"].count(",") == 3, "a bbox must have exactly 4 numbers"

    # The old flat field is gone, so nothing can round-trip a malformed bbox.
    assert "bbox" not in body


class _StubProducer:
    def close(self):
        pass
