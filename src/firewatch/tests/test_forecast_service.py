"""Tests for the fire_predictions storage (no Postgres required)."""

from datetime import date

from firewatch.services.forecast import (
    ensure_predictions_table,
    upsert_predictions,
)


class FakeCursor:
    def __init__(self, log):
        self._log = log

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self._log.append((sql, params))


class FakeConn:
    """Captures SQL executed against it (no real database)."""

    def __init__(self):
        self.log = []
        self.commits = 0

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        self.commits += 1


def _prediction(**overrides):
    prediction = {
        "target_date": date(2026, 10, 29),
        "province_code": "03",
        "province_name": "Alicante",
        "probability": 0.087,
        "probability_calibrated": 0.061,
        "model_name": "hgb",
        "model_version": "a1b2c3d4e5f6",
        "weather_source": "open-meteo-archive+forecast",
    }
    prediction.update(overrides)
    return prediction


def test_ensure_table_creates_the_prediction_schema():
    conn = FakeConn()

    ensure_predictions_table(conn)

    statements = [sql for sql, _ in conn.log]
    assert any(
        "CREATE TABLE IF NOT EXISTS fire_predictions" in sql for sql in statements
    )
    assert any(
        "PRIMARY KEY (target_date, province_code, model_name)" in sql
        for sql in statements
    )
    assert any("fire_predictions_date_prob_idx" in sql for sql in statements)
    assert conn.commits == 1


def test_upsert_is_idempotent_on_the_daily_natural_key():
    conn = FakeConn()

    written = upsert_predictions(
        conn,
        [
            _prediction(),
            _prediction(province_code="28", province_name="Madrid"),
        ],
    )

    assert written == 2
    assert conn.commits == 1
    inserts = [sql for sql, _ in conn.log if "INSERT INTO fire_predictions" in sql]
    assert len(inserts) == 2
    assert (
        "ON CONFLICT (target_date, province_code, model_name) DO UPDATE" in inserts[0]
    )
    assert "generated_at = CURRENT_TIMESTAMP" in inserts[0]


def test_upsert_writes_both_raw_and_calibrated_probabilities():
    conn = FakeConn()

    upsert_predictions(conn, [_prediction()])

    sql, params = conn.log[0]
    assert "probability_calibrated" in sql
    assert params[0] == date(2026, 10, 29)
    assert params[1] == "03"
    assert params[2] == "Alicante"
    assert params[3] == 0.087
    assert params[4] == 0.061
    assert params[5] == "hgb"
    assert params[6] == "a1b2c3d4e5f6"
    assert params[7] == "open-meteo-archive+forecast"
