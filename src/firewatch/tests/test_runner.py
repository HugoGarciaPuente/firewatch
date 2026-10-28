"""Tests for the CLI runner helpers (region resolution, argument wiring)."""

import logging

import pytest

from firewatch.config import resolve_regions, settings

def test_resolve_regions_explicit_subset():
    assert [label for label, _ in resolve_regions(regions_arg="canarias")] == ["canarias"]
    assert [label for label, _ in resolve_regions(regions_arg="peninsula")] == ["peninsula", "peninsula_sur"]


def test_resolve_regions_bbox_overrides_regions():
    regions = resolve_regions(bbox_arg="0,0,1,1", regions_arg="canarias,baleares")
    assert regions == [("bbox", "0,0,1,1")]


def test_resolve_regions_rejects_unknown():
    with pytest.raises(ValueError) as exc:
        resolve_regions(regions_arg="atlantis")
    assert "atlantis" in str(exc.value)


def test_region_bboxes_cover_expected_territories():
    """Canarias is south of 30N, Baleares is east of Greenwich, peninsula is 36-44N."""
    south, west, north, east = (float(v) for v in settings.REGION_BBOXES["canarias"].split(","))
    assert south < 30

    south, west, north, east = (float(v) for v in settings.REGION_BBOXES["baleares"].split(","))
    assert west > 0

    south, west, north, east = (float(v) for v in settings.REGION_BBOXES["peninsula"].split(","))
    assert south >= 37 and north < 44

    south, west, north, east = (float(v) for v in settings.REGION_BBOXES["peninsula_sur"].split(","))
    assert south <= 36 and north <= 38

def test_refresh_once_uses_trailing_window(monkeypatch):
    """refresh_once must request exactly the last N days, newest included."""
    import datetime as dt

    from firewatch import runner

    asked = []

    class FakeClient:
        def fetch_by_bbox(self, bbox, date=None):
            asked.append((bbox, date))
            return []

    today = dt.datetime.now(dt.timezone.utc).date()

    runner.refresh_once(
        FakeClient(),
        [("peninsula", "35.9,-9.6,43.9,3.6")],
        days=3,
        dry_run=True,
    )

    dates = sorted(d for _, d in asked)
    assert dates == [
        (today - dt.timedelta(days=2)).isoformat(),
        (today - dt.timedelta(days=1)).isoformat(),
        today.isoformat(),
    ]


def test_refresh_once_publishes_and_counts(monkeypatch):
    from firewatch import runner

    published = {"records": 0}

    def fake_run_ingestion(records, producer, topic=None):
        published["records"] += len(records)
        return len(records)

    class FakeProducer:
        def close(self):
            pass

    monkeypatch.setattr(runner, "run_ingestion", fake_run_ingestion)
    monkeypatch.setattr(runner, "create_producer", FakeProducer)

    class FakeClient:
        def fetch_by_bbox(self, bbox, date=None):
            return [{"acq_date": date, "latitude": 1.0, "longitude": 2.0}]

    rc = runner.refresh_once(FakeClient(), [("peninsula", "b")], days=1, dry_run=False)

    assert rc == 0
    assert published["records"] == 1


def test_refresh_once_dry_run_does_not_publish(monkeypatch):
    from firewatch import runner

    def boom(*a, **k):
        raise AssertionError("must not publish in dry-run")

    monkeypatch.setattr(runner, "run_ingestion", boom)
    monkeypatch.setattr(runner, "create_producer", boom)

    class FakeClient:
        def fetch_by_bbox(self, bbox, date=None):
            return [{"acq_date": date, "latitude": 1.0, "longitude": 2.0}]

    assert runner.refresh_once(FakeClient(), [("peninsula", "b")], days=1, dry_run=True) == 0


def test_daily_once_flag_is_wired():

    from firewatch.runner import build_parser

    args = build_parser().parse_args(["daily", "--once", "--days", "5"])
    assert args.once is True
    assert args.days == 5


def test_daily_loop_propagates_failure_and_dry_run(monkeypatch):
    """A partial refresh in loop mode must not be swallowed or publish in dry-run."""
    from firewatch import runner

    calls = {}

    def fake_refresh(client, regions, days, dry_run=False):
        calls["dry_run"] = dry_run
        return 1

    monkeypatch.setattr(runner, "refresh_once", fake_refresh)
    monkeypatch.setattr(runner, "FirmsClient", lambda: object())

    args = runner.build_parser().parse_args(
        ["daily", "--interval-minutes", "1", "--dry-run"]
    )

    assert runner.cmd_daily(args) == 1
    assert calls["dry_run"] is True


def test_backfill_reports_a_failed_day(monkeypatch):
    """A historical load with a failed day must not exit as a complete backfill."""
    from firewatch import runner

    class BrokenClient:
        def __init__(self, source=None):
            self.source = source

        def fetch_by_bbox(self, bbox, date=None):
            raise RuntimeError("FIRMS unavailable")

    monkeypatch.setattr(runner, "FirmsClient", BrokenClient)

    args = runner.build_parser().parse_args(
        [
            "backfill",
            "--regions",
            "canarias",
            "--start-date",
            "2026-06-01",
            "--end-date",
            "2026-06-01",
            "--dry-run",
        ]
    )

    assert runner.cmd_backfill(args) == 1


def test_geocode_backfill_leaves_failed_rows_null_and_returns_error(monkeypatch):
    """A Nominatim failure must not be persisted as "tried, no locality"."""
    import sys
    import types

    from firewatch.clients.geocode import GeocodeError

    from firewatch import runner

    class FakeCursor:
        def __init__(self, owner):
            self.owner = owner
            self.rows = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            self.owner.statements.append((sql, params))
            if "SELECT id, latitude, longitude" in sql:
                self.rows = [(1, 40.0, -3.0)]

        def fetchall(self):
            return self.rows

    class FakeConn:
        def __init__(self):
            self.statements = []
            self.commits = 0

        def cursor(self):
            return FakeCursor(self)

        def commit(self):
            self.commits += 1

        def close(self):
            pass

    class FakeGeocode:
        min_interval = 0

        def get_location_checked(self, latitude, longitude):
            raise GeocodeError("Nominatim down")

        def cache_stats(self):
            return {"hits": 0, "misses": 1}

    conn = FakeConn()
    monkeypatch.setitem(
        sys.modules,
        "psycopg2",
        types.SimpleNamespace(connect=lambda dsn: conn),
    )
    monkeypatch.setattr(runner, "_ensure_table", lambda c: None)
    monkeypatch.setattr(
        "firewatch.clients.geocode.GeoCodeClient",
        FakeGeocode,
    )

    args = runner.build_parser().parse_args(["geocode-backfill", "--limit", "1"])

    assert runner.cmd_geocode_backfill(args) == 1
    updates = [sql for sql, _ in conn.statements if sql.startswith("UPDATE fire_events")]
    assert updates == []


def test_update_intervals_default_to_source_aligned_values():
    """30 min for FIRMS NRT, 3 h for Open-Meteo regional model runs."""
    from firewatch.runner import build_parser

    assert build_parser().parse_args(["daily", "--once"]).interval_minutes == 30
    assert build_parser().parse_args(["forecast"]).interval_minutes == 180


def test_purge_out_of_scope_flag_is_wired():
    """Without this, `--dry-run` would silently delete instead of counting."""
    from firewatch import runner

    args = runner.build_parser().parse_args(["purge-out-of-scope"])
    assert args.dry_run is False
    assert args.func is runner.cmd_purge_out_of_scope

    args = runner.build_parser().parse_args(["purge-out-of-scope", "--dry-run"])
    assert args.dry_run is True


def test_map_provinces_flags_are_wired():
    from firewatch import runner

    args = runner.build_parser().parse_args(["map-provinces", "--dry-run"])
    assert args.func is runner.cmd_map_provinces
    assert args.dry_run is True


def test_panel_flags_are_wired():
    from firewatch import runner

    args = runner.build_parser().parse_args(
        [
            "panel",
            "--start-date",
            "2017-01-01",
            "--end-date",
            "2026-10-25",
            "--min-area-ha",
            "10",
            "--provinces",
            "01,28",
            "--output",
            "panel.csv",
        ]
    )
    assert args.func is runner.cmd_panel
    assert args.start_date == "2017-01-01"
    assert args.end_date == "2026-10-25"
    assert args.min_area_ha == 10.0
    assert args.provinces == "01,28"
    assert args.output == "panel.csv"


def test_province_weather_flags_are_wired():
    from firewatch import runner

    args = runner.build_parser().parse_args(
        [
            "province-weather",
            "--start-date",
            "2017-01-01",
            "--end-date",
            "2026-10-25",
            "--provinces",
            "28",
            "--output",
            "weather.csv",
            "--sleep",
            "0",
        ]
    )
    assert args.func is runner.cmd_province_weather
    assert args.start_date == "2017-01-01"
    assert args.end_date == "2026-10-25"
    assert args.provinces == "28"
    assert args.output == "weather.csv"
    assert args.sleep == 0.0


def test_forecast_flags_are_wired():
    from firewatch import runner

    args = runner.build_parser().parse_args(
        [
            "forecast",
            "--date",
            "2026-10-29",
            "--model",
            "data/models/hgb.joblib",
            "--provinces",
            "01,02",
            "--history-days",
            "35",
            "--sleep",
            "0",
            "--dry-run",
        ]
    )

    assert args.func is runner.cmd_forecast
    assert args.date == "2026-10-29"
    assert args.model == "data/models/hgb.joblib"
    assert args.provinces == "01,02"
    assert args.history_days == 35
    assert args.sleep == 0.0
    assert args.dry_run is True


def test_forecast_loop_flags_are_wired():
    from firewatch import runner

    args = runner.build_parser().parse_args(
        ["forecast", "--loop", "--interval-minutes", "120"]
    )

    assert args.func is runner.cmd_forecast
    assert args.loop is True
    assert args.interval_minutes == 120


def test_forecast_loop_runs_once_and_waits_for_the_next_round(monkeypatch):
    """The worker must actually schedule itself; a one-shot in compose is not enough."""
    from firewatch import runner

    args = runner.build_parser().parse_args(
        ["forecast", "--loop", "--interval-minutes", "5"]
    )
    calls = []

    def fake_run(_args):
        calls.append("run")
        return 0

    def stop_after_sleep(seconds):
        calls.append(seconds)
        raise KeyboardInterrupt

    monkeypatch.setattr(runner, "_forecast_once", fake_run)
    monkeypatch.setattr(runner.time, "sleep", stop_after_sleep)

    assert runner.cmd_forecast(args) == 0
    assert calls == ["run", 300]


def test_forecast_dry_run_does_not_touch_the_database(monkeypatch, tmp_path, caplog):
    """`--dry-run` must work without psycopg2 or a reachable Postgres."""
    import datetime as dt
    import sys

    import firewatch.clients.daily_weather as daily_weather
    import firewatch.ml.forecast as forecast_module
    from firewatch import runner
    from firewatch.ml.features import FEATURES

    target = dt.date(2026, 10, 29)
    start = target - dt.timedelta(days=35)
    rows = []
    for code in ("01", "02"):
        for offset in range(36):
            day = start + dt.timedelta(days=offset)
            rows.append(
                {
                    "province_code": code,
                    "day": day.isoformat(),
                    "tmax_c": 30.0,
                    "tmin_c": 15.0,
                    "rh_min": 20.0,
                    "wind_max_kmh": 10.0,
                    "precip_mm": 0.0,
                }
            )

    class FakeDailyWeather:
        def __init__(self, retries=None):
            self.closed = False
            self.forecast_calls = []

        @staticmethod
        def _code(latitude):
            return "01" if latitude == 42.8467 else "02"

        def fetch_daily(self, latitude, longitude, start_date, end_date):
            code = self._code(latitude)
            return [
                row
                for row in rows
                if row["province_code"] == code
                and start_date.isoformat() <= row["day"] <= end_date.isoformat()
            ]

        def fetch_forecast(self, latitude, longitude, start_date, end_date):
            self.forecast_calls.append((start_date, end_date))
            code = self._code(latitude)
            return [
                row
                for row in rows
                if row["province_code"] == code
                and start_date.isoformat() <= row["day"] <= end_date.isoformat()
            ]

        def close(self):
            self.closed = True

    class FakeModel:
        def predict_proba(self, X):
            import numpy as np

            return np.array([[0.9, 0.1]] * len(X))

    class FakeCalibrator:
        def predict(self, values):
            return values

    model_path = tmp_path / "model.joblib"
    model_path.write_bytes(b"stub")

    artifact = {
        "kind": "hgb",
        "features": list(FEATURES),
        "model": FakeModel(),
        "calibrator": FakeCalibrator(),
    }

    monkeypatch.setattr(daily_weather, "DailyWeatherClient", FakeDailyWeather)
    monkeypatch.setattr(
        forecast_module,
        "load_artifact",
        lambda path: artifact,
    )
    # If the dry-run path tried `import psycopg2`, it would fail loudly here.
    monkeypatch.setitem(sys.modules, "psycopg2", None)

    args = runner.build_parser().parse_args(
        [
            "forecast",
            "--date",
            target.isoformat(),
            "--model",
            str(model_path),
            "--provinces",
            "01,02",
            "--sleep",
            "0",
            "--dry-run",
        ]
    )

    with caplog.at_level(logging.INFO, logger="firewatch.runner"):
        assert runner.cmd_forecast(args) == 0

    assert "2 province(s)" in caplog.text
    assert "raw=0.1000" in caplog.text
    assert "not written to Postgres" in caplog.text


def test_forecast_rejects_a_history_shorter_than_the_dry_spell():
    from firewatch import runner

    args = runner.build_parser().parse_args(
        ["forecast", "--history-days", "10", "--dry-run"]
    )

    assert runner.cmd_forecast(args) == 1


def test_labels_flags_are_wired():
    """The parser must expose the label filters, not only the source."""
    from firewatch import runner
    from firewatch.config import settings

    args = runner.build_parser().parse_args(
        [
            "labels",
            "--source",
            "effis",
            "--start-date",
            "2026-07-01",
            "--end-date",
            "2026-07-31",
            "--min-area-ha",
            "2",
            "--limit",
            "10",
            "--country",
            "ES",
        ]
    )

    assert args.func is runner.cmd_labels
    assert args.source == "effis"
    assert args.start_date == "2026-07-01"
    assert args.end_date == "2026-07-31"
    assert args.min_area_ha == 2.0
    assert args.limit == 10
    assert args.country == "ES"
    assert args.page_size == settings.EFFIS_PAGE_SIZE
    assert args.dry_run is False


def test_labels_dry_run_does_not_touch_the_database(monkeypatch):
    """A pilot download must not require Postgres (or psycopg2)."""
    import sys

    from firewatch import runner
    from firewatch.config import settings

    seen = {}

    class StubEffis:
        def __init__(self, page_size=None):
            seen["page_size"] = page_size

        def fetch_burnt_areas(self, **kwargs):
            seen["kwargs"] = kwargs
            return []

        def close(self):
            seen["closed"] = True

    import firewatch.clients.effis as effis_module

    monkeypatch.setattr(effis_module, "EffisClient", StubEffis)
    # If the dry-run path tried `import psycopg2`, it would fail loudly here.
    monkeypatch.setitem(sys.modules, "psycopg2", None)

    args = runner.build_parser().parse_args(
        [
            "labels",
            "--start-date",
            "2026-07-01",
            "--end-date",
            "2026-07-31",
            "--min-area-ha",
            "2",
            "--limit",
            "10",
            "--dry-run",
        ]
    )

    assert runner.cmd_labels(args) == 0
    assert seen["page_size"] == settings.EFFIS_PAGE_SIZE
    assert seen["kwargs"] == {
        "start_date": "2026-07-01",
        "end_date": "2026-07-31",
        "min_area_ha": 2.0,
        "limit": 10,
        "country": settings.EFFIS_COUNTRY,
    }
    assert seen["closed"] is True


def test_backfill_passes_the_source_to_the_client(monkeypatch):
    """`--source` must reach the client, not just the parser: June needs SP.

    The NRT product only keeps ~4 months, so the summer backfill would silently
    miss June if the flag were parsed but ignored.
    """
    from firewatch import runner

    seen = {}

    class FakeClient:
        def __init__(self, source=None):
            seen["source"] = source

        def fetch_by_bbox(self, bbox, date=None):
            return []

    monkeypatch.setattr(runner, "FirmsClient", FakeClient)
    args = runner.build_parser().parse_args(
        [
            "backfill",
            "--regions",
            "canarias",
            "--start-date",
            "2026-06-01",
            "--end-date",
            "2026-06-01",
            "--source",
            "VIIRS_SNPP_SP",
            "--dry-run",
        ]
    )

    assert runner.cmd_backfill(args) == 0
    assert seen["source"] == "VIIRS_SNPP_SP"


# ---------------------------------------------------------------------------
# Refresh resilience: retries and honest exit codes
# ---------------------------------------------------------------------------


def test_refresh_retries_transient_failure(monkeypatch):
    """A flaky HTTP call must not silently drop a day from the refresh."""
    from firewatch import runner

    monkeypatch.setattr(runner.time, "sleep", lambda s: None)
    calls = {"n": 0}

    class Flaky:
        def fetch_by_bbox(self, bbox, date=None):
            calls["n"] += 1
            if calls["n"] <= 2:
                raise RuntimeError("connection reset")
            return []

    rc = runner.refresh_once(Flaky(), [("peninsula", "b")], days=1)
    assert rc == 0
    assert calls["n"] == 3


def test_refresh_exits_nonzero_when_permanently_failing(monkeypatch):
    """The scheduler must record a real failure, not a partial success."""
    from firewatch import runner

    monkeypatch.setattr(runner.time, "sleep", lambda s: None)
    calls = {"n": 0}

    class Dead:
        def fetch_by_bbox(self, bbox, date=None):
            calls["n"] += 1
            raise RuntimeError("kafka down")

    rc = runner.refresh_once(Dead(), [("peninsula", "b")], days=1)

    assert rc == 1
    assert calls["n"] == runner.REFRESH_RETRIES + 1


def test_refresh_backoff_grows_and_is_capped(monkeypatch):
    from firewatch import runner

    delays: list = []
    monkeypatch.setattr(runner.time, "sleep", delays.append)

    class Dead:
        def fetch_by_bbox(self, bbox, date=None):
            raise RuntimeError("boom")

    runner.refresh_once(Dead(), [("r", "b")], days=1, retries=4)

    assert delays[0] < delays[1] < delays[2], "backoff should grow"
    assert max(delays) <= runner.RETRY_MAX_DELAY, "backoff must stay capped"


def test_file_logging_is_enabled_by_default(tmp_path, monkeypatch):
    """A scheduled run's stdout is discarded; the log must reach a file."""
    from firewatch import runner

    monkeypatch.setattr(runner, "Path", lambda *a, **k: tmp_path / "logs" / "x")
    for handler in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(handler)

    runner.setup_logging(log_file=True)
    try:
        file_handlers = [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.FileHandler)
        ]
        assert file_handlers, "expected a file handler"
    finally:
        for handler in list(logging.getLogger().handlers):
            logging.getLogger().removeHandler(handler)
