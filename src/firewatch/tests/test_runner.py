"""Tests for the CLI runner helpers (region resolution, argument wiring)."""

import logging

import pytest

from firewatch.config import resolve_regions, settings

def test_resolve_regions_defaults_to_configured_scope():
    regions = resolve_regions()

    labels = [label for label, _ in regions]
    assert "peninsula" in labels
    assert "peninsula_sur" in labels
    assert "canarias" in labels
    assert "baleares" in labels

    for _, bbox in regions:
        parts = bbox.split(",")
        assert len(parts) == 4
        assert all(float(p) for p in parts)


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
