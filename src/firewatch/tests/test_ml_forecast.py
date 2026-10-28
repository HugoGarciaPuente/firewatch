"""Tests for the offline inference half of the 24 h forecast."""

from __future__ import annotations

from datetime import date, timedelta

import joblib
import numpy as np
import pytest

from firewatch.ml.features import FEATURES
from firewatch.ml.forecast import (
    ForecastError,
    artifact_version,
    build_forecast_frame,
    load_artifact,
    predict_frame,
)

TARGET = date(2026, 10, 29)


def _weather_rows(
    codes=("01", "02"),
    target: date = TARGET,
    history_days: int = 35,
):
    rows = []
    start = target - timedelta(days=history_days)
    for code in codes:
        for offset in range(history_days + 1):
            day = start + timedelta(days=offset)
            rows.append(
                {
                    "province_code": code,
                    "day": day.isoformat(),
                    "tmax_c": 30.0,
                    "tmin_c": 15.0,
                    "rh_min": 20.0,
                    "wind_max_kmh": 10.0,
                    # La lluvia del día objetivo no puede entrar en los
                    # antecedentes: precip_sum_7 y days_since_rain miran D-1.
                    "precip_mm": 100.0 if day == target else 0.0,
                }
            )
    return rows


def test_build_forecast_frame_does_not_leak_target_day_rain():
    frame = build_forecast_frame(
        _weather_rows(),
        TARGET,
        province_codes=("01", "02"),
    )

    assert len(frame) == 2
    assert set(frame["province_code"]) == {"01", "02"}
    row = frame.loc[frame["province_code"] == "01"].iloc[0]
    assert row["precip_mm"] == 100.0
    assert row["precip_sum_7"] == 0.0
    assert row["days_since_rain"] == 30.0
    assert row["tmax_lag1"] == 30.0
    assert set(FEATURES) <= set(frame.columns)


def test_build_forecast_frame_rejects_a_gap_in_the_history():
    rows = [
        row
        for row in _weather_rows(("01",))
        if row["day"] != (TARGET - timedelta(days=5)).isoformat()
    ]

    with pytest.raises(ForecastError, match="gap"):
        build_forecast_frame(rows, TARGET, province_codes=("01",))


def test_build_forecast_frame_rejects_a_truncated_dry_spell():
    with pytest.raises(ForecastError, match="at least 31"):
        build_forecast_frame(
            _weather_rows(("01",), history_days=10),
            TARGET,
            province_codes=("01",),
        )


def test_build_forecast_frame_rejects_a_missing_target_day():
    rows = [
        row
        for row in _weather_rows(("01",))
        if row["day"] != TARGET.isoformat()
    ]

    with pytest.raises(ForecastError, match="gap"):
        build_forecast_frame(rows, TARGET, province_codes=("01",))


def test_predict_frame_returns_raw_and_calibrated_values():
    frame = build_forecast_frame(
        _weather_rows(("01",)),
        TARGET,
        province_codes=("01",),
    )

    class FakeModel:
        def predict_proba(self, X):
            return np.array([[0.3, 0.7]] * len(X))

    class FakeCalibrator:
        def predict(self, values):
            return np.asarray(values) * 0.5

    rows = predict_frame(
        {"model": FakeModel(), "calibrator": FakeCalibrator()},
        frame,
    )

    assert len(rows) == 1
    assert rows[0]["province_code"] == "01"
    assert rows[0]["probability"] == pytest.approx(0.7)
    assert rows[0]["probability_calibrated"] == pytest.approx(0.35)


def test_load_artifact_rejects_a_different_feature_set(tmp_path):
    path = tmp_path / "bad.joblib"
    joblib.dump(
        {
            "kind": "hgb",
            "features": ["province_code"],
            "model": object(),
            "calibrator": None,
        },
        path,
    )

    with pytest.raises(ForecastError, match="different features"):
        load_artifact(path)


def test_artifact_version_is_short_and_content_addressed(tmp_path):
    first = tmp_path / "first.joblib"
    second = tmp_path / "second.joblib"
    first.write_bytes(b"one")
    second.write_bytes(b"two")

    version = artifact_version(first)

    assert len(version) == 12
    assert version != artifact_version(second)
