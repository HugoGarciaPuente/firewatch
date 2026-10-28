"""Tests for the offline ML feature builder."""

from __future__ import annotations

import pandas as pd
import pytest

from firewatch.ml.features import (
    FEATURES,
    add_features,
    load_dataset,
    split_dataset,
    vapour_pressure_deficit,
)
from firewatch.services.provinces import PROVINCES


def _raw_frame(precip: list[float], tmax: float = 30.0, rh_min: float = 20.0):
    days = pd.date_range("2020-01-01", periods=len(precip), freq="D")
    return pd.DataFrame(
        {
            "province_code": "01",
            "day": days,
            "label": 0,
            "tmax_c": tmax,
            "tmin_c": 15.0,
            "rh_min": rh_min,
            "wind_max_kmh": 10.0,
            "precip_mm": precip,
            "latitude": PROVINCES["01"].latitude,
            "longitude": PROVINCES["01"].longitude,
        }
    )


def test_days_since_rain_ignores_same_day_precipitation():
    """La lluvia del día D no puede reiniciar el contador de ese mismo día."""
    frame = _raw_frame([0.0, 0.0, 0.0, 100.0, 0.0, 0.0])
    result = add_features(frame)

    # Se descartan los tres primeros días por los lags; quedan D4, D5 y D6.
    assert result["days_since_rain"].tolist() == [3.0, 0.0, 1.0]


def test_precip_sum_7_excludes_the_target_day():
    frame = _raw_frame([0.0, 0.0, 0.0, 100.0, 0.0, 0.0])
    result = add_features(frame)

    # D4 solo puede sumar D1-D3 (0 mm), no los 100 mm de D4.
    assert result.iloc[0]["precip_sum_7"] == pytest.approx(0.0)
    # D5 sí puede sumar D4.
    assert result.iloc[1]["precip_sum_7"] == pytest.approx(100.0)


def test_vapour_pressure_deficit_is_zero_at_saturation():
    saturated = vapour_pressure_deficit(pd.Series([30.0]), pd.Series([100.0]))
    dry = vapour_pressure_deficit(pd.Series([30.0]), pd.Series([20.0]))

    assert saturated.iloc[0] == pytest.approx(0.0, abs=1e-9)
    assert dry.iloc[0] > 0.0


def test_split_dataset_respects_boundaries():
    frame = pd.DataFrame(
        {
            "day": pd.to_datetime(
                ["2024-12-31", "2025-01-01", "2025-12-31", "2026-01-01"]
            ),
            "label": [0, 1, 0, 1],
        }
    )

    train, valid, test = split_dataset(frame)

    assert train["day"].tolist() == [pd.Timestamp("2024-12-31")]
    assert valid["day"].tolist() == [
        pd.Timestamp("2025-01-01"),
        pd.Timestamp("2025-12-31"),
    ]
    assert test["day"].tolist() == [pd.Timestamp("2026-01-01")]


def test_split_dataset_rejects_empty_split():
    frame = pd.DataFrame(
        {"day": pd.to_datetime(["2026-01-01", "2026-01-02"]), "label": [0, 1]}
    )

    with pytest.raises(ValueError, match="train split is empty"):
        split_dataset(frame)


def test_load_dataset_uses_province_coordinates_and_feature_columns(tmp_path):
    panel = tmp_path / "panel.csv"
    weather = tmp_path / "weather.csv"
    days = pd.date_range("2020-01-01", periods=5, freq="D")
    pd.DataFrame(
        {
            "province_code": "01",
            "province_name": "Álava",
            "day": days.strftime("%Y-%m-%d"),
            "label": [0, 0, 1, 0, 0],
            "fire_count": [0, 0, 1, 0, 0],
            "area_ha": [0.0, 0.0, 20.0, 0.0, 0.0],
        }
    ).to_csv(panel, index=False)
    pd.DataFrame(
        {
            "province_code": "01",
            "day": days.strftime("%Y-%m-%d"),
            "tmax_c": 30.0,
            "tmin_c": 15.0,
            "rh_min": 20.0,
            "wind_max_kmh": 10.0,
            "precip_mm": 0.0,
        }
    ).to_csv(weather, index=False)

    result = load_dataset(panel, weather)

    # Cinco días menos los tres primeros, que no tienen lags completos.
    assert len(result) == 2
    assert set(FEATURES).issubset(result.columns)
    assert result["latitude"].iloc[0] == PROVINCES["01"].latitude
    assert result[list(FEATURES)].isna().sum().sum() == 0
