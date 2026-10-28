"""Tests for the ML training pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from firewatch.ml.train import climatology_predictions, run


def test_climatology_uses_training_only_and_falls_back_for_unknown_provinces():
    train = pd.DataFrame(
        {
            "province_code": ["01", "01", "02", "02"],
            "month": [7, 7, 7, 7],
            "label": [1, 0, 0, 0],
        }
    )
    target = pd.DataFrame(
        {"province_code": ["01", "02", "03"], "month": [7, 7, 7]}
    )

    result = climatology_predictions(train, target)

    # 01 tiene un positivo de dos días; 02 tiene cero (no debe caer al global);
    # 03 no aparece en train y usa la tasa global 1/4.
    assert result["climatology"].tolist() == pytest.approx([0.5, 0.0, 0.25])


def _write_tiny_dataset(tmp_path: Path) -> tuple[Path, Path]:
    days = pd.date_range("2024-01-01", "2026-01-31", freq="D")
    labels = pd.Series(0, index=days, dtype=int)
    for day in ("2024-07-15", "2025-07-15", "2026-01-15"):
        labels.loc[pd.Timestamp(day)] = 1

    panel = tmp_path / "panel.csv"
    weather = tmp_path / "weather.csv"
    pd.DataFrame(
        {
            "province_code": "01",
            "province_name": "Álava",
            "day": days.strftime("%Y-%m-%d"),
            "label": labels.to_numpy(),
            "fire_count": labels.to_numpy(),
            "area_ha": labels.to_numpy() * 20.0,
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
    return panel, weather


def test_run_writes_report_and_model_artifact(tmp_path):
    panel, weather = _write_tiny_dataset(tmp_path)
    output = tmp_path / "report.json"
    markdown = tmp_path / "report.md"
    models = tmp_path / "models"

    report = run(
        panel_path=panel,
        weather_path=weather,
        output_path=output,
        markdown_path=markdown,
        models_dir=models,
        include_hgb=False,
        include_importance=False,
    )

    assert output.exists()
    assert markdown.exists()
    assert (models / "logistic.joblib").exists()
    assert report["dataset"]["train"]["rows"] == 363
    assert report["dataset"]["validation"]["rows"] == 365
    assert report["dataset"]["test"]["rows"] == 31
    assert report["metrics"]["logistic"]["test"]["all"]["rows"] == 31
    assert "Baseline" in markdown.read_text(encoding="utf-8")

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["metrics"]["climatology"]["test"]["all"]["rows"] == 31
    assert payload["metrics"]["logistic"]["test"]["all"]["pr_auc"] is not None
