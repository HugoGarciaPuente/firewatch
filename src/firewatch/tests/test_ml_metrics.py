"""Tests for the ML ranking and calibration metrics."""

from __future__ import annotations

import pandas as pd
import pytest

from firewatch.ml.metrics import (
    daily_precision_at_k,
    evaluate_predictions,
    expected_calibration_error,
    lift,
    precision_at_k,
)


def test_precision_at_k_uses_score_order():
    labels = [0, 1, 1, 0]
    scores = [0.1, 0.9, 0.8, 0.2]

    assert precision_at_k(labels, scores, 1) == pytest.approx(1.0)
    assert precision_at_k(labels, scores, 2) == pytest.approx(1.0)
    assert precision_at_k(labels, scores, 3) == pytest.approx(2.0 / 3.0)
    assert precision_at_k(labels, scores, 10) == pytest.approx(0.5)


def test_daily_precision_at_k_weights_each_day_equally():
    frame = pd.DataFrame(
        {
            "day": ["d1", "d1", "d2", "d2"],
            "label": [1, 0, 0, 1],
            "score": [0.9, 0.1, 0.9, 0.1],
        }
    )

    # d1 acierta el top-1; d2 falla. La media es 0.5, no 0.5 por azar.
    assert daily_precision_at_k(frame, "score", 1) == pytest.approx(0.5)


def test_expected_calibration_error_detects_overconfidence():
    assert expected_calibration_error([0, 0], [0.9, 0.9]) == pytest.approx(0.9)
    assert expected_calibration_error([1, 1], [0.9, 0.9]) == pytest.approx(0.1)
    assert expected_calibration_error([0, 1], [0.0, 1.0]) == pytest.approx(0.0)


def test_evaluate_predictions_reports_ranking_and_probability_metrics():
    frame = pd.DataFrame(
        {
            "day": ["d1", "d1", "d2", "d2"],
            "month": [7, 7, 7, 7],
            "year": [2026, 2026, 2026, 2026],
            "label": [0, 1, 0, 1],
            "pred": [0.1, 0.9, 0.2, 0.8],
        }
    )

    metrics = evaluate_predictions(frame, "pred")

    assert metrics["rows"] == 4
    assert metrics["positives"] == 2
    assert metrics["pr_auc"] == pytest.approx(1.0)
    assert metrics["brier"] < 0.05
    assert metrics["daily_precision_at_k"]["1"] == pytest.approx(1.0)


def test_lift_returns_none_when_baseline_is_zero():
    model = {
        "pr_auc": 0.2,
        "roc_auc": 0.8,
        "daily_precision_at_k": {"5": 0.1},
        "global_precision_at_k": {"100": 0.2},
    }
    baseline = {
        "pr_auc": 0.1,
        "roc_auc": 0.4,
        "daily_precision_at_k": {"5": 0.05},
        "global_precision_at_k": {"100": 0.1},
    }

    values = lift(model, baseline)

    assert values["pr_auc"] == pytest.approx(2.0)
    assert values["daily_precision_at_k"]["5"] == pytest.approx(2.0)
    assert values["global_precision_at_k"]["100"] == pytest.approx(2.0)

    zero = lift(model, {**baseline, "pr_auc": 0.0})
    assert zero["pr_auc"] is None
