"""Metrics for the province-day fire forecast.

Precision@k is reported at two levels: per day (the operational question "which
k provinces should I watch today?") and globally (the ranking question "which
are the k riskiest province-days of the test period?"). PR-AUC is the headline
metric because the positive class is ~2 % and accuracy would be misleading.
"""

from __future__ import annotations

from typing import Dict, Iterable, List

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)

DAILY_KS = (1, 3, 5, 10)
GLOBAL_KS = (100, 500, 1000)
_EPS = 1e-6


def precision_at_k(y_true: Iterable, y_score: Iterable, k: int) -> float:
    """Fracción de positivos entre las ``k`` puntuaciones más altas."""
    labels = np.asarray(list(y_true), dtype=float)
    scores = np.asarray(list(y_score), dtype=float)
    if labels.size == 0:
        return float("nan")
    k = min(int(k), labels.size)
    if k <= 0:
        return float("nan")
    order = np.argsort(-scores, kind="mergesort")[:k]
    return float(labels[order].mean())


def daily_precision_at_k(
    df: pd.DataFrame,
    score_col: str,
    k: int,
    day_col: str = "day",
    label_col: str = "label",
) -> float:
    """Media de precision@k calculada día a día (cada día pesa lo mismo)."""
    values = [
        precision_at_k(group[label_col], group[score_col], k)
        for _, group in df.groupby(day_col, sort=False)
    ]
    values = [value for value in values if not np.isnan(value)]
    return float(np.mean(values)) if values else float("nan")


def expected_calibration_error(
    y_true: Iterable, y_prob: Iterable, n_bins: int = 10
) -> float:
    """ECE ponderado por el número de filas de cada bin."""
    labels = np.asarray(list(y_true), dtype=float)
    probs = np.asarray(list(y_prob), dtype=float)
    if labels.size == 0:
        return float("nan")
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    indices = np.digitize(probs, bins[1:-1], right=False)
    total = 0.0
    for index in range(n_bins):
        mask = indices == index
        if not mask.any():
            continue
        total += mask.mean() * abs(probs[mask].mean() - labels[mask].mean())
    return float(total)


def calibration_table(
    y_true: Iterable, y_prob: Iterable, n_bins: int = 10
) -> List[Dict]:
    """Tabla de fiabilidad: predicción media frente a frecuencia observada."""
    labels = np.asarray(list(y_true), dtype=float)
    probs = np.asarray(list(y_prob), dtype=float)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    indices = np.digitize(probs, bins[1:-1], right=False)
    table = []
    for index in range(n_bins):
        mask = indices == index
        if not mask.any():
            continue
        table.append(
            {
                "bin_low": float(bins[index]),
                "bin_high": float(bins[index + 1]),
                "count": int(mask.sum()),
                "mean_predicted": float(probs[mask].mean()),
                "observed_rate": float(labels[mask].mean()),
            }
        )
    return table


def evaluate_predictions(
    df: pd.DataFrame,
    score_col: str,
    label_col: str = "label",
) -> Dict:
    """Métricas completas para un conjunto de predicciones."""
    if len(df) == 0:
        return {
            "rows": 0,
            "positives": 0,
            "positive_rate": None,
            "pr_auc": None,
            "roc_auc": None,
            "brier": None,
            "log_loss": None,
            "ece": None,
            "daily_precision_at_k": {str(k): None for k in DAILY_KS},
            "global_precision_at_k": {str(k): None for k in GLOBAL_KS},
        }

    labels = df[label_col].astype(int).to_numpy()
    probs = np.clip(df[score_col].astype(float).to_numpy(), 0.0, 1.0)
    positives = int(labels.sum())

    result = {
        "rows": int(len(df)),
        "positives": positives,
        "positive_rate": float(positives / len(df)) if len(df) else float("nan"),
        "pr_auc": (
            float(average_precision_score(labels, probs)) if positives else float("nan")
        ),
        "roc_auc": (
            float(roc_auc_score(labels, probs))
            if 0 < positives < len(labels)
            else float("nan")
        ),
        "brier": float(brier_score_loss(labels, probs)),
        "log_loss": float(
            log_loss(labels, np.clip(probs, _EPS, 1.0 - _EPS), labels=[0, 1])
        ),
        "ece": expected_calibration_error(labels, probs),
        "daily_precision_at_k": {
            str(k): daily_precision_at_k(df, score_col, k) for k in DAILY_KS
        },
        "global_precision_at_k": {
            str(k): precision_at_k(labels, probs, k) for k in GLOBAL_KS
        },
    }
    return result


def evaluate_model(
    df: pd.DataFrame,
    score_col: str,
) -> Dict:
    """Métricas globales, de temporada y por año."""
    overall = evaluate_predictions(df, score_col)
    season = df[df["month"].between(5, 10)]
    per_year = {
        str(int(year)): evaluate_predictions(part, score_col)
        for year, part in df.groupby("year", sort=True)
    }
    return {
        "all": overall,
        "season": evaluate_predictions(season, score_col) if not season.empty else None,
        "per_year": per_year,
        "calibration": calibration_table(df["label"], df[score_col]),
    }


def lift(model: Dict, baseline: Dict) -> Dict:
    """Cociente modelo/línea base para las métricas de ranking."""
    values = {}
    for name in ("pr_auc", "roc_auc"):
        model_value = model.get(name)
        base = baseline.get(name)
        values[name] = (
            float(model_value / base)
            if model_value is not None
            and base
            and not np.isnan(base)
            and not np.isnan(model_value)
            else None
        )
    for scope in ("daily_precision_at_k", "global_precision_at_k"):
        values[scope] = {}
        for key, model_value in model.get(scope, {}).items():
            base = baseline.get(scope, {}).get(key)
            values[scope][key] = (
                float(model_value / base)
                if model_value is not None
                and base
                and not np.isnan(base)
                and not np.isnan(model_value)
                else None
            )
    return values
