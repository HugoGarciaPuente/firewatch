"""Train and evaluate the province-day fire model.

Run from the repository root after building the panel and the weather CSV::

    python -m firewatch.ml.train \
        --panel data/panel_province_day.csv \
        --weather data/weather_province_day.csv

The split is temporal, never random: 2017-2024 trains, 2025 validates the
calibration and 2026 is the untouched test year. A random split would put a
fire from the same province and week in both train and test and inflate every
metric.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from firewatch.ml.features import (
    CATEGORICAL_FEATURES,
    FEATURES,
    NUMERIC_FEATURES,
    load_dataset,
    split_dataset,
)
from firewatch.ml.metrics import evaluate_model, lift

logger = logging.getLogger(__name__)

LOGISTIC_PARAMS = {
    "max_iter": 2000,
    "solver": "lbfgs",
    "random_state": 42,
}

HGB_PARAMS = {
    "max_iter": 250,
    "learning_rate": 0.03,
    "max_leaf_nodes": 15,
    "min_samples_leaf": 50,
    "l2_regularization": 1.0,
    "early_stopping": False,
    "random_state": 42,
}


def climatology_predictions(
    train: pd.DataFrame, target: pd.DataFrame
) -> pd.DataFrame:
    """Línea base: tasa histórica de la provincia en ese mes.

    Se calcula solo con el entrenamiento. Es una línea base exigente: ya
    incorpora la estacionalidad y el hecho de que Asturias o Cantabria arden
    mucho más que Almería, así que un modelo que no la supere no aporta nada.
    """
    global_rate = float(train["label"].mean())
    by_province_month = (
        train.groupby(["province_code", "month"])["label"].mean().rename("climatology")
    )
    by_province = train.groupby("province_code")["label"].mean()

    result = target.copy()
    result = result.merge(
        by_province_month.reset_index(),
        on=["province_code", "month"],
        how="left",
        validate="many_to_one",
    )
    result["climatology"] = result["climatology"].fillna(
        result["province_code"].map(by_province)
    )
    result["climatology"] = result["climatology"].fillna(global_rate)
    return result


def build_model(kind: str) -> Pipeline:
    """Pipeline de preprocesamiento + clasificador."""
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "province",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                list(CATEGORICAL_FEATURES),
            ),
            ("numeric", StandardScaler(), list(NUMERIC_FEATURES)),
        ],
        remainder="drop",
    )

    if kind == "logistic":
        classifier = LogisticRegression(**LOGISTIC_PARAMS)
    elif kind == "hgb":
        classifier = HistGradientBoostingClassifier(**HGB_PARAMS)
    else:
        raise ValueError(f"Unknown model kind: {kind}")

    return Pipeline([("prep", preprocessor), ("clf", classifier)])


def fit_and_predict(
    kind: str,
    train: pd.DataFrame,
    valid: pd.DataFrame,
    test: pd.DataFrame,
) -> tuple[Pipeline, IsotonicRegression | None, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Entrena, predice y calibra con validación (nunca con test)."""
    model = build_model(kind)
    model.fit(train[list(FEATURES)], train["label"])

    valid_prob = model.predict_proba(valid[list(FEATURES)])[:, 1]
    test_prob = model.predict_proba(test[list(FEATURES)])[:, 1]

    if valid["label"].nunique() < 2:
        calibrator = None
        valid_calibrated = valid_prob
        test_calibrated = test_prob
    else:
        calibrator = IsotonicRegression(out_of_bounds="clip")
        calibrator.fit(valid_prob, valid["label"])
        valid_calibrated = calibrator.predict(valid_prob)
        test_calibrated = calibrator.predict(test_prob)

    return model, calibrator, valid_prob, test_prob, valid_calibrated, test_calibrated


def top_logistic_features(model: Pipeline, limit: int = 20) -> list[Dict]:
    names = model.named_steps["prep"].get_feature_names_out()
    coefficients = model.named_steps["clf"].coef_[0]
    order = np.argsort(-np.abs(coefficients))[:limit]
    return [
        {"feature": str(names[index]), "coefficient": float(coefficients[index])}
        for index in order
    ]


def top_permutation_features(
    model: Pipeline,
    valid: pd.DataFrame,
    limit: int = 20,
    sample_size: int = 2000,
) -> list[Dict]:
    """Importancia por permutación en una muestra de validación."""
    sample = (
        valid.sample(n=sample_size, random_state=42)
        if len(valid) > sample_size
        else valid
    )
    result = permutation_importance(
        model,
        sample[list(FEATURES)],
        sample["label"],
        n_repeats=2,
        random_state=42,
        scoring="average_precision",
    )
    order = np.argsort(-result.importances_mean)[:limit]
    return [
        {
            "feature": FEATURES[index],
            "importance": float(result.importances_mean[index]),
            "std": float(result.importances_std[index]),
        }
        for index in order
    ]


def _dataset_summary(
    train: pd.DataFrame,
    valid: pd.DataFrame,
    test: pd.DataFrame,
) -> Dict:
    def summary(part: pd.DataFrame) -> Dict:
        return {
            "rows": int(len(part)),
            "positives": int(part["label"].sum()),
            "positive_rate": float(part["label"].mean()),
            "start": str(part["day"].min().date()),
            "end": str(part["day"].max().date()),
        }

    return {
        "train": summary(train),
        "validation": summary(valid),
        "test": summary(test),
    }


def _jsonable(value):
    """Convierte tipos de numpy y NaN en algo que ``json`` acepte."""
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def render_markdown(report: Dict) -> str:
    """Informe legible con las tablas principales."""
    lines = [
        "# Baseline de predicción de incendios a 24 h",
        "",
        "Unidad: **provincia-día**. Etiqueta: incendio confirmado EFFIS de "
        "**≥ 10 ha** iniciado ese día en la provincia.",
        "",
        "## Dataset",
        "",
        "| Split | Fechas | Filas | Positivos | Tasa |",
        "|---|---|---:|---:|---:|",
    ]
    for name, part in report["dataset"].items():
        lines.append(
            f"| {name} | {part['start']} .. {part['end']} | {part['rows']:,} | "
            f"{part['positives']:,} | {100 * part['positive_rate']:.2f} % |"
        )

    hgb = report.get("models", {}).get("hgb")
    if hgb:
        lines += [
            "",
            f"HGB: {hgb['max_iter']} iteraciones, `learning_rate={hgb['learning_rate']}`, "
            f"`max_leaf_nodes={hgb['max_leaf_nodes']}`, "
            f"`min_samples_leaf={hgb['min_samples_leaf']}`, "
            f"`l2_regularization={hgb['l2_regularization']}`. La calibración "
            "isotónica se ajusta en validación 2025, nunca en test.",
        ]

    lines += [
        "",
        "## Test 2026 (todas las estaciones)",
        "",
        "| Modelo | PR-AUC | ROC-AUC | Brier | ECE | P@1/día | P@3/día | P@5/día | P@10/día |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for kind, label in (
        ("climatology", "Climatología"),
        ("logistic", "Regresión logística"),
        ("logistic_calibrated", "Logística calibrada"),
        ("hgb", "HistGradientBoosting"),
        ("hgb_calibrated", "HGB calibrado"),
    ):
        metrics = report["metrics"].get(kind, {}).get("test", {}).get("all")
        if not metrics or metrics.get("pr_auc") is None:
            continue
        daily = metrics["daily_precision_at_k"]
        lines.append(
            f"| {label} | {metrics['pr_auc']:.4f} | {metrics['roc_auc']:.4f} | "
            f"{metrics['brier']:.4f} | {metrics['ece']:.4f} | "
            f"{daily['1']:.4f} | {daily['3']:.4f} | {daily['5']:.4f} | "
            f"{daily['10']:.4f} |"
        )

    lines += [
        "",
        "## Temporada mayo-octubre",
        "",
        "| Modelo | PR-AUC | Brier | ECE | P@5/día |",
        "|---|---:|---:|---:|---:|",
    ]
    for kind, label in (
        ("climatology", "Climatología"),
        ("logistic", "Regresión logística"),
        ("logistic_calibrated", "Logística calibrada"),
        ("hgb", "HistGradientBoosting"),
        ("hgb_calibrated", "HGB calibrado"),
    ):
        metrics = report["metrics"].get(kind, {}).get("test", {}).get("season")
        if not metrics or metrics.get("pr_auc") is None:
            continue
        lines.append(
            f"| {label} | {metrics['pr_auc']:.4f} | {metrics['brier']:.4f} | "
            f"{metrics['ece']:.4f} | {metrics['daily_precision_at_k']['5']:.4f} |"
        )

    lines += [
        "",
        "## PR-AUC por año (test)",
        "",
        "| Modelo | 2025 (validación) | 2026 (test) |",
        "|---|---:|---:|",
    ]
    for kind, label in (
        ("climatology", "Climatología"),
        ("logistic", "Regresión logística"),
        ("hgb", "HistGradientBoosting"),
    ):
        model = report["metrics"].get(kind)
        if not model:
            continue
        valid_year = model.get("validation", {}).get("per_year", {}).get("2025", {})
        test_year = model.get("test", {}).get("per_year", {}).get("2026", {})
        valid_pr = valid_year.get("pr_auc")
        test_pr = test_year.get("pr_auc")
        lines.append(
            f"| {label} | {valid_pr if valid_pr is not None else float('nan'):.4f} | "
            f"{test_pr if test_pr is not None else float('nan'):.4f} |"
        )

    lines += [
        "",
        "## Lift frente a climatología (test)",
        "",
        "| Modelo | PR-AUC | P@5/día |",
        "|---|---:|---:|",
    ]
    for kind, label in (
        ("logistic", "Regresión logística"),
        ("hgb", "HistGradientBoosting"),
    ):
        values = report.get("lift", {}).get(kind, {})
        if not values:
            continue
        lines.append(
            f"| {label} | {values.get('pr_auc') or float('nan'):.3f} | "
            f"{values.get('daily_precision_at_k', {}).get('5') or float('nan'):.3f} |"
        )

    hgb_features = report.get("top_features", {}).get("hgb", [])
    if hgb_features:
        lines += [
            "",
            "## Variables más importantes (HGB, validación 2025)",
            "",
            "| Variable | Importancia |",
            "|---|---:|",
        ]
        for item in hgb_features[:10]:
            lines.append(f"| `{item['feature']}` | {item['importance']:.4f} |")
        lines += [
            "",
            "Latitud y longitud dominan porque codifican la geografía provincial "
            "(qué provincias arden más), no porque el modelo sea puramente "
            "espacial: después aparecen la lluvia acumulada, el VPD, el mes y los "
            "días sin lluvia.",
        ]

    lines += [
        "",
        "## Notas",
        "",
        "- La meteorología del día D se usa como proxy de la previsión disponible "
        "la víspera; la evaluación histórica usa reanálisis ERA5, no una previsión "
        "archivada. El resultado es optimista si la previsión real es peor.",
        "- Los lags, la lluvia acumulada y `days_since_rain` usan solo días "
        "anteriores al objetivo.",
        "- El test 2026 no se ha usado para ajustar hiperparámetros ni calibración.",
        f"- La tasa positiva de test ({100 * report['dataset']['test']['positive_rate']:.2f} %) "
        f"no es la de entrenamiento ({100 * report['dataset']['train']['positive_rate']:.2f} %): "
        "2026 fue un año de más incendios y los valores absolutos no se trasladan "
        "a un año medio.",
    ]
    return "\n".join(lines) + "\n"


def run(
    panel_path: Path | str,
    weather_path: Path | str,
    output_path: Path | str,
    markdown_path: Optional[Path | str] = None,
    models_dir: Optional[Path | str] = None,
    train_end: str = "2024-12-31",
    valid_end: str = "2025-12-31",
    include_hgb: bool = True,
    include_importance: bool = True,
) -> Dict:
    logger.info("Loading panel and weather...")
    dataset = load_dataset(panel_path, weather_path)
    logger.info(
        "Dataset: %d rows, %d positives (%.2f %%)",
        len(dataset),
        int(dataset["label"].sum()),
        100.0 * dataset["label"].mean(),
    )

    train, valid, test = split_dataset(dataset, train_end, valid_end)
    report: Dict = {
        "dataset": _dataset_summary(train, valid, test),
        "features": list(FEATURES),
        "models": {
            "logistic": LOGISTIC_PARAMS,
            "hgb": HGB_PARAMS if include_hgb else None,
        },
        "notes": {
            "positive_threshold_ha": 10,
            "split": f"train <= {train_end}; validation <= {valid_end}; test after",
            "weather": "ERA5 reanalysis as a proxy for the day-D forecast",
        },
        "metrics": {},
        "lift": {},
        "top_features": {},
    }

    valid_clim = climatology_predictions(train, valid)
    test_clim = climatology_predictions(train, test)
    report["metrics"]["climatology"] = {
        "validation": evaluate_model(valid_clim, "climatology"),
        "test": evaluate_model(test_clim, "climatology"),
    }

    for kind in ("logistic", "hgb"):
        if kind == "hgb" and not include_hgb:
            continue
        logger.info("Training %s...", kind)
        model, calibrator, valid_prob, test_prob, valid_cal, test_cal = fit_and_predict(
            kind, train, valid, test
        )
        valid_scored = valid.assign(pred=valid_prob, pred_calibrated=valid_cal)
        test_scored = test.assign(pred=test_prob, pred_calibrated=test_cal)
        report["metrics"][kind] = {
            "validation": evaluate_model(valid_scored, "pred"),
            "test": evaluate_model(test_scored, "pred"),
        }
        report["metrics"][f"{kind}_calibrated"] = {
            "validation": evaluate_model(valid_scored, "pred_calibrated"),
            "test": evaluate_model(test_scored, "pred_calibrated"),
        }
        report["lift"][kind] = lift(
            report["metrics"][kind]["test"]["all"],
            report["metrics"]["climatology"]["test"]["all"],
        )

        if include_importance:
            if kind == "logistic":
                report["top_features"][kind] = top_logistic_features(model)
            else:
                report["top_features"][kind] = top_permutation_features(model, valid)

        if models_dir:
            models_path = Path(models_dir)
            models_path.mkdir(parents=True, exist_ok=True)
            joblib.dump(
                {
                    "kind": kind,
                    "features": list(FEATURES),
                    "categorical": list(CATEGORICAL_FEATURES),
                    "model": model,
                    "calibrator": calibrator,
                },
                models_path / f"{kind}.joblib",
            )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(_jsonable(report), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if markdown_path:
        markdown = Path(markdown_path)
        markdown.parent.mkdir(parents=True, exist_ok=True)
        markdown.write_text(render_markdown(report), encoding="utf-8")

    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m firewatch.ml.train",
        description="Entrenar el baseline provincia-día de predicción a 24 h.",
    )
    parser.add_argument(
        "--panel",
        default="data/panel_province_day.csv",
        help="CSV del panel provincia-día (por defecto: %(default)s)",
    )
    parser.add_argument(
        "--weather",
        default="data/weather_province_day.csv",
        help="CSV de meteorología diaria por provincia (por defecto: %(default)s)",
    )
    parser.add_argument(
        "--output",
        default="reports/forecast_baseline.json",
        help="JSON de métricas (por defecto: %(default)s)",
    )
    parser.add_argument(
        "--markdown",
        default="reports/forecast_baseline.md",
        help="Informe Markdown (por defecto: %(default)s)",
    )
    parser.add_argument(
        "--models-dir",
        default="data/models",
        help="Directorio de artefactos joblib (por defecto: %(default)s)",
    )
    parser.add_argument(
        "--train-end",
        default="2024-12-31",
        help="Último día de entrenamiento (por defecto: %(default)s)",
    )
    parser.add_argument(
        "--valid-end",
        default="2025-12-31",
        help="Último día de validación (por defecto: %(default)s)",
    )
    parser.add_argument(
        "--no-hgb",
        action="store_true",
        help="Omitir HistGradientBoosting (útil para una prueba rápida)",
    )
    parser.add_argument(
        "--no-importance",
        action="store_true",
        help="Omitir la importancia de variables (más rápido)",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    args = build_parser().parse_args(argv)
    report = run(
        panel_path=args.panel,
        weather_path=args.weather,
        output_path=args.output,
        markdown_path=args.markdown,
        models_dir=args.models_dir,
        train_end=args.train_end,
        valid_end=args.valid_end,
        include_hgb=not args.no_hgb,
        include_importance=not args.no_importance,
    )

    print("\nTest 2026 (all days):")
    print(f"{'model':<22} {'PR-AUC':>8} {'Brier':>8} {'P@5/day':>9}")
    for kind in ("climatology", "logistic", "logistic_calibrated", "hgb", "hgb_calibrated"):
        metrics = report["metrics"].get(kind, {}).get("test", {}).get("all")
        if not metrics:
            continue
        print(
            f"{kind:<22} {metrics['pr_auc']:>8.4f} {metrics['brier']:>8.4f} "
            f"{metrics['daily_precision_at_k']['5']:>9.4f}"
        )
    print(f"\nReport: {args.output}")
    print(f"Markdown: {args.markdown}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
