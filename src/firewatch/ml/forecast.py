"""Inferencia del modelo provincial a 24 horas.

Este módulo es la mitad offline del pronóstico: carga el artefacto joblib,
construye las features del día objetivo a partir de la meteorología observada y
la previsión, y devuelve probabilidades. No escribe en Postgres y no debe
importarse desde la API: ``runner forecast`` lo importa dentro de la función
para que las imágenes de runtime sigan sin pandas ni scikit-learn.

La ventana de historia no es arbitraria. ``days_since_rain`` satura a los 30
días, así que con menos de 31 días anteriores el día objetivo se puntuaría con
una sequía truncada y el modelo vería una distribución distinta a la del
entrenamiento. ``runner forecast`` pide 35 días por margen.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import joblib
import numpy as np
import pandas as pd

from firewatch.ml.features import FEATURES, WEATHER_COLUMNS, add_features
from firewatch.services.provinces import PROVINCES, PROVINCE_CODES

logger = logging.getLogger(__name__)

MIN_HISTORY_DAYS = 31


class ForecastError(RuntimeError):
    """Error de datos o de artefacto que impide emitir un pronóstico completo."""


def artifact_version(path: Path | str) -> str:
    """Versión corta y reproducible del artefacto (hash del archivo)."""
    path = Path(path)
    if not path.exists():
        raise ForecastError(f"Model artifact not found: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def load_artifact(path: Path | str) -> Dict:
    """Carga el joblib y comprueba que pertenece a este pipeline de features.

    Un artefacto entrenado con otra lista de features produciría una predicción
    silenciosamente incorrecta o un error de forma dentro de sklearn. Es mejor
    fallar al cargar, con el nombre del archivo, que a mitad de la inferencia.
    """
    path = Path(path)
    if not path.exists():
        raise ForecastError(f"Model artifact not found: {path}")

    artifact = joblib.load(path)
    if not isinstance(artifact, dict):
        raise ForecastError(f"Model artifact is not a dictionary: {path}")

    missing = {"kind", "features", "model", "calibrator"} - set(artifact)
    if missing:
        raise ForecastError(
            f"Model artifact {path} is missing key(s): {', '.join(sorted(missing))}"
        )
    if tuple(artifact["features"]) != FEATURES:
        raise ForecastError(
            f"Model artifact {path} was trained with different features; "
            "retrain it with `python -m firewatch.ml.train`."
        )
    return artifact


def build_forecast_frame(
    weather_rows: Iterable[Dict],
    target_date: date | str,
    province_codes: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Construye una fila de features por provincia para ``target_date``.

    ``weather_rows`` debe contener la historia reciente y el propio día
    objetivo. Se valida que cada provincia tenga una serie diaria contigua y
    suficientemente larga: un hueco haría que los lags saltasen varios días y
    que ``days_since_rain`` contase una sequía que no ocurrió.
    """
    target = _as_date(target_date)
    codes = _validate_codes(province_codes)
    rows = list(weather_rows)
    if not rows:
        raise ForecastError("No weather rows supplied for the forecast")

    frame = pd.DataFrame(rows)
    required = {"province_code", "day", *WEATHER_COLUMNS}
    missing_columns = required - set(frame.columns)
    if missing_columns:
        raise ForecastError(
            f"Weather rows are missing column(s): {', '.join(sorted(missing_columns))}"
        )

    frame["province_code"] = frame["province_code"].astype(str)
    unknown = sorted(set(frame["province_code"]) - set(PROVINCES))
    if unknown:
        raise ForecastError(f"Unknown province code(s): {', '.join(unknown)}")

    frame["day"] = pd.to_datetime(frame["day"], errors="raise")
    duplicated = frame.duplicated(["province_code", "day"])
    if duplicated.any():
        pairs = frame.loc[duplicated, ["province_code", "day"]].drop_duplicates()
        raise ForecastError(
            "Duplicate weather row(s) for province/day: "
            + ", ".join(
                f"{code} {day.date()}" for code, day in pairs.itertuples(index=False)
            )
        )

    frame = frame[frame["province_code"].isin(codes)].copy()
    missing_provinces = sorted(set(codes) - set(frame["province_code"]))
    if missing_provinces:
        raise ForecastError(
            f"No weather rows for province(s): {', '.join(missing_provinces)}"
        )

    frame["latitude"] = frame["province_code"].map(
        lambda code: PROVINCES[code].latitude
    )
    frame["longitude"] = frame["province_code"].map(
        lambda code: PROVINCES[code].longitude
    )
    frame["province_name"] = frame["province_code"].map(
        lambda code: PROVINCES[code].name
    )

    incomplete = frame[list(WEATHER_COLUMNS)].isna().any(axis=1)
    if incomplete.any():
        provinces = sorted(set(frame.loc[incomplete, "province_code"]))
        raise ForecastError(
            "Weather has missing values for province(s): " + ", ".join(provinces)
        )

    _check_history(frame, target, codes)

    featured = add_features(frame)
    target_rows = featured[featured["day"] == pd.Timestamp(target)].copy()
    missing_target = sorted(set(codes) - set(target_rows["province_code"]))
    if missing_target:
        raise ForecastError(
            "No target-day features for province(s): " + ", ".join(missing_target)
        )

    target_rows = (
        target_rows.set_index("province_code").loc[list(codes)].reset_index()
    )
    missing_features = [feature for feature in FEATURES if feature not in target_rows]
    if missing_features:
        raise ForecastError(
            f"Feature frame is missing column(s): {', '.join(missing_features)}"
        )
    if target_rows[list(FEATURES)].isna().any().any():
        raise ForecastError("Target-day features contain NULL values")

    return target_rows


def predict_frame(artifact: Dict, frame: pd.DataFrame) -> List[Dict]:
    """Devuelve el score crudo y la probabilidad calibrada por provincia."""
    model = artifact["model"]
    probabilities = np.asarray(
        model.predict_proba(frame[list(FEATURES)])[:, 1],
        dtype=float,
    )

    calibrator = artifact.get("calibrator")
    if calibrator is None:
        logger.warning(
            "Model artifact has no calibrator; using the raw score as probability"
        )
        calibrated = probabilities.copy()
    else:
        calibrated = np.asarray(calibrator.predict(probabilities), dtype=float)

    result = frame[["province_code", "province_name"]].copy()
    result["probability"] = np.clip(probabilities, 0.0, 1.0)
    result["probability_calibrated"] = np.clip(calibrated, 0.0, 1.0)
    return result.to_dict("records")


def _validate_codes(province_codes: Sequence[str] | None) -> tuple[str, ...]:
    codes = tuple(province_codes) if province_codes is not None else PROVINCE_CODES
    if not codes:
        raise ForecastError("At least one province code is required")
    if len(set(codes)) != len(codes):
        raise ForecastError("Province codes must not be duplicated")
    unknown = [code for code in codes if code not in PROVINCES]
    if unknown:
        raise ForecastError(f"Unknown province code(s): {', '.join(unknown)}")
    return codes


def _check_history(
    frame: pd.DataFrame,
    target: date,
    codes: Sequence[str],
) -> None:
    """Exige una serie diaria contigua y al menos ``MIN_HISTORY_DAYS`` previos."""
    target_ts = pd.Timestamp(target)
    for code in codes:
        days = (
            frame.loc[
                (frame["province_code"] == code) & (frame["day"] <= target_ts),
                "day",
            ]
            .sort_values()
            .reset_index(drop=True)
        )
        if days.empty:
            raise ForecastError(f"No weather history for province {code}")
        first = days.iloc[0]
        history_days = (target_ts - first).days
        if history_days < MIN_HISTORY_DAYS:
            raise ForecastError(
                f"Province {code} has only {history_days} day(s) of history; "
                f"at least {MIN_HISTORY_DAYS} are required"
            )

        expected = pd.date_range(first, target_ts, freq="D").to_numpy()
        actual = days.to_numpy()
        if len(actual) != len(expected) or not np.array_equal(actual, expected):
            raise ForecastError(
                f"Province {code} has a gap in its weather history; "
                "lag features would span more than one day"
            )


def _as_date(value: date | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])
