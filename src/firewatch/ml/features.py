"""Feature engineering for the province-day fire model.

The panel contains one row per province and day. Weather for day D is used as a
proxy for the forecast that would be available when the prediction is issued on
D-1; the historical evaluation uses ERA5 reanalysis, not an archived forecast.
That distinction is stated in the report: the model is a 24-hour-ahead risk
model, but its retrospective score is optimistic if day-D weather forecasts are
worse than reanalysis.

Antecedent features (``days_since_rain``, the 7-day precipitation sum and the
lags) use only days before D. They do not peek at the target day.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from firewatch.services.provinces import PROVINCES

logger = logging.getLogger(__name__)

CATEGORICAL_FEATURES = ("province_code",)

NUMERIC_FEATURES = (
    "latitude",
    "longitude",
    "month",
    "doy_sin",
    "doy_cos",
    "tmax_c",
    "tmin_c",
    "rh_min",
    "wind_max_kmh",
    "precip_mm",
    "vpd_kpa",
    "tmax_lag1",
    "tmax_lag2",
    "tmax_lag3",
    "rh_min_lag1",
    "wind_max_lag1",
    "precip_lag1",
    "precip_sum_7",
    "tmax_mean_7",
    "days_since_rain",
)

FEATURES = (*CATEGORICAL_FEATURES, *NUMERIC_FEATURES)

WEATHER_COLUMNS = ("tmax_c", "tmin_c", "rh_min", "wind_max_kmh", "precip_mm")

# Un día sin lluvia se cuenta hasta 30; más allá la diferencia deja de aportar y
# el valor se dispara en veranos largos.
MAX_DRY_DAYS = 30


def load_dataset(
    panel_path: Path | str,
    weather_path: Path | str,
) -> pd.DataFrame:
    """Carga el panel y la meteorología, los cruza y construye las features.

    El merge es uno a uno: un panel con provincias/días duplicados o una
    meteorología duplicada indica un error de construcción y debe fallar, no
    duplicar filas silenciosamente.
    """
    panel = pd.read_csv(panel_path, dtype={"province_code": str})
    weather = pd.read_csv(weather_path, dtype={"province_code": str})

    for label, frame, required in (
        ("panel", panel, {"province_code", "day", "label"}),
        ("weather", weather, {"province_code", "day", *WEATHER_COLUMNS}),
    ):
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"{label} is missing columns: {sorted(missing)}")

    panel["day"] = pd.to_datetime(panel["day"], errors="raise")
    weather["day"] = pd.to_datetime(weather["day"], errors="raise")

    unknown = sorted(set(panel["province_code"]) - set(PROVINCES))
    if unknown:
        raise ValueError(
            f"Unknown province code(s) in the panel: {', '.join(unknown)}"
        )

    merged = panel.merge(
        weather,
        on=["province_code", "day"],
        how="left",
        validate="one_to_one",
    )

    missing_weather = merged[list(WEATHER_COLUMNS)].isna().any(axis=1)
    if missing_weather.any():
        logger.warning(
            "Dropping %d row(s) without complete weather", int(missing_weather.sum())
        )
        merged = merged.loc[~missing_weather].copy()

    merged["latitude"] = merged["province_code"].map(
        lambda code: PROVINCES[code].latitude
    )
    merged["longitude"] = merged["province_code"].map(
        lambda code: PROVINCES[code].longitude
    )
    merged["label"] = merged["label"].astype(int)

    _check_contiguous(merged)
    return add_features(merged)


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Añade calendario, VPD y la historia meteorológica de cada provincia."""
    frame = df.copy()
    frame["day"] = pd.to_datetime(frame["day"], errors="raise")
    frame = frame.sort_values(["province_code", "day"]).reset_index(drop=True)

    doy = frame["day"].dt.day_of_year.astype(float)
    frame["year"] = frame["day"].dt.year.astype(int)
    frame["month"] = frame["day"].dt.month.astype(int)
    frame["doy_sin"] = np.sin(2.0 * np.pi * doy / 365.25)
    frame["doy_cos"] = np.cos(2.0 * np.pi * doy / 365.25)
    frame["vpd_kpa"] = vapour_pressure_deficit(frame["tmax_c"], frame["rh_min"])

    groups = []
    for _, group in frame.groupby("province_code", sort=False):
        groups.append(_add_province_history(group))
    frame = pd.concat(groups, ignore_index=True)

    before = len(frame)
    frame = frame.dropna(subset=list(NUMERIC_FEATURES)).reset_index(drop=True)
    dropped = before - len(frame)
    if dropped:
        logger.info(
            "Dropped %d row(s) with incomplete lag features (panel edges)", dropped
        )
    return frame


def _add_province_history(group: pd.DataFrame) -> pd.DataFrame:
    """Lags y acumulados de una provincia; ``group`` debe ser un solo territorio."""
    group = group.sort_values("day").copy()
    precip = group["precip_mm"].astype(float)

    for lag in (1, 2, 3):
        group[f"tmax_lag{lag}"] = group["tmax_c"].shift(lag)
    group["rh_min_lag1"] = group["rh_min"].shift(1)
    group["wind_max_lag1"] = group["wind_max_kmh"].shift(1)
    group["precip_lag1"] = precip.shift(1)
    group["precip_sum_7"] = (
        precip.shift(1).rolling(7, min_periods=1).sum().astype(float)
    )
    group["tmax_mean_7"] = (
        group["tmax_c"].shift(1).rolling(7, min_periods=1).mean().astype(float)
    )

    # Días consecutivos sin lluvia ANTES del día D. Se mira precip[i-1], no
    # precip[i]: la lluvia del propio día objetivo no puede explicar la etiqueta
    # de ese mismo día en un pronóstico emitido la víspera.
    values = precip.fillna(0.0).to_numpy()
    days_since_rain = np.zeros(len(values), dtype=float)
    counter = 0
    for index in range(len(values)):
        if index > 0:
            counter = 0 if values[index - 1] >= 1.0 else min(
                counter + 1, MAX_DRY_DAYS
            )
        days_since_rain[index] = counter
    group["days_since_rain"] = days_since_rain

    # La primera fila no tiene historia: sus lags son NaN y se descartan en
    # add_features. rolling(min_periods=1) devolvería 0 y simularía un pasado
    # seco que no existe.
    first = group.index[0]
    lag_columns = [
        "tmax_lag1",
        "tmax_lag2",
        "tmax_lag3",
        "rh_min_lag1",
        "wind_max_lag1",
        "precip_lag1",
        "precip_sum_7",
        "tmax_mean_7",
    ]
    group.loc[first, lag_columns] = np.nan
    return group


def vapour_pressure_deficit(
    tmax_c: pd.Series, rh_min: pd.Series
) -> pd.Series:
    """Déficit de presión de vapor (kPa) a partir de Tmax y HR mínima.

    Es una medida directa de la sequedad del aire: a igual temperatura, una
    humedad mínima baja dispara el VPD. Se calcula con la fórmula de Magnus.
    """
    es = 0.6108 * np.exp(17.27 * tmax_c / (tmax_c + 237.3))
    ea = es * (rh_min / 100.0)
    return (es - ea).clip(lower=0.0)


def _check_contiguous(df: pd.DataFrame) -> None:
    """Avisa si a una provincia le falta un día; los lags serían engañosos."""
    for code, group in df.groupby("province_code", sort=False):
        days = group["day"].sort_values()
        gaps = days.diff().dropna().dt.days
        if not gaps.empty and (gaps != 1).any():
            logger.warning(
                "Province %s has a gap in the daily panel; lag features are "
                "computed over available rows, not calendar days.",
                code,
            )


def split_dataset(
    df: pd.DataFrame,
    train_end: str = "2024-12-31",
    valid_end: str = "2025-12-31",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split temporal: entrenamiento, validación y test, sin mezclar años."""
    train_end_ts = pd.Timestamp(train_end)
    valid_end_ts = pd.Timestamp(valid_end)
    if train_end_ts >= valid_end_ts:
        raise ValueError("train_end must be before valid_end")

    train = df[df["day"] <= train_end_ts].copy()
    valid = df[(df["day"] > train_end_ts) & (df["day"] <= valid_end_ts)].copy()
    test = df[df["day"] > valid_end_ts].copy()
    for name, part in (("train", train), ("validation", valid), ("test", test)):
        if part.empty:
            raise ValueError(f"{name} split is empty; check the date boundaries")
    return train, valid, test
