"""Persistencia de las predicciones provinciales a 24 horas.

``fire_predictions`` guarda el resultado de ``runner forecast``: una fila por
provincia y día objetivo. Es una tabla derivada, no una fuente de verdad. Se
puede borrar y regenerar; perder el volumen de Postgres no destruye las
etiquetas de EFFIS ni los focos de FIRMS.

La clave natural ``(target_date, province_code, model_name)`` hace el upsert
idempotente. Volver a ejecutar el pronóstico del mismo día reemplaza las
probabilidades y el ``generated_at``, pero no duplica filas. ``model_version``
queda como metadato para saber qué artefacto joblib produjo cada predicción.
"""

from __future__ import annotations

from typing import Dict, Iterable


def ensure_predictions_table(conn) -> None:
    """Crea la tabla de predicciones y su índice si no existen."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fire_predictions (
                target_date DATE NOT NULL,
                province_code TEXT NOT NULL,
                province_name TEXT NOT NULL,
                -- Score crudo del HGB: ordena mejor, pero no es una
                -- probabilidad calibrada. Se guarda junto al calibrado para
                -- que la API pueda elegir sin recalcular el modelo.
                probability DOUBLE PRECISION NOT NULL
                    CHECK (probability >= 0 AND probability <= 1),
                probability_calibrated DOUBLE PRECISION NOT NULL
                    CHECK (probability_calibrated >= 0 AND probability_calibrated <= 1),
                model_name TEXT NOT NULL,
                model_version TEXT NOT NULL,
                weather_source TEXT NOT NULL
                    DEFAULT 'open-meteo-archive+forecast',
                generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT fire_predictions_natural_key
                    PRIMARY KEY (target_date, province_code, model_name)
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS fire_predictions_date_prob_idx
                ON fire_predictions (target_date, probability DESC);
            """
        )
    conn.commit()


def upsert_predictions(conn, predictions: Iterable[Dict]) -> int:
    """Inserta o actualiza predicciones y devuelve cuántas se escribieron.

    Una sola transacción para todo el lote: si falla una fila, no queda un mapa
    a medias. El comando es idempotente y se puede repetir entero.
    """
    written = 0
    with conn.cursor() as cur:
        for prediction in predictions:
            cur.execute(
                """
                INSERT INTO fire_predictions
                    (target_date, province_code, province_name, probability,
                     probability_calibrated, model_name, model_version,
                     weather_source, generated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (target_date, province_code, model_name) DO UPDATE SET
                    province_name = EXCLUDED.province_name,
                    probability = EXCLUDED.probability,
                    probability_calibrated = EXCLUDED.probability_calibrated,
                    model_version = EXCLUDED.model_version,
                    weather_source = EXCLUDED.weather_source,
                    generated_at = CURRENT_TIMESTAMP
                """,
                (
                    prediction["target_date"],
                    prediction["province_code"],
                    prediction["province_name"],
                    float(prediction["probability"]),
                    float(prediction["probability_calibrated"]),
                    prediction["model_name"],
                    prediction["model_version"],
                    prediction.get(
                        "weather_source",
                        "open-meteo-archive+forecast",
                    ),
                ),
            )
            written += 1
    conn.commit()
    return written
