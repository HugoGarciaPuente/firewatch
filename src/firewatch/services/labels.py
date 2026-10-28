"""Persistencia de incendios confirmados, las etiquetas de entrenamiento.

``fire_events`` guarda detecciones de FIRMS, que no son incendios confirmados:
una anomalía térmica puede ser un rebrote, una quema agrícola o un falso
positivo. La tabla ``confirmed_fires`` mantiene las áreas quemadas de EFFIS, la
fuente de verdad de las etiquetas del modelo de predicción a 24 h.

La clave natural ``(source, source_id)`` hace el upsert idempotente: reimportar
el catálogo actualiza la superficie y la fecha de último cambio, pero no
duplica el incendio. ``municipality_code`` (INE) se rellenará más adelante y
nunca se sobrescribe con NULL, para que una reimportación de EFFIS no borre el
mapeo ya resuelto.
"""

from __future__ import annotations

import logging
from typing import Dict, Iterable

from firewatch.services.provinces import normalize_province

logger = logging.getLogger(__name__)


def ensure_confirmed_fires_table(conn) -> None:
    """Crea la tabla de etiquetas y sus índices si no existen."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS confirmed_fires (
                source VARCHAR(32) NOT NULL,
                source_id TEXT NOT NULL,
                country VARCHAR(2),
                firedate DATE NOT NULL,
                firedate_ts TIMESTAMPTZ,
                province TEXT,
                -- Código INE de provincia (dos dígitos). EFFIS publica islas y
                -- nombres bilingües; se normaliza con services/provinces.py.
                province_code CHAR(2),
                municipality TEXT,
                -- Código INE municipal. EFFIS no lo publica; se rellena después.
                municipality_code TEXT,
                area_ha DOUBLE PRECISION,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                bbox DOUBLE PRECISION[],
                last_update TIMESTAMPTZ,
                geom geometry(Point, 4326),
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT confirmed_fires_natural_key PRIMARY KEY (source, source_id)
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS confirmed_fires_firedate_idx
                ON confirmed_fires (firedate);
            CREATE INDEX IF NOT EXISTS confirmed_fires_province_idx
                ON confirmed_fires (province);
            CREATE INDEX IF NOT EXISTS confirmed_fires_municipality_idx
                ON confirmed_fires (municipality);
            CREATE INDEX IF NOT EXISTS confirmed_fires_geom_idx
                ON confirmed_fires USING GIST (geom);
            """
        )
        # La tabla ya existía antes de introducir el nivel provincial. El
        # ALTER es idempotente y evita exigir una migración manual.
        cur.execute(
            """
            ALTER TABLE confirmed_fires
                ADD COLUMN IF NOT EXISTS province_code CHAR(2);
            CREATE INDEX IF NOT EXISTS confirmed_fires_province_code_idx
                ON confirmed_fires (province_code);
            """
        )
    conn.commit()


def upsert_confirmed_fires(conn, records: Iterable[Dict]) -> int:
    """Inserta o actualiza etiquetas y devuelve cuántas se escribieron.

    Un fallo de una fila aborta la transacción completa en lugar de dejar un
    catálogo a medias: el comando es idempotente y se puede repetir entero.
    """
    written = 0
    with conn.cursor() as cur:
        for record in records:
            cur.execute(
                """
                INSERT INTO confirmed_fires
                    (source, source_id, country, firedate, firedate_ts,
                     province, province_code, municipality, municipality_code,
                     area_ha, latitude, longitude, bbox, last_update, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                ON CONFLICT (source, source_id) DO UPDATE SET
                    country           = EXCLUDED.country,
                    firedate          = EXCLUDED.firedate,
                    firedate_ts       = EXCLUDED.firedate_ts,
                    province          = EXCLUDED.province,
                    province_code     = COALESCE(
                        EXCLUDED.province_code,
                        confirmed_fires.province_code
                    ),
                    municipality      = EXCLUDED.municipality,
                    -- El mapeo INE es un trabajo posterior: no lo borra una
                    -- reimportación de EFFIS que llega sin código.
                    municipality_code = COALESCE(
                        EXCLUDED.municipality_code,
                        confirmed_fires.municipality_code
                    ),
                    area_ha           = EXCLUDED.area_ha,
                    latitude          = EXCLUDED.latitude,
                    longitude         = EXCLUDED.longitude,
                    bbox              = EXCLUDED.bbox,
                    last_update       = EXCLUDED.last_update,
                    geom              = EXCLUDED.geom,
                    ingested_at       = CURRENT_TIMESTAMP
                """,
                (
                    record["source"],
                    record["source_id"],
                    record.get("country"),
                    record["firedate"],
                    record.get("firedate_ts"),
                    record.get("province"),
                    normalize_province(record.get("province")),
                    record.get("municipality"),
                    record.get("municipality_code"),
                    record.get("area_ha"),
                    record["latitude"],
                    record["longitude"],
                    record.get("bbox"),
                    record.get("last_update"),
                    # ST_MakePoint recibe (x, y) = (longitud, latitud).
                    record["longitude"],
                    record["latitude"],
                ),
            )
            written += 1
    conn.commit()
    return written


def count_confirmed_fires(conn) -> int:
    """Número total de etiquetas almacenadas (para el resumen del comando)."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM confirmed_fires")
        return int(cur.fetchone()[0])


def map_province_codes(conn, dry_run: bool = False) -> tuple[int, list[str]]:
    """Rellena ``province_code`` a partir del nombre de provincia de EFFIS.

    Devuelve ``(filas_afectadas, nombres_no_reconocidos)``. En ``dry_run``
    cuenta las filas que cambiarían sin escribir nada. La actualización es
    idempotente: solo toca filas cuyo código actual no coincide con el mapeo,
    así que repetir el comando no reescribe las 12.000 etiquetas cada vez.

    Un nombre no reconocido se devuelve en la lista en lugar de asignarse a una
    provincia aproximada. Una etiqueta en la provincia equivocada es peor que
    una etiqueta sin provincia: la primera contamina el panel sin avisar.
    """
    affected = 0
    unmatched: list[str] = []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT province
            FROM confirmed_fires
            WHERE province IS NOT NULL AND province <> ''
            ORDER BY province
            """
        )
        names = [row[0] for row in cur.fetchall()]

        for name in names:
            code = normalize_province(name)
            if code is None:
                unmatched.append(name)
                continue

            if dry_run:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM confirmed_fires
                    WHERE province = %s
                      AND province_code IS DISTINCT FROM %s
                    """,
                    (name, code),
                )
                affected += int(cur.fetchone()[0])
            else:
                cur.execute(
                    """
                    UPDATE confirmed_fires
                    SET province_code = %s
                    WHERE province = %s
                      AND province_code IS DISTINCT FROM %s
                    """,
                    (code, name, code),
                )
                affected += max(cur.rowcount, 0)

    if not dry_run:
        conn.commit()
    return affected, unmatched
