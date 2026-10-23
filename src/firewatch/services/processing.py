"""Consumidor / procesador de Kafka para datos de FIRMS.

Consume registros crudos de FIRMS desde el topic de ingesta, los normaliza,
los enriquece con modelado de riesgo (humedad de combustible, meteorología, factor de propagación),
y los persiste en PostgreSQL/PostGIS usando un upsert idempotente en la clave
natural ``(acq_date, latitude, longitude)`` — reprocesar el mismo mensaje
nunca duplica filas.
"""

from __future__ import annotations

import contextlib
import json
import logging
from typing import Dict, Optional

from kafka import KafkaConsumer

from firewatch.config import settings
from firewatch.services.risk import score_event

logger = logging.getLogger(__name__)


def create_consumer(
    bootstrap_servers: str | None = None,
    topic: str | None = None,
    group_id: str = "firewatch-processing",
) -> KafkaConsumer:
    """Crea y devuelve un consumidor de Kafka configurado."""
    return KafkaConsumer(
        topic or settings.KAFKA_INGEST_TOPIC,
        bootstrap_servers=bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        session_timeout_ms=30000,        # 30s (por defecto 10s)
        heartbeat_interval_ms=3000,      # 3s
        max_poll_interval_ms=300000,     # 5min para procesamiento lento
        max_poll_records=500            # lote controlado
    )


def _ensure_table(conn) -> None:
    """Crea la extensión PostGIS, la tabla fire_events y las columnas de riesgo si no existen."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fire_events (
                id SERIAL PRIMARY KEY,
                acq_date DATE NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                brightness DOUBLE PRECISION,
                scan DOUBLE PRECISION,
                track DOUBLE PRECISION,
                acquisition_date DATE,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                geom geometry(Point, 4326),
                risk_level VARCHAR(20) DEFAULT 'low',
                risk_score DOUBLE PRECISION DEFAULT 0.0,
                risk_factors JSONB DEFAULT '{}'::jsonb,
                locality JSONB,
                CONSTRAINT fire_events_natural_key UNIQUE (acq_date, latitude, longitude)
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE fire_events ADD COLUMN IF NOT EXISTS risk_level VARCHAR(20) DEFAULT 'low';
            ALTER TABLE fire_events ADD COLUMN IF NOT EXISTS risk_score DOUBLE PRECISION DEFAULT 0.0;
            ALTER TABLE fire_events ADD COLUMN IF NOT EXISTS risk_factors JSONB DEFAULT '{}'::jsonb;
            -- Toponimia geocodificada. JSONB en vez de columnas porque el vocabulario
            -- de Nominatim varía por país (un municipio español es `city` o `town`),
            -- y porque NULL aquí es significativo: indica que está pendiente de `geocode-backfill`.
            ALTER TABLE fire_events ADD COLUMN IF NOT EXISTS locality JSONB;
            -- FIRMS reporta ángulos scan/track fraccionarios; INTEGER los truncaba a 0.
            ALTER TABLE fire_events
                ALTER COLUMN scan TYPE DOUBLE PRECISION USING scan::DOUBLE PRECISION;
            ALTER TABLE fire_events
                ALTER COLUMN track TYPE DOUBLE PRECISION USING track::DOUBLE PRECISION;
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS fire_events_geom_idx
                ON fire_events USING GIST (geom);
            CREATE INDEX IF NOT EXISTS fire_events_risk_level_idx
                ON fire_events (risk_level);
            """
        )
    conn.commit()


class InvalidRecord(ValueError):
    """Lanzada cuando un registro no puede persistirse (campos clave ausentes o inválidos)."""


def _first_present(record: Dict, *keys: str):
    """
    Devuelve la primera clave presente y no nula.

    Deliberadamente no es ``record.get(k) or record.get(k2)``: eso trataría un
    ``0.0`` legítimo (ej. latitud en el ecuador) como ausente.
    """
    for key in keys:
        value = record.get(key)
        if value is not None and value != "":
            return value
    return None


def record_to_row(record: Dict) -> tuple:
    """
    Convierte un diccionario crudo de FIRMS en una tupla para la base de datos.

    Lanza
    -----
    InvalidRecord
        Cuando ``acq_date`` o las coordenadas faltan o están fuera de rango, o caen en Argelia.
    """
    acq_date = _first_present(record, "acq_date", "date")
    if not acq_date or not isinstance(acq_date, str) or len(acq_date) != 10:
        raise InvalidRecord(f"missing or malformed acq_date: {acq_date!r}")

    raw_lat = _first_present(record, "latitude", "lat")
    raw_lon = _first_present(record, "longitude", "lon")
    if raw_lat is None or raw_lon is None:
        raise InvalidRecord("record has no latitude/longitude")

    try:
        latitude = float(raw_lat)
        longitude = float(raw_lon)
    except (TypeError, ValueError) as exc:
        raise InvalidRecord(f"non-numeric coordinates: {raw_lat!r}, {raw_lon!r}") from exc

    if not (-90.0 <= latitude <= 90.0):
        raise InvalidRecord(f"latitude out of range: {latitude}")
    if not (-180.0 <= longitude <= 180.0):
        raise InvalidRecord(f"longitude out of range: {longitude}")
    if 35.0 <= latitude < 37.0 and longitude > -1.8:
        raise InvalidRecord(
            f"coordinate ({latitude}, {longitude}) is in North Africa/Algeria (outside project scope)"
        )

    raw_brightness = _first_present(record, "brightness", "bright_ti4")
    try:
        brightness = float(raw_brightness) if raw_brightness is not None else None
    except (TypeError, ValueError):
        brightness = None

    def _optional_float(key):
        raw = _first_present(record, key)
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    scan = _optional_float("scan")
    track = _optional_float("track")
    return (acq_date, latitude, longitude, brightness, scan, track, acq_date)


def upsert_record(
    conn,
    record: Dict,
    weather: Optional[Dict] = None,
    location: Optional[Dict] = None,
) -> Dict:
    """
    Inserta de forma idempotente un registro de FIRMS en fire_events con evaluación de riesgo.

    Usa INSERT ... ON CONFLICT sobre la clave natural y recalcula el punto PostGIS a partir de lat/lon.

    ``location`` es la toponimia geocodificada. Nunca se sobrescribe con NULL: FIRMS reemite
    días recientes, por lo que una ingesta posterior del mismo punto puede llegar mientras
    la geocodificación está desactivada o Nominatim caído.
    """
    row = record_to_row(record)
    risk = score_event(record, weather=weather)
    risk_level = risk["risk_level"]
    risk_score = risk["score"]
    risk_factors_json = json.dumps(risk["factors"])
    locality_json = json.dumps(location, ensure_ascii=False) if location else None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO fire_events
                (acq_date, latitude, longitude, brightness, scan, track,
                 acquisition_date, risk_level, risk_score, risk_factors, geom, locality)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s::jsonb)
            ON CONFLICT (acq_date, latitude, longitude) DO UPDATE SET
                brightness       = EXCLUDED.brightness,
                scan             = EXCLUDED.scan,
                track            = EXCLUDED.track,
                acquisition_date = EXCLUDED.acquisition_date,
                risk_level       = EXCLUDED.risk_level,
                risk_score       = EXCLUDED.risk_score,
                risk_factors     = EXCLUDED.risk_factors,
                geom             = EXCLUDED.geom,
                locality         = COALESCE(EXCLUDED.locality, fire_events.locality),
                updated_at       = CURRENT_TIMESTAMP
            """,
            (*row, risk_level, risk_score, risk_factors_json, row[2], row[1], locality_json),
        )
    conn.commit()
    logger.info("Upserted FIRMS record (%s, %s) risk=%s (%.2f)", row[1], row[2], risk_level, risk_score)
    return risk


def consume_and_process(
    consumer=None,
    conn=None,
    batch_limit: Optional[int] = None,
    max_empty_polls: Optional[int] = None,
    weather_client=None,
    geocode_client=None,
) -> int:
    """
    Consume mensajes de Kafka, evalúa el riesgo físico y los persiste en PostgreSQL/PostGIS.

    Se ejecuta hasta procesar ``batch_limit`` mensajes (o indefinidamente si es None).
    Si ``max_empty_polls`` está definido, se detiene tras ese número de encuestas vacías consecutivas.

    Los registros malformados se registran y omiten para no bloquear la partición en caso de error.

    Devuelve el número de mensajes procesados correctamente.
    """
    close_conn = conn is None
    if conn is None:
        import psycopg2

        conn = psycopg2.connect(settings.POSTGRES_DSN)
    _ensure_table(conn)

    if consumer is None:
        consumer = create_consumer()

    if weather_client is None and getattr(settings, "ENABLE_WEATHER_ENRICHMENT", False):
        try:
            from firewatch.clients.weather import WeatherClient

            weather_client = WeatherClient()
        except Exception as exc:
            logger.warning("Could not initialize WeatherClient: %s", exc)

    if geocode_client is None and getattr(settings, "ENABLE_GEOCODING", False):
        try:
            from firewatch.clients.geocode import GeoCodeClient

            geocode_client = GeoCodeClient()
        except Exception as exc:
            logger.warning("Could not initialize GeoCodeClient: %s", exc)

    processed = 0
    skipped = 0
    empty_polls = 0
    try:
        while batch_limit is None or processed < batch_limit:
            batches: Dict = consumer.poll(timeout_ms=1000)
            if not batches:
                empty_polls += 1
                if max_empty_polls is not None and empty_polls >= max_empty_polls:
                    break
                continue
            empty_polls = 0
            for messages in batches.values():
                for message in messages:
                    err_func = getattr(message, "error", None)
                    if callable(err_func) and err_func():
                        logger.error("Kafka consumer error: %s", err_func())
                        continue
                    val = getattr(message, "value", message)
                    record = val() if callable(val) else val
                    if isinstance(record, (bytes, str)):
                        record = json.loads(record)

                    # Enriquecimiento meteorológico y toponímico opcional
                    weather = None
                    location = None
                    try:
                        lat = float(record.get("latitude") or record.get("lat") or 0)
                        lon = float(record.get("longitude") or record.get("lon") or 0)
                        if weather_client is not None:
                            weather = weather_client.get_weather(
                                lat,
                                lon,
                                date=record.get("acq_date"),
                                acq_time=record.get("acq_time"),
                            )
                        if geocode_client is not None:
                            location = geocode_client.get_location(lat, lon)
                    except Exception as exc:
                        logger.debug("Enrichment lookup failed: %s", exc)

                    # Un registro malformado no debe detener el consumidor
                    try:
                        upsert_record(conn, record, weather=weather, location=location)
                    except InvalidRecord as exc:
                        skipped += 1
                        logger.error("Discarding invalid FIRMS record: %s", exc)
                        continue
                    except Exception as exc:
                        skipped += 1
                        logger.exception(
                            "Failed to persist record (%s): %s", record.get("acq_date"), exc
                        )
                        with contextlib.suppress(Exception):
                            conn.rollback()
                        continue

                    processed += 1

                    if batch_limit is not None and processed >= batch_limit:
                        break
                if batch_limit is not None and processed >= batch_limit:
                    break
    except KeyboardInterrupt:
        logger.info("Consumer interrupted, shutting down")
    finally:
        if close_conn:
            conn.close()
            logger.info("Database connection closed")

    if skipped:
        logger.warning(
            "Processed %d messages, discarded %d invalid record(s)", processed, skipped
        )
    return processed
