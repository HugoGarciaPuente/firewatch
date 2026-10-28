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
import time
from typing import Dict, Optional

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

from firewatch.config import country_in_scope, settings, tracked_country_codes
from firewatch.services.risk import score_event

logger = logging.getLogger(__name__)

DB_RECONNECT_DELAYS = (1.0, 2.0, 5.0)


class DatabaseUnavailable(RuntimeError):
    """The processor could not recover a PostgreSQL connection."""


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
        # El offset solo se confirma después de que la fila esté en Postgres.
        # Con auto-commit, Kafka avanza la posición al hacer poll y un crash
        # puede perder mensajes que nunca llegaron a la base de datos.
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        session_timeout_ms=30000,        # 30s (por defecto 10s)
        heartbeat_interval_ms=3000,      # 3s
        max_poll_interval_ms=300000,     # 5min
        # La geocodificación inversa cuesta ~1.1 s por registro. Un lote de 500
        # tardaría ~9 min y superaría max_poll_interval_ms: el grupo expulsa al
        # consumidor a mitad de lote y lo reasigna. 25 lo mantiene por debajo.
        max_poll_records=25
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


def _connect_db():
    """Open a PostgreSQL connection without importing psycopg2 at module level."""
    import psycopg2

    return psycopg2.connect(settings.POSTGRES_DSN)


def _is_connection_error(exc: BaseException) -> bool:
    """True for failures that a fresh PostgreSQL connection can recover from."""
    try:
        import psycopg2
    except ImportError:
        return False
    return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))


def _reconnect_database(conn):
    """Close a broken connection and retry with bounded exponential backoff."""
    with contextlib.suppress(Exception):
        conn.close()

    last_error = None
    for delay in DB_RECONNECT_DELAYS:
        time.sleep(delay)
        try:
            new_conn = _connect_db()
            _ensure_table(new_conn)
            logger.warning("Reconnected to PostgreSQL; resuming processing")
            return new_conn
        except Exception as exc:
            last_error = exc
            logger.error("PostgreSQL reconnect failed: %s", exc)

    raise DatabaseUnavailable("could not reconnect to PostgreSQL") from last_error


def _upsert_with_reconnect(conn, record: Dict, weather, location, can_reconnect: bool):
    """Persist one record, reconnecting and retrying the same message on connection loss.

    The Kafka offset is not committed until this function returns, so a reconnect
    either persists the current message or stops the consumer; it never skips it.
    """
    attempt = 0
    while True:
        try:
            return conn, upsert_record(conn, record, weather=weather, location=location)
        except Exception as exc:
            if not can_reconnect or not _is_connection_error(exc):
                raise
            if attempt >= len(DB_RECONNECT_DELAYS):
                raise DatabaseUnavailable("could not persist record after reconnect") from exc

            logger.error("PostgreSQL connection lost while persisting record: %s", exc)
            with contextlib.suppress(Exception):
                conn.rollback()
            conn = _reconnect_database(conn)
            attempt += 1


def _commit_message(consumer, message) -> None:
    """Commit exactly the offset of ``message`` once it has been handled.

    Committing after each message (instead of ``consumer.commit()``) avoids
    marking later messages from the same poll as consumed before they are
    processed. Stub consumers used by tests may not expose ``commit``.
    """
    commit = getattr(consumer, "commit", None)
    if commit is None:
        return

    topic = getattr(message, "topic", None)
    partition = getattr(message, "partition", None)
    offset = getattr(message, "offset", None)
    if topic is None or partition is None or offset is None:
        # Compatibility with simple test doubles that only model ``value``.
        commit()
        return

    commit(
        {
            TopicPartition(topic, partition): OffsetAndMetadata(offset + 1, ""),
        }
    )


class InvalidRecord(ValueError):
    """Lanzada cuando un registro no puede persistirse (campos clave ausentes o inválidos)."""


class OutOfScope(InvalidRecord):
    """El punto geocodificado cae en un país fuera de ``FIREWATCH_COUNTRY_CODES``.

    Hereda de ``InvalidRecord`` porque el consumidor ya sabe descartar esos
    mensajes sin detener la partición; la excepción propia solo permite
    contarlos y registrarlos aparte.
    """


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

    Lanza ``OutOfScope`` si la localidad resuelta pertenece a un país fuera del ámbito
    (``FIREWATCH_COUNTRY_CODES``): el punto nunca debe llegar a la tabla, porque las cajas
    rectangulares también capturan Portugal y el sur de Francia.
    """
    row = record_to_row(record)
    if not country_in_scope(location):
        raise OutOfScope(
            f"point ({row[1]}, {row[2]}) is in "
            f"{location.get('country') or location.get('country_code')}, "
            f"outside {sorted(tracked_country_codes())}"
        )
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


def purge_out_of_scope(conn, dry_run: bool = False) -> int:
    """
    Elimina (o cuenta, con ``dry_run``) los focos ya guardados cuyo país
    geocodificado no pertenece a ``FIREWATCH_COUNTRY_CODES``.

    Existe porque el filtro del procesador solo protege las filas nuevas: el
    histórico se ingestó cuando el ámbito se definía únicamente por bbox, y esas
    cajas rectangulares incluyen Portugal y el sur de Francia. Es idempotente y
    deja intactas las filas sin país resuelto (NULL o ``{}``), que todavía pueden
    geocodificarse.
    """
    codes = sorted(tracked_country_codes())
    if not codes:
        # `<> ALL('{}')` es cierto para toda fila con país: un ámbito vacío
        # borraría el dataset entero en silencio.
        raise ValueError("FIREWATCH_COUNTRY_CODES is empty; refusing to purge everything")

    where = """
        COALESCE(locality->>'country_code', '') <> ''
        AND LOWER(locality->>'country_code') <> ALL(%s)
    """
    with conn.cursor() as cur:
        if dry_run:
            cur.execute(f"SELECT COUNT(*) FROM fire_events WHERE {where}", (codes,))
            return int(cur.fetchone()[0])
        cur.execute(f"DELETE FROM fire_events WHERE {where} RETURNING id", (codes,))
        removed = len(cur.fetchall())
    conn.commit()
    return removed


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

    Los registros inválidos o fuera de ámbito se registran, se confirman y se
    omiten para no bloquear la partición. Un fallo de persistencia no clasificado
    o una base de datos que no se puede recuperar detiene el consumidor para que
    Kafka reentregue el mensaje en lugar de perderlo en silencio.

    El offset de Kafka se confirma después del commit en PostgreSQL, mensaje a
    mensaje. Devuelve el número de mensajes persistidos correctamente.
    """
    close_conn = conn is None
    if conn is None:
        conn = _connect_db()
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
    out_of_scope = 0
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
                        _commit_message(consumer, message)
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

                    # Un registro malformado no debe detener el consumidor. Un
                    # fallo de conexión sí: se reintenta con una conexión nueva
                    # antes de confirmar el offset de este mensaje.
                    try:
                        conn, _ = _upsert_with_reconnect(
                            conn,
                            record,
                            weather=weather,
                            location=location,
                            can_reconnect=close_conn,
                        )
                    except OutOfScope as exc:
                        out_of_scope += 1
                        logger.info("Discarding out-of-scope FIRMS record: %s", exc)
                        _commit_message(consumer, message)
                        continue
                    except InvalidRecord as exc:
                        skipped += 1
                        logger.error("Discarding invalid FIRMS record: %s", exc)
                        _commit_message(consumer, message)
                        continue
                    except DatabaseUnavailable:
                        # No se confirma el offset: el mensaje se reintentará
                        # cuando el contenedor arranque de nuevo.
                        raise
                    except Exception as exc:
                        logger.exception(
                            "Failed to persist record (%s): %s", record.get("acq_date"), exc
                        )
                        with contextlib.suppress(Exception):
                            conn.rollback()
                        # Un error no clasificado no debe convertirse en una
                        # pérdida silenciosa: se detiene el consumidor y Kafka
                        # reentrega el mensaje.
                        raise

                    _commit_message(consumer, message)
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

    if skipped or out_of_scope:
        logger.warning(
            "Processed %d messages, discarded %d invalid and %d out-of-scope record(s)",
            processed,
            skipped,
            out_of_scope,
        )
    return processed
