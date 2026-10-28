"""Ejecutor por línea de comandos (CLI) del pipeline de FireWatch.

Ejecuta las operaciones puntuales y de larga duración del slice E2E sin
necesidad de levantar la API HTTP:

    ingestion :  descargar FIRMS -> publicar en Kafka (one-shot)
    backfill  :  descargar FIRMS para N días consecutivos -> Kafka (histórico)
    process   :  consumir Kafka -> upsert en PostgreSQL/PostGIS (continuo)
    daily     :  bucle que mantiene el dataset actualizado (re-consulta ventana rodante)
    labels    :  descargar incendios confirmados (EFFIS) -> PostgreSQL
    map-provinces : normalizar province de EFFIS -> código INE
    panel     : construir el panel provincia-día del modelo a 24 h
    province-weather : descargar la serie diaria de Open-Meteo por provincia
    forecast  : predecir el siguiente día UTC para las 52 provincias (o bucle)
    purge-out-of-scope : borrar focos guardados fuera de FIREWATCH_COUNTRY_CODES

Uso:
    python -m firewatch.runner ingest  [--bbox S,W,N,E] [--date AAAA-MM-DD] [--dry-run]
    python -m firewatch.runner backfill [--bbox S,W,N,E] [--start-date D] [--end-date D]
    python -m firewatch.runner daily    [--bbox S,W,N,E] [--days N] [--interval-minutes N]
    python -m firewatch.runner process [--limit N] [--max-empty-polls N]
    python -m firewatch.runner labels  [--source effis] [--start-date D] [--end-date D]
    python -m firewatch.runner map-provinces [--dry-run]
    python -m firewatch.runner panel [--start-date D] [--end-date D] [--min-area-ha N]
    python -m firewatch.runner province-weather [--start-date D] [--end-date D]
    python -m firewatch.runner forecast [--date AAAA-MM-DD] [--model RUTA] [--loop] [--dry-run]
    python -m firewatch.runner purge-out-of-scope [--dry-run]

Flujo E2E típico (en dos terminales):
    terminal 1:  python -m firewatch.runner process
    terminal 2:  python -m firewatch.runner ingest --bbox 36,-6,44,2
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
import time

from firewatch.clients.firms import FirmsClient
from firewatch.config import resolve_regions, settings, tracked_country_codes
from firewatch.services.ingestion import create_producer, run_ingestion
from firewatch.services.processing import (
    _ensure_table,
    consume_and_process,
    create_consumer,
    purge_out_of_scope,
)

logger = logging.getLogger("firewatch.runner")

# Política de reintentos para el refresco de FIRMS. Perder una ejecución congela
# silenciosamente el conjunto de datos, por lo que los fallos transitorios se
# reintentan en lugar de omitirse.
REFRESH_RETRIES = 3
RETRY_BASE_DELAY = 2
RETRY_MAX_DELAY = 30


def setup_logging(level: int = logging.INFO, log_file: bool = True) -> None:
    """
    Configura el registro por consola y, por defecto, un manejador de archivo rotativo.

    La salida estándar de una ejecución programada es descartada por el sistema operativo,
    por lo que sin un archivo un fallo no deja información para diagnosticar. ``logs/``
    está ignorado en git.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler()]

    if log_file:
        try:
            log_dir = Path(__file__).resolve().parents[2] / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            handlers.append(
                RotatingFileHandler(
                    log_dir / "firewatch.log",
                    maxBytes=2_000_000,
                    backupCount=5,
                    encoding="utf-8",
                )
            )
        except OSError as exc:  # sistema de archivos de solo lectura, permisos...
            print(f"Warning: file logging disabled ({exc})", file=sys.stderr)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )


def cmd_ingest(args: argparse.Namespace) -> int:
    """Descarga registros de FIRMS para el ámbito resuelto y los publica en Kafka."""
    regions = resolve_regions(args.bbox, args.regions)
    client = FirmsClient()

    all_records = []
    for label, bbox in regions:
        records = client.fetch_by_bbox(bbox, date=args.date or None)
        logger.info(
            "  %-11s %d registros (bbox=%s, date=%s)",
            label, len(records), bbox, args.date or "today",
        )
        all_records.extend(records)

    logger.info("Fetched %d FIRMS records across %d region(s)", len(all_records), len(regions))

    if args.dry_run:
        logger.info("Dry run: no Kafka publish. First record(s):")
        for record in all_records[:3]:
            logger.info("  %s", record)
        return 0

    producer = create_producer()
    try:
        published = run_ingestion(all_records, producer, topic=settings.KAFKA_INGEST_TOPIC)
    finally:
        producer.close()
    logger.info("Published %d records to topic '%s'", published, settings.KAFKA_INGEST_TOPIC)
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    """Descarga datos de FIRMS para varios días consecutivos y los publica en Kafka.

    La API de área de FIRMS solo expone un único día por petición, por lo que un
    conjunto de datos histórico se construye iterando fechas. Los días sin fuegos
    activos devuelven una respuesta vacía, lo cual es normal y se registra como tal.
    """
    regions = resolve_regions(args.bbox, args.regions)
    end = date.fromisoformat(args.end_date) if args.end_date else datetime.now(timezone.utc).date()
    start = date.fromisoformat(args.start_date) if args.start_date else end

    if start > end:
        logger.error("start-date (%s) is after end-date (%s)", start, end)
        return 1

    client = FirmsClient(source=args.source)
    producer = None
    total = 0
    failed = 0
    per_day = []

    try:
        for label, bbox in regions:
            for offset in range((end - start).days + 1):
                day = start + timedelta(days=offset)
                day_str = day.isoformat()
                try:
                    records = client.fetch_by_bbox(bbox, date=day_str)
                except Exception as exc:
                    failed += 1
                    logger.warning("FIRMS fetch failed for %s/%s: %s", label, day_str, exc)
                    per_day.append((label, day_str, 0, "error"))
                    continue

                count = len(records)
                per_day.append((label, day_str, count, "ok"))

                if count and not args.dry_run:
                    if producer is None:
                        producer = create_producer()
                    run_ingestion(records, producer, topic=settings.KAFKA_INGEST_TOPIC)
                    total += count
                elif count:
                    total += count
    finally:
        if producer is not None:
            producer.close()

    logger.info("Backfill summary:")
    for label, day_str, count, status in per_day:
        logger.info("  %-11s %s -> %d registros (%s)", label, day_str, count, status)
    logger.info("Backfill total: %d registros%s", total, " (dry-run, nada publicado)" if args.dry_run else "")
    if failed:
        logger.error(
            "Backfill incomplete: %d region/day fetch(es) failed. Re-run the "
            "missing dates before treating the historical load as complete.",
            failed,
        )
        return 1
    return 0


def refresh_once(
    client: FirmsClient,
    regions,
    days: int,
    dry_run: bool = False,
    retries: int = REFRESH_RETRIES,
) -> int:
    """
    Descarga la ventana rodante de ``days`` días una vez y la publica en Kafka.

    Compartido por ``daily`` (bucle) y ``daily --once`` (ejecución única). FIRMS
    revisa los días recientes conforme el producto satelital madura, por lo que
    re-consultar la ventana es lo que mejora la calidad del dato; el upsert posterior
    es idempotente, por lo que republicar un día solo refresca sus valores de riesgo.

    Los fallos por región se reintentan con retroceso exponencial. Un refresco perdido
    por reinicio del contenedor resultaría invisible una vez avanza el planificador,
    por lo que fallar prematuramente aquí congelaría silenciosamente el conjunto de datos.

    Devuelve ``0`` si todos los días/regiones se descargaron y publicaron, o
    ``1`` si alguno falló tras los reintentos.
    """
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(0, days - 1))
    logger.info("Window %s .. %s", start, end)

    producer = None
    total = 0
    failed = 0
    try:
        for label, bbox in regions:
            for offset in range((end - start).days + 1):
                day = start + timedelta(days=offset)
                day_str = day.isoformat()

                records = _fetch_with_retry(client, label, bbox, day_str, retries)
                if records is None:
                    failed += 1
                    continue
                if not records:
                    logger.info("  %-11s %s -> 0 registros", label, day_str)
                    continue
                if dry_run:
                    total += len(records)
                    logger.info("  %-11s %s -> %d (dry-run)", label, day_str, len(records))
                    continue
                if producer is None:
                    producer = create_producer()
                run_ingestion(records, producer, topic=settings.KAFKA_INGEST_TOPIC)
                total += len(records)
                logger.info("  %-11s %s -> %d publicados", label, day_str, len(records))
    finally:
        if producer is not None:
            producer.close()

    logger.info("Refresh finished: %d registros%s", total, " (dry-run)" if dry_run else "")
    if failed:
        # Salida distinta de cero para que el planificador registre un fallo real
        # en lugar de reportar éxito ante un refresco parcial.
        logger.error(
            "Refresh incomplete: %d region/day fetch(es) failed after %d attempts",
            failed,
            retries + 1,
        )
        return 1
    return 0


def _fetch_with_retry(client: FirmsClient, label: str, bbox: str, day: str, retries: int):
    """
    Descarga una región/día con retroceso exponencial (exponential backoff).

    Devuelve la lista de registros, o None si todos los intentos fallaron. Una llamada
    HTTP intermitente de otro modo perdería ese día del refresco por completo.
    """
    for attempt in range(retries + 1):
        try:
            return client.fetch_by_bbox(bbox, date=day)
        except Exception as exc:
            if attempt >= retries:
                logger.error(
                    "FIRMS fetch failed for %s/%s after %d attempts: %s",
                    label, day, attempt + 1, exc,
                )
                return None
            delay = min(RETRY_BASE_DELAY * (2 ** attempt), RETRY_MAX_DELAY)
            logger.warning(
                "FIRMS fetch failed for %s/%s (attempt %d/%d): %s — retrying in %ds",
                label, day, attempt + 1, retries + 1, exc, delay,
            )
            time.sleep(delay)
    return None


def cmd_daily(args: argparse.Namespace) -> int:
    """
    Mantiene el conjunto de datos actualizado automáticamente: cada día, descarga
    los últimos ``--days`` días de FIRMS y publica cualquier novedad en Kafka.

    Usa ``--once`` para una única pasada, que es lo que un planificador de SO debería llamar.
    """
    regions = resolve_regions(args.bbox, args.regions)
    interval = max(60, args.interval_minutes * 60)
    client = FirmsClient()

    if args.once:
        logger.info(
            "Single refresh run (regions=%s, window=%d days).",
            ", ".join(label for label, _ in regions),
            args.days,
        )
        return refresh_once(client, regions, args.days, args.dry_run)

    logger.info(
        "Daily updater started (regions=%s, window=%d days, every %d min). Ctrl+C to stop.",
        ", ".join(label for label, _ in regions),
        args.days,
        args.interval_minutes,
    )

    try:
        while True:
            try:
                rc = refresh_once(client, regions, args.days, dry_run=args.dry_run)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                logger.exception("Daily run failed: %s", exc)
                return 1
            if rc != 0:
                logger.error(
                    "Daily refresh failed (exit code %d); stopping so the "
                    "scheduler can restart the worker instead of reporting "
                    "a partial refresh as success.",
                    rc,
                )
                return rc
            logger.info("Próxima ejecución en %d min.", args.interval_minutes)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Daily updater stopped")
    return 0


def cmd_process(args: argparse.Namespace) -> int:
    """Consume desde Kafka y realiza upsert en PostgreSQL/PostGIS (continuo)."""
    consumer = create_consumer(
        topic=args.topic or settings.KAFKA_INGEST_TOPIC,
        group_id="firewatch-processing",
    )

    try:
        processed = consume_and_process(
            consumer=consumer,
            batch_limit=args.limit,
            max_empty_polls=args.max_empty_polls,
        )
    finally:
        consumer.close()

    logger.info("Processed %d messages. Exiting.", processed)
    return 0


def cmd_geocode_backfill(args: argparse.Namespace) -> int:
    """
    Completa la localidad de los puntos de fuego que aún no tienen una.

    La geocodificación inversa está limitada a 1 req/s por la política de uso de Nominatim,
    por lo que es deliberadamente un comando independiente en vez de parte de `process`:
    completar todo el histórico toma minutos, y bloquear la ingesta durante tanto tiempo
    congelaría el pipeline.

    Por defecto solo se tocan las filas cuya ``locality`` es NULL, por lo que
    reejecutarlo es seguro y rápido. Usa ``--recheck`` para reintentar también
    los puntos ya marcados como ``{}`` (sin topónimo en una pasada anterior),
    por ejemplo tras una mejora en los datos de OSM.
    """
    # psycopg2 y GeoCodeClient se importan de forma perezosa (lazy) dentro de la función.
    # La imagen de adquisición (`runner daily`) no incluye ni el driver de base de datos
    # ni el geocodificador, por lo que una importación a nivel de módulo haría fallar
    # las pruebas de límites de capas e incluiría código no utilizado en cada imagen.
    import psycopg2

    from firewatch.clients.geocode import GeoCodeClient, GeocodeError

    conn = psycopg2.connect(settings.POSTGRES_DSN)
    try:
        # Use the same schema initializer as the consumer. A minimal table here
        # would make the processor fail later on its scan/track migrations.
        _ensure_table(conn)

        where = "locality IS NULL"
        if args.recheck:
            where = "locality IS NULL OR locality = '{}'::jsonb"

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, latitude, longitude FROM fire_events
                WHERE {where}
                ORDER BY acq_date, id
                LIMIT %s
                """,
                (args.limit,),
            )
            pending = cur.fetchall()

        if not pending:
            logger.info("Nothing to geocode: every fire point already has a locality.")
            return 0

        client = GeoCodeClient()
        # ceil(): el limitador espera ~1.1s por petición, por lo que mostramos el tiempo real estimado.
        eta_min = (len(pending) * max(client.min_interval, 0) / 60.0) or 0
        logger.info(
            "Geocoding %d fire points at %.1fs/request (~%.0f min). Ctrl+C to stop; "
            "already-resolved rows are kept.",
            len(pending),
            client.min_interval,
            eta_min,
        )

        filled = unresolved = errors = 0
        try:
            for index, (row_id, lat, lon) in enumerate(pending, 1):
                try:
                    location = client.get_location_checked(lat, lon)
                except GeocodeError as exc:
                    # Un fallo transitorio NO se marca como "sin localidad":
                    # la fila queda NULL para que una ejecución posterior la reintente.
                    errors += 1
                    logger.warning(
                        "Geocoding failed for id=%s (%.4f, %.4f): %s; leaving locality NULL",
                        row_id,
                        lat,
                        lon,
                        exc,
                    )
                    continue

                with conn.cursor() as cur:
                    if location:
                        cur.execute(
                            "UPDATE fire_events SET locality = %s::jsonb WHERE id = %s",
                            (json.dumps(location, ensure_ascii=False), row_id),
                        )
                        filled += 1
                    else:
                        # Marcar como intentado con un objeto vacío para que la próxima ejecución
                        # no sature la API para un punto sin localidad (mar abierto, zona protegida).
                        # --recheck permite reintentar estos puntos.
                        cur.execute(
                            "UPDATE fire_events SET locality = '{}'::jsonb WHERE id = %s",
                            (row_id,),
                        )
                        unresolved += 1
                conn.commit()

                if index % 25 == 0 or index == len(pending):
                    logger.info(
                        "  %d/%d (%.0f%%)  resueltos=%d  sin localidad=%d  errores=%d",
                        index,
                        len(pending),
                        100.0 * index / len(pending),
                        filled,
                        unresolved,
                        errors,
                    )
        except KeyboardInterrupt:
            logger.warning(
                "Interrupted at %d/%d. Resolved rows were committed; re-run to continue.",
                filled + unresolved + errors,
                len(pending),
            )
            return 130

        stats = client.cache_stats()
        logger.info(
            "Geocoding complete: %d resolved, %d without locality, %d failed. "
            "cache hits=%d misses=%d",
            filled,
            unresolved,
            errors,
            stats["hits"],
            stats["misses"],
        )
        # Los puntos sin topónimo son un resultado válido; los fallos de red no.
        return 1 if errors else 0
    finally:
        conn.close()


def cmd_purge_out_of_scope(args: argparse.Namespace) -> int:
    """
    Borra del histórico los focos cuyo país geocodificado queda fuera del ámbito.

    El procesador ya descarta los nuevos (``FIREWATCH_COUNTRY_CODES``), pero los
    ingestados antes del filtro siguen en `fire_events`. Las cajas rectangulares
    de la península alcanzan el Algarve y los Pirineos franceses, así que sin esta
    limpieza el dashboard seguiría mostrándolos. ``--dry-run`` cuenta sin borrar.
    """
    # psycopg2 se importa aquí, no en el módulo: la imagen de adquisición no
    # incluye el driver y solo necesita `runner daily`.
    import psycopg2

    conn = psycopg2.connect(settings.POSTGRES_DSN)
    try:
        removed = purge_out_of_scope(conn, dry_run=args.dry_run)
    finally:
        conn.close()

    if args.dry_run:
        logger.info(
            "Out of scope (%s): %d row(s) would be deleted. Re-run without --dry-run to apply.",
            sorted(tracked_country_codes()),
            removed,
        )
    else:
        logger.info(
            "Out of scope (%s): %d row(s) deleted.",
            sorted(tracked_country_codes()),
            removed,
        )
    return 0


def cmd_labels(args: argparse.Namespace) -> int:
    """
    Descarga incendios confirmados de EFFIS y los guarda en ``confirmed_fires``.

    FIRMS da anomalías térmicas, no incendios confirmados; esta tabla es la
    fuente de las etiquetas del modelo de predicción a 24 h. La API de EFFIS no
    filtra por fecha, así que se pagina el catálogo y se filtra localmente.

    psycopg2 se importa de forma perezosa, igual que en ``geocode-backfill``:
    la imagen de adquisición solo ejecuta ``runner daily`` y no lleva driver de
    base de datos. La conexión se abre *antes* de descargar para que un DSN mal
    configurado falle en segundos, no después de quince minutos de catálogo.
    """
    from firewatch.clients.effis import EffisClient
    from firewatch.services.labels import (
        count_confirmed_fires,
        ensure_confirmed_fires_table,
        upsert_confirmed_fires,
    )

    conn = None
    try:
        if not args.dry_run:
            import psycopg2

            conn = psycopg2.connect(settings.POSTGRES_DSN)
            ensure_confirmed_fires_table(conn)

        client = EffisClient(page_size=args.page_size)
        try:
            records = client.fetch_burnt_areas(
                start_date=args.start_date,
                end_date=args.end_date,
                min_area_ha=args.min_area_ha,
                limit=args.limit,
                country=args.country,
            )
        finally:
            client.close()

        logger.info(
            "EFFIS (%s): %d confirmed fire(s) between %s and %s (min area %s ha)",
            args.country,
            len(records),
            args.start_date or "any",
            args.end_date or "any",
            args.min_area_ha,
        )

        if args.dry_run:
            logger.info("Dry run: nothing written. First record(s):")
            for record in records[:3]:
                logger.info("  %s", record)
            return 0

        if not records:
            logger.info("No confirmed fires matched the filters; nothing to write.")
            return 0

        stored = upsert_confirmed_fires(conn, records)
        total = count_confirmed_fires(conn)
        logger.info(
            "Upserted %d confirmed fire(s); confirmed_fires now has %d row(s)",
            stored,
            total,
        )
        return 0
    finally:
        if conn is not None:
            conn.close()


def cmd_map_provinces(args: argparse.Namespace) -> int:
    """Normaliza el campo ``province`` de EFFIS al código INE de dos dígitos.

    EFFIS mezcla islas y nombres bilingües con las provincias; sin este paso el
    panel tendría 57 unidades en lugar de 52. La operación es idempotente y solo
    actualiza las filas cuyo código no coincide, así que se puede repetir tras
    cada importación.
    """
    import psycopg2

    from firewatch.services.labels import (
        ensure_confirmed_fires_table,
        map_province_codes,
    )

    conn = psycopg2.connect(settings.POSTGRES_DSN)
    try:
        ensure_confirmed_fires_table(conn)
        affected, unmatched = map_province_codes(conn, dry_run=args.dry_run)
    finally:
        conn.close()

    if args.dry_run:
        logger.info(
            "Dry run: %d row(s) would receive a province code. Re-run without "
            "--dry-run to apply.",
            affected,
        )
    else:
        logger.info("%d row(s) updated with a province code.", affected)

    if unmatched:
        logger.error(
            "Province name(s) without an INE mapping: %s. Add them to "
            "services/provinces.py before building the panel.",
            ", ".join(repr(name) for name in unmatched),
        )
        return 1
    return 0


def cmd_panel(args: argparse.Namespace) -> int:
    """Construye el panel provincia-día (etiquetas EFFIS ≥ ``min_area_ha``)."""
    import psycopg2

    from firewatch.services.panel import build_panel, write_panel_csv

    start = date.fromisoformat(args.start_date)
    conn = psycopg2.connect(settings.POSTGRES_DSN)
    try:
        if args.end_date:
            end = date.fromisoformat(args.end_date)
        else:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(firedate) FROM confirmed_fires")
                max_date = cur.fetchone()[0]
            if max_date is None:
                logger.error("confirmed_fires is empty; run `runner labels` first.")
                return 1
            end = date.fromisoformat(str(max_date)[:10])

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM confirmed_fires WHERE province_code IS NOT NULL"
            )
            mapped = int(cur.fetchone()[0])
        if mapped == 0:
            logger.error(
                "No confirmed_fires have province_code; run `runner map-provinces` "
                "before building the panel."
            )
            return 1

        province_codes = None
        if args.provinces:
            province_codes = [
                code.strip() for code in args.provinces.split(",") if code.strip()
            ]

        rows = build_panel(
            conn,
            start,
            end,
            min_area_ha=args.min_area_ha,
            province_codes=province_codes,
        )
    finally:
        conn.close()

    written = write_panel_csv(args.output, rows)
    positives = sum(row["label"] for row in rows)
    logger.info(
        "Panel %s .. %s: %d row(s), %d positive(s) (%.2f %%), min area %s ha -> %s",
        start,
        end,
        written,
        positives,
        100.0 * positives / written if written else 0.0,
        args.min_area_ha,
        args.output,
    )
    return 0


def cmd_province_weather(args: argparse.Namespace) -> int:
    """Descarga la serie diaria de Open-Meteo para las provincias del panel.

    Una petición por provincia y rango completo (52 en total), no una por día.
    El archivo ERA5 responde con ~3.500 días por provincia; la previsión es solo
    una red para los días más recientes. Un fallo de una provincia no cancela las
    demás, pero el comando devuelve 1 para que un panel incompleto no pase por
    bueno.
    """
    from firewatch.clients.daily_weather import (
        DailyWeatherClient,
        write_weather_csv,
    )
    from firewatch.services.provinces import PROVINCES, PROVINCE_CODES

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date) if args.end_date else date.today()
    if start > end:
        logger.error("start-date (%s) is after end-date (%s)", start, end)
        return 1

    codes = (
        [code.strip() for code in args.provinces.split(",") if code.strip()]
        if args.provinces
        else list(PROVINCE_CODES)
    )
    unknown = [code for code in codes if code not in PROVINCES]
    if unknown:
        logger.error("Unknown province code(s): %s", ", ".join(unknown))
        return 1

    client = DailyWeatherClient(retries=args.retries)
    rows = []
    failed = []
    try:
        for index, code in enumerate(codes, 1):
            province = PROVINCES[code]
            logger.info(
                "  [%d/%d] %s (%s) %.4f, %.4f",
                index,
                len(codes),
                code,
                province.name,
                province.latitude,
                province.longitude,
            )
            province_rows = client.fetch_daily(
                province.latitude,
                province.longitude,
                start,
                end,
            )
            if not province_rows:
                failed.append(code)
                logger.warning("No daily weather returned for %s", code)
            else:
                for row in province_rows:
                    row["province_code"] = code
                rows.extend(province_rows)
                logger.info("    %d day(s)", len(province_rows))

            if index < len(codes) and args.sleep > 0:
                time.sleep(args.sleep)
    finally:
        client.close()

    written = write_weather_csv(args.output, rows)
    logger.info(
        "Province weather %s .. %s: %d row(s) -> %s",
        start,
        end,
        written,
        args.output,
    )
    if failed:
        logger.error(
            "Weather incomplete for province(s): %s. Re-run after checking "
            "the upstream; the CSV contains the rest.",
            ", ".join(failed),
        )
        return 1
    return 0


def _fetch_forecast_weather(
    client,
    province,
    start: date,
    target: date,
    history_days: int,
) -> list[dict]:
    """Devuelve la historia contigua y la previsión del día objetivo.

    El archivo ERA5 puede no haber publicado los últimos días. Si faltan, se
    rellenan desde la API de previsión; el día objetivo se pide siempre a la
    previsión, nunca al archivo. Un hueco o una variable a NULL es un fallo de
    la provincia, no algo que se pueda imputar sin cambiar la predicción.
    """
    from firewatch.ml.features import WEATHER_COLUMNS

    history_end = target - timedelta(days=1)
    history = client.fetch_daily(
        province.latitude,
        province.longitude,
        start,
        history_end,
    )
    by_day = {row["day"]: row for row in history}
    expected = [
        (start + timedelta(days=offset)).isoformat()
        for offset in range(history_days)
    ]

    missing = [day for day in expected if day not in by_day]
    if missing:
        logger.warning(
            "  %s: %d history day(s) missing from the archive; asking the "
            "forecast API",
            province.code,
            len(missing),
        )
        fallback = client.fetch_forecast(
            province.latitude,
            province.longitude,
            start,
            history_end,
        )
        for row in fallback:
            by_day.setdefault(row["day"], row)

    target_rows = client.fetch_forecast(
        province.latitude,
        province.longitude,
        target,
        target,
    )
    target_row = next(
        (row for row in target_rows if row["day"] == target.isoformat()),
        None,
    )
    if target_row is None:
        raise RuntimeError(f"no forecast returned for {target.isoformat()}")
    by_day[target.isoformat()] = target_row

    selected = []
    missing_days = []
    for day in [*expected, target.isoformat()]:
        row = by_day.get(day)
        if row is None:
            missing_days.append(day)
        else:
            selected.append(row)
    if missing_days:
        raise RuntimeError(f"missing weather day(s): {', '.join(missing_days)}")

    incomplete = [
        row["day"]
        for row in selected
        if any(row.get(column) is None for column in WEATHER_COLUMNS)
    ]
    if incomplete:
        raise RuntimeError(
            "incomplete weather variable(s) on: " + ", ".join(incomplete)
        )

    for row in selected:
        row["province_code"] = province.code
    return selected


def cmd_forecast(args: argparse.Namespace) -> int:
    """Ejecuta el pronóstico una vez o en un bucle de actualización.

    El modo ``--loop`` es el que usa el contenedor ``forecast``: comparte la
    cadencia del refresco principal, pero no depende de él. La inferencia usa
    meteorología de Open-Meteo, no los focos de FIRMS. Un fallo conserva las
    predicciones anteriores y se reintenta en la siguiente vuelta.
    """
    if not getattr(args, "loop", False):
        return _forecast_once(args)

    interval = max(60, getattr(args, "interval_minutes", 180) * 60)
    logger.info(
        "Forecast loop started (every %d min). Ctrl+C to stop.",
        interval // 60,
    )
    try:
        while True:
            try:
                code = _forecast_once(args)
                if code != 0:
                    logger.error(
                        "Forecast run failed with exit code %d; keeping the "
                        "previous predictions and retrying.",
                        code,
                    )
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                logger.exception("Forecast loop run failed: %s", exc)
            logger.info("Next forecast attempt in %d min.", interval // 60)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Forecast loop stopped")
    return 0


def _forecast_once(args: argparse.Namespace) -> int:
    """Predice el siguiente día UTC para cada provincia y lo guarda en Postgres.

    La inferencia es deliberadamente offline: `firewatch.ml.forecast` necesita
    pandas y scikit-learn. La API solo leerá la tabla, y este import diferido es
    lo que mantiene esas dependencias fuera de las imágenes web y de ingesta.
    """
    from firewatch.clients.daily_weather import DailyWeatherClient
    from firewatch.ml.forecast import (
        MIN_HISTORY_DAYS,
        ForecastError,
        artifact_version,
        build_forecast_frame,
        load_artifact,
        predict_frame,
    )
    from firewatch.services.provinces import PROVINCES, PROVINCE_CODES

    if args.history_days < MIN_HISTORY_DAYS:
        logger.error(
            "--history-days must be at least %d: with less history, "
            "days_since_rain is truncated and is not the feature the model "
            "was trained on.",
            MIN_HISTORY_DAYS,
        )
        return 1

    target = (
        date.fromisoformat(args.date)
        if args.date
        else datetime.now(timezone.utc).date() + timedelta(days=1)
    )
    codes = (
        [code.strip() for code in args.provinces.split(",") if code.strip()]
        if args.provinces
        else list(PROVINCE_CODES)
    )
    unknown = [code for code in codes if code not in PROVINCES]
    if unknown:
        logger.error("Unknown province code(s): %s", ", ".join(unknown))
        return 1
    if len(set(codes)) != len(codes):
        logger.error("Province codes must not be duplicated.")
        return 1

    model_path = Path(args.model)
    try:
        artifact = load_artifact(model_path)
        version = artifact_version(model_path)
    except ForecastError as exc:
        logger.error("%s", exc)
        return 1

    start = target - timedelta(days=args.history_days)
    logger.info(
        "Forecast %s: %d province(s), history %s .. %s, model %s-%s",
        target,
        len(codes),
        start,
        target - timedelta(days=1),
        artifact["kind"],
        version,
    )

    client = DailyWeatherClient(retries=args.retries)
    weather_rows = []
    failed = []
    try:
        for index, code in enumerate(codes, 1):
            province = PROVINCES[code]
            logger.info(
                "  [%d/%d] %s (%s) %.4f, %.4f",
                index,
                len(codes),
                code,
                province.name,
                province.latitude,
                province.longitude,
            )
            try:
                province_rows = _fetch_forecast_weather(
                    client,
                    province,
                    start,
                    target,
                    args.history_days,
                )
            except (RuntimeError, ValueError) as exc:
                # Una provincia no invalida el diagnóstico de las demás; el
                # lote se escribe solo si todas están completas.
                failed.append((code, str(exc)))
                logger.warning("  %s: %s", code, exc)
            else:
                weather_rows.extend(province_rows)
                logger.info("    %d day(s)", len(province_rows))

            if index < len(codes) and args.sleep > 0:
                time.sleep(args.sleep)
    finally:
        client.close()

    if failed:
        logger.error(
            "Forecast incomplete for province(s): %s. No rows written; the "
            "previous forecast is preserved.",
            ", ".join(f"{code} ({reason})" for code, reason in failed),
        )
        return 1

    try:
        frame = build_forecast_frame(weather_rows, target, province_codes=codes)
        predictions = predict_frame(artifact, frame)
    except ForecastError as exc:
        logger.error("Cannot build the forecast: %s", exc)
        return 1

    predictions.sort(key=lambda row: row["probability"], reverse=True)
    logger.info(
        "Forecast for %s: %d province(s), model %s-%s",
        target,
        len(predictions),
        artifact["kind"],
        version,
    )
    for rank, row in enumerate(predictions, 1):
        logger.info(
            "  %2d. %s %-26s raw=%.4f calibrated=%.4f",
            rank,
            row["province_code"],
            row["province_name"],
            row["probability"],
            row["probability_calibrated"],
        )

    if args.dry_run:
        logger.info(
            "Dry run: %d prediction(s) not written to Postgres.",
            len(predictions),
        )
        return 0

    import psycopg2

    from firewatch.services.forecast import (
        ensure_predictions_table,
        upsert_predictions,
    )

    records = [
        {
            "target_date": target,
            "province_code": row["province_code"],
            "province_name": row["province_name"],
            "probability": row["probability"],
            "probability_calibrated": row["probability_calibrated"],
            "model_name": artifact["kind"],
            "model_version": version,
            "weather_source": "open-meteo-archive+forecast",
        }
        for row in predictions
    ]

    try:
        conn = psycopg2.connect(settings.POSTGRES_DSN)
    except psycopg2.OperationalError as exc:
        logger.error("Database unavailable: %s", exc)
        return 1

    try:
        ensure_predictions_table(conn)
        written = upsert_predictions(conn, records)
    finally:
        conn.close()

    logger.info(
        "Wrote %d prediction(s) for %s to fire_predictions.",
        written,
        target,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="firewatch",
        description="Ejecutor del pipeline FireWatch: ingesta (FIRMS->Kafka) y procesamiento (Kafka->PostGIS).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="One-shot: descargar de FIRMS y publicar en Kafka")
    p_ingest.add_argument("--bbox", default=None, help="sur,oeste,norte,este (anula --regions)")
    p_ingest.add_argument(
        "--regions",
        default=None,
        help=f"Regiones separadas por coma (por defecto: {settings.FIREWATCH_REGIONS}). "
        f"Disponibles: {', '.join(sorted(settings.REGION_BBOXES))}",
    )
    p_ingest.add_argument("--date", default=None, help="AAAA-MM-DD (por defecto: hoy)")
    p_ingest.add_argument("--dry-run", action="store_true", help="Descargar sin publicar en Kafka")
    p_ingest.set_defaults(func=cmd_ingest)

    p_backfill = sub.add_parser(
        "backfill",
        help="Descargar de FIRMS para un rango de fechas (una petición por día) y publicar en Kafka",
    )
    p_backfill.add_argument("--bbox", default=None, help="sur,oeste,norte,este (anula --regions)")
    p_backfill.add_argument(
        "--regions",
        default=None,
        help=f"Regiones separadas por coma (por defecto: {settings.FIREWATCH_REGIONS}). "
        f"Disponibles: {', '.join(sorted(settings.REGION_BBOXES))}",
    )
    p_backfill.add_argument("--start-date", default=None, help="AAAA-MM-DD (inclusive)")
    p_backfill.add_argument("--end-date", default=None, help="AAAA-MM-DD (inclusive; por defecto: hoy)")
    p_backfill.add_argument(
        "--source",
        default=None,
        help="Producto de FIRMS (por defecto NASA_FIRMS_SOURCE=%s). El NRT solo "
        "llega ~4 meses atrás; para junio usa VIIRS_SNPP_SP."
        % settings.NASA_FIRMS_SOURCE,
    )
    p_backfill.add_argument("--dry-run", action="store_true", help="Descargar sin publicar")
    p_backfill.set_defaults(func=cmd_backfill)

    p_daily = sub.add_parser(
        "daily",
        help="Bucle: re-consultar ventana rodante de días y publicar (mantiene los datos actualizados)",
    )
    p_daily.add_argument("--bbox", default=None, help="sur,oeste,norte,este (anula --regions)")
    p_daily.add_argument(
        "--regions",
        default=None,
        help=f"Regiones separadas por coma (por defecto: {settings.FIREWATCH_REGIONS}). "
        f"Disponibles: {', '.join(sorted(settings.REGION_BBOXES))}",
    )
    p_daily.add_argument(
        "--days", type=int, default=3, help="Tamaño de la ventana rodante en días (por defecto: 3)"
    )
    p_daily.add_argument(
        "--interval-minutes",
        type=int,
        default=30,
        help="Minutos entre ejecuciones (por defecto: 30). Ignorado con --once",
    )
    p_daily.add_argument(
        "--once",
        action="store_true",
        help="Realizar una sola pasada de refresco y salir (para planificadores del SO)",
    )
    p_daily.add_argument("--dry-run", action="store_true", help="Descargar sin publicar")
    p_daily.set_defaults(func=cmd_daily)

    p_process = sub.add_parser("process", help="Continuo: consumir de Kafka y hacer upsert en PostGIS")
    p_process.add_argument("--topic", default=None, help=f"Topic de Kafka (por defecto: {settings.KAFKA_INGEST_TOPIC})")
    p_process.add_argument("--limit", type=int, default=None, help="Procesar como máximo N mensajes y salir")
    p_process.add_argument(
        "--max-empty-polls",
        type=int,
        default=None,
        help="Salir tras N encuestas vacías consecutivas (útil para CI; por defecto: indefinido)",
    )
    p_process.set_defaults(func=cmd_process)

    p_labels = sub.add_parser(
        "labels",
        help="Descargar incendios confirmados (EFFIS) y guardarlos en PostgreSQL",
    )
    p_labels.add_argument(
        "--source",
        default="effis",
        choices=("effis",),
        help="Fuente de etiquetas (por defecto: effis)",
    )
    p_labels.add_argument(
        "--start-date",
        default=None,
        help="AAAA-MM-DD, inclusive (por defecto: sin límite inferior)",
    )
    p_labels.add_argument(
        "--end-date",
        default=None,
        help="AAAA-MM-DD, inclusive (por defecto: sin límite superior)",
    )
    p_labels.add_argument(
        "--min-area-ha",
        type=float,
        default=settings.EFFIS_MIN_AREA_HA,
        help="Superficie mínima en hectáreas (por defecto: %(default)s)",
    )
    p_labels.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Máximo de etiquetas a descargar (útil para una muestra piloto)",
    )
    p_labels.add_argument(
        "--country",
        default=settings.EFFIS_COUNTRY,
        help="Código ISO-3166 alpha-2 (por defecto: %(default)s)",
    )
    p_labels.add_argument(
        "--page-size",
        type=int,
        default=settings.EFFIS_PAGE_SIZE,
        help="Registros por página de EFFIS (por defecto: %(default)s)",
    )
    p_labels.add_argument(
        "--dry-run",
        action="store_true",
        help="Descargar y mostrar una muestra sin escribir en la base de datos",
    )
    p_labels.set_defaults(func=cmd_labels)

    p_map = sub.add_parser(
        "map-provinces",
        help="Normalizar los nombres de provincia de EFFIS a códigos INE",
    )
    p_map.add_argument(
        "--dry-run",
        action="store_true",
        help="Contar las filas que cambiarían sin escribir en la base de datos",
    )
    p_map.set_defaults(func=cmd_map_provinces)

    p_panel = sub.add_parser(
        "panel",
        help="Construir el panel provincia-día (etiquetas EFFIS) para el modelo a 24 h",
    )
    p_panel.add_argument(
        "--start-date",
        default="2017-01-01",
        help="AAAA-MM-DD, inclusive (por defecto: %(default)s)",
    )
    p_panel.add_argument(
        "--end-date",
        default=None,
        help="AAAA-MM-DD, inclusive (por defecto: última fecha con etiquetas)",
    )
    p_panel.add_argument(
        "--min-area-ha",
        type=float,
        default=10.0,
        help="Superficie mínima del incendio confirmado en hectáreas (por defecto: %(default)s)",
    )
    p_panel.add_argument(
        "--provinces",
        default=None,
        help="Códigos INE separados por coma (por defecto: las 52 provincias)",
    )
    p_panel.add_argument(
        "--output",
        default="data/panel_province_day.csv",
        help="Ruta del CSV de salida (por defecto: %(default)s)",
    )
    p_panel.set_defaults(func=cmd_panel)

    p_weather = sub.add_parser(
        "province-weather",
        help="Descargar la serie diaria de Open-Meteo para las provincias",
    )
    p_weather.add_argument(
        "--start-date",
        default="2017-01-01",
        help="AAAA-MM-DD, inclusive (por defecto: %(default)s)",
    )
    p_weather.add_argument(
        "--end-date",
        default=None,
        help="AAAA-MM-DD, inclusive (por defecto: hoy)",
    )
    p_weather.add_argument(
        "--provinces",
        default=None,
        help="Códigos INE separados por coma (por defecto: las 52 provincias)",
    )
    p_weather.add_argument(
        "--output",
        default="data/weather_province_day.csv",
        help="Ruta del CSV de salida (por defecto: %(default)s)",
    )
    p_weather.add_argument(
        "--sleep",
        type=float,
        default=2.0,
        help="Segundos entre peticiones; Open-Meteo limita la ráfaga (por defecto: %(default)s)",
    )
    p_weather.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Reintentos por provincia (por defecto: %(default)s)",
    )
    p_weather.set_defaults(func=cmd_province_weather)

    p_forecast = sub.add_parser(
        "forecast",
        help="Predecir el siguiente día UTC para las provincias y guardarlo en Postgres",
    )
    p_forecast.add_argument(
        "--date",
        default=None,
        help="AAAA-MM-DD objetivo (por defecto: mañana UTC)",
    )
    p_forecast.add_argument(
        "--model",
        default="data/models/hgb.joblib",
        help="Artefacto joblib entrenado (por defecto: %(default)s)",
    )
    p_forecast.add_argument(
        "--provinces",
        default=None,
        help="Códigos INE separados por coma (por defecto: las 52 provincias)",
    )
    p_forecast.add_argument(
        "--history-days",
        type=int,
        default=35,
        help="Días de historia previos al objetivo (mínimo 31; por defecto: %(default)s)",
    )
    p_forecast.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Segundos entre provincias; Open-Meteo limita la ráfaga (por defecto: %(default)s)",
    )
    p_forecast.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Reintentos por provincia (por defecto: %(default)s)",
    )
    p_forecast.add_argument(
        "--loop",
        action="store_true",
        help="Repetir el pronóstico cada --interval-minutes (worker de actualización)",
    )
    p_forecast.add_argument(
        "--interval-minutes",
        type=int,
        default=180,
        help="Minutos entre ejecuciones con --loop (por defecto: %(default)s = 3 h)",
    )
    p_forecast.add_argument(
        "--dry-run",
        action="store_true",
        help="Predecir e imprimir sin escribir en Postgres",
    )
    p_forecast.set_defaults(func=cmd_forecast)

    p_geocode = sub.add_parser(
        "geocode-backfill",
        help="Completar localidades faltantes para focos almacenados (Nominatim, con límite de tasa)",
    )
    p_geocode.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Máximo de puntos a resolver en esta pasada (por defecto: 1000)",
    )
    p_geocode.add_argument(
        "--recheck",
        action="store_true",
        help="Reintentar también puntos marcados previamente sin localidad",
    )
    p_geocode.set_defaults(func=cmd_geocode_backfill)

    p_purge = sub.add_parser(
        "purge-out-of-scope",
        help="Eliminar focos guardados cuyo país geocodificado no está en "
        f"{settings.FIREWATCH_COUNTRY_CODES!r} (por defecto: solo España)",
    )
    p_purge.add_argument(
        "--dry-run",
        action="store_true",
        help="Contar las filas afectadas sin borrarlas",
    )
    p_purge.set_defaults(func=cmd_purge_out_of_scope)

    return parser


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())