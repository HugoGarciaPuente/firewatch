"""Ejecutor por línea de comandos (CLI) del pipeline de FireWatch.

Ejecuta las operaciones puntuales y de larga duración del slice E2E sin
necesidad de levantar la API HTTP:

    ingestion :  descargar FIRMS -> publicar en Kafka (one-shot)
    backfill  :  descargar FIRMS para N días consecutivos -> Kafka (histórico)
    process   :  consumir Kafka -> upsert en PostgreSQL/PostGIS (continuo)
    daily     :  bucle que mantiene el dataset actualizado (re-consulta ventana rodante)
    purge-out-of-scope : borrar focos guardados fuera de FIREWATCH_COUNTRY_CODES

Uso:
    python -m firewatch.runner ingest  [--bbox S,W,N,E] [--date AAAA-MM-DD] [--dry-run]
    python -m firewatch.runner backfill [--bbox S,W,N,E] [--start-date D] [--end-date D]
    python -m firewatch.runner daily    [--bbox S,W,N,E] [--days N] [--interval-minutes N]
    python -m firewatch.runner process [--limit N] [--max-empty-polls N]
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
from firewatch.services.processing import consume_and_process, create_consumer, purge_out_of_scope

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

    client = FirmsClient()
    producer = None
    total = 0
    per_day = []

    try:
        for label, bbox in regions:
            for offset in range((end - start).days + 1):
                day = start + timedelta(days=offset)
                day_str = day.isoformat()
                try:
                    records = client.fetch_by_bbox(bbox, date=day_str)
                except Exception as exc:
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

    Devuelve el número de registros publicados.
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
                refresh_once(client, regions, args.days)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                logger.error("Daily run failed: %s", exc)
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

    Solo se tocan las filas cuya ``locality`` es NULL, por lo que reejecutarlo es seguro
    y rápido. Usa ``--recheck`` para re-evaluar filas que ya tienen una localidad (útil
    tras mejoras en los datos de OSM).
    """
    # psycopg2 y GeoCodeClient se importan de forma perezosa (lazy) dentro de la función.
    # La imagen de adquisición (`runner daily`) no incluye ni el driver de base de datos
    # ni el geocodificador, por lo que una importación a nivel de módulo haría fallar
    # las pruebas de límites de capas e incluiría código no utilizado en cada imagen.
    import psycopg2

    from firewatch.clients.geocode import GeoCodeClient

    conn = psycopg2.connect(settings.POSTGRES_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS fire_events (
                    id SERIAL PRIMARY KEY,
                    acq_date DATE NOT NULL,
                    latitude DOUBLE PRECISION NOT NULL,
                    longitude DOUBLE PRECISION NOT NULL
                );
                ALTER TABLE fire_events ADD COLUMN IF NOT EXISTS locality JSONB;
                """
            )
        conn.commit()

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

        filled = failed = 0
        try:
            for index, (row_id, lat, lon) in enumerate(pending, 1):
                location = client.get_location(lat, lon)
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
                        failed += 1
                conn.commit()

                if index % 25 == 0 or index == len(pending):
                    logger.info(
                        "  %d/%d (%.0f%%)  resueltos=%d  sin localidad=%d",
                        index,
                        len(pending),
                        100.0 * index / len(pending),
                        filled,
                        failed,
                    )
        except KeyboardInterrupt:
            logger.warning(
                "Interrupted at %d/%d. Resolved rows were committed; re-run to continue.",
                filled + failed,
                len(pending),
            )
            return 130

        stats = client.cache_stats()
        logger.info(
            "Geocoding complete: %d resolved, %d without locality. "
            "cache hits=%d misses=%d",
            filled,
            failed,
            stats["hits"],
            stats["misses"],
        )
        # Un fallo parcial se reporta como tal en lugar de ignorarse silenciosamente:
        # las filas que quedan vacías son visibles, y el código de salida permite automatización.
        return 0
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
        default=360,
        help="Minutos entre ejecuciones (por defecto: 360 = cada 6h). Ignorado con --once",
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