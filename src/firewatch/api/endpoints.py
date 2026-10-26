"""Endpoints de la API de FireWatch."""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

import psycopg2
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from psycopg2.extras import RealDictCursor

from firewatch.api.auth import require_session
from firewatch.clients.firms import FirmsClient
from firewatch.config import resolve_regions, settings
from firewatch.services.ingestion import create_producer, run_ingestion

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/fire",
    tags=["fire"],
    # Cada endpoint de fire requiere una sesión de navegador. La clave de API compartida
    # nunca entra al navegador; solo se intercambia por una cookie en POST /auth/login.
    dependencies=[Depends(require_session)],
)


def _parse_bbox(bbox: str) -> tuple[float, float, float, float]:
    """Parsea 'sur,oeste,norte,este' a números de punto flotante."""
    try:
        south, west, north, east = (float(p) for p in bbox.split(","))
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="bbox must be 'south,west,north,east'",
        ) from exc
    return south, west, north, east


@router.get("/events", response_model=List[dict])
def list_fire_events(
    bbox: Optional[str] = Query(
        None, description="Caja delimitadora 'sur,oeste,norte,este'"
    ),
    risk_level: Optional[str] = Query(
        None, description="Filtrar por nivel de riesgo: low, moderate, high, extreme"
    ),
    min_risk_score: Optional[float] = Query(
        None, ge=0.0, le=1.0, description="Puntuación mínima de riesgo (0.0 a 1.0)"
    ),
    date: Optional[str] = Query(
        None, description="Fecha de adquisición exacta AAAA-MM-DD"
    ),
    limit: int = Query(100, ge=1, le=1000),
) -> List[dict]:
    """
    Lista eventos de fuego almacenados en PostgreSQL/PostGIS, opcionalmente
    filtrados por caja delimitadora, nivel de riesgo, score mínimo y fecha concreta.
    """
    try:
        conn = psycopg2.connect(settings.POSTGRES_DSN)
    except psycopg2.OperationalError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        ) from exc

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            conditions = ["1=1"]
            sql_params: List[Any] = []

            if bbox:
                south, west, north, east = _parse_bbox(bbox)
                conditions.append("geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)")
                sql_params.extend([west, south, east, north])

            if risk_level:
                conditions.append("LOWER(risk_level) = %s")
                sql_params.append(risk_level.strip().lower())

            if min_risk_score is not None:
                conditions.append("risk_score >= %s")
                sql_params.append(min_risk_score)

            if date:
                conditions.append("acq_date = %s")
                sql_params.append(date)

            sql_params.append(limit)

            where_clause = " AND ".join(conditions)
            cur.execute(
                f"""
                SELECT acq_date, latitude, longitude, brightness,
                       scan, track, acquisition_date, updated_at,
                       risk_level, risk_score, risk_factors, locality
                FROM fire_events
                WHERE {where_clause}
                ORDER BY acq_date DESC, risk_score DESC, latitude, longitude
                LIMIT %s
                """,
                tuple(sql_params),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


@router.get("/dates")
def list_available_dates() -> List[dict]:
    """
    Devuelve cada fecha de adquisición almacenada en la base de datos con su desglose
    diario, ordenadas de más reciente a más antigua. Alimenta el selector del dashboard:
    cada entrada incluye totales y distribución de riesgo para que la interfaz muestre
    el contenido de cada día sin necesidad de descargar todos los eventos individuales.
    """
    try:
        conn = psycopg2.connect(settings.POSTGRES_DSN)
    except psycopg2.OperationalError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        ) from exc

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT acq_date,
                       COUNT(*) AS total,
                       MAX(risk_score) AS max_risk_score,
                       COUNT(*) FILTER (WHERE LOWER(risk_level) = 'extreme') AS extreme,
                       COUNT(*) FILTER (WHERE LOWER(risk_level) = 'high')     AS high,
                       COUNT(*) FILTER (WHERE LOWER(risk_level) = 'moderate') AS moderate,
                       COUNT(*) FILTER (WHERE LOWER(risk_level) = 'low')      AS low
                FROM fire_events
                GROUP BY acq_date
                ORDER BY acq_date DESC
                """
            )
            return [
                {
                    "date": row["acq_date"].isoformat(),
                    "total": row["total"],
                    "max_risk_score": round(float(row["max_risk_score"] or 0), 4),
                    "by_risk_level": {
                        "extreme": row["extreme"],
                        "high": row["high"],
                        "moderate": row["moderate"],
                        "low": row["low"],
                    },
                }
                for row in cur.fetchall()
            ]
    finally:
        conn.close()


@router.get("/stats")
def fire_stats(date: Optional[str] = Query(None, description="Día exacto AAAA-MM-DD")) -> dict:
    """
    Devuelve totales y distribución de riesgo de los eventos de fuego almacenados.

    Cuando se especifica ``date``, los cálculos reflejan exclusivamente ese día de
    adquisición, de modo que los KPIs del dashboard sigan el día seleccionado en vez
    del archivo histórico completo.
    """
    try:
        conn = psycopg2.connect(settings.POSTGRES_DSN)
    except psycopg2.OperationalError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        ) from exc
    try:
        with conn.cursor() as cur:
            conditions = "WHERE acq_date = %s" if date else ""
            sql_params = (date,) if date else ()

            cur.execute(
                f"""
                SELECT COUNT(*) AS total,
                       MIN(acq_date) AS first_date,
                       MAX(acq_date) AS last_date,
                       COALESCE(MAX(risk_score), 0) AS max_risk_score
                FROM fire_events
                {conditions};
                """,
                sql_params,
            )
            total, first_date, last_date, max_score = cur.fetchone()

            cur.execute(
                f"""
                SELECT COALESCE(risk_level, 'unknown') AS level, COUNT(*)
                FROM fire_events
                {conditions}
                GROUP BY risk_level;
                """,
                sql_params,
            )
            by_level = {row[0]: row[1] for row in cur.fetchall()}

        return {
            "total": total,
            "first_date": first_date.isoformat() if first_date else None,
            "last_date": last_date.isoformat() if last_date else None,
            "max_risk_score": round(float(max_score or 0), 4),
            "scope": date or "all",
            "by_risk_level": by_level,
        }
    finally:
        conn.close()


@router.get("/locality-stats")
def locality_stats(
    date: Optional[str] = Query(None, description="Día exacto AAAA-MM-DD"),
    country: Optional[str] = Query(
        None, description="País o código ISO (es, pt, fr) para restringir la agregación"
    ),
    limit: int = Query(15, ge=1, le=100),
) -> dict:
    """
    Agrega los focos por provincia y municipio para la sección de estadísticas.

    ``locality`` es el JSONB que escribe el geocodificador inverso. ``province`` es
    la unidad administrativa intermedia (provincia española, departamento francés,
    distrito portugués) y ``community`` el nivel superior (comunidad autónoma o
    región). Las filas sin localidad resuelta (mar abierto, fallo de Nominatim) no
    entran en los rankings, pero sí en los totales.
    """
    try:
        conn = psycopg2.connect(settings.POSTGRES_DSN)
    except psycopg2.OperationalError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        ) from exc

    try:
        with conn.cursor() as cur:
            conditions: List[str] = []
            params: List[Any] = []
            if date:
                conditions.append("acq_date = %s")
                params.append(date)
            if country:
                conditions.append(
                    "(LOWER(locality->>'country') = LOWER(%s) "
                    "OR LOWER(locality->>'country_code') = LOWER(%s))"
                )
                params.extend([country, country])

            where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
            located_conditions = [
                "locality->>'locality' IS NOT NULL",
                "locality->>'locality' <> ''",
            ]
            located_conditions.extend(conditions)
            located_where = "WHERE " + " AND ".join(located_conditions)

            cur.execute(
                f"""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (
                           WHERE locality->>'locality' IS NOT NULL
                             AND locality->>'locality' <> ''
                       ) AS located,
                       COUNT(DISTINCT locality->>'locality') FILTER (
                           WHERE locality->>'locality' IS NOT NULL
                             AND locality->>'locality' <> ''
                       ) AS municipalities,
                       COUNT(DISTINCT locality->>'province') FILTER (
                           WHERE locality->>'province' IS NOT NULL
                             AND locality->>'province' <> ''
                       ) AS provinces,
                       COUNT(DISTINCT acq_date) AS days,
                       MIN(acq_date) AS first_date,
                       MAX(acq_date) AS last_date,
                       COALESCE(MAX(risk_score), 0) AS max_risk_score
                FROM fire_events
                {where};
                """,
                tuple(params),
            )
            (
                total,
                located,
                municipalities,
                provinces,
                days,
                first_date,
                last_date,
                max_score,
            ) = cur.fetchone()

            cur.execute(
                f"""
                SELECT COALESCE(risk_level, 'unknown') AS level, COUNT(*)
                FROM fire_events
                {where}
                GROUP BY risk_level;
                """,
                tuple(params),
            )
            by_level = {"extreme": 0, "high": 0, "moderate": 0, "low": 0}
            for level, count in cur.fetchall():
                by_level[str(level).lower()] = count

            cur.execute(
                f"""
                SELECT COALESCE(locality->>'province', 'Sin provincia') AS province,
                       COALESCE(locality->>'community', '') AS community,
                       COALESCE(locality->>'country', '') AS country,
                       COUNT(*) AS events,
                       COUNT(DISTINCT locality->>'locality') AS municipalities,
                       COUNT(*) FILTER (
                           WHERE LOWER(risk_level) IN ('high', 'extreme')
                       ) AS high,
                       COALESCE(MAX(risk_score), 0) AS max_risk_score
                FROM fire_events
                {located_where}
                GROUP BY 1, 2, 3
                ORDER BY events DESC, province
                LIMIT %s;
                """,
                tuple(params) + (limit,),
            )
            province_rows = cur.fetchall()

            cur.execute(
                f"""
                SELECT locality->>'locality' AS municipality,
                       COALESCE(locality->>'province', '') AS province,
                       COALESCE(locality->>'community', '') AS community,
                       COALESCE(locality->>'country', '') AS country,
                       COUNT(*) AS events,
                       COUNT(*) FILTER (
                           WHERE LOWER(risk_level) IN ('high', 'extreme')
                       ) AS high,
                       COALESCE(MAX(risk_score), 0) AS max_risk_score
                FROM fire_events
                {located_where}
                GROUP BY 1, 2, 3, 4
                ORDER BY events DESC, municipality
                LIMIT %s;
                """,
                tuple(params) + (limit,),
            )
            municipality_rows = cur.fetchall()

            cur.execute(
                f"""
                SELECT acq_date,
                       COUNT(*) AS events,
                       COUNT(*) FILTER (
                           WHERE LOWER(risk_level) IN ('high', 'extreme')
                       ) AS high,
                       COALESCE(MAX(risk_score), 0) AS max_risk_score
                FROM fire_events
                {where}
                GROUP BY acq_date
                ORDER BY acq_date;
                """,
                tuple(params),
            )
            daily_rows = cur.fetchall()

        return {
            "scope": date or "all",
            "country": country,
            "totals": {
                "events": total,
                "located": located,
                "municipalities": municipalities,
                "provinces": provinces,
                "days": days,
                "first_date": first_date.isoformat() if first_date else None,
                "last_date": last_date.isoformat() if last_date else None,
                "max_risk_score": round(float(max_score or 0), 4),
            },
            "by_risk_level": by_level,
            "provinces": [
                {
                    "province": row[0],
                    "community": row[1],
                    "country": row[2],
                    "events": row[3],
                    "municipalities": row[4],
                    "high": row[5],
                    "max_risk_score": round(float(row[6] or 0), 4),
                }
                for row in province_rows
            ],
            "municipalities": [
                {
                    "municipality": row[0],
                    "province": row[1],
                    "community": row[2],
                    "country": row[3],
                    "events": row[4],
                    "high": row[5],
                    "max_risk_score": round(float(row[6] or 0), 4),
                }
                for row in municipality_rows
            ],
            "daily": [
                {
                    "date": row[0].isoformat(),
                    "events": row[1],
                    "high": row[2],
                    "max_risk_score": round(float(row[3] or 0), 4),
                }
                for row in daily_rows
            ],
        }
    finally:
        conn.close()


@router.post("/ingest", status_code=status.HTTP_202_ACCEPTED)
def trigger_ingest(
    background: BackgroundTasks,
    bbox: Optional[str] = None,
    date: Optional[str] = None,
) -> dict:
    """
    Planifica una ejecución de ingesta de FIRMS y responde inmediatamente.

    El trabajo se delega a una tarea en segundo plano (background task) para evitar
    bloqueos por denegación de servicio contra el dashboard. Este handler es síncrono,
    por lo que FastAPI lo ejecuta en el threadpool realizando llamadas HTTP bloqueantes
    a FIRMS con un tiempo de espera de 30s por región. Tres regiones podrían bloquear
    un worker durante 90s, dejando esperando a `/fire/events` -- el endpoint vital para el
    dashboard.

    Devuelve 202 con el identificador del trabajo. El progreso se puede observar mediante
    ``/api/v1/fire/stats``: una vez el procesador consuma los registros publicados, los
    conteos del día aumentan.
    """
    regions = resolve_regions(bbox, None)

    # `resolve_regions` lanza excepción ante un nombre de región desconocido. Validar aquí
    # asegura que un error tipográfico devuelva un 4xx inmediato que el cliente pueda ver,
    # en lugar de un fallo oculto en una tarea en segundo plano tras haber respondido 202.
    job_id = uuid.uuid4().hex[:12]
    logger.info(
        "Ingest job %s scheduled: regions=%s date=%s",
        job_id,
        [label for label, _ in regions],
        date or "today",
    )

    background.add_task(_run_ingest, job_id, list(regions), date or None)

    return {
        "status": "accepted",
        "job_id": job_id,
        "regions": [{"label": label, "bbox": r_bbox} for label, r_bbox in regions],
        "date": date or "today",
        "note": (
            "Ingestion runs in the background. Poll /api/v1/fire/stats to see "
            "the new records once the processor has consumed them."
        ),
    }


def _run_ingest(job_id: str, regions: list, date: Optional[str]) -> None:
    """
    Realiza la descarga y publicación real para un trabajo planificado.

    Se ejecuta tras haberse enviado la respuesta HTTP. Los fallos se registran, no se elevan:
    una excepción aquí quedaría huérfana de petición y el emisor ya recibió su código 202.
    """
    try:
        client = FirmsClient()
    except ValueError as exc:
        # La respuesta 202 ya se envió: un fallo aquí no puede elevarse, solo
        # registrarse con el suficiente detalle para diagnosticarlo.
        logger.error("Ingest job %s: FIRMS client unavailable: %s", job_id, exc)
        return

    records: list = []
    per_region: Dict[str, int] = {}

    for label, region_bbox in regions:
        try:
            fetched = client.fetch_by_bbox(region_bbox, date=date)
        except Exception as exc:  # httpx.HTTPError y errores de parseo
            logger.error("Ingest job %s: FIRMS fetch failed for %s: %s", job_id, label, exc)
            continue
        per_region[label] = len(fetched)
        records.extend(fetched)

    if not records:
        logger.warning("Ingest job %s produced no records", job_id)
        return

    try:
        producer = create_producer()
    except Exception as exc:
        logger.error("Ingest job %s: Kafka unavailable: %s", job_id, exc)
        return

    try:
        published = run_ingestion(records, producer, topic=settings.KAFKA_INGEST_TOPIC)
    finally:
        producer.close()

    logger.info(
        "Ingest job %s done: fetched=%d published=%d per_region=%s",
        job_id,
        len(records),
        published,
        per_region,
    )
