"""Adaptador del catálogo de incendios confirmados de EFFIS.

EFFIS (Copernicus Emergency Management Service) publica las áreas quemadas
perimetradas de Europa. A diferencia de FIRMS, que solo detecta anomalías
térmicas, cada registro de EFFIS es un incendio confirmado con fecha, provincia,
municipio y superficie: es la fuente de las etiquetas del modelo de predicción
a 24 h.

Endpoint:
  GET {base}/?country=ES&limit={n}&offset={n}

La API no filtra por rango de fechas (el parámetro de año se ignora), así que se
pagina el catálogo completo y se filtra localmente por ``firedate`` y
``area_ha``. El catálogo de España ronda los 12.700 registros y la API tarda
~15 s por página de 250, por lo que una importación completa es una operación
de minutos, no de segundos.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Dict, List, Optional

import httpx

from firewatch.config import settings

logger = logging.getLogger(__name__)


class EffisError(RuntimeError):
    """La API de EFFIS no devolvió una página válida tras los reintentos."""


def _parse_datetime(value) -> Optional[datetime]:
    """Convierte un timestamp ISO-8601 de EFFIS, o ``None`` si no es válido."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _parse_date(value) -> Optional[date]:
    """Extrae la fecha local de un ``firedate`` ISO-8601."""
    parsed = _parse_datetime(value)
    if parsed is not None:
        return parsed.date()
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _as_float(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _valid_point(latitude: float, longitude: float) -> bool:
    return -90.0 <= latitude <= 90.0 and -180.0 <= longitude <= 180.0


def _centroid(row: Dict) -> tuple[Optional[float], Optional[float]]:
    """Devuelve ``(lat, lon)`` del centroide, con la caja como respaldo.

    EFFIS da ``centroid`` como GeoJSON ``Point`` (coordenadas ``[lon, lat]``).
    Algunas áreas pequeñas solo traen ``bbox``; el centro de la caja sigue
    siendo una posición válida para etiquetar un municipio.
    """
    centroid = row.get("centroid")
    if isinstance(centroid, dict):
        coordinates = centroid.get("coordinates")
        if isinstance(coordinates, (list, tuple)) and len(coordinates) >= 2:
            longitude = _as_float(coordinates[0])
            latitude = _as_float(coordinates[1])
            if (
                latitude is not None
                and longitude is not None
                and _valid_point(latitude, longitude)
            ):
                return latitude, longitude

    bbox = row.get("bbox")
    if isinstance(bbox, (list, tuple)) and len(bbox) == 4:
        min_lon, min_lat, max_lon, max_lat = (_as_float(v) for v in bbox)
        if None not in (min_lon, min_lat, max_lon, max_lat):
            latitude = (min_lat + max_lat) / 2.0
            longitude = (min_lon + max_lon) / 2.0
            if _valid_point(latitude, longitude):
                return latitude, longitude

    return None, None


def normalize_effis_area(row: Dict, country: Optional[str] = None) -> Optional[Dict]:
    """Normaliza un área de EFFIS al registro que consume ``confirmed_fires``.

    Descarta el registro si no tiene una fecha utilizable o una posición: sin
    fecha no se puede etiquetar un día, y sin punto no se puede asignar a un
    municipio. El perímetro (``shape``) se ignora a propósito; para las
    etiquetas basta el centroide y la superficie, y guardar todos los polígonos
    multiplicaría el tamaño de la tabla sin aportar al modelo.
    """
    if not isinstance(row, dict):
        return None

    source_id = row.get("id")
    if source_id is None:
        return None

    firedate_ts = _parse_datetime(row.get("firedate"))
    firedate = firedate_ts.date() if firedate_ts else _parse_date(row.get("firedate"))
    if firedate is None:
        logger.warning("EFFIS area %s has no usable firedate; skipping", source_id)
        return None

    latitude, longitude = _centroid(row)
    if latitude is None or longitude is None:
        logger.warning("EFFIS area %s has no centroid/bbox; skipping", source_id)
        return None

    bbox = row.get("bbox")
    if isinstance(bbox, (list, tuple)) and len(bbox) == 4:
        bbox = [_as_float(value) for value in bbox]
        if None in bbox:
            bbox = None
    else:
        bbox = None

    return {
        "source": "effis",
        "source_id": str(source_id),
        "country": str(row.get("country") or country or "").strip().upper() or None,
        "firedate": firedate,
        "firedate_ts": firedate_ts,
        "province": row.get("province") or None,
        "municipality": row.get("commune") or None,
        # EFFIS no publica el código INE; se rellena en una fase posterior.
        "municipality_code": None,
        "area_ha": _as_float(row.get("area_ha")),
        "latitude": latitude,
        "longitude": longitude,
        "bbox": bbox,
        "last_update": _parse_datetime(row.get("lastupdate")),
    }


class EffisClient:
    """Cliente paginado del catálogo de áreas quemadas de EFFIS."""

    def __init__(
        self,
        client: Optional[httpx.Client] = None,
        base_url: Optional[str] = None,
        country: Optional[str] = None,
        page_size: Optional[int] = None,
        retries: int = 2,
        retry_delay: float = 2.0,
    ):
        self._client = client or httpx.Client(timeout=120.0, follow_redirects=True)
        # La API redirige 301 de /current a /current/; pedir la barra final
        # evita una petición extra (y un 502) en cada página.
        self.base_url = (base_url or settings.EFFIS_BASE_URL).rstrip("/") + "/"
        self.country = (country or settings.EFFIS_COUNTRY).strip().upper()
        if len(self.country) != 2:
            # Sin un código válido la API devuelve el catálogo europeo entero,
            # no solo España: el filtro de ámbito dejaría de existir.
            raise ValueError(
                f"EFFIS_COUNTRY must be a 2-letter ISO-3166 code, got {self.country!r}"
            )
        self.page_size = max(1, int(page_size or settings.EFFIS_PAGE_SIZE))
        self.retries = max(0, int(retries))
        self.retry_delay = max(0.0, float(retry_delay))

    def close(self) -> None:
        self._client.close()

    def fetch_burnt_areas(
        self,
        start_date=None,
        end_date=None,
        min_area_ha: Optional[float] = None,
        limit: Optional[int] = None,
        country: Optional[str] = None,
    ) -> List[Dict]:
        """Descarga y filtra las áreas quemadas del país configurado.

        Parámetros
        ----------
        start_date, end_date : str | datetime.date, opcional
            Rango inclusivo sobre la fecha local de ``firedate``.
        min_area_ha : float, opcional
            Superficie mínima. Los registros sin superficie se descartan cuando
            se pide un mínimo; sin mínimo se conservan con ``area_ha=None``.
        limit : int, opcional
            Corta la descarga al alcanzar N registros válidos. Útil para una
            muestra piloto sin esperar al catálogo completo.
        country : str, opcional
            Código ISO-3166 alpha-2. Sobrescribe el del cliente.

        Devuelve una lista de diccionarios normalizados (ver
        ``normalize_effis_area``).
        """
        start = _parse_date(start_date) if start_date else None
        end = _parse_date(end_date) if end_date else None
        if start and end and start > end:
            raise ValueError(f"start_date ({start}) is after end_date ({end})")
        if limit is not None and int(limit) <= 0:
            return []

        min_area = _as_float(min_area_ha) if min_area_ha is not None else None
        wanted_country = (country or self.country).strip().upper()

        results: List[Dict] = []
        offset = 0
        total: Optional[int] = None

        while True:
            page = self._fetch_page(offset, wanted_country)
            if total is None:
                total = int(page.get("count") or 0)

            rows = page.get("results") or []
            if not rows:
                break

            for row in rows:
                record = normalize_effis_area(row, country=wanted_country)
                if record is None:
                    continue
                # El filtro de país de la API se respeta, pero no se da por
                # hecho: una respuesta con Portugal o Francia no debe entrar en
                # la tabla de etiquetas de España.
                if wanted_country and record["country"] not in (None, wanted_country):
                    continue
                if start and record["firedate"] < start:
                    continue
                if end and record["firedate"] > end:
                    continue
                if min_area is not None:
                    area = record["area_ha"]
                    if area is None or area < min_area:
                        continue
                results.append(record)
                if limit is not None and len(results) >= limit:
                    return results

            offset += len(rows)
            if total and offset >= total:
                break
            if not total and len(rows) < self.page_size:
                break

        return results

    def _fetch_page(self, offset: int, country: str) -> Dict:
        params = {"country": country, "limit": self.page_size, "offset": offset}
        last_error: Optional[Exception] = None

        for attempt in range(self.retries + 1):
            try:
                response = self._client.get(
                    self.base_url,
                    params=params,
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict) or "results" not in data:
                    raise EffisError(
                        f"unexpected EFFIS payload at offset {offset}: "
                        f"{type(data).__name__}"
                    )
                return data
            except (httpx.HTTPError, ValueError, EffisError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    break
                delay = self.retry_delay * (2 ** attempt)
                logger.warning(
                    "EFFIS page offset=%d failed (attempt %d/%d): %s — retrying in %.1fs",
                    offset,
                    attempt + 1,
                    self.retries + 1,
                    exc,
                    delay,
                )
                if delay:
                    time.sleep(delay)

        raise EffisError(
            f"EFFIS request failed at offset {offset}: {last_error}"
        ) from last_error
