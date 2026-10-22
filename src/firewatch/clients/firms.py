"""
Adaptador del cliente de la API de NASA FIRMS.

Envuelve la API REST de NASA FIRMS para obtener datos de anomalías térmicas.
Todas las llamadas externas a la API pasan a través de este cliente para que
puedan simularse/mockearse en los tests.

Endpoint:
  GET {base}/area/csv/{MAP_KEY}/{SOURCE}/{AREA_COORDINATES}/{DAY_RANGE}[/{DATE}]

La API de área de FIRMS no tiene endpoint anónimo: exige una clave, provista
mediante la variable de entorno ``NASA_FIRMS_API_KEY``. Sin clave el cliente se
niega a construirse, en lugar de inventar una URL que no existe.
"""

from __future__ import annotations

import csv
import io
import logging
from typing import Dict, List, Optional

import httpx

from firewatch.config import settings

logger = logging.getLogger(__name__)


def _format_area_coordinates(bbox: str) -> str:
    """
    Convierte el bbox 'sur,oeste,norte,este' a coordenadas FIRMS 'oeste,sur,este,norte'.
    """
    if bbox.strip().lower() == "world":
        return "world"
    parts = [p.strip() for p in bbox.split(",")]
    if len(parts) == 4:
        s, w, n, e = parts
        return f"{w},{s},{e},{n}"
    return bbox


class FirmsClient:
    """Cliente para datos de anomalías térmicas de NASA FIRMS."""

    def __init__(
        self,
        client: Optional[httpx.Client] = None,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        source: str = "VIIRS_SNPP_NRT",
    ):
        resolved_key = (
            api_key if api_key is not None else settings.NASA_FIRMS_API_KEY
        ).strip()
        if not resolved_key:
            # Se valida antes de crear el cliente HTTP: no hay motivo para
            # abrir un pool de conexiones que nunca se va a usar.
            raise ValueError(
                "NASA_FIRMS_API_KEY is required: the FIRMS area API has no "
                "anonymous endpoint. Get a key at "
                "https://firms.modaps.eosdis.nasa.gov/api/area/ and set it in .env"
            )

        self._client = client or httpx.Client()
        self.base_url = (base_url or settings.NASA_FIRMS_BASE_URL).rstrip("/")
        self.api_key = resolved_key
        self.source = source

    def fetch_by_bbox(self, bbox: str, date: Optional[str] = None) -> List[Dict]:
        """
        Descarga anomalías térmicas de FIRMS para una caja delimitadora (bbox).

        Parámetros
        ----------
        bbox : str
            Coordenadas separadas por coma "sur,oeste,norte,este" (o "world").
        date : str, opcional
            AAAA-MM-DD. Si se omite, la API devuelve los datos más recientes.

        Retorna
        -------
        list[dict]
            Registros de FIRMS como diccionarios (campos originales de la API).
        """
        area_coords = _format_area_coordinates(bbox)
        day_range = 1

        # La clave va en la ruta porque FIRMS lo exige así; no se puede pasar
        # como cabecera. Coordenadas ya reordenadas a oeste,sur,este,norte.
        url = (
            f"{self.base_url}/area/csv/{self.api_key}/"
            f"{self.source}/{area_coords}/{day_range}"
        )
        if date:
            url = f"{url}/{date}"

        response = self._client.get(url, timeout=30.0)
        response.raise_for_status()

        # Intentar parsear JSON primero (si es un mock/fixture o respuesta JSON)
        try:
            data = response.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "features" in data:
                return data["features"]
            if isinstance(data, dict):
                logger.warning("Estructura JSON inesperada de FIRMS: %s", data)
                return []
        except Exception:
            pass

        # Parsear CSV (respuesta oficial de NASA FIRMS)
        text = response.text.strip()
        if not text:
            return []
        reader = csv.DictReader(io.StringIO(text))
        return list(reader)
