"""
Adaptador del cliente de la API de NASA FIRMS.

Envuelve la API REST de NASA FIRMS para obtener datos de anomalías térmicas.
Todas las llamadas externas a la API pasan a través de este cliente para que
puedan simularse/mockearse en los tests.

Endpoint:
  GET {base}/area/csv/{MAP_KEY}/{SOURCE}/{AREA_COORDINATES}/{DAY_RANGE}[/{DATE}]

La API de área de FIRMS requiere una clave provista mediante la variable de
entorno ``NASA_FIRMS_API_KEY``.
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
        self._client = client or httpx.Client()
        self.base_url = (base_url or settings.NASA_FIRMS_BASE_URL).rstrip("/")
        self.api_key = (api_key if api_key is not None else settings.NASA_FIRMS_API_KEY).strip()
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

        if self.api_key:
            url = f"{self.base_url}/area/csv/{self.api_key}/{self.source}/{area_coords}/{day_range}"
            if date:
                url = f"{url}/{date}"
            params = None
        else:
            # Fallback cuando no hay clave de API configurada
            url = f"{self.base_url}/area/csv"
            params = {"bbox": bbox, "format": "json"}
            if date:
                params["date"] = date

        response = self._client.get(url, params=params, timeout=30.0)
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
