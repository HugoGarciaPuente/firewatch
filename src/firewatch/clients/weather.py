"""
Adaptador del cliente de la API meteorológica (Fase 2).

Envuelve la API de Open-Meteo para obtener temperatura, humedad relativa y velocidad del viento.
Todas las llamadas externas pasan a través de este cliente para poder simularse en los tests.

Una carga masiva histórica genera miles de registros, por lo que el cliente mantiene una
caché en memoria indexada por celdas geográficas de 0.25° (~25 km). El clima no varía
significativamente en unos pocos kilómetros, manteniendo las peticiones HTTP proporcionales
al número de celdas distintas en lugar de al número de focos detectados.
"""

from __future__ import annotations

import logging
import threading
from typing import Dict, Optional

import httpx

from firewatch.config import settings

logger = logging.getLogger(__name__)


class WeatherClient:
    """Cliente para obtener métricas meteorológicas actuales (temperatura, humedad, viento)."""

    def __init__(
        self,
        client: Optional[httpx.Client] = None,
        base_url: Optional[str] = None,
        cell_degrees: Optional[float] = None,
    ):
        self._client = client or httpx.Client()
        self.base_url = (base_url or settings.OPEN_METEO_BASE_URL).rstrip("/")
        self.cell_degrees = (
            cell_degrees if cell_degrees is not None else float(settings.WEATHER_CACHE_CELL)
        )
        self._cache: Dict[tuple, Optional[Dict[str, float]]] = {}
        self._lock = threading.Lock()

    def _cell(self, latitude: float, longitude: float) -> tuple:
        """Ajusta las coordenadas a una celda de cuadrícula para compartir consultas cercanas."""
        return (
            round(latitude / self.cell_degrees),
            round(longitude / self.cell_degrees),
        )

    def get_weather(self, latitude: float, longitude: float) -> Optional[Dict[str, float]]:
        """
        Obtiene las condiciones meteorológicas actuales para las coordenadas dadas.

        Retorna
        -------
        dict o None
            {
                "temperature_c": float,
                "relative_humidity": float,
                "wind_speed_kmh": float,
            }

        Los resultados se cachean por celda geográfica; las consultas fallidas también
        se cachean para que un fallo en el upstream no provoque un reintento por registro.
        """
        key = self._cell(latitude, longitude)

        with self._lock:
            if key in self._cache:
                return self._cache[key]

        result = self._fetch(latitude, longitude)

        with self._lock:
            self._cache[key] = result
        return result

    def _fetch(self, latitude: float, longitude: float) -> Optional[Dict[str, float]]:
        params = {
            "latitude": round(latitude, 4),
            "longitude": round(longitude, 4),
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        }
        try:
            response = self._client.get(self.base_url, params=params, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            current = data.get("current", {})
            return {
                "temperature_c": float(current.get("temperature_2m", 25.0)),
                "relative_humidity": float(current.get("relative_humidity_2m", 45.0)),
                "wind_speed_kmh": float(current.get("wind_speed_10m", 15.0)),
            }
        except Exception as exc:
            logger.warning("Consulta meteorológica fallida para (%.4f, %.4f): %s", latitude, longitude, exc)
            return None