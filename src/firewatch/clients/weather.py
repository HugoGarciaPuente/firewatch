"""
Adaptador del cliente de la API meteorológica (Fase 2).

Envuelve la API de Open-Meteo para obtener temperatura, humedad relativa y velocidad del viento.
Todas las llamadas externas pasan a través de este cliente para poder simularse en los tests.

Una carga masiva histórica genera miles de registros, por lo que el cliente mantiene una
caché en memoria indexada por (celda geográfica de 0.25°, fecha). El clima no varía
significativamente en unos pocos kilómetros, manteniendo las peticiones HTTP proporcionales
al número de celdas distintas en lugar de al número de focos detectados.

La fecha forma parte de la clave: un incendio del 7 de octubre se puntúa con el clima de ese
día, no con el que haga el día en que se ejecuta el backfill. Con fecha se descarga la serie
horaria completa y se selecciona la hora del sobrevuelo del satélite (``acq_time``); los
focos nocturnos se puntúan con la noche, no con el máximo de la tarde. Sin ``acq_time`` se
usan los extremos del día (máxima temperatura, mínima humedad, máximo viento), que es la
agregación conservadora estándar de los índices de peligro.

Si la fecha queda fuera de la ventana histórica que cubre ``OPEN_METEO_BASE_URL``, la
petición falla y se devuelve ``None``; el histórico antiguo requiere la API de archivo (ERA5).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Dict, Optional

import httpx

from firewatch.config import settings

logger = logging.getLogger(__name__)

# Un upstream caído se reintenta pasado este tiempo, no en cada registro ni nunca.
_FAILURE_TTL_SECONDS = 300.0


class WeatherClient:
    """Cliente de métricas meteorológicas actuales o de una fecha concreta."""

    def __init__(
        self,
        client: Optional[httpx.Client] = None,
        base_url: Optional[str] = None,
        cell_degrees: Optional[float] = None,
        failure_ttl: float = _FAILURE_TTL_SECONDS,
    ):
        self._client = client or httpx.Client()
        self.base_url = (base_url or settings.OPEN_METEO_BASE_URL).rstrip("/")
        self.cell_degrees = (
            cell_degrees if cell_degrees is not None else float(settings.WEATHER_CACHE_CELL)
        )
        self.failure_ttl = failure_ttl
        # Con fecha se guarda la serie horaria completa; sin fecha, las métricas actuales.
        self._cache: Dict[tuple, Dict] = {}
        self._failures: Dict[tuple, float] = {}
        self._lock = threading.Lock()

    def _cell(self, latitude: float, longitude: float) -> tuple:
        """Ajusta las coordenadas a una celda de cuadrícula para compartir consultas cercanas."""
        return (
            round(latitude / self.cell_degrees),
            round(longitude / self.cell_degrees),
        )

    def get_weather(
        self,
        latitude: float,
        longitude: float,
        date: Optional[str] = None,
        acq_time: Optional[str] = None,
    ) -> Optional[Dict[str, float]]:
        """
        Obtiene las condiciones meteorológicas para las coordenadas, la fecha y la hora dadas.

        Parámetros
        ----------
        date : str, opcional
            Fecha ISO (``YYYY-MM-DD``) del incendio. Si se omite, se consultan las
            condiciones actuales.
        acq_time : str, opcional
            Hora UTC del sobrevuelo en formato HHMM de FIRMS (``"1345"``, ``"211"``).
            Se selecciona la hora entera más cercana. Si se omite o no es válida, se
            usan los extremos del día.

        Retorna
        -------
        dict o None
            {
                "temperature_c": float,       # hora del sobrevuelo o máxima del día
                "relative_humidity": float,   # hora del sobrevuelo o mínima del día
                "wind_speed_kmh": float,      # hora del sobrevuelo o máximo del día
            }

        La serie horaria se cachea por ``(celda, fecha)``, de modo que dos focos de la
        misma celda y día con horas distintas comparten una sola petición HTTP. Los
        fallos se cachean unos minutos: una caída del upstream no provoca un reintento
        por registro, pero tampoco envenena la caché durante todo el proceso.
        """
        date_key = str(date)[:10] if date is not None else None
        key = self._cell(latitude, longitude) + (date_key,)

        with self._lock:
            cached = self._cache.get(key)
            if cached is None:
                failed_at = self._failures.get(key)
                if failed_at is not None and (time.monotonic() - failed_at) < self.failure_ttl:
                    return None

        if cached is None:
            cached = self._fetch(latitude, longitude, date_key)
            with self._lock:
                if cached is None:
                    self._failures[key] = time.monotonic()
                else:
                    self._cache[key] = cached
                    self._failures.pop(key, None)
            if cached is None:
                return None

        if date_key is None:
            return cached
        return _metrics_at_hour(cached, _acq_hour(acq_time))

    def _fetch(
        self,
        latitude: float,
        longitude: float,
        date: Optional[str] = None,
    ) -> Optional[Dict]:
        params = {
            "latitude": round(latitude, 4),
            "longitude": round(longitude, 4),
        }
        if date is None:
            params["current"] = "temperature_2m,relative_humidity_2m,wind_speed_10m"
        else:
            # 24 h del día en UTC: FIRMS da acq_time en UTC y Open-Meteo usa GMT por
            # defecto, así que la hora del sobrevuelo se indexa sin conversión.
            params["hourly"] = "temperature_2m,relative_humidity_2m,wind_speed_10m"
            params["start_date"] = date
            params["end_date"] = date
            params["timezone"] = "GMT"

        try:
            response = self._client.get(self.base_url, params=params, timeout=10.0)
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, httpx.InvalidURL, ValueError) as exc:
            logger.warning(
                "Consulta meteorológica fallida para (%.4f, %.4f) %s: %s",
                latitude,
                longitude,
                date or "actual",
                exc,
            )
            return None

        if not isinstance(data, dict):
            logger.warning("Respuesta meteorológica inesperada para %s", date or "actual")
            return None

        if date is None:
            return _current_metrics(data.get("current") or {})

        hourly = data.get("hourly") or {}
        return hourly if _has_hourly_series(hourly) else None


def _acq_hour(acq_time: Optional[str]) -> Optional[int]:
    """Hora UTC (0-23) más cercana al sobrevuelo a partir del HHMM de FIRMS.

    FIRMS no rellena con ceros: ``"211"`` es 02:11, no 21:10. Por eso se parsea
    como entero y se separan las dos últimas cifras como minutos.
    """
    if acq_time is None:
        return None
    try:
        value = int(str(acq_time).strip())
    except (TypeError, ValueError):
        return None
    if not 0 <= value <= 2359:
        return None
    hour, minute = divmod(value, 100)
    if hour > 23 or minute > 59:
        return None
    return min(23, int(round(hour + minute / 60.0)))


def _has_hourly_series(hourly: Dict) -> bool:
    """Comprueba que la respuesta trae las tres series con algún valor útil."""
    try:
        return all(
            isinstance(hourly[key], list) and any(v is not None for v in hourly[key])
            for key in ("temperature_2m", "relative_humidity_2m", "wind_speed_10m")
        )
    except (KeyError, TypeError):
        return False


def _metrics_at_hour(hourly: Dict, hour: Optional[int]) -> Optional[Dict[str, float]]:
    """Selecciona la hora del sobrevuelo; sin hora, cae a los extremos del día."""
    if hour is None:
        return _daily_extremes(hourly)

    try:
        temperature = hourly["temperature_2m"][hour]
        humidity = hourly["relative_humidity_2m"][hour]
        wind = hourly["wind_speed_10m"][hour]
    except (KeyError, IndexError, TypeError):
        return None

    if temperature is None or humidity is None or wind is None:
        return None

    return {
        "temperature_c": float(temperature),
        "relative_humidity": float(humidity),
        "wind_speed_kmh": float(wind),
    }


def _current_metrics(current: Dict) -> Optional[Dict[str, float]]:
    """Extrae las métricas actuales; ``None`` si falta cualquier campo.

    No se inventan valores de relleno: un 25 °C / 45 % fabricado cambiaría el
    nivel de riesgo de un evento sin que nadie lo note.
    """
    try:
        return {
            "temperature_c": float(current["temperature_2m"]),
            "relative_humidity": float(current["relative_humidity_2m"]),
            "wind_speed_kmh": float(current["wind_speed_10m"]),
        }
    except (KeyError, TypeError, ValueError):
        return None


def _daily_extremes(hourly: Dict) -> Optional[Dict[str, float]]:
    """Agrega las horas del día a los extremos usados por el índice de peligro."""
    try:
        temps = [float(v) for v in hourly["temperature_2m"] if v is not None]
        humidities = [float(v) for v in hourly["relative_humidity_2m"] if v is not None]
        winds = [float(v) for v in hourly["wind_speed_10m"] if v is not None]
    except (KeyError, TypeError, ValueError):
        return None

    if not temps or not humidities or not winds:
        return None

    return {
        "temperature_c": max(temps),
        "relative_humidity": min(humidities),
        "wind_speed_kmh": max(winds),
    }
