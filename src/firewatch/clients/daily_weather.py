"""Descarga masiva de la serie diaria de Open-Meteo para el panel provincial.

``WeatherClient`` está diseñado para puntuar un foco concreto: pide una serie
horaria por celda y fecha. El panel provincial necesita lo contrario, 52
provincias × ~3.500 días, y una petición por día serían más de 180.000 llamadas.

El archivo ERA5 de Open-Meteo acepta un rango de fechas y devuelve la serie
diaria completa en una sola respuesta (~127 KB por provincia y 10 años). Este
cliente encapsula esa consulta, con reintentos y una caída a la API de previsión
para los días más recientes. No se usa para puntuar focos: el riesgo operativo
sigue usando ``WeatherClient``.
"""

from __future__ import annotations

import csv
import logging
import time
from datetime import date as date_cls, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import httpx

from firewatch.config import settings

logger = logging.getLogger(__name__)

DAILY_VARIABLES = (
    "temperature_2m_max",
    "temperature_2m_min",
    "relative_humidity_2m_min",
    "wind_speed_10m_max",
    "precipitation_sum",
)

# Nombre de la variable de Open-Meteo -> nombre de la columna del panel.
COLUMN_MAP = {
    "temperature_2m_max": "tmax_c",
    "temperature_2m_min": "tmin_c",
    "relative_humidity_2m_min": "rh_min",
    "wind_speed_10m_max": "wind_max_kmh",
    "precipitation_sum": "precip_mm",
}

WEATHER_FIELDS = ("province_code", "day", *COLUMN_MAP.values())


class DailyWeatherClient:
    """Cliente de la serie diaria de Open-Meteo (archivo ERA5 / previsión)."""

    def __init__(
        self,
        client: Optional[httpx.Client] = None,
        archive_url: Optional[str] = None,
        forecast_url: Optional[str] = None,
        retries: int = 2,
        retry_delay: float = 2.0,
        rate_limit_delay: float = 15.0,
        forecast_window_days: int = 92,
    ):
        self._client = client or httpx.Client()
        self.archive_url = (
            archive_url or settings.OPEN_METEO_ARCHIVE_URL
        ).rstrip("/")
        self.forecast_url = (
            forecast_url or settings.OPEN_METEO_BASE_URL
        ).rstrip("/")
        self.retries = retries
        self.retry_delay = retry_delay
        self.rate_limit_delay = rate_limit_delay
        self.forecast_window_days = forecast_window_days

    def fetch_daily(
        self,
        latitude: float,
        longitude: float,
        start_date: date_cls | str,
        end_date: date_cls | str,
    ) -> List[Dict]:
        """Devuelve una fila por día con las variables diarias de Open-Meteo.

        La respuesta del archivo se usa tal cual: no se rellenan huecos con
        valores inventados. Un día sin datos se omite, y el entrenamiento decide
        si descarta esa fila o la imputa. Inventar 25 °C y 45 % de humedad
        cambiaría el riesgo de una provincia sin que nadie lo viera.
        """
        start = _iso(start_date)
        end = _iso(end_date)
        if start > end:
            raise ValueError(f"start_date ({start}) must be on or before end_date ({end})")

        params = self._daily_params(latitude, longitude, start, end)
        rows = self._fetch(self.archive_url, params)
        if not rows and self._within_forecast_window(start, end):
            # La previsión cubre los últimos ~92 días y sirve de red cuando el
            # archivo aún no ha publicado un día reciente. No se intenta para
            # un rango de 2017-2026: la API de previsión rechaza rangos largos
            # con 400 y solo añadiría reintentos inútiles.
            rows = self._fetch(self.forecast_url, params)
        return rows

    def fetch_forecast(
        self,
        latitude: float,
        longitude: float,
        start_date: date_cls | str,
        end_date: date_cls | str,
    ) -> List[Dict]:
        """Pide la previsión directamente, sin intentar el archivo ERA5.

        El día objetivo de ``runner forecast`` es futuro: el archivo no lo
        tiene. Llamar primero al archivo solo añadiría una petición inútil y
        haría ambiguo de dónde salió cada valor. La previsión es la única
        fuente válida para ese día.
        """
        start = _iso(start_date)
        end = _iso(end_date)
        if start > end:
            raise ValueError(f"start_date ({start}) must be on or before end_date ({end})")
        return self._fetch(
            self.forecast_url,
            self._daily_params(latitude, longitude, start, end),
        )

    def _daily_params(
        self,
        latitude: float,
        longitude: float,
        start: str,
        end: str,
    ) -> Dict:
        """Parámetros comunes del archivo y de la previsión."""
        return {
            "latitude": round(latitude, 4),
            "longitude": round(longitude, 4),
            "daily": ",".join(DAILY_VARIABLES),
            "start_date": start,
            "end_date": end,
            "timezone": "GMT",
        }

    def _within_forecast_window(self, start: str, end: str) -> bool:
        start_day = date_cls.fromisoformat(start)
        end_day = date_cls.fromisoformat(end)
        today = date_cls.today()
        return (
            (end_day - start_day).days <= self.forecast_window_days
            and end_day >= today - timedelta(days=self.forecast_window_days)
        )

    def _fetch(self, url: str, params: Dict) -> List[Dict]:
        for attempt in range(self.retries + 1):
            try:
                response = self._client.get(url, params=params, timeout=120.0)
                response.raise_for_status()
                data = response.json()
                return _parse_daily(data)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    retry_after = _retry_after_seconds(exc.response)
                    delay = retry_after if retry_after is not None else (
                        self.rate_limit_delay * (attempt + 1)
                    )
                    message = "rate limited (HTTP 429)"
                else:
                    delay = self.retry_delay * (2**attempt)
                    message = str(exc)
                if attempt >= self.retries:
                    logger.warning(
                        "Daily weather request failed after %d attempts (%s): %s",
                        attempt + 1,
                        url,
                        message,
                    )
                    return []
                logger.warning(
                    "Daily weather request failed (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    self.retries + 1,
                    message,
                    delay,
                )
                time.sleep(delay)
            except (httpx.HTTPError, httpx.InvalidURL, ValueError, TypeError) as exc:
                if attempt >= self.retries:
                    logger.warning(
                        "Daily weather request failed after %d attempts (%s): %s",
                        attempt + 1,
                        url,
                        exc,
                    )
                    return []
                delay = self.retry_delay * (2**attempt)
                logger.warning(
                    "Daily weather request failed (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    self.retries + 1,
                    exc,
                    delay,
                )
                time.sleep(delay)
        return []

    def close(self) -> None:
        self._client.close()


def _iso(value: date_cls | str) -> str:
    return value.isoformat() if isinstance(value, date_cls) else str(value)[:10]


def _retry_after_seconds(response: httpx.Response) -> Optional[float]:
    """Lee ``Retry-After`` en segundos; las fechas HTTP se ignoran por sencillez."""
    raw = response.headers.get("Retry-After")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _parse_daily(data: Dict) -> List[Dict]:
    """Convierte la respuesta de Open-Meteo en filas ``{day, ...}``.

    Una respuesta con el bloque ``daily`` vacío (habitual en el borde de la
    ventana de previsión) devuelve una lista vacía, no una excepción.
    """
    daily = (data or {}).get("daily") or {}
    times = daily.get("time") or []
    if not times:
        return []

    rows: List[Dict] = []
    for index, day in enumerate(times):
        row = {"day": str(day)[:10]}
        has_value = False
        for source, target in COLUMN_MAP.items():
            series = daily.get(source) or []
            value = series[index] if index < len(series) else None
            row[target] = float(value) if value is not None else None
            has_value = has_value or value is not None
        # Un día con las cinco variables a NULL no aporta nada y solo añade
        # ruido al merge; se omite. Un día con algún hueco se conserva.
        if has_value:
            rows.append(row)
    return rows


def write_weather_csv(path: Path | str, rows: Iterable[Dict]) -> int:
    """Escribe la serie diaria en CSV y devuelve el número de filas."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=WEATHER_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in WEATHER_FIELDS})
            written += 1
    return written
