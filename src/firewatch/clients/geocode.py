"""
Cliente de geocodificación inversa (Nominatim / OpenStreetMap).

FIRMS devuelve las anomalías térmicas como coordenadas sin información toponímica:
sin nombre de lugar, municipio ni región administrativa. Para mostrar una ubicación
legible junto a las coordenadas, el punto debe resolverse contra una toponimia nombrada.

Envuelve el endpoint reverse de Nominatim para que todas las llamadas externas pasen
por un único adaptador simulable, de forma análoga a ``clients/weather.py``.

Por qué NO se cachea por celda geográfica:
------------------------------------------
``WeatherClient`` cachea en celdas de 0.25 grados (~25 km) porque las variables macroclimáticas
no varían sensiblemente en esa distancia. Una *localidad* sí lo hace: dos focos separados
500 m pueden pertenecer a municipios distintos en una frontera, y devolver el nombre del
centro de la celda asignaría una etiqueta errónea a un incendio real. Por ello, este cliente
cachea por coordenada exacta.

Límite de tasa (Rate limiting):
-------------------------------
La política de uso de la OpenStreetMap Foundation para Nominatim limita este uso a 1 req/s.
El cliente impone el intervalo internamente y cachea también las respuestas fallidas para
no repetir intentos continuos ante incidencias del servicio upstream.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Dict, Optional

import httpx

from firewatch.config import settings

logger = logging.getLogger(__name__)

# El vocabulario de asentamientos en Nominatim varía según el territorio (un municipio
# español puede catalogarse como `city`, `town` o `village` según su tamaño).
# `county` o `state` representan la provincia en la mayor parte del área cubierta.
_LOCALITY_KEYS = (
    "city",
    "town",
    "village",
    "municipality",
    "hamlet",
    "suburb",
    "isolated_dwelling",
    "farm",
)


class GeoCodeClient:
    """Geocodifica inversamente coordenadas a nombres de lugar mediante Nominatim."""

    def __init__(
        self,
        client: Optional[httpx.Client] = None,
        base_url: Optional[str] = None,
        min_interval: Optional[float] = None,
    ):
        self._client = client or httpx.Client()
        self.base_url = (base_url or settings.NOMINATIM_BASE_URL).rstrip("/")
        self.min_interval = (
            min_interval if min_interval is not None else float(settings.GEOCODE_MIN_INTERVAL)
        )
        # Indexado por coordenada exacta, deliberadamente NO por celda redondeada.
        self._cache: Dict[tuple, Optional[Dict[str, str]]] = {}
        self._lock = threading.Lock()
        self._last_call = 0.0
        self.hits = 0
        self.misses = 0

    @staticmethod
    def _key(latitude: float, longitude: float) -> tuple:
        """Clave de caché por coordenada exacta redondeada a 4 decimales (~11 m)."""
        return (round(float(latitude), 4), round(float(longitude), 4))

    def get_location(self, latitude: float, longitude: float) -> Optional[Dict[str, str]]:
        """
        Geocodifica inversamente una coordenada.

        Retorna
        -------
        dict o None
            {
                "locality": str,     # municipio / localidad, "" si es desconocido
                "province": str,     # provincia / departamento / distrito
                "community": str,    # comunidad autónoma / región
                "country": str,
                "country_code": str, # ej. "es", "fr", "pt"
                "display_name": str,
            }

        ``None`` indica que la consulta falló o el punto carece de localidad mapeada (mar abierto).
        Ambos se cachean para no repetir llamadas innecesarias.
        """
        try:
            key = self._key(latitude, longitude)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "Coordenadas inválidas para geocodificación inversa (%r, %r): %s",
                latitude,
                longitude,
                exc,
            )
            return None

        with self._lock:
            if key in self._cache:
                self.hits += 1
                return self._cache[key]

        self.misses += 1
        result = self._fetch(latitude, longitude)

        with self._lock:
            self._cache[key] = result
        return result

    def _throttle(self) -> None:
        """Espera hasta que haya transcurrido al menos min_interval desde la última petición."""
        if self.min_interval <= 0:
            return
        with self._lock:
            wait = self.min_interval - (time.monotonic() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()

    def _fetch(self, latitude: float, longitude: float) -> Optional[Dict[str, str]]:
        params = {
            "format": "jsonv2",
            "lat": round(float(latitude), 5),
            "lon": round(float(longitude), 5),
            # zoom=10 resuelve a nivel de municipio; zooms mayores devuelven nombres de calle
            "zoom": 10,
            "addressdetails": 1,
        }
        headers = {"User-Agent": settings.NOMINATIM_USER_AGENT}
        try:
            self._throttle()
            response = self._client.get(
                f"{self.base_url}/reverse", params=params, headers=headers, timeout=15.0
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            logger.warning(
                "Geocodificación inversa fallida para (%.4f, %.4f): %s", latitude, longitude, exc
            )
            return None

        return self._parse(data)

    @staticmethod
    def _parse(data: Dict) -> Optional[Dict[str, str]]:
        """Convierte la respuesta de Nominatim en el diccionario plano que persiste la aplicación."""
        address = data.get("address") or {}
        locality = ""
        for key in _LOCALITY_KEYS:
            value = address.get(key)
            if value:
                locality = str(value)
                break

        display_name = str(data.get("display_name") or "")
        if not locality and not display_name:
            # Nominatim responde {"error": "Unable to geocode"} para puntos sin localidad mapeada
            return None

        # La provincia NO es `state` en España: ahí `state` es la comunidad
        # autónoma (Extremadura), y la provincia (Cáceres) llega en
        # `state_district` o `province`. En Francia `county` es el departamento
        # (Ariège) y `state` la región (Occitanie); en Portugal `county` es el
        # distrito (Aveiro). Por eso el orden es provincia > state_district >
        # county > state, y `community` conserva siempre el nivel superior.
        return {
            "locality": locality,
            "province": str(
                address.get("province")
                or address.get("state_district")
                or address.get("county")
                or address.get("state")
                or ""
            ),
            "community": str(address.get("state") or ""),
            "country": str(address.get("country") or ""),
            "country_code": str(address.get("country_code") or ""),
            "display_name": display_name,
        }

    def cache_stats(self) -> Dict[str, int]:
        with self._lock:
            return {"hits": self.hits, "misses": self.misses, "size": len(self._cache)}
