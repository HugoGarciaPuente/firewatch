"""Límite de tasa (rate limiting) por IP de cliente para los endpoints de autenticación.

La clave de API compartida es lo único que separa el acceso público del conjunto
de datos de incendios, y ``POST /auth/login`` recibe intentos de acceso. Sin un mecanismo
de limitación, los ataques de fuerza bruta son factibles y difíciles de contener.

Implementa un contador de ventana fija en memoria dentro del proceso.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import DefaultDict, Tuple

from fastapi import HTTPException, Request, status

# Política por defecto: 5 intentos de login por minuto por cliente. Suficiente para que un
# usuario que se equivoque al teclear no quede bloqueado, y suficientemente restrictivo
# para impedir ataques de fuerza bruta sobre claves de longitud adecuada.
DEFAULT_LIMIT = 5
DEFAULT_WINDOW_SECONDS = 60.0


class RateLimiter:
    """Contador de ventana fija indexado por dirección de cliente."""

    def __init__(self, limit: int = DEFAULT_LIMIT, window_seconds: float = DEFAULT_WINDOW_SECONDS):
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        self.limit = limit
        self.window_seconds = window_seconds
        self._hits: DefaultDict[str, list[float]] = defaultdict(list)
        self._lock = threading.Lock()

    @staticmethod
    def client_address(request: Request) -> str:
        """
        Resuelve la dirección del emisor de la petición.

        Detrás de un proxy inverso, ``request.client.host`` representa al proxy. Con
        ``--proxy-headers`` en uvicorn, ``X-Forwarded-For`` contiene la IP real del cliente.
        """
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            # La primera entrada a la izquierda es el cliente original; el resto son proxies.
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def check(self, key: str) -> Tuple[bool, int, float]:
        """
        Registra un intento.

        Devuelve ``(permitido, restantes, segundos_para_reintento)``. Un intento rechazado
        consume igualmente su ranura para evitar que peticiones masivas mantengan la ventana abierta.
        """
        now = time.monotonic()
        with self._lock:
            hits = [t for t in self._hits[key] if now - t < self.window_seconds]
            if len(hits) >= self.limit:
                oldest = min(hits)
                retry_after = max(0.0, self.window_seconds - (now - oldest))
                self._hits[key] = hits
                return False, 0, retry_after
            hits.append(now)
            self._hits[key] = hits
            remaining = self.limit - len(hits)
        return True, remaining, 0.0

    def reset(self, key: str | None = None) -> None:
        """Limpia la ventana de un cliente o todas si key es None."""
        with self._lock:
            if key is None:
                self._hits.clear()
            else:
                self._hits.pop(key, None)

    def _prune(self) -> None:
        """Elimina ventanas completamente expiradas de forma oportunista."""
        now = time.monotonic()
        with self._lock:
            for key in [k for k, v in self._hits.items() if all(now - t >= self.window_seconds for t in v)]:
                del self._hits[key]

    def enforce(self, request: Request) -> str:
        """
        Lanza HTTP 429 si el cliente superó el límite; en caso contrario devuelve su dirección.

        Lanza HTTPException con cabecera ``Retry-After``.
        """
        self._prune()
        address = self.client_address(request)
        allowed, remaining, retry_after = self.check(address)
        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    f"Too many authentication attempts. Try again in "
                    f"{int(retry_after) + 1} seconds."
                ),
                headers={
                    "Retry-After": str(int(retry_after) + 1),
                    "X-RateLimit-Limit": str(self.limit),
                    "X-RateLimit-Remaining": "0",
                },
            )
        return address


# Instancia única compartida por las rutas de autenticación a nivel de módulo.
login_limiter = RateLimiter()


def reset_login_limiter() -> None:
    """Limpia todos los contadores. Usado por los tests para evitar contaminación cruzada."""
    login_limiter.reset()
