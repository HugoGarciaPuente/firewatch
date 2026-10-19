"""
Autenticación para la API de FireWatch.

Dos mecanismos deliberadamente separados:

- :func:`require_api_key` -- autenticación de máquina/servicio. El cliente presenta la
  ``API_KEY`` compartida en la cabecera ``X-API-Key``. Utilizado por ``POST /auth/login``
  para intercambiar la clave por una sesión de navegador.
- :func:`require_session` -- autenticación de navegador. El cliente presenta una cookie
  de sesión firmada emitida por dicho intercambio. Utilizado por cada endpoint que invoca el dashboard.

Mantenerlos separados es fundamental: la clave de API nunca reside en el código
JavaScript del navegador ni en una URL, por lo que un ataque XSS persistente o un
enlace compartido no pueden filtrarla.

La cookie de sesión está firmada con HMAC-SHA256 de la biblioteca estándar en lugar de
añadir dependencias externas. La carga útil es deliberadamente mínima (marcas temporales de
emisión y expiración exclusivamente) -- la cookie concede acceso, no transporta datos.
"""

from __future__ import annotations

import base64
import hmac
import json
import logging
import time
from hashlib import sha256
from typing import Optional

from fastapi import Cookie, Header, HTTPException, Request, status

from firewatch.config import settings

logger = logging.getLogger(__name__)

SESSION_COOKIE = "firewatch_session"
_UNAUTHORIZED = status.HTTP_401_UNAUTHORIZED


def _source(request: Request) -> str:
    """Dirección del cliente obtenida por mejor esfuerzo para el registro de auditoría (ver ratelimit.py)."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# ---------------------------------------------------------------------------
# Clave de API (API key)
# ---------------------------------------------------------------------------


def verify_api_key(provided: Optional[str]) -> bool:
    """
    Comparación en tiempo constante contra la clave configurada.

    ``compare_digest`` evita la filtración de la clave mediante análisis de tiempos de respuesta.
    """
    expected = settings.API_KEY
    if not expected or not provided:
        return False
    return hmac.compare_digest(provided, expected)


async def require_api_key(
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
) -> str:
    """
    Dependencia que protege los endpoints de servicio: exige una cabecera ``X-API-Key`` válida.

    Los rechazos se registran con la dirección de origen y el User-Agent. Este nivel de detalle
    es esencial: sin él, un acierto tras miles de intentos produciría una línea de log idéntica
    y resultaría imposible de detectar.
    """
    if not verify_api_key(x_api_key):
        logger.warning(
            "Petición rechazada con API key ausente o inválida desde %s ua=%r",
            _source(request),
            request.headers.get("user-agent", "-")[:80],
        )
        raise HTTPException(
            status_code=_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return "api_key"


# ---------------------------------------------------------------------------
# Cookie de sesión
# ---------------------------------------------------------------------------


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def issue_session(max_age: Optional[int] = None) -> str:
    """Crea un token de sesión firmado válido durante ``max_age`` segundos."""
    ttl = max_age if max_age is not None else settings.SESSION_MAX_AGE
    now = int(time.time())
    payload = _b64encode(json.dumps({"iat": now, "exp": now + ttl}).encode())
    signature = hmac.new(
        settings.SESSION_SECRET.encode(), payload.encode(), sha256
    ).digest()
    return payload + "." + _b64encode(signature)


def verify_session(token: Optional[str]) -> bool:
    """
    Valida la firma y expiración de un token de sesión.

    La firma se comprueba antes de confiar en la carga útil, y una entrada malformada
    nunca eleva una excepción: una cookie no válida simplemente no es una sesión válida.
    """
    secret = settings.SESSION_SECRET
    if not secret or not token:
        return False

    payload, _, signature = token.rpartition(".")
    if not payload or not signature:
        return False

    expected = hmac.new(secret.encode(), payload.encode(), sha256).digest()
    try:
        provided_sig = _b64decode(signature)
    except Exception:
        return False
    if not hmac.compare_digest(provided_sig, expected):
        return False

    try:
        claims = json.loads(_b64decode(payload))
    except Exception:
        return False

    return int(time.time()) < int(claims.get("exp", 0))


async def require_session(firewatch_session: Optional[str] = Cookie(default=None)) -> str:
    """Dependencia que protege los endpoints del navegador: exige una cookie de sesión válida."""
    if not verify_session(firewatch_session):
        logger.warning("Petición rechazada sin una sesión válida")
        raise HTTPException(
            status_code=_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Session"},
        )
    return "session"


def set_session_cookie(response, token: str) -> None:
    """
    Adjunta la cookie de sesión a la respuesta.

    ``httponly`` impide el acceso desde JS, ``secure`` previene su envío en HTTP plano,
    y ``samesite=strict`` bloquea envíos cruzados entre sitios (CSRF).
    """
    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        max_age=settings.SESSION_MAX_AGE,
        httponly=True,
        secure=settings.SESSION_COOKIE_SECURE,
        samesite="strict",
        path="/",
    )


def clear_session_cookie(response) -> None:
    response.delete_cookie(key=SESSION_COOKIE, path="/")
