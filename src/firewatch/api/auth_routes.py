"""Endpoints de inicio y cierre de sesión y página de login.

El navegador nunca almacena la clave de API. Envía la clave una sola vez, recibe
una cookie de sesión firmada y olvida la clave. Esto mantiene la credencial de larga
duración fuera de JavaScript, de las URLs y del almacenamiento del navegador.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response
from fastapi.responses import FileResponse

from firewatch.api.auth import (
    clear_session_cookie,
    issue_session,
    require_api_key,
    set_session_cookie,
    verify_session,
)
from firewatch.api.ratelimit import login_limiter
from firewatch.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(tags=["auth"])

STATIC_DIR = Path(__file__).resolve().parents[1] / "static"


def enforce_login_limit(request: Request) -> str:
    """
    Aplica límite de tasa (rate limiting) sobre ``POST /auth/login`` por dirección de cliente.

    Se declara como dependencia y se sitúa en PRIMER lugar en la ruta para resolverse
    antes de ``require_api_key``. El orden importa: FastAPI resuelve dependencias en el orden
    de los parámetros, por lo que colocar el chequeo dentro del cuerpo del handler solo se
    ejecutaría tras haber aceptado la clave, permitiendo intentos fallidos ilimitados sin freno.
    Limitar inicios de sesión exitosos carecería de sentido: el ataque de fuerza bruta es el que falla.
    """
    return login_limiter.enforce(request)


@router.post("/auth/login")
def login(
    address: str = Depends(enforce_login_limit),
    response: Response = None,
    x_api_key: str = Depends(require_api_key),
) -> dict:
    """
    Intercambia la clave de API por una cookie de sesión.

    ``require_api_key`` ya validó la cabecera, por lo que alcanzar este punto
    significa que la credencial era válida.
    """
    set_session_cookie(response, issue_session())
    logger.info("Session issued for %s", address)
    return {"status": "authenticated", "expires_in": settings.SESSION_MAX_AGE}


@router.post("/auth/logout")
def logout(response: Response) -> dict:
    """Elimina la cookie de sesión. Es seguro invocarlo sin una sesión activa."""
    clear_session_cookie(response)
    return {"status": "signed-out"}


@router.get("/auth/session")
def session_status(
    firewatch_session: Optional[str] = Cookie(default=None),
) -> dict:
    """Endpoint ligero que consulta el dashboard para decidir si mostrar el formulario de login."""
    return {"authenticated": verify_session(firewatch_session)}


@router.get("/login", include_in_schema=False)
def login_page():
    """Sirve la página de login. Debe permanecer público o nadie podría autenticarse."""
    login_html = STATIC_DIR / "login.html"
    if not login_html.exists():
        raise HTTPException(status_code=404, detail="Login page not found")
    return FileResponse(login_html)
