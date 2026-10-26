"""Aplicación FastAPI para FireWatch."""

from pathlib import Path

import logging

import psycopg2
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from firewatch.api.auth import SESSION_COOKIE, require_session, verify_session
from firewatch.api.auth_routes import router as auth_router
from firewatch.api.endpoints import router
from firewatch.config import settings


def _configure_app_logging() -> None:
    """
    Hace visibles los registros de nivel INFO propios de la aplicación.

    uvicorn instala su propia configuración de logging, lo que deja el logger raíz
    en WARNING. Cualquier mensaje a nivel INFO de `firewatch.*` era descartado.

    El manejador se adjunta al logger `firewatch` en vez de a la raíz para que los
    logs de acceso y error propios de uvicorn conserven su configuración.
    """
    app_logger = logging.getLogger("firewatch")
    if any(getattr(h, "_firewatch", False) for h in app_logger.handlers):
        return  # Ya configurado, ej. reimportado por un test
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s"))
    handler._firewatch = True  # Marcador para que el condicional superior sea fiable
    app_logger.addHandler(handler)
    app_logger.setLevel(logging.INFO)
    # Detener propagación para que los registros no se dupliquen si el root logger está configurado
    app_logger.propagate = False


_configure_app_logging()

# La documentación interactiva expone todo el contrato de la API. Es una ayuda de desarrollo;
# exponerla en un despliegue público ofrece reconocimiento gratuito a atacantes.
IS_PRODUCTION = settings.APP_ENV == "production"

STATIC_DIR = Path(__file__).resolve().parents[1] / "static"

app = FastAPI(
    title="FireWatch",
    version=settings.APP_VERSION,
    docs_url=None if IS_PRODUCTION else "/docs",
    redoc_url=None if IS_PRODUCTION else "/redoc",
    openapi_url=None if IS_PRODUCTION else "/openapi.json",
    description="""
    FireWatch: Sistema de alerta temprana de incendios forestales.

    - Pipeline NASA FIRMS -> Kafka -> PostgreSQL/PostGIS -> FastAPI
    - Fase 2: Modelado de riesgo físico y enriquecimiento meteorológico
    - Dashboard Web GIS interactivo en /dashboard
    """,
)

app.include_router(auth_router)
app.include_router(router, prefix="/api/v1")

if STATIC_DIR.exists():
    # El montaje estático sirve la estructura del dashboard, por lo que debe situarse tras
    # la misma validación de sesión que /dashboard. Sin esto, /static/index.html sería accesible
    # públicamente y la única barrera sería una redirección JS de cliente eludible desactivando JavaScript.
    #
    # login.html deliberadamente NO se sirve por aquí: el router de auth lo lee directamente
    # con FileResponse, e incluye sus propios estilos CSS en línea sin depender de estos activos protegidos.
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.middleware("http")
async def guard_static_assets(request: Request, call_next):
    """
    Exige sesión activa para cualquier recurso bajo ``/static``.

    El método ``mount()`` de Starlette no admite dependencias por montaje, por lo que este
    middleware cumple dicha función validando la cookie de sesión.
    """
    if request.url.path.startswith("/static/") and not verify_session(
        request.cookies.get(SESSION_COOKIE)
    ):
        return JSONResponse(status_code=401, content={"detail": "Authentication required"})
    return await call_next(request)


@app.get("/", include_in_schema=False)
def root():
    """Endpoint raíz que retorna información básica y punto de entrada al dashboard."""
    return {
        "name": "FireWatch",
        "version": settings.APP_VERSION,
        "status": "running",
        "dashboard": "/dashboard",
    }


@app.get("/dashboard", include_in_schema=False)
def dashboard(_: str = Depends(require_session)):
    """Dashboard GIS interactivo con mapa. Requiere sesión activa."""
    if STATIC_DIR.exists() and (STATIC_DIR / "index.html").exists():
        return FileResponse(STATIC_DIR / "index.html")
    raise HTTPException(status_code=404, detail="Dashboard static files not found")


@app.get("/stats", include_in_schema=False)
def stats_page(_: str = Depends(require_session)):
    """Sección de estadísticas agregadas por provincia y municipio. Requiere sesión."""
    if STATIC_DIR.exists() and (STATIC_DIR / "stats.html").exists():
        return FileResponse(STATIC_DIR / "stats.html")
    raise HTTPException(status_code=404, detail="Statistics static files not found")


@app.get("/health", include_in_schema=False)
def health():
    """Sonda de salud básica del microservicio."""
    return {"status": "healthy"}


@app.get("/health/db", include_in_schema=False)
def health_db():
    """Comprobación de conectividad con PostgreSQL/PostGIS."""
    try:
        conn = psycopg2.connect(settings.POSTGRES_DSN, connect_timeout=3)
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Database unreachable: {exc}",
        ) from exc