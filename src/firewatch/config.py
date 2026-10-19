import os
from pathlib import Path
from dotenv import load_dotenv

# Carga el archivo .env desde la raíz del proyecto (src/firewatch/config.py -> subir 2 niveles)
load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")


class Settings:
    """Configuración cargada a partir de variables de entorno."""

    # NASA FIRMS
    NASA_FIRMS_BASE_URL: str = os.getenv(
        "NASA_FIRMS_BASE_URL", "https://firms.modaps.eosdis.nasa.gov/api"
    )
    NASA_FIRMS_BBOX: str = os.getenv("NASA_FIRMS_BBOX", "-90,-180,90,180")  # global
    NASA_FIRMS_DATE: str = os.getenv("NASA_FIRMS_DATE", "")  # vacío = hoy
    # Secreto: registrarse en https://firms.modaps.eosdis.nasa.gov/api/area/
    NASA_FIRMS_API_KEY: str = os.getenv("NASA_FIRMS_API_KEY", "")

    # Regiones de seguimiento. El área continental europea genera decenas de
    # miles de focos diarios que no aportan a un aviso de riesgo sobre estos
    # territorios, así que el scope se limita a España (península e islas).
    #
    # Para la Península se usan dos cajas para seguir la diagonal SW-NE de la costa:
    # 1. `peninsula`: cuerpo principal y norte (37.0°N a 43.9°N, -9.6°W a 3.6°E),
    #    llegando hasta Cap de Creus al este (3.32°E) pero manteniéndose al norte
    #    de la costa argelina (que termina en ~36.91°N).
    # 2. `peninsula_sur`: franja sur de Andalucía (35.9°N a 37.1°N, -9.6°W a -1.8°W),
    #    incluyendo Tarifa (36.0°N) y Cabo de Gata (-2.19°W), pero cortando en
    #    -1.8°W para no captar la costa argelina que se sitúa al este.
    REGION_BBOXES: dict = {
        "peninsula": "37.0,-9.6,43.9,3.6",
        "peninsula_sur": "35.9,-9.6,37.1,-1.8",
        "canarias": "27.4,-18.7,29.6,-13.1",
        "baleares": "38.5,1.2,40.7,4.7",
    }
    # Regiones activas por defecto (se pueden sobreescribir por CLI).
    FIREWATCH_REGIONS: str = os.getenv(
        "FIREWATCH_REGIONS", "peninsula,peninsula_sur,canarias,baleares"
    )

    # Kafka (Fase 1: topic de ingesta; Fase 2: topic de alertas)
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_INGEST_TOPIC: str = os.getenv("KAFKA_INGEST_TOPIC", "firms-ingest")
    KAFKA_RISK_TOPIC: str = os.getenv("KAFKA_RISK_TOPIC", "fire-risk-alerts")

    # Adaptador de meteorología / Open-Meteo (Fase 2)
    OPEN_METEO_BASE_URL: str = os.getenv(
        "OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1/forecast"
    )
    ENABLE_WEATHER_ENRICHMENT: bool = os.getenv(
        "ENABLE_WEATHER_ENRICHMENT", "true"
    ).lower() in ("true", "1", "yes")
    # Tamaño de celda (grados) de la caché de clima. 0.25 ≈ 25 km: el clima no
    # varía de forma apreciable en esa escala y reduce las llamadas a Open-Meteo
    # de una por registro a una por celda.
    WEATHER_CACHE_CELL: float = os.getenv("WEATHER_CACHE_CELL", "0.25")

    # Geocodificación inversa / Nominatim. FIRMS solo devuelve coordenadas, así
    # que la localidad hay que resolverla contra OpenStreetMap.
    NOMINATIM_BASE_URL: str = os.getenv(
        "NOMINATIM_BASE_URL", "https://nominatim.openstreetmap.org"
    )
    # La política de uso de Nominatim exige identificar el cliente y limita a
    # 1 petición/segundo. Un User-Agent honrado es un requisito, no una cortesía.
    NOMINATIM_USER_AGENT: str = os.getenv(
        "NOMINATIM_USER_AGENT", "FireWatch/0.2 (university practice project)"
    )
    # Segundos mínimos entre peticiones. 1.1 da margen sobre el límite de 1/s.
    GEOCODE_MIN_INTERVAL: float = os.getenv("GEOCODE_MIN_INTERVAL", "1.1")
    # La geocodificación es un extra: si Nominatim está caído, el foco se guarda
    # sin localidad y `runner geocode-backfill` rellena los huecos después.
    ENABLE_GEOCODING: bool = os.getenv("ENABLE_GEOCODING", "true").lower() in (
        "true",
        "1",
        "yes",
    )

    # PostgreSQL/PostGIS
    POSTGRES_DSN: str = os.getenv(
        "POSTGRES_DSN", "postgresql://firewatch:firewatch@localhost:5432/firewatch"
    )

    # Aplicación
    APP_NAME: str = os.getenv("APP_NAME", "firewatch")
    APP_VERSION: str = os.getenv("APP_VERSION", "0.2.0")
    APP_ENV: str = os.getenv("APP_ENV", "development")  # development | production

    # Autenticación (ver api/auth.py). API_KEY es la clave de operador compartida;
    # SESSION_SECRET firma las cookies del navegador y debe ser un secreto *distinto*,
    # para que la filtración de uno no comprometa el otro.
    API_KEY: str = os.getenv("API_KEY", "")
    SESSION_SECRET: str = os.getenv("SESSION_SECRET", "")
    SESSION_MAX_AGE: int = int(os.getenv("SESSION_MAX_AGE", "43200"))  # 12h
    # Activar a true al servir sobre HTTPS; false para desarrollo local en HTTP.
    SESSION_COOKIE_SECURE: bool = os.getenv(
        "SESSION_COOKIE_SECURE", "false"
    ).lower() in ("true", "1", "yes")


settings = Settings()


def _validate_settings() -> None:
    """
    Falla de forma explícita en tiempo de importación cuando faltan secretos de producción.

    Una clave ausente que degrade silenciosamente a "sin autenticación" es mucho
    peor que un proceso que se niegue a arrancar.
    """
    if settings.APP_ENV != "production":
        return

    missing = [n for n in ("API_KEY", "SESSION_SECRET") if not getattr(settings, n)]
    if missing:
        raise RuntimeError(
            "APP_ENV=production requires these environment variables: "
            + ", ".join(missing)
            + '. Generate SESSION_SECRET with: python -c "import secrets;'
            ' print(secrets.token_urlsafe(48))"'
        )
    if settings.API_KEY == settings.SESSION_SECRET:
        raise RuntimeError(
            "API_KEY and SESSION_SECRET must differ. Using the same value means "
            "anyone holding one credential can forge sessions."
        )
    if len(settings.API_KEY) < 16:
        raise RuntimeError("API_KEY must be at least 16 characters.")


_validate_settings()


def resolve_regions(bbox_arg=None, regions_arg=None):
    """
    Devuelve ``[(etiqueta, bbox), ...]`` para las áreas a monitorizar.

    Prioridad: ``bbox`` explícito > ``regions`` explícito > ``FIREWATCH_REGIONS``
    del entorno. Lanza ``SystemExit`` ante una región desconocida para que un error
    tipográfico falle de inmediato en lugar de no ingerir nada en silencio.

    Reside aquí (no en ``runner``) para que tanto la CLI como la API puedan usarlo
    sin que la API tenga que importar la capa de la CLI.
    """
    if bbox_arg:
        return [("bbox", bbox_arg)]

    source = regions_arg or settings.FIREWATCH_REGIONS
    names = [r.strip().lower() for r in source.split(",") if r.strip()]

    # Si se pide 'peninsula' sin especificar 'peninsula_sur', incluir ambas
    # para que cubra la península completa de forma transparente.
    expanded = []
    for r in names:
        expanded.append(r)
        if r == "peninsula" and "peninsula_sur" not in names and "peninsula_sur" in settings.REGION_BBOXES:
            expanded.append("peninsula_sur")

    unknown = [r for r in expanded if r not in settings.REGION_BBOXES]
    if unknown:
        raise ValueError(
            f"Unknown region(s): {', '.join(unknown)}. "
            f"Available: {', '.join(sorted(settings.REGION_BBOXES))}"
        )
    return [(name, settings.REGION_BBOXES[name]) for name in expanded]
