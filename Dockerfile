# FireWatch — un único Dockerfile, cuatro targets por nivel (tier).
#
#   --target acquisition  ->  refresh   (FIRMS/Open-Meteo -> Kafka)
#   --target processing   ->  processor (Kafka -> scoring -> PostGIS)
#   --target forecast     ->  forecast  (Open-Meteo -> ML -> fire_predictions)
#   --target api          ->  api       (FastAPI + dashboard)
#
# Cada target instala exclusivamente los requisitos de su propio nivel (requirements/*.txt),
# por lo que acquisition no contiene driver de base de datos ni framework web.
#
# Ventajas del enfoque multi-stage frente a cuatro Dockerfiles independientes: una base compartida
# garantiza que la imagen base del SO, la versión de Python y el usuario no-root no diverjan.
#
# Los secretos NO se incrustan en la imagen. Cada contenedor los recibe en tiempo de ejecución
# desde el archivo de entorno en compose, por lo que .dockerignore excluye el archivo .env.

# --- Base compartida: SO, Python, usuario sin privilegios (non-root) ----
FROM python:3.14-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN useradd --create-home --uid 10001 firewatch \
    && mkdir -p /app/logs \
    && chown -R firewatch:firewatch /app

# --- Código fuente de la aplicación, compartido por los cuatro targets ----
FROM base AS source

COPY src/ ./src/
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir --no-deps -e .

# =========================================================================
# Tier 1: acquisition (adquisición e ingesta)
# =========================================================================
FROM source AS acquisition

COPY requirements/base.txt requirements/acquisition.txt ./requirements/
# Instala únicamente base.txt. Sin psycopg2-binary ni fastapi: este nivel
# no puede escribir directamente en la base de datos ni por error accidental.
RUN pip install --no-cache-dir -r requirements/acquisition.txt

USER firewatch
CMD ["python", "-m", "firewatch.runner", "daily", "--days", "3", "--interval-minutes", "30"]

# =========================================================================
# Tier 2: processing (procesamiento y persistencia)
# =========================================================================
FROM source AS processing

COPY requirements/base.txt requirements/processing.txt ./requirements/
RUN pip install --no-cache-dir -r requirements/processing.txt

USER firewatch
CMD ["python", "-m", "firewatch.runner", "process"]

# =========================================================================
# Tier 4: forecast (inferencia ML y persistencia de fire_predictions)
#
# Se define antes de api a propósito: el último stage es la imagen por defecto
# si un servicio olvida build.target, y ese fallback debe seguir siendo api.
# =========================================================================
FROM source AS forecast

COPY requirements/base.txt requirements/processing.txt requirements/forecast.txt ./requirements/
RUN pip install --no-cache-dir -r requirements/forecast.txt

USER firewatch
# 3 h: los modelos regionales de Open-Meteo se actualizan con esa cadencia.
# El modelo se monta desde ./data/models en solo lectura, no se hornea.
CMD ["python", "-m", "firewatch.runner", "forecast", "--loop", "--interval-minutes", "180"]

# =========================================================================
# Tier 3: api (servidor web y dashboard)
# =========================================================================
FROM source AS api

COPY requirements/base.txt requirements/processing.txt requirements/api.txt ./requirements/
RUN pip install --no-cache-dir -r requirements/api.txt

USER firewatch
EXPOSE 8000

# httpx está presente en base.txt y la API lo importa a través de clients/firms.py.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import httpx,sys; sys.exit(0 if httpx.get('http://127.0.0.1:8000/health', timeout=4).status_code==200 else 1)"

CMD ["uvicorn", "firewatch.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]

# Sin HEALTHCHECK en acquisition, processing ni forecast deliberadamente: estas
# imágenes no ejecutan servidor web, por lo que una sonda HTTP no aplicaría.
