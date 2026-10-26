# FireWatch

Sistema de monitorización de incendios forestales en España. Descarga anomalías térmicas de satélite desde NASA FIRMS, les añade datos meteorológicos de Open-Meteo (de la fecha y hora de cada foco), calcula un índice de riesgo para cada foco y lo muestra en un mapa web.

## Cómo funciona

```
NASA FIRMS -> ingesta -> Kafka -> procesador (+ Open-Meteo) -> PostgreSQL/PostGIS -> API FastAPI + dashboard
```

1. El servicio de ingesta pide a FIRMS los focos de las últimas fechas y los publica en un topic de Kafka.
2. El procesador los consume, consulta temperatura, humedad y viento para la fecha y hora del sobrevuelo, calcula el riesgo y guarda el resultado en PostGIS.
3. La API expone los datos y sirve un dashboard con Leaflet (mapa de puntos, mapa de calor, clústeres y un calendario para elegir el día).

Desde el dashboard se accede a `/stats`, una sección de estadísticas agregadas por provincia y municipio (histórico completo o un día concreto, filtrable por país), con distribución de riesgo, evolución diaria y rankings de focos.

El riesgo es un score de 0 a 1 que combina la temperatura de brillo del satélite, el viento y la humedad del combustible fino (fórmula de Simard). Se clasifica en `low`, `moderate`, `high` o `extreme`.

El clima se pide para el día del foco y, cuando FIRMS da `acq_time`, para la hora del sobrevuelo (UTC): un foco nocturno se puntúa con la noche, no con el máximo de la tarde. Si no hay hora, se usan los extremos del día (máxima temperatura, mínima humedad y máximo viento).

La zona vigilada es España: península, Baleares y Canarias. Las cajas geográficas están ajustadas para dejar fuera la costa de Argelia.

Los datos de FIRMS son puntos (centros de píxel), no perímetros de incendio, así que el mapa muestra puntos y no polígonos.

## Estructura

Hay tres servicios propios y dos de infraestructura, todos en `docker-compose.yml`. Los tres propios salen del mismo `Dockerfile` con distintos targets, y cada uno instala solo las dependencias que necesita (`requirements/`). Por ejemplo, la imagen de ingesta no lleva `psycopg2`, así que no puede escribir en la base de datos aunque alguien lo intente.

| Servicio | Qué hace |
|---|---|
| `refresh` | Bucle de ingesta FIRMS -> Kafka |
| `processor` | Kafka -> cálculo de riesgo -> PostGIS |
| `api` | FastAPI y dashboard (puerto 8080, solo en localhost) |
| `kafka` | Broker en modo KRaft, sin ZooKeeper |
| `postgres` | PostgreSQL 16 con PostGIS |

Código:

```
src/firewatch/
├── config.py        # configuración y bounding boxes
├── runner.py        # CLI (ingest, backfill, daily, process, geocode-backfill)
├── clients/         # FIRMS, Open-Meteo (caché por celda 0.25° y fecha), Nominatim
├── services/        # ingestion, processing, risk
├── api/             # app FastAPI, endpoints, autenticación, rate limit
├── static/          # dashboard (HTML, JS, CSS)
└── tests/
```

## Uso

### Puesta en marcha

Hace falta Docker con Compose v2 y una clave gratuita de [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/api/area/).

```bash
cp .env.example .env
```

Edita `.env` y rellena:

- `NASA_FIRMS_API_KEY`: tu clave de la NASA.
- `POSTGRES_PASSWORD`: contraseña de la base de datos.
- `API_KEY`: clave para entrar al dashboard (mínimo 16 caracteres).
- `SESSION_SECRET`: secreto para firmar las cookies de sesión (distinto de `API_KEY`).
- `API_PORT`: puerto del dashboard, 8080 por defecto.

Después:

```bash
docker compose up -d --build
docker compose ps
```

El dashboard queda en http://localhost:8080/dashboard. Al entrar pide la `API_KEY` y, si es correcta, crea una cookie de sesión de 12 horas. Con `APP_ENV=production` se desactivan `/docs` y `/redoc`, y la API no arranca si las claves faltan o son débiles.

La contraseña de Postgres solo se aplica la primera vez que se crea el volumen. Si la cambias después, hay que hacer `ALTER USER firewatch WITH PASSWORD '...';` o borrar el volumen con `docker compose down -v` (esto borra los datos).

### Comandos habituales

```bash
docker compose logs -f refresh       # logs de la ingesta
docker compose logs -f processor     # logs del procesador
docker compose build api && docker compose up -d api   # reconstruir un servicio
docker compose down                  # parar conservando los datos
```

### CLI

Todo se lanza con `python -m firewatch.runner <comando>`:

```bash
# Ingesta puntual (hoy, o una fecha o zona concreta)
python -m firewatch.runner ingest
python -m firewatch.runner ingest --date 2026-10-15
python -m firewatch.runner ingest --bbox 37.0,-9.6,43.9,3.6

# Carga de un rango de fechas
python -m firewatch.runner backfill --start-date 2026-10-01 --end-date 2026-10-15

# Refresco periódico de los últimos días (es lo que ejecuta el contenedor refresh)
python -m firewatch.runner daily --days 3 --interval-minutes 360
python -m firewatch.runner daily --once --days 3

# Consumir de Kafka y guardar en PostGIS
python -m firewatch.runner process
python -m firewatch.runner process --max-empty-polls 3

# Rellenar municipio y provincia de eventos antiguos
python -m firewatch.runner geocode-backfill --limit 50
```

El modo `daily` vuelve a consultar los últimos días porque la NASA sigue publicando focos de forma tardía. Si alguna región falla tras los reintentos, el proceso termina con código 1.

### API

Salvo `/`, `/health`, `/health/db` y las rutas `/login` y `/auth/*`, todo requiere sesión.

| Endpoint | Descripción |
|---|---|
| `GET /api/v1/fire/events` | Focos. Filtros: `date`, `risk_level`, `min_risk_score`, `bbox` (`S,W,N,E`), `limit` |
| `GET /api/v1/fire/stats` | Conteos por nivel de riesgo y score máximo, global o de un día (`?date=`) |
| `GET /api/v1/fire/dates` | Fechas con datos, con total y riesgo máximo de cada una |
| `GET /api/v1/fire/locality-stats` | Agregación por provincia y municipio; filtros `date`, `country`, `limit` |
| `POST /api/v1/fire/ingest` | Lanza una ingesta manual (`?bbox=&date=`) |
| `GET /dashboard` | Interfaz web (mapa) |
| `GET /stats` | Interfaz web de estadísticas |

### Tests

```bash
python -m pytest
```

Son 127 tests que no necesitan red ni contenedores: usan `httpx.MockTransport` y fakes en memoria para Kafka y la base de datos.
