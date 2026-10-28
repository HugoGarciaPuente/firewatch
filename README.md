# FireWatch

Sistema de monitorización de incendios forestales en España. Descarga anomalías térmicas de satélite desde NASA FIRMS, les añade datos meteorológicos de Open-Meteo (de la fecha y hora de cada foco), calcula un índice de riesgo para cada foco y lo muestra en un mapa web.

## Cómo funciona

```
NASA FIRMS -> ingesta -> Kafka -> procesador (+ Open-Meteo) -> PostgreSQL/PostGIS -> API FastAPI + dashboard
```

1. El servicio de ingesta pide a FIRMS los focos de las últimas fechas y los publica en un topic de Kafka.
2. El procesador los consume, consulta temperatura, humedad y viento para la fecha y hora del sobrevuelo, calcula el riesgo y guarda el resultado en PostGIS.
3. La API expone los datos y sirve un dashboard con Leaflet (mapa de puntos, mapa de calor, clústeres y un calendario para elegir el día).

El consumidor confirma el offset de Kafka solo después del `commit` en PostgreSQL. Si PostgreSQL se reinicia, reintenta con una conexión nueva el mismo mensaje; si no puede recuperarse, termina con error para que Docker lo reinicie. La ingesta manual ya no está en el dashboard: `POST /api/v1/fire/ingest` queda para automatización.

Desde el dashboard se accede a `/stats`, una sección de estadísticas agregadas por provincia y municipio, con filtros de periodo (último día, semana, mes o año, una fecha concreta o un rango) y de provincia, distribución de riesgo, evolución diaria y rankings de focos.

También se accede a `/forecast`, una sección independiente con su propio mapa Leaflet: cada provincia se colorea según la probabilidad calibrada de incendio confirmado ≥10 ha para el día objetivo, y la barra lateral ordena las 52 provincias por score crudo.

La última sección, `/sources`, es informativa: enumera las APIs y datos externos (NASA FIRMS, EFFIS, Open-Meteo, Nominatim/OpenStreetMap, INE y la cartografía base), explica en dos fichas el cálculo heurístico del riesgo de un foco y el modelo ML a 24 h, y recuerda las limitaciones (puntos, no perímetros; probabilidad, no alerta oficial).

El riesgo es un score de 0 a 1 que combina la temperatura de brillo del satélite, el viento y la humedad del combustible fino (fórmula de Simard). Se clasifica en `low`, `moderate`, `high` o `extreme`.

El clima se pide para el día del foco y, cuando FIRMS da `acq_time`, para la hora del sobrevuelo (UTC): un foco nocturno se puntúa con la noche, no con el máximo de la tarde. Si no hay hora, se usan los extremos del día (máxima temperatura, mínima humedad y máximo viento). Para fechas fuera de la ventana de previsión de Open-Meteo (~30 días) se consulta su archivo ERA5, con las mismas variables horarias.

La zona vigilada es España: península, Baleares y Canarias. Las cajas geográficas están ajustadas para dejar fuera la costa de Argelia. Al ser cajas rectangulares, también capturan puntos de Portugal y del sur de Francia; el país real se conoce al geocodificar el punto, así que el procesador descarta lo que no esté en `FIREWATCH_COUNTRY_CODES` (por defecto `es`) y `runner purge-out-of-scope` limpia el histórico ingestado antes de ese filtro.

Los datos de FIRMS son puntos (centros de píxel), no perímetros de incendio, así que el mapa de focos muestra puntos y no polígonos. La sección `/forecast` colorea límites provinciales administrativos para representar la probabilidad; tampoco son perímetros de incendio.

## Estructura

Hay cuatro servicios propios y dos de infraestructura, todos en `docker-compose.yml`. Los cuatro propios salen del mismo `Dockerfile` con distintos targets, y cada uno instala solo las dependencias que necesita (`requirements/`). Por ejemplo, la imagen de ingesta no lleva `psycopg2`, así que no puede escribir en la base de datos aunque alguien lo intente.

| Servicio | Qué hace |
|---|---|
| `refresh` | Bucle de ingesta FIRMS -> Kafka (cada 30 min) |
| `processor` | Kafka -> cálculo de riesgo -> PostGIS |
| `forecast` | Worker que regenera `fire_predictions` cada 3 h con el modelo ML |
| `api` | FastAPI, dashboard, estadísticas y mapa de pronóstico (puerto 8080, solo en localhost) |
| `kafka` | Broker en modo KRaft, sin ZooKeeper |
| `postgres` | PostgreSQL 16 con PostGIS |

Código:

```
src/firewatch/
├── config.py        # configuración y bounding boxes
├── runner.py        # CLI (ingest, backfill, daily, process, labels, panel, province-weather, ...)
├── clients/         # FIRMS, EFFIS, Open-Meteo (caché por celda 0.25° y fecha), Nominatim
├── services/        # ingestion, processing, risk, labels, provinces, panel, forecast
├── ml/              # features, métricas, entrenamiento e inferencia del modelo a 24 h
├── api/             # app FastAPI, endpoints, autenticación, rate limit
├── static/          # dashboard, estadísticas y mapa de pronóstico (HTML, JS, CSS, GeoJSON)
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
- `REFRESH_INTERVAL_MINUTES`: cada cuántos minutos se refresca FIRMS (30 por defecto).
- `FORECAST_INTERVAL_MINUTES`: cada cuántos minutos se regenera el pronóstico (180 por defecto).

Después:

```bash
docker compose up -d --build
docker compose ps
```

El dashboard queda en http://localhost:8080/dashboard. Al entrar pide la `API_KEY` y, si es correcta, crea una cookie de sesión de 12 horas. Con `APP_ENV=production` se desactivan `/docs` y `/redoc`, y la API no arranca si las claves faltan o son débiles.

El servicio `refresh` consulta FIRMS cada `REFRESH_INTERVAL_MINUTES` (30 min por defecto): la latencia NRT es de 1–3 h, así que 30 min recoge cada lote nuevo sin reenviar la ventana de 3 días demasiado a menudo. El servicio `forecast` monta `./data/models` en solo lectura y ejecuta `runner forecast --loop` cada `FORECAST_INTERVAL_MINUTES` (3 h por defecto), que es la cadencia de los modelos regionales de Open-Meteo. Genera antes el artefacto con `python -m firewatch.ml.train`; si falta `data/models/hgb.joblib`, el worker registra el error y conserva las predicciones anteriores. El pronóstico no depende de que termine el refresco de FIRMS, pero con estas cadencias el mapa no queda desfasado. Si cambias los intervalos en `.env`, recrea los workers con `docker compose up -d refresh forecast`.

La contraseña de Postgres solo se aplica la primera vez que se crea el volumen. Si la cambias después, hay que hacer `ALTER USER firewatch WITH PASSWORD '...';` o borrar el volumen con `docker compose down -v` (esto borra los datos).

### Comandos habituales

```bash
docker compose logs -f refresh       # logs de la ingesta
docker compose logs -f processor     # logs del procesador
docker compose logs -f forecast      # logs del worker de pronóstico
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
# El NRT solo conserva ~4 meses: junio se pide al producto estándar (SP)
python -m firewatch.runner backfill --start-date 2026-06-01 --end-date 2026-06-30 --source VIIRS_SNPP_SP

# Refresco periódico de los últimos días (es lo que ejecuta el contenedor refresh)
python -m firewatch.runner daily --days 3 --interval-minutes 30
python -m firewatch.runner daily --once --days 3

# Consumir de Kafka y guardar en PostGIS
python -m firewatch.runner process
python -m firewatch.runner process --max-empty-polls 3

# Rellenar municipio y provincia de eventos antiguos
python -m firewatch.runner geocode-backfill --limit 50

# Borrar focos ya guardados fuera del ámbito (Portugal, sur de Francia)
python -m firewatch.runner purge-out-of-scope --dry-run
python -m firewatch.runner purge-out-of-scope

# Panel provincia-día para el modelo (necesita Postgres)
python -m firewatch.runner panel --start-date 2017-01-01 --end-date 2026-10-25 --min-area-ha 10 --output data/panel_province_day.csv

# Meteorología diaria por provincia (Open-Meteo; una petición por provincia)
python -m firewatch.runner province-weather --start-date 2017-01-01 --end-date 2026-10-25 --output data/weather_province_day.csv --sleep 2

# Entrenar el baseline de predicción a 24 h (offline, necesita pandas/scikit-learn)
python -m firewatch.ml.train

# Predecir el siguiente día UTC para las 52 provincias (no escribe en Postgres)
python -m firewatch.runner forecast --dry-run

# Guardar el pronóstico en fire_predictions (necesita Postgres)
python -m firewatch.runner forecast

# Worker periódico: es lo que ejecuta el contenedor forecast
python -m firewatch.runner forecast --loop --interval-minutes 180
```

El modo `daily` vuelve a consultar los últimos días porque la NASA sigue publicando focos de forma tardía. Si alguna región falla tras los reintentos, el proceso termina con código 1; `backfill` hace lo mismo si falla cualquier día. En `geocode-backfill`, un fallo de Nominatim deja la localidad en `NULL` y termina con código 1; `{}` solo se escribe cuando el punto no tiene topónimo.

### Incendios confirmados (etiquetas)

`fire_events` guarda detecciones de FIRMS, no incendios confirmados. Para el
modelo de predicción a 24 h, `runner labels` descarga las áreas quemadas de
EFFIS (fecha, provincia, municipio y superficie) y las guarda en
`confirmed_fires`:

```bash
# Muestra piloto, sin escribir en la base de datos
python -m firewatch.runner labels --start-date 2026-07-01 --end-date 2026-07-31 --limit 20 --dry-run

# Catálogo completo de España (>= 1 ha)
python -m firewatch.runner labels --min-area-ha 1
```

La API de EFFIS no filtra por fecha y tarda unos 15 s por página de 250
registros, así que la primera importación completa tarda varios minutos. El
comando es idempotente: reimportar actualiza la superficie y la fecha de último
cambio sin duplicar incendios. Necesita `psycopg2`, así que se ejecuta en el
contenedor `processor` o `api`, no en `refresh`:

```bash
docker compose exec processor python -m firewatch.runner labels --min-area-ha 1
```

### Modelo de predicción a 24 h (offline)

El modelo trabaja a nivel **provincia-día**: para cada provincia y cada día,
predice si se iniciará al menos un incendio confirmado de ≥ 10 ha. No sustituye
al score de riesgo de FIRMS; es una capa distinta que se entrena con las
etiquetas de EFFIS.

1. Construir el panel provincia-día (52 provincias, 2017-2026):

```bash
python -m firewatch.runner panel --start-date 2017-01-01 --end-date 2026-10-25 --min-area-ha 10 --output data/panel_province_day.csv
```

2. Descargar la meteorología diaria de cada provincia. Es una petición por
   provincia; Open-Meteo limita las peticiones, así que conviene espaciarlas:

```bash
python -m firewatch.runner province-weather --start-date 2017-01-01 --end-date 2026-10-25 --output data/weather_province_day.csv --sleep 2
```

3. Entrenar y evaluar:

```bash
python -m firewatch.ml.train
```

El comando escribe `reports/forecast_baseline.json` y
`reports/forecast_baseline.md`, y deja los artefactos en `data/models/`.

El split es temporal: 2017-2024 entrena, 2025 valida y 2026 es test. No se
mezclan años al azar porque un incendio de la misma provincia y semana en train
y test inflaría las métricas. La etiqueta positiva es un incendio EFFIS de
≥ 10 ha. La meteorología del día D se usa como proxy de la previsión disponible
la víspera; la evaluación histórica usa reanálisis ERA5, no una previsión
archivada, así que el resultado es optimista si la previsión real es peor.

En la ejecución de referencia (`reports/forecast_baseline.md`), el
HistGradientBoosting obtiene PR-AUC 0,267 en test frente a 0,162 de la
climatología (lift 1,65) y precisión diaria en el top-5 de 0,142 frente a
0,126. La regresión logística es solo una línea base y no mejora la
climatología en el top-5. El entrenamiento usa `requirements-dev.txt`; la
inferencia operativa tiene su propia imagen (`forecast.txt`) para no cargar
`pandas` ni `scikit-learn` en la API.

4. Generar el pronóstico operativo. El comando pide 35 días de historia a
   Open-Meteo (el archivo ERA5) y la previsión del día objetivo, construye las
   mismas features del entrenamiento y escribe una fila por provincia en
   `fire_predictions`:

```bash
# Ver las 52 probabilidades sin tocar la base de datos
python -m firewatch.runner forecast --dry-run

# Escribirlas (idempotente: repetir el mismo día reemplaza, no duplica)
python -m firewatch.runner forecast
```

El día objetivo por defecto es **mañana UTC**, no una ventana móvil de 24 h. La
historia se queda en 35 días porque `days_since_rain` satura a los 30: con menos,
el modelo vería una sequía truncada distinta a la del entrenamiento. El día
objetivo se pide siempre a la API de previsión, nunca al archivo. Si una
provincia falla o le falta un día, el comando termina con código 1 y no escribe
nada; el pronóstico anterior se conserva.

El contenedor `forecast` ejecuta ese mismo comando con `--loop`: al arrancar
genera el pronóstico y después repite cada 3 h. `refresh` lo hace cada 30 min.
La inferencia corre en su propia imagen (`pandas` y `scikit-learn` en
`requirements/forecast.txt`), nunca en la API ni en la ingesta. El artefacto se
monta desde `./data/models`; si se reentrena, la siguiente vuelta usa el nuevo
hash de modelo.

La API lee `fire_predictions` en `GET /api/v1/fire/forecast`. El endpoint usa la
última `target_date` disponible (no `date.today()`) y devuelve 404 si todavía no
hay filas. La sección `/forecast` es independiente del dashboard de FIRMS y de su
selector de día: el día objetivo suele ser mañana UTC y todavía no tiene focos.
El mapa colorea los límites provinciales con la probabilidad calibrada; la tabla
ordena por score crudo, que es el que mejor rankea, y muestra ambas cifras. Los
polígonos son límites administrativos, no perímetros de incendio.

### API

Salvo `/`, `/health`, `/health/db` y las rutas `/login` y `/auth/*`, todo requiere sesión.

| Endpoint | Descripción |
|---|---|
| `GET /api/v1/fire/events` | Focos. Filtros: `date`, `risk_level`, `min_risk_score`, `bbox` (`S,W,N,E`), `limit` |
| `GET /api/v1/fire/stats` | Conteos por nivel de riesgo y score máximo, global o de un día (`?date=`) |
| `GET /api/v1/fire/dates` | Fechas con datos, con total y riesgo máximo de cada una |
| `GET /api/v1/fire/locality-stats` | Agregación por provincia y municipio; filtros `date`, `start_date`, `end_date`, `province`, `limit` |
| `GET /api/v1/fire/provinces` | Provincias con focos (solo el ámbito vigilado), para el selector |
| `GET /api/v1/fire/forecast` | Pronóstico ML provincial a 24 h; por defecto, la última `target_date` y el modelo más reciente. Filtros opcionales `date`, `model` |
| `POST /api/v1/fire/ingest` | Lanza una ingesta manual por API (`?bbox=&date=`). No está expuesto en el dashboard |
| `GET /dashboard` | Interfaz web (mapa de focos FIRMS) |
| `GET /stats` | Interfaz web de estadísticas |
| `GET /forecast` | Interfaz web del mapa provincial de pronóstico a 24 h |
| `GET /sources` | Fuentes, APIs y metodología del sistema |

### Tests

```bash
python -m pytest
```

Son 243 tests que no necesitan red ni contenedores: usan `httpx.MockTransport` y fakes en memoria para Kafka y la base de datos.
