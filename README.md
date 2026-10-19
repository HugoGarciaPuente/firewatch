# 🔥 FireWatch

Sistema de alerta temprana y monitorización de incendios forestales en tiempo real basado en una **arquitectura de microservicios desacoplada**.

El sistema ingesta anomalías térmicas satelitales desde **NASA FIRMS**, las publica asíncronamente en **Apache Kafka**, las enriquece con variables meteorológicas de **Open-Meteo**, calcula índices de riesgo físico de ignición y propagación, persiste los eventos geolocalizados en **PostgreSQL / PostGIS**, y los expone mediante una **API REST en FastAPI** y un **dashboard GIS interactivo con Leaflet**.

```
NASA FIRMS ──► Ingestión ──► Kafka (topic) ──► Procesador + Open-Meteo ──► PostgreSQL/PostGIS ──► FastAPI
   (fetch)       (publish)   (firms-ingest)      (consume, scoring)             (fire_events)      (/events, /stats,
                                                       │                                            /dates, /dashboard)
                                                       ▼
                                            Alertas de Riesgo (Kafka)
                                              (fire-risk-alerts)
```

---

## 📋 Índice

1. [Pila Tecnológica y Decisiones de Diseño](#-pila-tecnológica-y-decisiones-de-diseño)
2. [Arquitectura del Sistema: Tres Tiers e Infraestructura](#-arquitectura-del-sistema-tres-tiers-e-infraestructura)
3. [Ámbito Geográfico y Bounding Boxes](#-ámbito-geográfico-y-bounding-boxes)
4. [Modelo de Riesgo Físico y Enriquecimiento Meteorológico](#-modelo-de-riesgo-físico-y-enriquecimiento-meteorológico)
5. [Seguridad y Control de Acceso](#-seguridad-y-control-de-acceso)
6. [Automatización y Refresco Continuo](#-automatización-y-refresco-continuo)
7. [Dashboard GIS Interactivo](#-dashboard-gis-interactivo)
8. [Referencia de la API REST](#-referencia-de-la-api-rest)
9. [CLI del Runner (`firewatch.runner`)](#-cli-del-runner-firewatchrunner)
10. [Puesta en Marcha Rápida con Docker](#-puesta-en-marcha-rápida-con-docker)
11. [Batería de Pruebas Automatizadas](#-batería-de-pruebas-automatizadas)
12. [Estructura del Proyecto](#-estructura-del-proyecto)

---

## 🛠️ Pila Tecnológica y Decisiones de Diseño

| Componente | Tecnología | Función y Racional de Selección |
|---|---|---|
| **Fuente Externa** | NASA FIRMS REST API | Datos satelitales en casi tiempo real (NRT) de sensores VIIRS y MODIS (`/area/csv`). |
| **Broker de Mensajería** | Apache Kafka 3.9 (KRaft) | Desacopla la ingesta externa del procesamiento. El modo **KRaft** elimina ZooKeeper, simplificando la operación en un único proceso. |
| **Meteorología** | Open-Meteo API | Temperatura, humedad relativa y viento. Incluye **caché espacial por celda de 0.25° (~25 km)** para evitar peticiones redundantes. |
| **Persistencia Espacial** | PostgreSQL 16 + PostGIS 3.4 | Base de datos relacional con tipos nativos `geometry(Point, 4326)` e índices espaciales GiST. |
| **Modelo de Riesgo** | `services/risk.py` | Cálculo físico de humedad de combustible fino (fórmula Simard EMC), índice de propagación y scoring normalizado (0.0 a 1.0). |
| **Backend REST** | FastAPI + Uvicorn | Framework asíncrono moderno con validación mediante Pydantic y documentación OpenAPI interactiva. |
| **Dashboard GIS** | Leaflet + Vanilla JS | Mapa web interactivo sin dependencias de compilación ni frameworks pesados (React/Node). |
| **Orquestación** | Docker Compose | 5 contenedores orquestados con healthchecks activos, dependencias ordenadas y volúmenes persistentes. |

### 💡 Justificación de Decisiones de Simplicidad (Principio KISS)

- **Kafka en modo KRaft vs. ZooKeeper:** Tradicionalmente Kafka requería un clúster de ZooKeeper separado para gestionar metadatos. El modo KRaft unifica el broker y el controlador en un único contenedor, reduciendo consumo de RAM, puertos expuestos y puntos de fallo.
- **PostgreSQL / PostGIS vs. NoSQL:** Las consultas de alertas de incendios son inherentemente espaciales (bounding boxes, proximidad) y transaccionales (upsert idempotente por fecha y coordenadas). PostGIS ofrece soporte espacial estándar de grado de producción superior a colecciones GeoJSON en bases documentales.
- **Frontend Vanilla JS vs. SPA Framework (React / Vue):** El dashboard consta de un mapa Leaflet con heatmap y un selector de calendario. Montar React, Node.js, Webpack/Vite y `node_modules` añadiría cientos de megabytes de imágenes y complejidad de build sin aportar valor técnico a la visualización.
- **Autenticación Clave Compartida + Cookie HMAC vs. OAuth2 / JWT / Auth0:** Para un panel operativo con un único perfil de operador, desplegar un servidor OAuth2 o una tabla de usuarios en BD es sobreingeniería innecesaria. Se optó por una API key intercambiada una única vez por una cookie de sesión firmada criptográficamente con HMAC-SHA256 (`HttpOnly`, `SameSite=Strict`).
- **Puntos térmicos vs. Polígonos de perímetro:** La API `area` de FIRMS entrega **anomalías térmicas puntuales** (centros de píxel del satélite), no el perímetro real del incendio. Generar polígonos (como convex hulls) inventaría geometría falsa uniendo focos distantes. Se descartaron polígonos para mantener fidelidad científica a los datos fuente.

---

## 🏗️ Arquitectura del Sistema: Tres Tiers e Infraestructura

El sistema separa responsabilidades en **tres microservicios de aplicación** y **dos servicios de infraestructura**. Se compilan a partir de un **único `Dockerfile` multi-stage** con targets independientes (`--target`):

```
Tier 1: Adquisición    firewatch:acquisition    NASA FIRMS + Open-Meteo ──► Kafka
Tier 2: Procesamiento  firewatch:processing     Kafka ──► Scoring de Riesgo ──► PostGIS
Tier 3: API & UI       firewatch:api            FastAPI + Dashboard Leaflet
Infraestructura        postgres, kafka          (contenedores dedicados, ciclo de vida propio)
```

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ RED INTERNA DOCKER (`firewatch` bridge)                                         │
│                                                                                 │
│  [refresh] (Tier 1)                                                             │
│       │                                                                         │
│       ▼ (firewatch-kafka:29092)                                                 │
│   [kafka] (KRaft Broker)                                                        │
│       │                                                                         │
│       ▼                                                                         │
│  [processor] (Tier 2) ──(HTTP)──► Open-Meteo                                    │
│       │                                                                         │
│       ▼ (postgres:5432)                                                         │
│  [postgres] (PostGIS 16)                                                        │
│       ▲                                                                         │
│       │                                                                         │
│  [api] (Tier 3) ◄──────────── (HTTP :8080) ──────────── [Navegador / Usuario]   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Servicios en `docker-compose.yml`

| Servicio | Target Dockerfile | Imagen | Rol | Publicado al Host |
|---|---|---|---|---|
| `refresh` | `acquisition` | `firewatch:acquisition` | Bucle desatendido de ingesta FIRMS -> Kafka | Ninguno (solo interno) |
| `processor` | `processing` | `firewatch:processing` | Consumidor Kafka -> Enriquecimiento -> PostGIS | Ninguno (solo interno) |
| `api` | `api` | `firewatch:api` | Servidor web FastAPI y frontend estático | `127.0.0.1:8080` |
| `kafka` | — | `apache/kafka:3.9.0` | Broker y controlador KRaft | `127.0.0.1:9092` / `9093` |
| `postgres` | — | `postgis/postgis:16-3.4` | Base de datos espacial PostgreSQL + PostGIS | `127.0.0.1:5432` |

### Aislamiento de Dependencias y "The Enforceable Property"

Las dependencias están segregadas en la carpeta `requirements/` (`base.txt`, `acquisition.txt`, `processing.txt`, `api.txt`). Cada imagen instala estrictamente lo que necesita su ruta de ejecución:

```bash
docker exec firewatch-refresh python -c "import psycopg2"
# ModuleNotFoundError: No module named 'psycopg2'
```

- **Garantía física:** El contenedor de adquisición (`refresh`) no incluye el driver de PostgreSQL (`psycopg2`), por lo que le es **físicamente imposible escribir en la base de datos**. Esto asegura que ningún desarrollador pueda saltarse Kafka por atajo.
- **El procesador** no instala frameworks web (`fastapi`, `uvicorn`, `pydantic`).
- **Verificación automática:** La suite de pruebas incluye [`test_tiers.py`](file:///c:/Users/Chugo/Desktop/Prácticas/Práctica_2/src/firewatch/tests/test_tiers.py), un test que analiza el Árbol de Sintaxis Abstracta (AST) de cada archivo Python y valida que ninguna imagen adquiera librerías no autorizadas.

### Métricas de Imagen Medidas en Disco

El particionamiento en tres imágenes no busca reducir almacenamiento, sino garantizar límites arquitectónicos y trazabilidad:

| Imagen | Tamaño Total | Tamaño `site-packages` |
|---|---|---|
| `firewatch:acquisition` | 192 MB | 18 MB |
| `firewatch:processing` | 209 MB | 30 MB |
| `firewatch:api` | 262 MB | 69 MB |
| *(Monolito previo)* | *263 MB* | *69 MB* |

### Red y Ciclo de Vida
- **Red puente `firewatch`:** La comunicación entre microservicios se realiza exclusivamente mediante nombres de host internos (`firewatch-kafka:29092`, `postgres:5432`), nunca a través de los puertos mapeados al host.
- **Arranque ordenado:** Las directivas `depends_on: condition: service_healthy` aseguran que `processor` y `refresh` esperen a que Kafka y PostgreSQL respondan afirmativamente a sus healthchecks antes de iniciar, evitando condiciones de carrera en arranques en frío.

---

## 📍 Ámbito Geográfico y Bounding Boxes

El sistema monitoriza el territorio de España. Se descartó una caja delimitadora continental europea porque generaba decenas de miles de anomalías diarias ajenas (un incremento de ~76× en registros) sin aportar valor a la gestión de riesgo territorial.

### Exclusión Estricta de la Costa de Argelia

Para evitar capturar anomalías térmicas del norte de África sin comprometer la franja sur andaluza (Tarifa, costa de Málaga, Granada y Almería hasta Cabo de Gata), el ámbito peninsular se divide en dos cajas calibradas:

| Región | Bounding Box (`sur, oeste, norte, este`) | Cobertura Territorial | Criterio de Exclusión de Argelia |
|---|---|---|---|
| `peninsula` | `37.0, -9.6, 43.9, 3.6` | Cuerpo principal y norte (desde Galicia hasta Cap de Creus `3.32°E`). | Límite sur fijado en **`37.0°N`**, al norte de toda la costa argelina (máximo en ~`36.91°N`). |
| `peninsula_sur` | `35.9, -9.6, 37.1, -1.8` | Franja sur andaluza (Tarifa `36.0°N`, Cádiz, Málaga, Granada y Almería). | Límite este fijado en **`-1.8°W`** (Cabo de Gata en `-2.19°W`), cortando antes de la costa argelina. |
| `baleares` | `38.5, 1.2, 40.7, 4.7` | Formentera (`38.64°N`), Ibiza, Mallorca y Menorca. | Límite sur en `38.5°N`, a más de 170 km de la costa norteafricana. |
| `canarias` | `27.4, -18.7, 29.6, -13.1` | Archipiélago Canario en el océano Atlántico. | Ámbito oceánico independiente. |

```
                       43.9°N ┌───────────────────────────┐ 3.6°E
                              │                           │
                              │     peninsula             │
                       37.0°N ├─────────────┬─────────────┘
                              │peninsula_sur│
                       35.9°N └─────────────┘-1.8°W
                            -9.6°W
                                         (Argelia: lat < 37.0°N y lon > -1.8°W) -> DESCARTADO
```

- **Expansión automática:** `resolve_regions()` en `runner.py` expande la región `peninsula` a ambas cajas de forma transparente al usuario.
- **Defensa en profundidad en el consumidor:** `record_to_row` en `services/processing.py` descarta activamente cualquier registro que caiga en `35.0 <= lat < 37.0` con `lon > -1.8` clasificándolo como `InvalidRecord`.

---

## 🌲 Modelo de Riesgo Físico y Enriquecimiento Meteorológico

Cada anomalía térmica detectada por satélite se enriquece en tiempo real con meteorología local para evaluar su severidad física:

```
Anomalía FIRMS (lat, lon, brillo) + Open-Meteo (T, H, W)
                    │
                    ▼
          services/risk.py
   ├── 1. Humedad de Combustible (Simard EMC)
   ├── 2. Índice de Propagación Potencial
   └── 3. Puntuación Normalizada (0.0 a 1.0)
                    │
                    ▼
   Categoría de Riesgo: low | moderate | high | extreme
                    │
   (Si es high o extreme) ──► Topic Kafka `fire-risk-alerts`
```

### 1. Variables Meteorológicas y Caché Espacial
El servicio consulta Open-Meteo para obtener:
- Temperatura ambiente ($T$, en °C)
- Humedad relativa ($H$, en %)
- Velocidad del viento ($W$, en km/h)

> **Caché espacial por celda de 0.25° (~25 km):** Sin caché, un backfill masivo generaría miles de peticiones HTTP a una API pública gratuita. La clase `WeatherClient` agrupa las consultas en celdas de cuadrícula de 0.25°. Si varios focos distan menos de 25 km, comparten la lectura meteorológica. Además, cachea fallos transitorios para no ralentizar el procesador si la API externa experimenta caídas.

### 2. Humedad de Combustible Fino (Fórmula Simard EMC)
El Equilibrio de Contenido de Humedad (EMC) modela la sequedad del combustible vegetal ligero en función de la humedad relativa y la temperatura:
- Si $H < 10\%$: $EMC = 0.03229 + 0.281073 \cdot H - 0.000578 \cdot H \cdot T$
- Si $10\% \le H \le 50\%$: $EMC = 2.22749 + 0.160107 \cdot H - 0.01478 \cdot T$
- Si $H > 50\%$: $EMC = 21.0606 + 0.005565 \cdot H^2 - 0.00035 \cdot H \cdot T - 0.483199 \cdot H$

### 3. Índice de Propagación y Scoring
- **Propagación ($ISI$):** Combina la velocidad del viento y la sequedad del combustible mediante una función exponencial.
- **Score Normalizado ($0.0$ a $1.0$):** Pondera la temperatura de brillo del satélite ($35\%$), el índice de propagación por viento ($35\%$) y la sequedad del combustible ($30\%$).
- **Categorías:** `low` ($<0.30$), `moderate` ($0.30 - 0.55$), `high` ($0.55 - 0.75$) y `extreme` ($\ge 0.75$).

---

## 🔐 Seguridad y Control de Acceso

El sistema implementa un modelo de autenticación desacoplado de dos fases:

```
Navegador                                Servidor
   │   POST /auth/login                     │
   │   X-API-Key: <clave>            ──────►│  require_api_key (tiempo constante)
   │                                        │  Verifica Rate Limiting por IP
   │   Set-Cookie: firewatch_session ◄──────│  Cookie HMAC-SHA256 (12h, HttpOnly)
   │                                        │
   │   GET /api/v1/fire/stats               │
   │   Cookie: firewatch_session     ──────►│  require_session (valida firma)
```

1. **La clave nunca reside en el navegador:** El usuario introduce la clave en `/login`. El frontend la envía una sola vez en el encabezado `X-API-Key` y limpia inmediatamente el campo del formulario. La clave nunca se guarda en `localStorage` ni en variables globales de JS, eliminando el riesgo de robo por Cross-Site Scripting (XSS).
2. **Cookie de sesión firmada con HMAC-SHA256:** El servidor emite la cookie `firewatch_session` configurada con `HttpOnly`, `SameSite=Strict`, `Path=/` y caducidad de 12 horas.
3. **Rate Limiting activo contra fuerza bruta:** El endpoint `/auth/login` incorpora un limitador en memoria por dirección IP que bloquea intentos repetidos (retornando HTTP 429 con cabecera `Retry-After`).
4. **Protección de rutas estáticas:** El middleware `guard_static_assets` intercepta `/static/*` y `/dashboard`. Si no existe sesión válida, redirige a `/login`. La página `login.html` es completamente autocontenida (estilos CSS en línea) para no depender de `/static`, impidiendo accesos no autorizados al código fuente del cliente.
5. **Comportamiento en producción (`APP_ENV=production`):**
   - Desactiva automáticamente `/docs`, `/redoc` y `/openapi.json` para evitar exposición de la API.
   - Bloquea el arranque del contenedor si `API_KEY` o `SESSION_SECRET` están ausentes, si la clave tiene menos de 16 caracteres, o si ambos secretos son idénticos.

---

## 🔄 Automatización y Refresco Continuo

La actualización de datos se ejecuta de forma desatendida mediante el contenedor `refresh` de Docker Compose:

```bash
python -m firewatch.runner daily --days 3 --interval-minutes 360
```

### ¿Por qué re-consultar días recientes?
A diferencia de datos meteorológicos históricos que son estáticos, la NASA emite datos satelitales NRT (*Near Real-Time*) con revisiones continuas a medida que se procesan las órbitas completas. Re-consultar la ventana rodante de los últimos 3 días permite incorporar focos que la NASA publica con desfase.
- **Impacto real medido:** En la verificación en vivo del 08/10, re-consultar el día 07/10 actualizó el registro de 6 focos iniciales a **50 focos disponibles (31 de ellos clasificados como `high`)**.

### Resiliencia y Manejo de Errores
- **Backoff exponencial:** Ante fallos de conexión con NASA FIRMS, el cliente reintenta hasta 3 veces con esperas crecientes (2s -> 4s -> 8s, con tope en 30s).
- **Código de salida estricto:** Si tras los reintentos alguna región o día falla, el proceso finaliza con código de salida **`1`**. No se permite un "éxito parcial silencioso" que congele el conjunto de datos sin que los operadores lo noten.
- **Trazabilidad:** Los registros se emiten a `stdout` del contenedor (`docker compose logs -f refresh`) y a un archivo rotativo en el volumen compartido `/app/logs/firewatch.log`.

---

## 🗺️ Dashboard GIS Interactivo

Accesible en `http://localhost:8080/dashboard` tras iniciar sesión:

- **Carga inteligente:** Al abrir la interfaz, detecta y **selecciona automáticamente el día más reciente con datos disponibles**, ajustando el mapa y los KPIs a esa fecha sin requerir interacción manual.
- **Mapa base oscuro:** Capa base de alta visibilidad (*Stadia Maps Alidade Smooth Dark*).
- **Capas espaciales siempre visibles:**
  - **Puntos térmicos:** Círculos con radio proporcional al score de riesgo y coloreados por severidad (`low` verde, `moderate` amarillo, `high` naranja, `extreme` rojo).
  - **Heatmap (Mapa de calor):** Densidad ponderada según el índice de riesgo de cada foco.
  - **Clústeres dinámicos:** Agrupación inteligente que se disgrega al hacer zoom.
- **Selector mensual tipo calendario:** Rejilla interactiva con puntos de color que indican la severidad máxima registrada cada día. Permite navegar entre meses y filtrar la vista en un clic.
- **Fila superior de KPIs reactiva:** Los indicadores (Total de focos, Extremo, Alto, Moderado, Bajo, Score Máximo) se recalculan instantáneamente para el día seleccionado.
- **Inspector de focos:** Popup detallado al hacer clic en cualquier anomalía que muestra temperatura de brillo, humedad de combustible Simard (EMC), viento, temperatura ambiente y municipio/provincia geocodificado.
- **Cache-busting de activos estáticos:** Referencias a `app.js` y `style.css` versionadas (`?v=N`) para evitar el uso de archivos en caché del navegador tras actualizaciones.

---

## 📡 Referencia de la API REST

Todos los endpoints `/api/v1/fire/*` y `/dashboard` requieren cookie de sesión activa.

| Método | Endpoint | Acceso | Descripción y Parámetros |
|---|---|---|---|
| `GET` | `/` | Público | Información básica de la versión y enlace al dashboard. |
| `GET` | `/health` | Público | Comprobación de salud básica del microservicio API. |
| `GET` | `/health/db` | Público | Sonda de conectividad con PostgreSQL y PostGIS (retorna 503 si falla). |
| `GET` | `/login` | Público | Formulario web de inicio de sesión. |
| `POST` | `/auth/login` | Público | Intercambia cabecera `X-API-Key` por cookie de sesión (con rate limiting). |
| `POST` | `/auth/logout` | Público | Cierra la sesión revocando la cookie del cliente. |
| `GET` | `/auth/session` | Público | Verifica el estado de autenticación actual (`authenticated: true/false`). |
| `GET` | `/dashboard` | Sesión | Interfaz gráfica interactiva del dashboard GIS. |
| `GET` | `/api/v1/fire/events` | Sesión | Listado de anomalías térmicas. Parámetros opcionales: `date` (`YYYY-MM-DD`), `risk_level`, `min_risk_score`, `bbox` (`S,W,N,E`), `limit` (def. 100). |
| `GET` | `/api/v1/fire/stats` | Sesión | Estadísticas agregadas globales o de un día (`?date=YYYY-MM-DD`): conteos por riesgo y score máximo. |
| `GET` | `/api/v1/fire/dates` | Sesión | Calendario histórico de fechas disponibles con totales y nivel de riesgo máximo por día. |
| `POST` | `/api/v1/fire/ingest` | Sesión | Disparo manual de ingesta asíncrona hacia Kafka (`?bbox=&date=`). |

---

## 💻 CLI del Runner (`firewatch.runner`)

La aplicación incluye una interfaz de línea de comandos unificada para tareas operativas:

### 1. Ingesta puntual (One-shot)
Descarga el día actual (o una fecha concreta) para las regiones configuradas y lo publica en Kafka:
```bash
python -m firewatch.runner ingest
python -m firewatch.runner ingest --date 2026-10-15
python -m firewatch.runner ingest --bbox 37.0,-9.6,43.9,3.6
```

### 2. Carga histórica (Backfill)
Descarga un rango de fechas día por día (de acuerdo con la restricción de la API de FIRMS):
```bash
python -m firewatch.runner backfill --start-date 2026-10-01 --end-date 2026-10-15
```

### 3. Bucle de refresco desatendido (`daily`)
Mantiene los datos actualizados consultando la ventana rodante reciente:
```bash
# Bucle periódico continuo (por defecto cada 6 horas y 3 días de ventana)
python -m firewatch.runner daily --days 3 --interval-minutes 360

# Ejecutar una única pasada y salir
python -m firewatch.runner daily --once --days 3
```

### 4. Consumo y procesamiento hacia PostGIS (`process`)
Ejecuta el consumidor de Kafka que calcula el riesgo y persiste en PostGIS:
```bash
# Modo continuo
python -m firewatch.runner process

# Procesar mensajes pendientes y salir tras detectar el topic vacío
python -m firewatch.runner process --max-empty-polls 3
```

### 5. Geocodificación inversa histórica (`geocode-backfill`)
Rellena la toponimia (municipio y provincia) de eventos históricos que carecen de ella:
```bash
python -m firewatch.runner geocode-backfill --limit 50
```

---

## 🚀 Puesta en Marcha Rápida con Docker

### Requisitos Previos
- Docker Desktop (con soporte para Docker Compose v2).
- Clave de API gratuita de NASA FIRMS ([obtener aquí en segundos](https://firms.modaps.eosdis.nasa.gov/api/area/)).

### 1. Configurar variables de entorno
Copia la plantilla y edita las credenciales:
```bash
cp .env.example .env
```
Asegúrate de definir en `.env`:
- `NASA_FIRMS_API_KEY`: tu clave de la NASA.
- `POSTGRES_PASSWORD`: contraseña segura para PostgreSQL.
- `API_KEY`: clave de acceso al sistema (mínimo 16 caracteres).
- `SESSION_SECRET`: clave para firmar cookies HMAC (debe ser distinta de `API_KEY`).
- `API_PORT`: puerto del dashboard (por defecto `8080`).

### 2. Levantar la infraestructura completa
```bash
docker compose up -d --build
```

Comprueba el estado de los 5 contenedores:
```bash
docker compose ps
```

### 3. Acceder al sistema
- **Dashboard GIS:** [http://localhost:8080/dashboard](http://localhost:8080/dashboard)
- **Login:** [http://localhost:8080/login](http://localhost:8080/login) *(inicia sesión con tu `API_KEY`)*
- **Documentación OpenAPI:** [http://localhost:8080/docs](http://localhost:8080/docs) *(solo en desarrollo)*

### Comandos útiles de operación
```bash
# Reconstruir únicamente un microservicio tras modificar su código
docker compose build api && docker compose up -d api

# Ver logs en tiempo real del bucle de refresco o del procesador
docker compose logs -f refresh
docker compose logs -f processor

# Detener los contenedores preservando la base de datos
docker compose down

# Detener y eliminar volúmenes (¡ATENCIÓN: borra los datos de PostgreSQL!)
docker compose down -v
```

> [!WARNING]
> La contraseña de PostgreSQL solo se configura durante la primera inicialización del volumen `pg_data`. Si modificas `POSTGRES_PASSWORD` en `.env` a posteriori, debes actualizarla mediante `ALTER USER firewatch WITH PASSWORD '...';` o reiniciar el volumen con `docker compose down -v`.

---

## 🧪 Batería de Pruebas Automatizadas

El proyecto cuenta con **123 pruebas unitarias y de integración** automatizadas con `pytest`.

Todas las pruebas son completamente **herméticas**: no realizan peticiones de red externas (usan `httpx.MockTransport`) ni requieren contenedores levantados (utilizan fakes en memoria para Kafka y la base de datos).

```bash
python -m pytest
```

### Cobertura de la Suite de Tests

| Archivo de Test | Alcance |
|---|---|
| `test_clients.py` | Clientes HTTP de NASA FIRMS y Open-Meteo, conversión de bboxes, parseo CSV y **caché espacial de 0.25° con cacheo de errores**. |
| `test_ingestion.py` | Publicación idempotente en Kafka, control de duplicados y transaccionalidad. |
| `test_processing.py` | Consumo de Kafka, mapeo `record_to_row`, fallback de canales térmicos, upsert PostGIS y descarte de coordenadas argelinas. |
| `test_risk.py` | Humedad de combustible fino (Simard), propagación por viento, matriz multi-criterio y alertas `high`/`extreme`. |
| `test_regions.py` | Validación matemática de los Bounding Boxes, exclusión de la costa de Argelia y cobertura completa de península, Baleares y Canarias. |
| `test_ratelimit.py` | Limitador de tasa en `/auth/login`, mitigación de fuerza bruta, expiración de ventanas y aislamiento por cliente. |
| `test_api.py` | Endpoints REST, healthchecks, filtros de fecha/riesgo, autenticación por sesión y bloqueo en modo producción. |
| `test_tiers.py` | **Guardia estructural de dependencias**: analiza el AST del código fuente y verifica que `acquisition` no contenga `psycopg2` y `processing` no contenga `fastapi`. |
| `test_runner.py` | CLI, resolución de regiones, bucle `daily`, reintentos con backoff exponencial y códigos de salida de error. |
| `test_geocode.py` | Geocodificación inversa con Nominatim, preservación de toponimias en upserts y tolerancia ante puntos marítimos. |

---

## 📁 Estructura del Proyecto

```
firewatch/
├── Dockerfile                 # Dockerfile multi-stage con 3 targets (acquisition, processing, api)
├── docker-compose.yml         # Orquestación de 5 servicios (api, processor, refresh, postgres, kafka)
├── pyproject.toml             # Configuración del paquete Python e instalación editable
├── README.md                  # Documentación maestra unificada del sistema
├── requirements.txt           # Dependencias para entorno de desarrollo local completo
├── requirements/              # Dependencias segregadas por microservicio
│   ├── base.txt               # Librerías compartidas (httpx, kafka-python, pydantic-settings)
│   ├── acquisition.txt        # Servicio de ingesta y refresco (sin psycopg2, sin fastapi)
│   ├── processing.txt         # Servicio de consumo y scoring (sin fastapi)
│   └── api.txt                # API FastAPI y servidor web
├── src/
│   ├── firewatch/             # Paquete principal de la aplicación
│   │   ├── config.py          # Configuración centralizada, variables de entorno y Bounding Boxes
│   │   ├── runner.py          # CLI unificado (ingest, backfill, daily, process, geocode-backfill)
│   │   ├── clients/           # Clientes HTTP externos desacoplados
│   │   │   ├── firms.py       # Adaptador oficial de NASA FIRMS API
│   │   │   ├── weather.py     # Cliente Open-Meteo con caché espacial por celda de 0.25°
│   │   │   └── geocode.py     # Cliente de geocodificación inversa con Nominatim
│   │   ├── services/          # Lógica de negocio y procesamiento de datos
│   │   │   ├── ingestion.py   # Productor de Kafka con publicación idempotente
│   │   │   ├── processing.py  # Consumidor de Kafka, validación geográfica y upsert PostGIS
│   │   │   └── risk.py        # Modelo físico de combustible (Simard) y propagación
│   │   ├── api/               # Capa web y endpoints REST
│   │   │   ├── main.py        # Aplicación FastAPI, configuración y middleware de estáticos
│   │   │   ├── endpoints.py   # Rutas de datos (`/events`, `/stats`, `/dates`, `/ingest`)
│   │   │   ├── auth.py        # Dependencias de seguridad (`require_api_key`, `require_session`)
│   │   │   ├── auth_routes.py # Rutas de sesión (`/auth/login`, `/auth/logout`, `/login`)
│   │   │   └── ratelimit.py   # Limitador de tasa contra ataques de fuerza bruta
│   │   ├── static/            # Interfaz gráfica del dashboard
│   │   │   ├── index.html     # Dashboard GIS con Leaflet
│   │   │   ├── login.html     # Página de login autocontenida
│   │   │   ├── app.js         # Lógica interactiva del mapa, heatmap, clústeres y calendario
│   │   │   └── style.css      # Hoja de estilos moderna en tema oscuro
│   │   └── tests/             # Batería completa de 123 tests automatizados
│   └── fixtures/              # Muestras JSON/CSV para pruebas unitarias
└── logs/                      # Volumen de logs rotativos de la aplicación
```