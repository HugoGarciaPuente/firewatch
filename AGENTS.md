# FireWatch AGENTS.md

## Project Structure Overview

```
firewatch/
├── Dockerfile                 # One Dockerfile, 4 targets: acquisition/processing/forecast/api
├── .dockerignore              # Keeps .env and tests out of the image
├── docker-compose.yml          # 6 services; each app service names its build target
├── .env.example                # Environment variables template
├── .gitignore                 # .env, caches, logs
├── pyproject.toml              # Python project config (setuptools + pytest)
├── requirements.txt            # Union for local installs (aggregates requirements/api.txt)
├── requirements/               # Per-tier dependency split, consumed by Dockerfile --target
│   ├── README.md               # Why the split exists, and what it does NOT buy
│   ├── base.txt                # Shared by all four tiers
│   ├── acquisition.txt         # refresh  (no psycopg2, no fastapi)
│   ├── processing.txt          # processor (no fastapi)
│   ├── forecast.txt            # forecast worker (pandas/sklearn, no fastapi)
│   └── api.txt                 # api (everything)
├── src/
│   ├── firewatch/              # Main Python package
│   │   ├── __init__.py
│   │   ├── config.py           # Settings: env vars + REGION_BBOXES
│   │   ├── runner.py           # CLI: ingest / backfill / daily / process / panel / forecast / ...
│   │   ├── clients/            # External API clients (all outbound calls here)
│   │   │   ├── __init__.py
│   │   │   ├── firms.py        # NASA FIRMS API client
│   │   │   ├── weather.py      # Open-Meteo hourly series, cached per (0.25° cell, date)
│   │   │   ├── daily_weather.py # Open-Meteo daily archive + forecast for provinces
│   │   │   └── geocode.py      # Nominatim reverse geocoding, cached per exact point
│   │   ├── services/           # Business services
│   │   │   ├── __init__.py
│   │   │   ├── ingestion.py    # Kafka producer ingestion
│   │   │   ├── processing.py   # Kafka consumer -> PostGIS writer
│   │   │   ├── risk.py         # Risk scoring (Phase 2)
│   │   │   ├── provinces.py    # INE province catalogue and aliases
│   │   │   ├── panel.py        # Province-day panel for the ML model
│   │   │   └── forecast.py     # fire_predictions table + idempotent upsert
│   │   ├── ml/                 # ML pipeline (training dev-only; inference forecast tier)
│   │   │   ├── __init__.py
│   │   │   ├── features.py     # Lagged weather, seasonality, leakage guards
│   │   │   ├── metrics.py      # PR-AUC, Brier, ECE, precision@k, lift
│   │   │   ├── forecast.py     # Inference frame, artifact guard, raw/calibrated scores
│   │   │   └── train.py        # Temporal split, climatology/logistic/HGB
│   │   ├── api/                # FastAPI application
│   │   │   ├── __init__.py
│   │   │   ├── auth.py         # API key + signed session cookie
│   │   │   ├── auth_routes.py  # /auth/login, /auth/logout, login page
│   │   │   ├── ratelimit.py    # Login throttle: 5 attempts/min per client
│   │   │   ├── main.py         # FastAPI app, docs gating, /static guard
│   │   │   └── endpoints.py    # API endpoints
│   │   ├── static/             # Dashboard assets (session-guarded)
│   │   │   ├── index.html
│   │   │   ├── stats.html      # Statistics section (session-guarded)
│   │   │   ├── forecast.html   # Provincial forecast map (session-guarded)
│   │   │   ├── sources.html    # Sources, APIs and methodology (session-guarded)
│   │   │   ├── login.html      # Self-contained: cannot load /static
│   │   │   ├── app.js
│   │   │   ├── stats.js
│   │   │   ├── forecast.js
│   │   │   ├── data/spain-provinces.geojson  # 52 INE province boundaries
│   │   │   └── style.css
│   │   └── tests/              # Test suite (mocked, no live calls)
│   │       ├── __init__.py
│   │       ├── conftest.py
│   │       ├── test_clients.py
│   │       ├── test_config.py
│   │       ├── test_daily_weather.py
│   │       ├── test_effis.py
│   │       ├── test_forecast_service.py
│   │       ├── test_geocode.py
│   │       ├── test_ingestion.py
│   │       ├── test_labels.py
│   │       ├── test_ml_features.py
│   │       ├── test_ml_forecast.py
│   │       ├── test_ml_metrics.py
│   │       ├── test_ml_train.py
│   │       ├── test_panel.py
│   │       ├── test_processing.py
│   │       ├── test_provinces.py
│   │       ├── test_ratelimit.py
│   │       ├── test_regions.py
│   │       ├── test_risk.py
│   │       ├── test_api.py
│   │       ├── test_tiers.py     # Guards the per-tier dependency split
│   │       └── test_runner.py
│   └── fixtures/               # JSON fixtures for tests
│       └── firms_sample.json   # Sample NASA FIRMS response
```

## Key Responsibilities

- **Agents**: Follow the implementation plan, preserve existing code, add working E2E behavior
- **Constraints**: env vars for secrets, dedicated clients/adapters, fixtures/mocks for tests, idempotent ingestion, separate risk-scoring code, Docker Compose for infra
- **Tests**: keep only assertions that can fail for a real reason. No tautologies,
  no tests for options no caller uses, no duplicate of a property another test
  already pins. A test that accepts both outcomes catches nothing.

## Domain Scope

Tracking covers **only Spain** (Península, Baleares and Canarias): the bboxes in
`settings.REGION_BBOXES` define what FIRMS is asked for, and
`FIREWATCH_COUNTRY_CODES` (default `es`) defines what is kept once the point is
geocoded. Do not widen the default scope: a continental European bbox produces
~76× more daily records with no value for these territories.
`resolve_regions()` in `config.py` is the single place that resolves the boxes
(`--bbox` > `--regions` > `FIREWATCH_REGIONS`); it lives in `config` so the API
can use it without importing the CLI. `country_in_scope()` is the equivalent for
the country allowlist.

## Available Commands

- `docker compose up -d --build` - **Start everything** (api, processor, forecast, refresh, postgres, kafka)
- `docker compose build api` - Rebuild one tier only (target: api/processing/forecast/acquisition)
- `docker compose logs -f refresh` - Watch the FIRMS refresh loop
- `docker compose logs -f forecast` - Watch the 3 h forecast worker
- `docker compose ps` - Confirm the six services are up (only `api`, `postgres` and `kafka` expose a healthcheck)
- `pip install -e .` or `pip install -r requirements-dev.txt` - Local dev setup
- `python -m pytest` - Run tests (243 tests, no live services required)
- `python -m ruff check src` - Static checks
- `python -m firewatch.runner ingest` - One-shot FIRMS fetch -> Kafka
- `python -m firewatch.runner backfill --start-date D --end-date D` - Historical load (one request per day)
- `python -m firewatch.runner daily --days 3 --interval-minutes 30` - Loop that keeps data fresh
- `python -m firewatch.runner process` - Consumer -> PostGIS (continuous)
- `python -m firewatch.runner geocode-backfill --limit N` - Fill in missing localities (rate-limited)
- `python -m firewatch.runner purge-out-of-scope [--dry-run]` - Delete stored points whose geocoded country is outside `FIREWATCH_COUNTRY_CODES` (idempotent)
- `python -m firewatch.runner labels --min-area-ha 1` - Import EFFIS confirmed fires (needs `psycopg2`)
- `python -m firewatch.runner map-provinces` - Fill `confirmed_fires.province_code` from the INE catalogue
- `python -m firewatch.runner panel --start-date D --end-date D --min-area-ha 10 --output data/panel_province_day.csv` - Build the province-day panel (needs Postgres)
- `python -m firewatch.runner province-weather --start-date D --end-date D --output data/weather_province_day.csv --sleep 2` - Download daily weather for every province
- `python -m firewatch.ml.train` - Train and evaluate the 24 h baseline (offline; pandas/scikit-learn)
- `python -m firewatch.runner forecast --dry-run` - Predict the next UTC day for the 52 provinces without writing
- `python -m firewatch.runner forecast` - Same, but upsert into `fire_predictions` (needs Postgres; dev deps for sklearn)
- `python -m firewatch.runner forecast --loop --interval-minutes 180` - Worker loop; this is what the `forecast` Compose service runs

## Gotchas

- **Never call Nominatim from a test.** `conftest.py` blocks outbound sockets
  so this fails loudly. It already happened once: `test_processing` built a
  real `GeoCodeClient` and geocoded its fixture against the live service, which
  returned "Manaus, Brasil" for a Spanish bounding box and consumed the rate
  limit budget.
- **Nominatim returns HTTP 429 if you exceed 1 req/s.** `GEOCODE_MIN_INTERVAL`
  defaults to 1.1 s. A backfill of a few hundred points takes over 10 min; run
  it with `runner geocode-backfill --limit N` in chunks. Stop the `processor`
  first if it is also geocoding, or they share the same budget.
- **Locality is never overwritten with NULL.** The upsert uses
  `COALESCE(EXCLUDED.locality, fire_events.locality)`, because FIRMS reissues
  recent days and a Nominatim outage must not blank every name in the database.
- **`locality = '{}'` means "tried, no locality found"** (open sea). It is not
  the same as NULL, which means "not attempted yet" or "the lookup failed".
  `get_location_checked` raises `GeocodeError` on HTTP/parse failures;
  `geocode-backfill` leaves those rows NULL and exits 1 instead of writing `{}`.
  `--recheck` retries only the rows already marked `{}`.
- **`locality.province` is the province, not the autonomous community.**
  Nominatim puts the community in `address.state` (Extremadura) and the
  province in `address.province`/`state_district` (Cáceres); `locality.community`
  keeps the community. Grouping by a `state`-derived value merges Cáceres and
  Badajoz into "Extremadura", which defeats the whole statistics section. The
  order is `province` > `state_district` > `county` > `state`: in France
  `county` is the département (Ariège) and in Portugal the district (Aveiro).
- **Statistics presets are anchored to the last date with data, not to today.**
  FIRMS NRT lags and the current day can be empty, so "último día/semana/mes/
  año" computed from `date.today()` would open empty windows. `/stats` reads
  `last_date` from `/api/v1/fire/stats`; the API range is inclusive on both ends.
- **The province selector filters, it does not define the dataset scope.**
  `/api/v1/fire/provinces` returns every `locality.province` in the table, and
  the table only holds the tracked country. "Todas las provincias" means the
  whole dataset, not "the province where I am looking": do not silently apply a
  second geographic filter in the browser.
- **The bboxes are rectangles: `peninsula` reaches the Algarve and southern
  France.** A point visible to FIRMS is not necessarily in Spain. The real
  country only arrives with the reverse geocoding, so `upsert_record` raises
  `OutOfScope` when `locality.country_code` is not in `FIREWATCH_COUNTRY_CODES`
  (default `es`) and the consumer counts it without storing it. Rows with no
  country yet are kept on purpose (they can still be resolved later);
  `runner purge-out-of-scope` cleans the ones stored before the filter existed.
  It is idempotent, but deleting is forever: run `--dry-run` first and remember
  that widening the allowlist does not bring the rows back.
- **A multi-month backfill is a single-threaded geocoding job.** ~14k points at
  `GEOCODE_MIN_INTERVAL` (1.1 s) is ~4 h of Nominatim. Do not run
  `geocode-backfill` at the same time: they share the 1 req/s budget. The
  processor keeps consuming while the API serves partial data, so the dashboard
  fills in as the job advances.
- **FIRMS NRT and SP cover different windows.** The area API's
  `data_availability` endpoint is the source of truth: SNPP_NRT starts
  2026-07-01 and SNPP_SP ends 2026-06-30, so a summer backfill needs
  `--source VIIRS_SNPP_SP` for June; NRT keeps only ~4 months. SNPP NRT also has
  a hole on 2026-07-11..16 (NOAA-20/21 return data those days), filled with
  `VIIRS_NOAA21_NRT`: re-running SNPP later adds different pixel centres for the
  same fires instead of replacing them.
- **Open-Meteo's forecast API only keeps ~30 days.** `WeatherClient` uses the
  ERA5 archive for older dates and retries the archive when the forecast returns
  a 200 with 24 nulls. This is not cosmetic: without it the score silently falls
  back to brightness alone, and a summer backfill looks successful while its
  risk values are not comparable to the rest. `test_clients` pins both paths.
- **`max_poll_records` must fit inside `max_poll_interval_ms`.** Geocoding costs
  ~1.1 s per record, so the old 500-record batch took ~9 min and Kafka evicted
  the consumer mid-batch; a large backfill would rebalance and reprocess. 25
  keeps the batch under the 5-minute poll interval, and `test_processing` pins
  the arithmetic instead of the number alone.
- **Kafka offsets are committed after PostgreSQL, not on poll.**
  `create_consumer` sets `enable_auto_commit=False`; `_commit_message` commits
  the exact `offset + 1` only after `upsert_record` has committed. Invalid and
  out-of-scope records are classified and committed; DB connection errors are
  retried with a fresh connection, and an unrecoverable one stops the consumer.
  Do not re-enable auto-commit: it can acknowledge messages that never reached
  Postgres.
- **The processor must reconnect after a PostgreSQL restart.** `_reconnect_database`
  retries with 1 s, 2 s and 5 s backoff, recreates the schema with `_ensure_table`
  and retries the same Kafka message. If all attempts fail, `DatabaseUnavailable`
  propagates so Compose restarts the processor; do not swallow it and continue.
- **Adding an import can break a container, not just the code.**
  `requirements/` is split per tier, so a new third-party import in
  `services/` means only the tiers that reach it get the package. The image
  still builds; it dies at startup instead. `test_tiers.py` walks the real
  import graph and names the file to fix. Run it after touching imports.
- **The tiers share more dependencies than the structure suggests.** All four
  need `httpx` and `kafka-python`: `services/processing.py` reaches
  `clients/weather.py` via the risk scorer, `api/endpoints.py` reaches
  `services/ingestion.py`, and `runner.py` imports `services.processing` at
  module level. Only the web stack, `psycopg2` and the ML stack are separable.
- **A missing `build.target` in compose fails silently.** The Dockerfile's last
  stage becomes the default image, so a typo builds the wrong tier without
  error. `test_tiers.py::test_compose_build_targets_are_explicit` guards it.
- **`docker compose down` does not delete data**, but `down -v` deletes the
  Postgres volume. The ingested events in `fire_events` live there and nowhere
  else; losing the volume means re-running the backfill.
- **Static assets are cache-busted with `?v=N`.** After editing `app.js` or
  `style.css`, bump `N` in every HTML page that links the asset
  (`static/index.html`, `stats.html`, `forecast.html`, `sources.html`), otherwise
  the browser keeps serving the cached copy and the change appears to have no
  effect.
- **The dashboard has no manual ingest UI.** `POST /api/v1/fire/ingest` remains
  for automation, but the "Nueva Ingesta" button, modal and submit handler were
  removed deliberately. Do not reintroduce a browser path that launches FIRMS
  ingests; ingest is the `refresh` service or the CLI.
- **FIRMS serves one day per request.** No date ranges. `backfill` iterates
  day by day; this is inherent to the API, not a bug.
- **FIRMS reissues recent days** as the product matures. `runner daily`
  deliberately re-fetches the trailing window; the upsert is idempotent so
  this only refreshes risk values.
- **Cache the hourly weather series by `(cell, date)`.** A backfill is thousands
  of records; one Open-Meteo request per record would hammer a free public API.
  The date is part of the key: a fire from 7 October must not be scored with the
  weather of the day the backfill runs. One request returns the 24 h of that day;
  `get_weather(..., acq_time=...)` selects the hour nearest the overpass, and
  without a valid `acq_time` it falls back to daily extremes. Failures are cached
  for 5 minutes, so a dead upstream does not retry per record but also does not
  poison the whole run.
- **FIRMS `acq_time` is HHMM UTC without zero padding.** `"211"` is 02:11, not
  21:10: parse it as an integer and treat the last two digits as minutes.
  Open-Meteo is requested with `timezone=GMT`, so no conversion is needed. A
  night overpass must be scored with the night weather, not with the afternoon
  maximum.
- **FIRMS gives an anomaly point, not a fire perimeter.** Do not draw
  polygons to "show the affected area": a convex hull over distant points
  represents no real zone, and an arbitrary buffer is invented geometry. An
  earlier implementation was removed for exactly this reason.
- **`confirmed_fires` is not `fire_events`.** FIRMS detections are not
  confirmed fires; `runner labels` downloads EFFIS burnt areas into a separate
  table for the 24 h model. EFFIS ignores date filters, so the command pages
  the whole Spanish catalogue (~12.7k records, ~15 s per 250) and filters
  locally; `--limit` is the pilot shortcut. It needs `psycopg2`, so run it in
  `processor`/`api`, not in the acquisition image. `municipality_code` (INE)
  is filled later and the upsert uses `COALESCE`, so a re-import cannot erase
  it.
- **`peninsula` and `baleares` bboxes overlap** (Menorca 1.2°E, mainland
  3.3°E). Scope filtering uses the union; per-region counts need priority
  assignment.
- **Keeping the processor running consumes messages continuously.** A
  long-running `runner process` will drain the topic, so a subsequent
  bounded `process --max-empty-polls N` may report 0 messages because the
  background instance already consumed them.
- **A refresh loop only counts once it is actually running.** Implementing
  `runner daily` makes nothing automatic by itself — that is a silent
  failure mode where the code looks right and nothing runs. The refresh is the
  `refresh` Compose service; it polls every 30 min
  (`REFRESH_INTERVAL_MINUTES`). Check `docker compose ps` and
  `docker compose logs refresh`.
- **A forecast loop only counts once it is running.** The `forecast` Compose
  service runs `runner forecast --loop`; if it is down, the model artifact is
  missing, or the loop dies, `/forecast` keeps serving the previous target date.
  Check `docker compose ps` and `docker compose logs forecast`. The default
  cadence is 3 h (`FORECAST_INTERVAL_MINUTES`), while `refresh` polls FIRMS every
  30 min (`REFRESH_INTERVAL_MINUTES`). The forecast does not depend on FIRMS: it
  uses Open-Meteo history and forecast, and 3 h matches the regional model runs.
- **Never let the refresh report partial success.** `refresh_once()` returns 1
  if any region/day failed after retries. Do not "helpfully" swallow that:
  a silent partial refresh freezes the dataset unnoticed.
- **There is one Kafka topic: `firms-ingest`.** Risk scores live in Postgres,
  not in a second stream. An earlier `fire-risk-alerts` topic had zero
  consumers, duplicated `fire_events` and emitted 59% duplicates; it was
  removed. Do not add an output topic without a real consumer and a dedup key.
- **Kafka creates topics on the first `send()`.** `KAFKA_AUTO_CREATE_TOPICS_ENABLE`
  is on, so deleting a topic is not enough: a producer still running on an old
  image recreates it with the next message. Rebuild and restart the publishers
  first, then delete the topic.
- **FIRMS NRT data for the current day is incomplete.** Early in the day it can
  be empty and `last_date` may trail by one day; that is latency, not a bug.
  Later in the day the first overpasses do appear (21 records on 2026-10-23), so
  do not assume today is always absent.
- **Recent days change size a lot.** On 2026-10-08, re-fetching 2026-10-07
  went from 6 stored records to 50 available (31 of them `high`). Re-running
  the refresh is the entire point.
- **Auth: one shared API key, exchanged for a session cookie.** The browser
  never holds the key. `require_api_key`
  guards `POST /auth/login`; `require_session` guards `/dashboard` and every
  `/api/v1/fire/*` route.
- **The login throttle runs before the credential check.** `RateLimiter` is a
  dependency declared ahead of `require_api_key` (5 attempts/min per client),
  so a blocked caller never reaches the key comparison. Reordering the
  dependencies turns the throttle into a no-op after the first guess.
- **`APP_ENV=production` disables `/docs`, `/redoc`, `/openapi.json`** and
  refuses to boot without a valid `API_KEY` and a *different* `SESSION_SECRET`.
  Do not add a "temporarily" bypass: a silent auth downgrade is worse than a
  failed start.
- **Only tests that touch auth need the secrets set before importing config.**
  `test_api.py` sets `API_KEY`/`SESSION_SECRET` via `os.environ.setdefault` at
  the top because `_validate_settings()` raises on import under
  `APP_ENV=production`. Plain config tests do not need it: validation is
  skipped outside production.
- **`.env` must never be committed.** `.gitignore` now covers it. Verify with
  `git check-ignore .env` before the first commit.
- **PowerShell `Set-Content -Encoding UTF8` writes a BOM**, which breaks
  `pyproject.toml` parsing and mangles non-ASCII in HTML. Prefer the editor or
  `write` for those files; strip the BOM if you must use the shell.
- **`/static` is behind the session middleware, and `login.html` must stay
  self-contained.** Starlette's `mount()` has no per-mount `dependencies`
  parameter, so `guard_static_assets` is a middleware. Because the login page
  can no longer fetch `/static/style.css`, it inlines the CSS variables it
  needs. If you restyle the login page, keep it self-contained or you will
  reintroduce the bypass by making it depend on protected assets again.
- **`/static/index.html` must never be publicly reachable.** Serving the
  dashboard shell without auth reduces the gate to a client-side JS redirect,
  which anyone bypasses by disabling JavaScript.
- **The ML dependencies live in the forecast tier, not in the web or ingestion
  tiers.** `requirements/forecast.txt` installs `pandas` and `scikit-learn` for
  the `forecast` worker; `pyproject.toml` keeps them under `dev` for host
  training. `firewatch.ml` is imported by the forecast worker through
  `runner forecast`, never at module level by the API, processor or acquisition
  entry points. If you import it from those at top level, the image still builds
  and then dies at startup; `test_tiers.py` walks the real graph. The worker
  reads the joblib artifact from the read-only `./data/models` mount, so a
  re-train changes the model on the next loop without rebuilding the image.
- **The ML split is temporal: 2017-2024 train, 2025 validation, 2026 test.**
  Do not replace it with `train_test_split(random_state=...)`: a fire from the
  same province and week would land on both sides and inflate every metric.
  Hyperparameters and isotonic calibration are fitted on 2025 only; 2026 is
  read once for the final report.
- **Day-D weather is a reanalysis proxy for a forecast, not an archived
  forecast.** `tmax_c`, `rh_min`, `wind_max_kmh` and `precip_mm` are ERA5
  values for the target day. The 24 h model assumes the day-D forecast is
  available on D-1; the historical score is optimistic if the real forecast is
  worse. The antecedent features (`days_since_rain`, `precip_sum_7`, the lags)
  use only days before D, and `precip_sum_7` must never include D's rain.
- **`year` is not a feature.** It is kept for the split and the per-year
  report. As a feature it would ask the model to extrapolate to 2026, a value
  it never saw in training.
- **Raw HGB ranks better than calibrated HGB; calibrated probabilities are
  better calibrated.** Isotonic regression creates ties and lowers PR-AUC
  (0.244 vs 0.267 in the reference run) while cutting ECE (0.0069 vs 0.0144).
  Report both and pick per use case; do not silently replace the ranking score
  with the calibrated one. `runner forecast` stores both in `fire_predictions`:
  `probability` is the raw score, `probability_calibrated` the isotonic one.
- **`runner forecast` needs 35 days of history, not 14.** `days_since_rain`
  saturates at 30 in training; with a shorter window the target day gets a
  truncated dry spell and the model sees a different feature distribution.
  `build_forecast_frame` rejects fewer than 31 days, and the CLI defaults to 35.
  Do not lower `--history-days` to "make it faster": it is one archive request
  per province regardless of the range.
- **The target day is always requested from the forecast API, never ERA5.**
  `DailyWeatherClient.fetch_forecast` exists for exactly this. `fetch_daily`
  may fall back to the forecast for the recent history tail, but mixing the
  target day with archive data would silently use a different variable than the
  one the model is predicting with. If the forecast has no row for the target,
  the province fails.
- **The forecast is all-or-nothing.** `runner forecast` computes all requested
  provinces first and writes them in one transaction. If one province is
  missing a day, a variable is NULL, or the forecast is absent, it exits 1 and
  writes nothing; the previous `fire_predictions` rows stay. Do not "helpfully"
  write the 51 that worked: a partial map looks complete and is worse than a
  failed run. `--dry-run` is the safe way to inspect coverage.
- **`fire_predictions` is derived and regenerable.** It is not a second copy of
  `fire_events` or `confirmed_fires`; losing the Postgres volume only means
  re-running `runner forecast`. The table is created by the job, not by the API,
  and the upsert key is `(target_date, province_code, model_name)`. Re-running
  the same day replaces rows and `generated_at`.
- **The forecast API defaults to the latest `target_date`, not today.**
  `GET /api/v1/fire/forecast` returns 404 when `fire_predictions` is empty or the
  table does not exist yet, because `runner forecast` creates it. The target day
  is usually tomorrow UTC and has no `fire_events` rows. It exposes both
  `probability` (raw, used for ranking) and `probability_calibrated` (shown as
  the probability); do not silently replace one with the other.
- **The forecast is a separate page, not a dashboard panel or layer.** `/forecast`
  serves `forecast.html` + `forecast.js` and fetches
  `/static/data/spain-provinces.geojson` plus `/api/v1/fire/forecast`. The
  dashboard only links to it. The province polygons are administrative
  boundaries for coloring the probability; they are not fire perimeters and must
  not be described as affected area. The GeoJSON is committed (unlike `data/`),
  uses the same INE `province_code` as `PROVINCES`, and `test_provinces.py` pins
  all 52 features. The map uses calibrated probability; the list is ranked by raw
  score.
- **`/sources` is a citation page, not another dashboard.** It serves
  `sources.html` and has no data calls; the only script is the inline logout
  handler. It must stay linked from every protected shell (`index.html`,
  `stats.html`, `forecast.html`) and behind `require_session`, like the other
  pages. The page is the place to update API names, licences and the short
  explanations of the heuristic risk score and the ML forecast; do not duplicate
  those claims in the dashboard.
- **`/data/` is gitignored and regenerable.** The panel (186k rows), the weather
  CSV and the `.joblib` artifacts do not belong in the repository. Rebuild them
  with `runner panel`, `runner province-weather` and `python -m
  firewatch.ml.train`. `reports/forecast_baseline.*` is the committed summary.
- **Open-Meteo rate-limits the province weather download.** `province-weather`
  sends one request per province for the full 2017-2026 range. A 429 burst can
  survive 15/30/45 s backoffs; use `--sleep 2` and, if a province still fails,
  fetch it alone later. Do not lower the sleep to "finish faster": the retry
  budget is shared by every province in the run.
- **The panel requires Postgres; the weather download does not.** `runner
  panel` reads `confirmed_fires` and writes a CSV, so run it where
  `POSTGRES_DSN` is reachable. `runner province-weather` only calls Open-Meteo
  and can run on the host. Neither is part of a container startup path.
- **A report with a new feature or hyperparameter is not the old report.**
  Re-run `python -m firewatch.ml.train` after changing `features.py` or the HGB
  parameters; the Markdown is generated, not hand-edited. Check the validation
  PR-AUC before looking at 2026, or you will tune on the test year.
