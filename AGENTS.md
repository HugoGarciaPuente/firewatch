# FireWatch AGENTS.md

## Project Structure Overview

```
firewatch/
├── Dockerfile                 # One Dockerfile, 3 targets: acquisition/processing/api
├── .dockerignore              # Keeps .env and tests out of the image
├── docker-compose.yml          # 5 services; each app service names its build target
├── .env.example                # Environment variables template
├── .gitignore                 # .env, caches, logs
├── pyproject.toml              # Python project config (setuptools + pytest)
├── requirements.txt            # Union for local installs (aggregates requirements/api.txt)
├── requirements/               # Per-tier dependency split, consumed by Dockerfile --target
│   ├── README.md               # Why the split exists, and what it does NOT buy
│   ├── base.txt                # Shared by all three tiers
│   ├── acquisition.txt         # refresh  (no psycopg2, no fastapi)
│   ├── processing.txt          # processor (no fastapi)
│   └── api.txt                 # api (everything)
├── src/
│   ├── firewatch/              # Main Python package
│   │   ├── __init__.py
│   │   ├── config.py           # Settings: env vars + REGION_BBOXES
│   │   ├── runner.py           # CLI: ingest / backfill / daily / process / geocode-backfill
│   │   ├── clients/            # External API clients (all outbound calls here)
│   │   │   ├── __init__.py
│   │   │   ├── firms.py        # NASA FIRMS API client
│   │   │   ├── weather.py      # Open-Meteo client, cached per 0.25° cell
│   │   │   └── geocode.py      # Nominatim reverse geocoding, cached per exact point
│   │   ├── services/           # Business services
│   │   │   ├── __init__.py
│   │   │   ├── ingestion.py    # Kafka producer ingestion
│   │   │   ├── processing.py   # Kafka consumer -> PostGIS writer
│   │   │   └── risk.py         # Risk scoring (Phase 2)
│   │   ├── api/                # FastAPI application
│   │   │   ├── __init__.py
│   │   │   ├── auth.py         # API key + signed session cookie
│   │   │   ├── auth_routes.py  # /auth/login, /auth/logout, login page
│   │   │   ├── ratelimit.py    # Login throttle: 5 attempts/min per client
│   │   │   ├── main.py         # FastAPI app, docs gating, /static guard
│   │   │   └── endpoints.py    # API endpoints
│   │   ├── static/             # Dashboard assets (session-guarded)
│   │   │   ├── index.html
│   │   │   ├── login.html      # Self-contained: cannot load /static
│   │   │   ├── app.js
│   │   │   └── style.css
│   │   └── tests/              # Test suite (mocked, no live calls)
│   │       ├── __init__.py
│   │       ├── conftest.py
│   │       ├── test_clients.py
│   │       ├── test_config.py
│   │       ├── test_geocode.py
│   │       ├── test_ingestion.py
│   │       ├── test_processing.py
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

Tracking covers **only Península Ibérica, Baleares and Canarias** (see
`settings.REGION_BBOXES`). Do not widen the default scope: a continental
European bbox produces ~76× more daily records with no value for these
territories. `resolve_regions()` in `config.py` is the single place that
resolves scope (`--bbox` > `--regions` > `FIREWATCH_REGIONS`); it lives in
`config` so the API can use it without importing the CLI.

## Available Commands

- `docker compose up -d --build` - **Start everything** (api, processor, refresh, postgres, kafka)
- `docker compose build api` - Rebuild one tier only (target: api/processing/acquisition)
- `docker compose logs -f refresh` - Watch the FIRMS refresh loop
- `docker compose ps` - Confirm the five services are up (only `api`, `postgres` and `kafka` expose a healthcheck)
- `pip install -e .` or `pip install -r requirements-dev.txt` - Local dev setup
- `pytest` - Run tests (114 tests, no live services required)
- `python -m firewatch.runner ingest` - One-shot FIRMS fetch -> Kafka
- `python -m firewatch.runner backfill --start-date D --end-date D` - Historical load (one request per day)
- `python -m firewatch.runner daily --days 3` - Loop that keeps data fresh
- `python -m firewatch.runner process` - Consumer -> PostGIS (continuous)
- `python -m firewatch.runner geocode-backfill --limit N` - Fill in missing localities (rate-limited)

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
  the same as NULL, which means "not attempted yet". `--recheck` retries the
  former.
- **Adding an import can break a container, not just the code.**
  `requirements/` is split per tier, so a new third-party import in
  `services/` means only the tiers that reach it get the package. The image
  still builds; it dies at startup instead. `test_tiers.py` walks the real
  import graph and names the file to fix. Run it after touching imports.
- **The tiers share more dependencies than the structure suggests.** All three
  need `httpx` and `kafka-python`: `services/processing.py` reaches
  `clients/weather.py` via the risk scorer, and `api/endpoints.py` reaches
  `services/ingestion.py`. Only the web stack and `psycopg2` are separable.
- **A missing `build.target` in compose fails silently.** The Dockerfile's last
  stage becomes the default image, so a typo builds the wrong tier without
  error. `test_tiers.py::test_compose_build_targets_are_explicit` guards it.
- **`docker compose down` does not delete data**, but `down -v` deletes the
  Postgres volume. The ingested events in `fire_events` live there and nowhere
  else; losing the volume means re-running the backfill.
- **Static assets are cache-busted with `?v=N`.** After editing `app.js` or
  `style.css`, bump `N` in `static/index.html`, otherwise the browser keeps
  serving the cached copy and the change appears to have no effect.
- **FIRMS serves one day per request.** No date ranges. `backfill` iterates
  day by day; this is inherent to the API, not a bug.
- **FIRMS reissues recent days** as the product matures. `runner daily`
  deliberately re-fetches the trailing window; the upsert is idempotent so
  this only refreshes risk values.
- **Cache outbound weather calls by `(cell, date)`.** A backfill is thousands
  of records; one Open-Meteo request per record would hammer a free public API.
  The date is part of the key: a fire from 7 October must not be scored with the
  weather of the day the backfill runs. Failures are cached for 5 minutes, so a
  dead upstream does not retry per record but also does not poison the whole run.
- **FIRMS gives an anomaly point, not a fire perimeter.** Do not draw
  polygons to "show the affected area": a convex hull over distant points
  represents no real zone, and an arbitrary buffer is invented geometry. An
  earlier implementation was removed for exactly this reason.
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
  `refresh` Compose service; check `docker compose ps` and
  `docker compose logs refresh`.
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
- **FIRMS has no data for the current day.** `last_date` normally trails by
  one day; that is NRT latency, not a bug.
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
