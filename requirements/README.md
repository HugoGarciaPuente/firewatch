# Dependency tiers

The Dockerfile installs exactly one tier file per target:

| Target | File | What it adds |
|---|---|---|
| `acquisition` | `acquisition.txt` | FIRMS/Open-Meteo clients and the Kafka producer |
| `processing` | `processing.txt` | Kafka consumer, PostGIS writer, EFFIS labels |
| `forecast` | `forecast.txt` | ML inference worker, database access, no web stack |
| `api` | `api.txt` | FastAPI, dashboard, database access |

`base.txt` is shared by all four. `requirements.txt` is the union used by
`pip install -e .` on the host; the image build never reads it.

The split is not a security boundary. All four tiers need `httpx` and
`kafka-python`, and the processing tier reaches the weather client through the
risk scorer. What it buys is narrower: acquisition cannot import `psycopg2`, so
a failed refetch cannot write to the database, and acquisition/processing do not
carry the web stack. `src/firewatch/tests/test_tiers.py` walks the real import
graph and fails if a tier's requirements are missing a package.

`pandas` and `scikit-learn` are installed only in the `forecast` tier, which
loads the trained joblib artifact and writes `fire_predictions`. They are not in
the API, processor or acquisition images. Offline training still uses
`requirements-dev.txt`.
