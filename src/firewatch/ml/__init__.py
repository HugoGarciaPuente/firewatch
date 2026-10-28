"""Offline ML pipeline for the 24-hour province-level fire forecast.

This package is intentionally outside the runtime tiers: training and inference
need pandas and scikit-learn, which the API, processor and acquisition images do
not carry. `runner forecast` writes predictions to Postgres; the API will read
that table, never this package.
"""
