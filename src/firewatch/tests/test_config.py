"""Tests pinning how Settings converts environment strings.

``os.getenv`` always returns ``str``. A ``float`` annotation converts nothing by
itself, so these tests pin the conversions the annotation promises. Without
them the regression is silent until a caller divides by a string and dies with
``TypeError`` far from the real cause.
"""

from firewatch.config import settings


def test_numeric_settings_are_real_numbers():
    assert isinstance(settings.WEATHER_CACHE_CELL, float)
    assert isinstance(settings.GEOCODE_MIN_INTERVAL, float)
    assert isinstance(settings.SESSION_MAX_AGE, int)


def test_numeric_settings_survive_arithmetic():
    """The concrete failure mode is the operation, not the type name."""
    assert 40.0 / settings.WEATHER_CACHE_CELL > 0
    assert settings.GEOCODE_MIN_INTERVAL + 0.4 > 1.4


def test_boolean_settings_are_real_booleans():
    """``bool("false")`` is True, so these must not be raw env strings."""
    assert isinstance(settings.ENABLE_GEOCODING, bool)
    assert isinstance(settings.ENABLE_WEATHER_ENRICHMENT, bool)
