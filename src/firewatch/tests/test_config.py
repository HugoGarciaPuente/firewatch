"""Tests pinning how Settings converts environment strings.

``os.getenv`` always returns ``str``. A ``float`` annotation converts nothing by
itself, so these tests pin the conversions the annotation promises. Without
them the regression is silent until a caller divides by a string and dies with
``TypeError`` far from the real cause.
"""

from firewatch.config import country_in_scope, settings, tracked_country_codes


def test_numeric_settings_are_real_numbers():
    """``os.getenv`` returns str; a float annotation converts nothing by itself."""
    assert isinstance(settings.WEATHER_CACHE_CELL, float)
    assert isinstance(settings.GEOCODE_MIN_INTERVAL, float)
    assert isinstance(settings.SESSION_MAX_AGE, int)
    # The concrete failure mode is the operation, not the type name.
    assert 40.0 / settings.WEATHER_CACHE_CELL > 0


def test_boolean_settings_are_real_booleans():
    """``bool("false")`` is True, so these must not be raw env strings."""
    assert isinstance(settings.ENABLE_GEOCODING, bool)
    assert isinstance(settings.ENABLE_WEATHER_ENRICHMENT, bool)


def test_tracked_country_codes_parses_the_env_list(monkeypatch):
    monkeypatch.setattr(settings, "FIREWATCH_COUNTRY_CODES", " ES , pt ,")
    assert tracked_country_codes() == {"es", "pt"}


def test_country_in_scope_uses_the_geocoded_country_code(monkeypatch):
    """The bboxes also reach Portugal and France: the country decides, not the bbox."""
    monkeypatch.setattr(settings, "FIREWATCH_COUNTRY_CODES", "es")
    assert country_in_scope({"country_code": "es", "country": "España"})
    assert country_in_scope({"country_code": "ES"})
    assert not country_in_scope({"country_code": "pt", "country": "Portugal"})
    assert not country_in_scope({"country_code": "fr", "country": "France"})


def test_country_in_scope_keeps_points_without_a_country():
    """NULL/{} means "not resolved yet"; rejecting it would lose recoverable rows."""
    assert country_in_scope(None)
    assert country_in_scope({})
    assert country_in_scope({"locality": "Alarcón"})
