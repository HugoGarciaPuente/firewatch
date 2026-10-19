"""Tests for the Phase 2 risk-scoring scaffold."""

from firewatch.services.risk import score_event


def test_none_brightness_is_low_risk():
    result = score_event({"acq_date": "2024-08-15", "latitude": 1.0, "longitude": 2.0})

    assert result["risk_level"] == "low"
    assert result["score"] == 0.0
    assert "brightness" in result["factors"]


def test_moderate_brightness_scores_proportionally():
    # 350 -> (350-300)/100 = 0.5 -> "high" boundary (>= 0.5)
    result = score_event({"brightness": 350.0})

    assert result["score"] == 0.5
    assert result["risk_level"] == "high"


def test_very_high_brightness_is_extreme():
    result = score_event({"brightness": 400.0})

    assert result["score"] == 1.0
    assert result["risk_level"] == "extreme"


def test_low_brightness_is_low_risk():
    result = score_event({"brightness": 320.5})

    assert result["score"] == 0.205
    assert result["risk_level"] == "low"


def test_score_is_clamped_out_of_range_values():
    assert score_event({"brightness": 1000.0})["score"] == 1.0
    assert score_event({"brightness": 10.0})["score"] == 0.0


def test_return_shape_is_stable_for_phase_2():
    """Callers (Phase 2 integration) rely on this contract."""
    result = score_event({"brightness": 330.0})

    assert set(result.keys()) == {"risk_level", "score", "factors"}
    assert result["risk_level"] in ("low", "moderate", "high", "extreme")
    assert isinstance(result["score"], float)


def test_calculate_fuel_moisture_variations():
    from firewatch.services.risk import calculate_fuel_moisture

    # Low humidity, high temp -> very dry fuel (< 10%)
    dry = calculate_fuel_moisture(temperature_c=38.0, relative_humidity=15.0)
    assert dry < 10.0

    # High humidity, cooler temp -> moist fuel (> 15%)
    moist = calculate_fuel_moisture(temperature_c=18.0, relative_humidity=75.0)
    assert moist > 15.0


def test_calculate_spread_index_increases_with_wind():
    from firewatch.services.risk import calculate_spread_index

    calm = calculate_spread_index(wind_speed_kmh=5.0, fuel_moisture=10.0)
    windy = calculate_spread_index(wind_speed_kmh=45.0, fuel_moisture=10.0)

    assert windy > calm


def test_multi_factor_risk_scoring_with_weather():
    weather = {
        "temperature_c": 40.0,
        "relative_humidity": 12.0,
        "wind_speed_kmh": 35.0,
    }
    event = {"brightness": 380.0}

    result = score_event(event, weather=weather)

    assert result["risk_level"] in ("high", "extreme")
    assert "fuel_moisture_pct" in result["factors"]
    assert "spread_index" in result["factors"]
    assert "wind_speed_kmh" in result["factors"]
    assert result["factors"]["fuel_moisture_pct"] < 10.0
