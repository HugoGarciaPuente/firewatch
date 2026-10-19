"""Evaluación y modelado de riesgo para eventos de fuego (Fase 2).

Integra:
- Intensidad de anomalía térmica (brillo en Kelvin)
- Cálculo de humedad de combustible ligero (contenido de humedad de equilibrio según temperatura y humedad)
- Velocidad del viento y modelado de propagación
- Clasificación por categorías de riesgo ('low', 'moderate', 'high', 'extreme')

Contrato:
    score_event(event, weather=None) -> {"risk_level": str, "score": float, "factors": dict}
"""

from __future__ import annotations

from typing import Dict, Optional

# Niveles de riesgo en orden ascendente
RISK_LEVELS = ("low", "moderate", "high", "extreme")


def _brightness_to_score(brightness: float | None) -> float:
    """Línea base: brillo térmico de FIRMS (Kelvin) mapeado a 0..1."""
    if brightness is None:
        return 0.0
    # 300 K = base ambiental/anomalía baja -> 0.0 ; 400+ K -> 1.0
    return max(0.0, min(1.0, (brightness - 300.0) / 100.0))


def _score_to_level(score: float) -> str:
    """Mapea el score continuo normalizado 0..1 a categorías discretas de riesgo."""
    if score < 0.25:
        return "low"
    if score < 0.5:
        return "moderate"
    if score < 0.75:
        return "high"
    return "extreme"


def calculate_fuel_moisture(temperature_c: float, relative_humidity: float) -> float:
    """
    Calcula el porcentaje de humedad de combustible fino/10 horas en función de
    la temperatura (°C) y la humedad relativa (%).

    Devuelve la humedad estimada de combustible (típicamente entre 1% y 35%).
    Valores inferiores a 10% representan combustible peligrosamente seco propenso a ignición rápida.
    """
    h = max(0.0, min(100.0, relative_humidity))
    t = temperature_c

    # Fórmula adaptada de Simard para el equilibrio de contenido de humedad (EMC)
    if h < 10.0:
        emc = 0.03229 + (0.281073 * h) - (0.000578 * h * t)
    elif h < 50.0:
        emc = 2.22749 + (0.160107 * h) - (0.01478 * t)
    else:
        emc = 21.0606 + (0.005565 * (h ** 2)) - (0.00035 * h * t) - (0.483199 * h)

    return max(1.0, min(35.0, round(emc, 2)))


def calculate_spread_index(wind_speed_kmh: float, fuel_moisture: float) -> float:
    """
    Estima el índice de propagación del fuego según la velocidad del viento y la sequedad del combustible.

    Valores más altos indican condiciones propicias para una propagación acelerada.
    """
    # El factor de viento incrementa el potencial de propagación
    wind_factor = 1.0 + (max(0.0, wind_speed_kmh) / 25.0) ** 1.3
    # El combustible seco acelera la propagación
    dryness_multiplier = max(0.5, (30.0 - min(30.0, fuel_moisture)) / 10.0)
    return round(wind_factor * dryness_multiplier, 2)


def score_event(event: Dict, weather: Optional[Dict] = None) -> Dict:
    """
    Calcula la evaluación de riesgo para un evento individual de FIRMS.

    Combina la anomalía de brillo con las condiciones ambientales (temperatura,
    humedad relativa, velocidad del viento) cuando están disponibles.
    """
    raw_b = event.get("brightness") if event.get("brightness") is not None else event.get("bright_ti4")
    brightness = float(raw_b) if raw_b is not None else None

    # El clima puede pasarse explícitamente o incrustado en el evento
    w = weather or event.get("weather") or {}
    temp_c = w.get("temperature_c") if "temperature_c" in w else event.get("temperature_c")
    rh = w.get("relative_humidity") if "relative_humidity" in w else event.get("relative_humidity")
    wind = w.get("wind_speed_kmh") if "wind_speed_kmh" in w else event.get("wind_speed_kmh")

    factors: Dict = {"brightness": brightness}

    b_score = _brightness_to_score(brightness)

    if temp_c is not None and rh is not None and wind is not None:
        temp_c = float(temp_c)
        rh = float(rh)
        wind = float(wind)

        fuel_moisture = calculate_fuel_moisture(temp_c, rh)
        spread_idx = calculate_spread_index(wind, fuel_moisture)

        # Score de sequedad: <8% sequedad extrema (score 1.0), >25% bajo riesgo (score 0.0)
        fuel_score = max(0.0, min(1.0, (25.0 - fuel_moisture) / 18.0))
        # Score de viento: 0..60 km/h mapeado a 0..1
        wind_score = max(0.0, min(1.0, wind / 60.0))

        # Evaluación multicriterio ponderada
        composite_score = (0.45 * b_score) + (0.35 * fuel_score) + (0.20 * wind_score)
        score = max(0.0, min(1.0, composite_score))

        factors.update(
            {
                "temperature_c": temp_c,
                "relative_humidity": rh,
                "wind_speed_kmh": wind,
                "fuel_moisture_pct": fuel_moisture,
                "spread_index": spread_idx,
            }
        )
    else:
        # Línea base retrocompatible cuando no se aportan datos de clima
        score = b_score

    return {
        "risk_level": _score_to_level(score),
        "score": round(score, 4),
        "factors": factors,
    }
