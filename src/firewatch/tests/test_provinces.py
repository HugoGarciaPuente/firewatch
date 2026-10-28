"""Tests for the INE province catalogue and EFFIS name normalization."""

import json
from pathlib import Path

from firewatch.services.provinces import (
    PROVINCES,
    PROVINCE_CODES,
    normalize_province,
    province_name,
)

# The 57 province-like values EFFIS actually returns for Spain. Keeping the
# list here makes a new upstream value fail the test instead of silently
# creating a 58th "province".
EFFIS_PROVINCES = [
    "A Coruña",
    "Albacete",
    "Alicante/Alacant",
    "Almería",
    "Araba/Álava",
    "Asturias",
    "Ávila",
    "Badajoz",
    "Barcelona",
    "Bizkaia",
    "Burgos",
    "Cáceres",
    "Cádiz",
    "Cantabria",
    "Castellón/Castelló",
    "Ceuta",
    "Ciudad Real",
    "Córdoba",
    "Cuenca",
    "Eivissa y Formentera",
    "El Hierro",
    "Gipuzkoa",
    "Girona",
    "Granada",
    "Gran Canaria",
    "Guadalajara",
    "Huelva",
    "Huesca",
    "Jaén",
    "La Gomera",
    "La Palma",
    "La Rioja",
    "León",
    "Lleida",
    "Lugo",
    "Madrid",
    "Málaga",
    "Mallorca",
    "Menorca",
    "Murcia",
    "Navarra",
    "Oriente",
    "Ourense",
    "Palencia",
    "Pontevedra",
    "Salamanca",
    "Segovia",
    "Sevilla",
    "Soria",
    "Tarragona",
    "Tenerife",
    "Teruel",
    "Toledo",
    "Valencia/València",
    "Valladolid",
    "Zamora",
    "Zaragoza",
]


def test_catalogue_has_the_52_ine_provinces():
    assert len(PROVINCES) == len(PROVINCE_CODES) == 52
    assert set(PROVINCES) == {f"{code:02d}" for code in range(1, 53)}


def test_every_effis_province_maps_to_a_known_code():
    missing = [name for name in EFFIS_PROVINCES if normalize_province(name) is None]
    assert missing == []
    # Melilla has no EFFIS fires in the catalogue; the other 51 units appear.
    assert len({normalize_province(name) for name in EFFIS_PROVINCES}) == 51


def test_islands_map_to_their_province():
    for island in ("Tenerife", "La Palma", "La Gomera", "El Hierro"):
        assert normalize_province(island) == "38"
    assert normalize_province("Gran Canaria") == "35"
    for island in ("Mallorca", "Menorca", "Eivissa y Formentera"):
        assert normalize_province(island) == "07"


def test_bilingual_and_accented_names_are_equivalents():
    assert normalize_province("Valencia/València") == normalize_province("Valencia") == "46"
    assert normalize_province("Alicante/Alacant") == normalize_province("Alicante") == "03"
    assert normalize_province("Castellón/Castelló") == normalize_province("Castellon") == "12"
    assert normalize_province("Araba/Álava") == normalize_province("Alava") == "01"
    assert normalize_province("  ávila  ") == "05"


def test_unknown_province_returns_none_instead_of_a_guess():
    assert normalize_province("Nueva Provincia") is None
    assert normalize_province("") is None
    assert normalize_province(None) is None


def test_province_name_round_trips():
    assert province_name("28") == "Madrid"
    assert province_name("07") == "Illes Balears"
    assert province_name("99") is None


def test_coordinates_fall_inside_the_spanish_territories():
    """A swapped coordinate would silently fetch weather from the Atlantic."""
    for province in PROVINCES.values():
        assert 27.0 <= province.latitude <= 44.0, province
        assert -19.0 <= province.longitude <= 5.0, province


def test_forecast_geojson_covers_the_same_52_provinces():
    """The map colors by province_code; a missing feature would leave a hole."""
    path = (
        Path(__file__).resolve().parents[1]
        / "static"
        / "data"
        / "spain-provinces.geojson"
    )
    data = json.loads(path.read_text(encoding="utf-8"))
    features = data["features"]

    assert data["type"] == "FeatureCollection"
    assert len(features) == 52
    codes = {feature["properties"]["province_code"] for feature in features}
    assert codes == set(PROVINCES)
    assert all(feature["properties"]["province_name"] for feature in features)
    assert all(feature["geometry"]["type"] in {"Polygon", "MultiPolygon"} for feature in features)
