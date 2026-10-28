"""Catálogo de provincias y normalización de los nombres de EFFIS.

EFFIS no publica el código INE de la provincia y su campo ``province`` mezcla
tres cosas: provincias, islas y nombres bilingües. Por ejemplo, ``Tenerife`` y
``La Palma`` son islas de la provincia de Santa Cruz de Tenerife, ``Mallorca``
lo es de Illes Balears y ``Valencia/València`` es la misma provincia que
``Valencia``. Agrupar por el texto crudo crearía 57 unidades en lugar de las 52
provincias reales (50 provincias + Ceuta + Melilla) y rompería la serie
histórica de cada territorio.

Este módulo es la única fuente de verdad para ese mapeo. El código de dos
dígitos es el INE: ``01`` Álava ... ``52`` Melilla. Las coordenadas son las de
la capital de la provincia y se usan como punto representativo para la
meteorología; a escala provincial son una aproximación razonable y evitan
depender de geometrías administrativas.
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Province:
    """Provincia INE con un punto representativo para el clima."""

    code: str
    name: str
    latitude: float
    longitude: float


# Códigos INE de dos dígitos. El nombre es el usado en la interfaz; para las
# provincias bilingües se conserva la forma más reconocible en español.
PROVINCES: dict[str, Province] = {
    "01": Province("01", "Álava", 42.8467, -2.6716),
    "02": Province("02", "Albacete", 38.9943, -1.8585),
    "03": Province("03", "Alicante", 38.3452, -0.4815),
    "04": Province("04", "Almería", 36.8340, -2.4637),
    "05": Province("05", "Ávila", 40.6566, -4.6818),
    "06": Province("06", "Badajoz", 38.8794, -6.9707),
    "07": Province("07", "Illes Balears", 39.5696, 2.6502),
    "08": Province("08", "Barcelona", 41.3874, 2.1686),
    "09": Province("09", "Burgos", 42.3439, -3.6969),
    "10": Province("10", "Cáceres", 39.4713, -6.3722),
    "11": Province("11", "Cádiz", 36.5271, -6.2886),
    "12": Province("12", "Castellón", 39.9864, -0.0513),
    "13": Province("13", "Ciudad Real", 38.9863, -3.9291),
    "14": Province("14", "Córdoba", 37.8882, -4.7794),
    "15": Province("15", "A Coruña", 43.3623, -8.4115),
    "16": Province("16", "Cuenca", 40.0704, -2.1374),
    "17": Province("17", "Girona", 41.9794, 2.8214),
    "18": Province("18", "Granada", 37.1773, -3.5986),
    "19": Province("19", "Guadalajara", 40.6333, -3.1667),
    "20": Province("20", "Gipuzkoa", 43.3183, -1.9812),
    "21": Province("21", "Huelva", 37.2614, -6.9447),
    "22": Province("22", "Huesca", 42.1401, -0.4089),
    "23": Province("23", "Jaén", 37.7796, -3.7849),
    "24": Province("24", "León", 42.5987, -5.5671),
    "25": Province("25", "Lleida", 41.6176, 0.6200),
    "26": Province("26", "La Rioja", 42.4627, -2.4449),
    "27": Province("27", "Lugo", 43.0121, -7.5559),
    "28": Province("28", "Madrid", 40.4168, -3.7038),
    "29": Province("29", "Málaga", 36.7213, -4.4214),
    "30": Province("30", "Murcia", 37.9922, -1.1307),
    "31": Province("31", "Navarra", 42.8125, -1.6458),
    "32": Province("32", "Ourense", 42.3358, -7.8639),
    "33": Province("33", "Asturias", 43.3619, -5.8494),
    "34": Province("34", "Palencia", 42.0095, -4.5288),
    "35": Province("35", "Las Palmas", 28.1235, -15.4363),
    "36": Province("36", "Pontevedra", 42.4310, -8.6444),
    "37": Province("37", "Salamanca", 40.9701, -5.6635),
    "38": Province("38", "Santa Cruz de Tenerife", 28.4636, -16.2518),
    "39": Province("39", "Cantabria", 43.4623, -3.8099),
    "40": Province("40", "Segovia", 40.9429, -4.1088),
    "41": Province("41", "Sevilla", 37.3891, -5.9845),
    "42": Province("42", "Soria", 41.7666, -2.4790),
    "43": Province("43", "Tarragona", 41.1189, 1.2445),
    "44": Province("44", "Teruel", 40.3456, -1.1065),
    "45": Province("45", "Toledo", 39.8628, -4.0273),
    "46": Province("46", "Valencia", 39.4699, -0.3763),
    "47": Province("47", "Valladolid", 41.6523, -4.7245),
    "48": Province("48", "Bizkaia", 43.2630, -2.9350),
    "49": Province("49", "Zamora", 41.5033, -5.7446),
    "50": Province("50", "Zaragoza", 41.6488, -0.8891),
    "51": Province("51", "Ceuta", 35.8894, -5.3213),
    "52": Province("52", "Melilla", 35.2923, -2.9381),
}

# Variantes que aparecen en EFFIS y no coinciden con el nombre canónico. Las
# islas se asignan a su provincia: Tenerife, La Palma, La Gomera y El Hierro a
# Santa Cruz de Tenerife; Gran Canaria a Las Palmas; Mallorca, Menorca y
# Eivissa y Formentera a Illes Balears. ``Oriente`` es una comarca asturiana
# que EFFIS usa como provincia en un registro sin municipio; se conserva para
# que el mapeo no pierda esa fila aunque el umbral de 10 ha la descarte.
PROVINCE_ALIASES: dict[str, str] = {
    "A Coruña": "15",
    "Coruña, A": "15",
    "Albacete": "02",
    "Alicante": "03",
    "Alicante/Alacant": "03",
    "Almería": "04",
    "Álava": "01",
    "Araba/Álava": "01",
    "Asturias": "33",
    "Oriente": "33",
    "Ávila": "05",
    "Badajoz": "06",
    "Baleares": "07",
    "Illes Balears": "07",
    "Eivissa y Formentera": "07",
    "Mallorca": "07",
    "Menorca": "07",
    "Barcelona": "08",
    "Burgos": "09",
    "Cáceres": "10",
    "Cádiz": "11",
    "Cantabria": "39",
    "Castellón": "12",
    "Castellón/Castelló": "12",
    "Ceuta": "51",
    "Ciudad Real": "13",
    "Córdoba": "14",
    "Cuenca": "16",
    "Girona": "17",
    "Granada": "18",
    "Guadalajara": "19",
    "Gipuzkoa": "20",
    "Huelva": "21",
    "Huesca": "22",
    "Jaén": "23",
    "La Rioja": "26",
    "Las Palmas": "35",
    "Gran Canaria": "35",
    "León": "24",
    "Lleida": "25",
    "Lugo": "27",
    "Madrid": "28",
    "Málaga": "29",
    "Melilla": "52",
    "Murcia": "30",
    "Navarra": "31",
    "Ourense": "32",
    "Palencia": "34",
    "Pontevedra": "36",
    "Salamanca": "37",
    "Santa Cruz de Tenerife": "38",
    "Tenerife": "38",
    "La Palma": "38",
    "La Gomera": "38",
    "El Hierro": "38",
    "Segovia": "40",
    "Sevilla": "41",
    "Soria": "42",
    "Tarragona": "43",
    "Teruel": "44",
    "Toledo": "45",
    "Valencia": "46",
    "Valencia/València": "46",
    "Valladolid": "47",
    "Bizkaia": "48",
    "Zamora": "49",
    "Zaragoza": "50",
}

PROVINCE_CODES: tuple[str, ...] = tuple(PROVINCES)


def _key(name: str) -> str:
    """Normaliza mayúsculas, tildes y espacios para comparar nombres."""
    decomposed = unicodedata.normalize("NFKD", str(name).strip())
    without_accents = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(without_accents.casefold().split())


_ALIAS_BY_KEY: dict[str, str] = {
    _key(alias): code for alias, code in PROVINCE_ALIASES.items()
}
# Los nombres canónicos también son alias válidos. ``setdefault`` deja ganar la
# variante explícita si alguna vez hubiera una colisión.
for _code, _province in PROVINCES.items():
    _ALIAS_BY_KEY.setdefault(_key(_province.name), _code)


def normalize_province(name: Optional[str]) -> Optional[str]:
    """Devuelve el código INE de ``name`` o ``None`` si no se reconoce.

    Devolver ``None`` es deliberado: una provincia desconocida debe quedar
    visible para que el comando de mapeo la reporte, no asignarse en silencio a
    una provincia parecida.
    """
    if not name:
        return None
    return _ALIAS_BY_KEY.get(_key(name))


def province_name(code: str) -> Optional[str]:
    """Nombre canónico de una provincia, o ``None`` si el código no es INE."""
    province = PROVINCES.get(code)
    return province.name if province else None
