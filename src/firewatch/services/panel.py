"""Panel provincia-día para el modelo de predicción a 24 horas.

La unidad de predicción es ``(provincia, día)``: la etiqueta vale 1 si ese día
empezó al menos un incendio confirmado de ``min_area_ha`` hectáreas o más en la
provincia. Es una decisión de alcance deliberada: a nivel municipal el panel
tiene decenas de millones de filas y la mayoría de los municipios no tiene
ningún positivo; a nivel provincial quedan ~186.000 filas para 2017-2026, con
una tasa positiva de ~2 % en temporada.

El panel incluye todos los días, no solo la temporada de mayo a octubre. La
estacionalidad es una señal que el modelo debe aprender, y los negativos de
invierno son reales, no un artefacto. Las métricas se reportan también
restringidas a temporada para que la comparación operativa sea honesta.
"""

from __future__ import annotations

import csv
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence

from firewatch.services.provinces import PROVINCES, PROVINCE_CODES

logger = logging.getLogger(__name__)

PANEL_FIELDS = (
    "province_code",
    "province_name",
    "day",
    "label",
    "fire_count",
    "area_ha",
)


def _as_date(value) -> date:
    """Acepta ``date`` o ``datetime`` y devuelve siempre ``date``."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _validate_codes(province_codes: Optional[Sequence[str]]) -> tuple[str, ...]:
    codes = tuple(province_codes) if province_codes is not None else PROVINCE_CODES
    unknown = [code for code in codes if code not in PROVINCES]
    if unknown:
        raise ValueError(
            f"Unknown province code(s): {', '.join(unknown)}. "
            f"Available: {', '.join(PROVINCE_CODES)}"
        )
    return codes


def assemble_panel(
    province_codes: Sequence[str],
    start_date: date,
    end_date: date,
    label_rows: Iterable[tuple],
) -> Iterator[Dict]:
    """Cruza etiquetas dispersas con el producto cartesiano provincia × día.

    ``label_rows`` son tuplas ``(province_code, day, fire_count, area_ha)``.
    Los pares sin incendio se emiten con ``label=0`` y ``fire_count=0``; sin
    esta expansión el modelo solo vería los días con fuego y no podría aprender
    la línea base.
    """
    labels: Dict[tuple, tuple] = {}
    for code, day, fire_count, area_ha in label_rows:
        labels[(code, _as_date(day))] = (int(fire_count), float(area_ha or 0.0))

    days = (end_date - start_date).days + 1
    if days <= 0:
        raise ValueError(
            f"start_date ({start_date}) must be on or before end_date ({end_date})"
        )

    for code in province_codes:
        name = PROVINCES[code].name
        for offset in range(days):
            day = start_date + timedelta(days=offset)
            fire_count, area_ha = labels.get((code, day), (0, 0.0))
            yield {
                "province_code": code,
                "province_name": name,
                "day": day.isoformat(),
                "label": 1 if fire_count > 0 else 0,
                "fire_count": fire_count,
                "area_ha": round(area_ha, 4),
            }


def build_panel(
    conn,
    start_date: date,
    end_date: date,
    min_area_ha: float = 10.0,
    province_codes: Optional[Sequence[str]] = None,
) -> List[Dict]:
    """Lee las etiquetas de Postgres y devuelve el panel provincia-día.

    El filtro de superficie y de fechas se hace en SQL para no traerse las
    12.000 etiquetas completas cuando solo interesa una ventana y un umbral.
    """
    codes = _validate_codes(province_codes)
    if start_date > end_date:
        raise ValueError(
            f"start_date ({start_date}) must be on or before end_date ({end_date})"
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT province_code, firedate, COUNT(*), COALESCE(SUM(area_ha), 0)
            FROM confirmed_fires
            WHERE area_ha >= %s
              AND province_code IS NOT NULL
              AND firedate BETWEEN %s AND %s
            GROUP BY province_code, firedate
            """,
            (min_area_ha, start_date, end_date),
        )
        label_rows = cur.fetchall()

    return list(assemble_panel(codes, start_date, end_date, label_rows))


def write_panel_csv(path: Path | str, rows: Iterable[Dict]) -> int:
    """Escribe el panel en CSV y devuelve cuántas filas se han escrito."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PANEL_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            written += 1
    return written
