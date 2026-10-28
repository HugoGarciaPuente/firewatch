"""Tests for the province-day panel (no Postgres required)."""

import csv
from datetime import date

import pytest

from firewatch.services.panel import (
    PANEL_FIELDS,
    assemble_panel,
    build_panel,
    write_panel_csv,
)


def test_assemble_panel_expands_every_province_day():
    rows = list(
        assemble_panel(
            ("01", "02"),
            date(2024, 1, 1),
            date(2024, 1, 3),
            [
                ("01", date(2024, 1, 2), 2, 30.5),
                ("02", date(2024, 1, 3), 1, 4.0),
            ],
        )
    )

    assert len(rows) == 6
    by_key = {(row["province_code"], row["day"]): row for row in rows}
    assert by_key[("01", "2024-01-02")]["label"] == 1
    assert by_key[("01", "2024-01-02")]["fire_count"] == 2
    assert by_key[("01", "2024-01-02")]["area_ha"] == 30.5
    assert by_key[("02", "2024-01-03")]["label"] == 1
    assert by_key[("01", "2024-01-01")]["label"] == 0
    assert by_key[("01", "2024-01-01")]["fire_count"] == 0
    assert by_key[("02", "2024-01-02")]["label"] == 0


def test_assemble_panel_keeps_provinces_without_any_fire():
    rows = list(assemble_panel(("52",), date(2024, 1, 1), date(2024, 1, 2), []))
    assert [row["label"] for row in rows] == [0, 0]
    assert rows[0]["province_name"] == "Melilla"


def test_assemble_panel_rejects_an_inverted_range():
    with pytest.raises(ValueError, match="start_date"):
        list(assemble_panel(("01",), date(2024, 1, 2), date(2024, 1, 1), []))


def test_write_panel_csv_has_a_stable_header(tmp_path):
    path = tmp_path / "panel.csv"
    rows = assemble_panel(("01",), date(2024, 1, 1), date(2024, 1, 1), [])

    written = write_panel_csv(path, rows)

    assert written == 1
    with path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert tuple(reader.fieldnames) == PANEL_FIELDS
        assert list(reader)[0]["province_code"] == "01"


class PanelCursor:
    def __init__(self, rows, log):
        self._rows = rows
        self._log = log

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self._log.append((sql, params))

    def fetchall(self):
        return self._rows


class PanelConn:
    def __init__(self, rows):
        self.rows = rows
        self.log = []

    def cursor(self):
        return PanelCursor(self.rows, self.log)


def test_build_panel_filters_by_area_and_date_in_sql():
    conn = PanelConn([("01", date(2024, 1, 2), 1, 12.0)])

    rows = build_panel(
        conn,
        date(2024, 1, 1),
        date(2024, 1, 2),
        min_area_ha=10.0,
        province_codes=("01",),
    )

    sql, params = conn.log[0]
    assert "area_ha >= %s" in sql
    assert params == (10.0, date(2024, 1, 1), date(2024, 1, 2))
    assert len(rows) == 2
    assert rows[1]["label"] == 1


def test_build_panel_rejects_an_unknown_province_code():
    with pytest.raises(ValueError, match="99"):
        build_panel(
            PanelConn([]),
            date(2024, 1, 1),
            date(2024, 1, 2),
            province_codes=("99",),
        )
