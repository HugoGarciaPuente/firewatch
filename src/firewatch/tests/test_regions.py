"""Tests pinning the geographic scope of the tracked regions.

The boxes are a product decision, not a tuning knob. Each one here was widened
or narrowed because measured data showed the previous edge pulled in territory
the project does not track, so the reasoning is recorded next to the assertion.
"""

from firewatch.config import resolve_regions, settings

# REGION_BBOXES is a class attribute on Settings, so it is read off the instance.
REGION_BBOXES = settings.REGION_BBOXES


def _box(name):
    south, west, north, east = (float(v) for v in REGION_BBOXES[name].split(","))
    return south, west, north, east


def test_baleares_western_edge_excludes_the_algerian_coast():
    """The western edge was 0.4E, which reached into Algeria.

    Measured: with 0.4E the box admitted 78 Algerian fire points and nothing
    Spanish. 1.2E still clears Ibiza, the westernmost Balearic land (~1.22E),
    so trimming the edge cost no data at all.
    """
    south, west, north, east = _box("baleares")
    assert west >= 1.2, "baleares must not extend west into the Algerian coast"


def test_baleares_still_covers_every_balearic_island():
    """A trim that dropped Mallorca or Menorca would be a silent data loss."""
    south, west, north, east = _box("baleares")

    islands = {
        "Ibiza": (1.22, 38.60),
        "Mallorca": (2.35, 39.20),
        "Menorca": (3.85, 39.80),
    }
    for name, (lon, lat) in islands.items():
        assert west <= lon <= east, f"{name} falls outside the western/eastern bounds"
        assert south <= lat <= north, f"{name} falls outside the southern/northern bounds"


def test_baleares_trim_cost_no_fire_points():
    """Alcanar (Tarragona, mainland) was inside the old box at ~0.54E.

    It is still covered because `peninsula` spans -9.6E..3.6E, so narrowing
    `baleares` to 1.2E removes it from this region without removing it from the
    tracked scope. That is what made the trim free.
    """
    _, peninsula_west, _, peninsula_east = _box("peninsula")
    _, baleares_west, _, _ = _box("baleares")

    alcanar_lon = 0.54
    assert peninsula_west <= alcanar_lon <= peninsula_east, (
        "Alcanar must stay inside `peninsula`, otherwise the baleares trim "
        "silently dropped real data"
    )
    assert alcanar_lon < baleares_west, (
        "Alcanar is mainland; keeping it inside baleares was a misattribution"
    )


def test_peninsula_still_reaches_the_mainland_eastern_cap():
    """Cap de Creus is the easternmost point of mainland Spain, at ~3.3E."""
    _, _, _, east = _box("peninsula")
    assert east >= 3.3, "peninsula must still include Cap de Creus"


def test_peninsula_still_includes_tarifa():
    """Tarifa is the southernmost point of Spain, at ~36.0N."""
    south, _, north, _ = _box("peninsula_sur")
    assert south <= 36.0 <= north, "peninsula_sur must include Tarifa"


def test_boxes_exclude_algerian_territory():
    """Algeria's Mediterranean coastline is strictly south of 37.0N for longitudes > -1.8W."""
    # Main peninsula box stays at 37.0N, strictly north of Algeria (max 36.91N)
    p_south, _, _, _ = _box("peninsula")
    assert p_south >= 37.0, "peninsula must stay north of Algeria (>= 37.0N)"

    # South peninsula box cuts off at -1.8W, west of Algeria's coastline
    _, _, _, ps_east = _box("peninsula_sur")
    assert ps_east <= -1.8, "peninsula_sur must cut off west of Algeria (<= -1.8W)"


def test_default_scope_is_the_documented_regions():
    labels = [label for label, _ in resolve_regions()]
    assert set(labels) == {"peninsula", "peninsula_sur", "canarias", "baleares"}


def test_every_region_is_well_formed():
    """south < north and west < east for all boxes.

    Not enforced elsewhere: a south/north inversion produces an empty query
    that looks exactly like "no fires today".
    """
    for name in REGION_BBOXES:
        south, west, north, east = _box(name)
        assert south < north, f"{name}: south ({south}) must be below north ({north})"
        assert west < east, f"{name}: west ({west}) must be below east ({east})"
