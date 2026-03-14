"""Zone definitions for CS2 maps.

Each map is divided into ~15-25 strategic zones defined as axis-aligned
bounding boxes in the game's coordinate space (X, Y). Z is ignored for
zone assignment — vertical separation is handled by map-specific logic
where needed (e.g., Nuke upper/lower).

Zones represent strategically distinct positions with different angles,
rotation paths, and site access. Names are internal labels, not
community callouts.

Stale clusters group zones into broad map control regions for TTL-decayed
position bucketing (see chimera D023).

Coordinate values are approximate and should be calibrated against actual
tick data from parsed demos. Run `python -m cs2_tools.state.zones --calibrate`
with Parquet data to validate and adjust boundaries.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Zone:
    """A strategic zone on a CS2 map."""
    name: str
    cluster: str  # Stale cluster: "a_area", "b_area", "mid", "spawn"
    x_min: float
    x_max: float
    y_min: float
    y_max: float
    z_min: float | None = None  # Vertical bounds (e.g., Nuke upper/lower)
    z_max: float | None = None


# Stale clusters for TTL-decayed position bucketing.
# When a position expires from "fresh" (0-5s) to "stale" (5-15s),
# the specific zone collapses to its cluster.
CLUSTERS = ("a_area", "b_area", "mid", "spawn")


# ---------------------------------------------------------------------------
# Map zone definitions
#
# Coordinates are in Source engine units. These are approximate starting
# points derived from known CS2 map layouts. They MUST be validated against
# actual tick data before use in production.
#
# To calibrate: load a Parquet tick file, plot player positions colored by
# round phase / site, and adjust boundaries to match actual player density.
# ---------------------------------------------------------------------------

MIRAGE_ZONES: list[Zone] = [
    # T side
    Zone("t_spawn",      "spawn",  1200, 1600,  -600,   200),
    Zone("t_ramp",       "a_area", 1000, 1400,   200,   800),
    Zone("palace",       "a_area",  600, 1100,   800,  1400),
    Zone("t_apartments", "b_area", 1200, 1700, -1800, -1200),
    # Mid
    Zone("top_mid",      "mid",     400,  900,  -400,   200),
    Zone("mid",          "mid",    -200,  400,  -600,   200),
    Zone("mid_window",   "mid",    -200,  200,   200,   600),
    Zone("underpass",    "mid",    -200,  400,  -600, -1000),
    Zone("connector",    "mid",    -400,   0,    200,   800),
    Zone("catwalk",      "a_area", -400,  200,   600,  1000),
    # A site
    Zone("a_site",       "a_area", -600,  200,   800,  1400),
    Zone("a_ramp",       "a_area",  200,  700,   600,  1200),
    Zone("tetris",       "a_area", -200,  200,  1000,  1400),
    Zone("jungle",       "a_area", -600, -200,   400,   800),
    Zone("ct_stairs",    "a_area", -800, -400,   600,  1200),
    Zone("sandwich",     "a_area", -400,   0,   1200,  1600),
    # B site
    Zone("b_site",       "b_area", -800, -200, -1800, -1200),
    Zone("b_short",      "b_area", -200,  400, -1200,  -600),
    Zone("market",       "b_area", -800, -200,  -800,  -400),
    Zone("bench",        "b_area", -600, -200, -1600, -1200),
    Zone("b_van",        "b_area", -400,   0,  -1800, -1400),
    # CT side
    Zone("ct_spawn",     "spawn",  -800, -200,  -200,   400),
    Zone("kitchen",      "spawn",  -800, -400,  -600,  -200),
]

INFERNO_ZONES: list[Zone] = [
    # T side
    Zone("t_spawn",      "spawn",  -800, -200, -1600, -1000),
    Zone("t_ramp",       "mid",    -200,  400, -1400,  -800),
    Zone("second_mid",   "mid",    -200,  400,  -800,  -200),
    # Mid
    Zone("mid",          "mid",     200,  800,  -600,   200),
    Zone("alt_mid",      "mid",     400,  800,  -1200,  -600),
    # Banana / B area
    Zone("banana_top",   "b_area",  800, 1400,  -800,  -200),
    Zone("banana_bot",   "b_area",  800, 1400,  -200,   400),
    Zone("b_site",       "b_area", 1200, 1800,   200,   800),
    Zone("b_ct",         "b_area", 1200, 1800,  -200,   200),
    Zone("dark",         "b_area", 1400, 1800,   600,  1000),
    # A area
    Zone("apartments",   "a_area",  200,  800,   400,  1000),
    Zone("apps_balcony", "a_area",  400,  800,   800,  1200),
    Zone("a_short",      "a_area",  -200, 400,   600,  1200),
    Zone("a_long",       "a_area", -600, -200,   400,  1000),
    Zone("a_site",       "a_area", -600,  200,  1000,  1600),
    Zone("pit",          "a_area", -800, -200,  1200,  1600),
    Zone("library",      "a_area", -600, -200,   600,  1000),
    Zone("arch",         "a_area",  200,  600,  1000,  1400),
    # CT side
    Zone("ct_spawn",     "spawn",   600, 1200,   600,  1200),
    Zone("ct_b",         "spawn",  1000, 1400,   200,   600),
]

NUKE_ZONES: list[Zone] = [
    # T side / Outside
    Zone("t_spawn",      "spawn",  -1600, -1000,  -800,  -200),
    Zone("t_outside",    "spawn",  -1200,  -600,  -200,   400),
    Zone("lobby",        "mid",     -800,  -200,  -400,   200),
    Zone("radio",        "mid",     -600,    0,    200,   800),
    # Outside / yard
    Zone("outside",      "mid",    -1000,  -200,   600,  1400),
    Zone("secret",       "b_area",  -800,  -200,  1200,  1800),
    Zone("main",         "a_area",  -200,   400,  -400,   200),
    # A site (upper)
    Zone("a_site",       "a_area",   200,   800,   200,   800, z_min=0),
    Zone("hut",          "a_area",   600,  1000,   200,   600, z_min=0),
    Zone("heaven",       "a_area",   200,   800,   600,  1200, z_min=200),
    Zone("hell",         "a_area",   200,   600,   200,   600, z_min=-200, z_max=0),
    Zone("a_ramp",       "a_area",    0,    400,  -200,   400),
    Zone("squeaky",      "a_area",  -200,   200,   200,   600),
    # B site (lower)
    Zone("b_site",       "b_area",   200,   800,   200,   800, z_max=-200),
    Zone("b_ramp",       "b_area",    0,    400,   800,  1400, z_max=-100),
    Zone("b_back",       "b_area",   600,  1000,   200,   800, z_max=-200),
    Zone("vents",        "b_area",   200,   600,  -200,   200),
    Zone("decon",        "b_area",   400,   800,  -400,   0),
    # CT side
    Zone("ct_spawn",     "spawn",    800,  1400,   400,  1000),
]

OVERPASS_ZONES: list[Zone] = [
    # T side
    Zone("t_spawn",      "spawn",  -2000, -1400,  -400,   200),
    Zone("t_connector",  "mid",    -1400,  -800,  -200,   400),
    # Mid / Connector
    Zone("mid",          "mid",     -800,  -200,  -200,   400),
    Zone("toilets",      "mid",     -400,   200,   400,  1000),
    Zone("playground",   "mid",     -800,  -200,   400,  1000),
    # A area
    Zone("a_long",       "a_area", -1200,  -600,   800,  1400),
    Zone("a_short",      "a_area",  -600,    0,    800,  1200),
    Zone("a_site",       "a_area",  -400,   200,  1200,  1800),
    Zone("truck",        "a_area",   200,   600,  1200,  1600),
    Zone("bank",         "a_area",  -600,  -200,  1400,  1800),
    Zone("bins",         "a_area",    0,    400,  1600,  2000),
    # B area
    Zone("b_short",      "b_area",  -800,  -200, -1000,  -400),
    Zone("b_long",       "b_area", -1400,  -800,  -800,  -200),
    Zone("monster",      "b_area", -1200,  -600, -1400,  -800),
    Zone("b_site",       "b_area",  -600,    0,  -1400,  -800),
    Zone("water",        "b_area",  -200,   400, -1200,  -600),
    Zone("pillar",       "b_area",  -400,    0,   -800,  -400),
    Zone("heaven_b",     "b_area",  -200,   400,  -800,  -200),
    # CT side
    Zone("ct_spawn",     "spawn",    200,   800,   400,  1000),
]

# Map name -> zone list lookup
MAP_ZONES: dict[str, list[Zone]] = {
    "de_mirage": MIRAGE_ZONES,
    "de_inferno": INFERNO_ZONES,
    "de_nuke": NUKE_ZONES,
    "de_overpass": OVERPASS_ZONES,
}

# Map name -> stale cluster definitions (derived from zone cluster fields)
MAP_CLUSTERS: dict[str, set[str]] = {
    map_name: {z.cluster for z in zones}
    for map_name, zones in MAP_ZONES.items()
}


def get_zone(map_name: str, x: float, y: float, z: float | None = None) -> Zone | None:
    """Look up the strategic zone for a position on a given map.

    Args:
        map_name: Map identifier (e.g., "de_mirage").
        x: X coordinate in Source engine units.
        y: Y coordinate in Source engine units.
        z: Optional Z coordinate for maps with vertical separation (e.g., Nuke).

    Returns:
        The matching Zone, or None if the position doesn't fall in any defined zone.
    """
    zones = MAP_ZONES.get(map_name)
    if zones is None:
        return None

    for zone in zones:
        if not (zone.x_min <= x <= zone.x_max and zone.y_min <= y <= zone.y_max):
            continue
        # Check vertical bounds if defined on the zone
        if zone.z_min is not None and z is not None and z < zone.z_min:
            continue
        if zone.z_max is not None and z is not None and z > zone.z_max:
            continue
        return zone

    return None


def get_cluster(map_name: str, x: float, y: float, z: float | None = None) -> str | None:
    """Get the stale cluster for a position (coarse zone grouping).

    Args:
        map_name: Map identifier (e.g., "de_mirage").
        x: X coordinate.
        y: Y coordinate.
        z: Optional Z coordinate.

    Returns:
        Cluster name ("a_area", "b_area", "mid", "spawn") or None.
    """
    zone = get_zone(map_name, x, y, z)
    return zone.cluster if zone else None
