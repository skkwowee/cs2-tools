"""Extract strategic state fingerprints from CS2 tick data.

Given a Parquet tick snapshot at a specific tick, produces the 14-dimension
state used for contrastive reward bucketing in Chimera's GRPO training.

See chimera decisions.md D023 for the full state schema.
"""

from dataclasses import dataclass, field

import polars as pl

from cs2_tools.state.weapons import WeaponClass, classify_loadout
from cs2_tools.state.zones import Zone, get_zone, get_cluster


# TTL thresholds for position recency (in seconds).
# Tick rate is 64 Hz for CS2 demos parsed by awpy.
TICK_RATE = 64
FRESH_TTL_S = 5.0
STALE_TTL_S = 15.0
FRESH_TTL_TICKS = int(FRESH_TTL_S * TICK_RATE)
STALE_TTL_TICKS = int(STALE_TTL_S * TICK_RATE)


@dataclass(frozen=True)
class PlayerPosition:
    """A known player position with recency."""
    name: str
    side: str  # "T" or "CT"
    zone: str  # Zone name (fresh) or cluster name (stale)
    weapon_class: WeaponClass
    recency: str  # "fresh" or "stale"
    health_tier: str  # "healthy" or "critical"


@dataclass
class GameState:
    """The 14-dimension strategic state fingerprint."""
    # Dimension 1-4: Core state (must match exactly for bucketing)
    side: str  # POV player side: "T" or "CT"
    alive_t: int
    alive_ct: int
    bomb_status: str  # "not_planted", "planted", "dropped"

    # Dimension 5-7: Context
    map_name: str
    economy_matchup: str  # e.g., "mirror_full", "eco_vs_full"
    round_time_bucket: str  # "early", "mid", "late", "post_plant"

    # Dimension 8-10: POV player state
    pov_health: str  # "healthy" (>=30) or "critical" (<30)
    pov_weapon_class: WeaponClass
    pov_zone: str  # Zone name

    # Dimension 11-14: Other players (variable length)
    fresh_enemies: list[PlayerPosition] = field(default_factory=list)
    fresh_allies: list[PlayerPosition] = field(default_factory=list)
    stale_enemies: dict[str, int] = field(default_factory=dict)  # cluster -> count
    stale_allies: dict[str, int] = field(default_factory=dict)  # cluster -> count

    # Metadata (not used for bucketing)
    tick: int = 0
    round_num: int = 0
    pov_name: str = ""
    is_active_fight: bool = False  # True if enemies on screen — SFT only, skip GRPO


def classify_health(health: int) -> str:
    """Classify health into strategic tiers."""
    return "critical" if health < 30 else "healthy"


def classify_economy_matchup(
    pov_side: str,
    t_equip_avg: float,
    ct_equip_avg: float,
) -> str:
    """Classify the economy matchup between sides.

    Uses average equipment value per alive player to determine buy state,
    then combines both sides into a matchup category.
    """
    def _buy_tier(avg_equip: float) -> str:
        if avg_equip >= 3500:
            return "full"
        elif avg_equip >= 2000:
            return "half"
        elif avg_equip >= 1000:
            return "force"
        else:
            return "eco"

    t_tier = _buy_tier(t_equip_avg)
    ct_tier = _buy_tier(ct_equip_avg)

    # Normalize to POV perspective: (pov_tier, enemy_tier)
    if pov_side == "T":
        pov_tier, enemy_tier = t_tier, ct_tier
    else:
        pov_tier, enemy_tier = ct_tier, t_tier

    # Collapse into ~6 meaningful matchup categories
    if pov_tier == enemy_tier:
        return f"mirror_{pov_tier}"
    if pov_tier == "eco" and enemy_tier == "full":
        return "eco_vs_full"
    if pov_tier == "full" and enemy_tier == "eco":
        return "full_vs_eco"
    if pov_tier in ("force", "half") and enemy_tier == "full":
        return "force_vs_full"
    if pov_tier == "full" and enemy_tier in ("force", "half"):
        return "full_vs_force"
    return f"{pov_tier}_vs_{enemy_tier}"


def classify_round_time(
    current_tick: int,
    freeze_end_tick: int,
    bomb_planted: bool,
) -> str:
    """Classify round time into strategic buckets."""
    if bomb_planted:
        return "post_plant"
    elapsed_ticks = current_tick - freeze_end_tick
    elapsed_s = elapsed_ticks / TICK_RATE
    if elapsed_s < 30:
        return "early"
    elif elapsed_s < 60:
        return "mid"
    else:
        return "late"


def _get_last_known_positions(
    ticks_df: pl.DataFrame,
    current_tick: int,
    round_num: int,
    alive_players: set[str],
    pov_name: str,
    map_name: str,
) -> dict[str, tuple[PlayerPosition, int]]:
    """Find last known positions for all alive players except POV.

    Returns dict of player_name -> (PlayerPosition, last_seen_tick).
    """
    # Get all ticks for this round up to current tick
    round_ticks = ticks_df.filter(
        (pl.col("round_num") == round_num)
        & (pl.col("tick") <= current_tick)
        & (pl.col("name").is_in(alive_players - {pov_name}))
        & (pl.col("health") > 0)
    )

    if round_ticks.is_empty():
        return {}

    # Get most recent tick per player
    latest = round_ticks.group_by("name").agg(pl.col("tick").max().alias("last_tick"))
    latest_ticks = latest.join(
        round_ticks, left_on=["name", "last_tick"], right_on=["name", "tick"], how="inner"
    )

    positions = {}
    for row in latest_ticks.to_dicts():
        name = row["name"]
        last_tick = row["last_tick"]
        x, y = row.get("X", 0), row.get("Y", 0)
        z = row.get("Z", None)
        side = row.get("side", "").upper()
        health = row.get("health", 100)
        inventory = row.get("inventory", [])

        zone = get_zone(map_name, x, y, z)
        tick_age = current_tick - last_tick
        weapon_class = classify_loadout(inventory)

        if tick_age <= FRESH_TTL_TICKS:
            recency = "fresh"
            zone_name = zone.name if zone else "unknown"
        elif tick_age <= STALE_TTL_TICKS:
            recency = "stale"
            zone_name = zone.cluster if zone else "unknown"
        else:
            # Expired — skip this player
            continue

        pos = PlayerPosition(
            name=name,
            side=side,
            zone=zone_name,
            weapon_class=weapon_class,
            recency=recency,
            health_tier=classify_health(health),
        )
        positions[name] = (pos, last_tick)

    return positions


def extract_state(
    ticks_df: pl.DataFrame,
    tick: int,
    round_num: int,
    pov_name: str,
    map_name: str,
    freeze_end_tick: int,
    bomb_status: str,
    round_start_equip: dict[str, float] | None = None,
) -> GameState:
    """Extract the full 14-dimension strategic state at a given tick.

    Args:
        ticks_df: Full tick-level Parquet data (all players, all rounds).
        tick: The tick to extract state at.
        round_num: The round number.
        pov_name: The POV player's name.
        map_name: Map identifier (e.g., "de_mirage").
        freeze_end_tick: The tick when freeze time ended for this round.
        bomb_status: Current bomb status ("not_planted", "planted", "dropped").
        round_start_equip: Optional dict with keys "t_avg" and "ct_avg" for
            average equipment value per alive player at round start.

    Returns:
        A GameState with all 14 dimensions populated.
    """
    # Get current snapshot
    snap = ticks_df.filter(
        (pl.col("tick") == tick) & (pl.col("round_num") == round_num)
    )
    if snap.is_empty():
        # Fall back to nearest earlier tick
        round_ticks = ticks_df.filter(pl.col("round_num") == round_num)
        available = round_ticks.filter(pl.col("tick") <= tick).select("tick").unique().sort("tick")
        if available.is_empty():
            raise ValueError(f"No tick data for round {round_num} at or before tick {tick}")
        nearest_tick = available.tail(1).item(0, 0)
        snap = round_ticks.filter(pl.col("tick") == nearest_tick)
        tick = nearest_tick

    players = snap.to_dicts()

    # Find POV player
    pov = None
    for p in players:
        if p["name"] == pov_name:
            pov = p
            break
    if pov is None:
        raise ValueError(f"POV player '{pov_name}' not found at tick {tick}")

    pov_side = pov["side"].upper()
    pov_health = pov.get("health", 100)
    pov_inventory = pov.get("inventory", [])
    pov_x, pov_y = pov.get("X", 0), pov.get("Y", 0)
    pov_z = pov.get("Z", None)

    # Alive counts
    alive_t = sum(1 for p in players if p.get("side", "").upper() == "T" and p.get("health", 0) > 0)
    alive_ct = sum(1 for p in players if p.get("side", "").upper() == "CT" and p.get("health", 0) > 0)

    # POV zone
    pov_zone_obj = get_zone(map_name, pov_x, pov_y, pov_z)
    pov_zone = pov_zone_obj.name if pov_zone_obj else "unknown"

    # Economy matchup
    if round_start_equip:
        economy = classify_economy_matchup(
            pov_side, round_start_equip["t_avg"], round_start_equip["ct_avg"]
        )
    else:
        # Estimate from current snapshot equipment values
        t_equip = [p.get("current_equip_value", 0) or 0 for p in players
                    if p.get("side", "").upper() == "T" and p.get("health", 0) > 0]
        ct_equip = [p.get("current_equip_value", 0) or 0 for p in players
                     if p.get("side", "").upper() == "CT" and p.get("health", 0) > 0]
        t_avg = sum(t_equip) / max(len(t_equip), 1)
        ct_avg = sum(ct_equip) / max(len(ct_equip), 1)
        economy = classify_economy_matchup(pov_side, t_avg, ct_avg)

    # Round time
    round_time = classify_round_time(tick, freeze_end_tick, bomb_status == "planted")

    # Get positions of other players with TTL
    alive_names = {p["name"] for p in players if p.get("health", 0) > 0}
    known_positions = _get_last_known_positions(
        ticks_df, tick, round_num, alive_names, pov_name, map_name
    )

    # Split into fresh/stale, enemy/ally
    enemy_side = "CT" if pov_side == "T" else "T"
    fresh_enemies = []
    fresh_allies = []
    stale_enemies: dict[str, int] = {}
    stale_allies: dict[str, int] = {}

    for name, (pos, _last_tick) in known_positions.items():
        is_enemy = pos.side == enemy_side
        if pos.recency == "fresh":
            if is_enemy:
                fresh_enemies.append(pos)
            else:
                fresh_allies.append(pos)
        else:  # stale
            target = stale_enemies if is_enemy else stale_allies
            target[pos.zone] = target.get(pos.zone, 0) + 1

    return GameState(
        side=pov_side,
        alive_t=alive_t,
        alive_ct=alive_ct,
        bomb_status=bomb_status,
        map_name=map_name,
        economy_matchup=economy,
        round_time_bucket=round_time,
        pov_health=classify_health(pov_health),
        pov_weapon_class=classify_loadout(pov_inventory),
        pov_zone=pov_zone,
        fresh_enemies=fresh_enemies,
        fresh_allies=fresh_allies,
        stale_enemies=stale_enemies,
        stale_allies=stale_allies,
        tick=tick,
        round_num=round_num,
        pov_name=pov_name,
    )
