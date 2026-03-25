#!/usr/bin/env python3
"""Analyze state space overlap across parsed demos.

Discretizes game states along key axes and measures how dense/sparse
the coverage is — i.e., how many distinct state buckets appear multiple
times across different demos.
"""

import ast
import sys
from pathlib import Path
from collections import Counter

import polars as pl
from awpy import Demo

PLAYER_PROPS = ["X", "Y", "Z", "health", "armor_value", "inventory",
                "current_equip_value", "balance", "yaw", "pitch"]

# Position grid size in game units (~128 units ≈ 1 player width)
POS_GRID = 256


def classify_economy(equip_value: int) -> str:
    """Bucket economy into tiers."""
    if equip_value < 2000:
        return "eco"
    elif equip_value < 3500:
        return "force"
    else:
        return "full_buy"


def analyze_demo(dem_path: Path) -> list[dict]:
    """Parse one demo and return per-tick state descriptors."""
    print(f"  Parsing {dem_path.name}...")
    dem = Demo(str(dem_path))
    dem.parse(player_props=PLAYER_PROPS)

    header = dem.header if isinstance(dem.header, dict) else {}
    map_name = header.get("map_name", "unknown")
    ticks_df = dem.ticks
    rounds_df = dem.rounds
    bomb_df = dem.bomb

    if ticks_df is None or ticks_df.is_empty():
        print(f"    No tick data")
        return []

    # Compute per-tick aggregate states
    states = []

    # Group by (round_num, tick)
    round_ticks = ticks_df.group_by(["round_num", "tick"]).agg([
        # Alive counts per side
        (pl.col("health").filter(pl.col("side") == "t") > 0).sum().alias("alive_t"),
        (pl.col("health").filter(pl.col("side") == "ct") > 0).sum().alias("alive_ct"),
        # Mean equip value per side
        pl.col("current_equip_value").filter(pl.col("side") == "t").mean().alias("mean_equip_t"),
        pl.col("current_equip_value").filter(pl.col("side") == "ct").mean().alias("mean_equip_ct"),
        # Player positions (discretized) - collect all
        (pl.col("X") / POS_GRID).round(0).cast(pl.Int32).alias("grid_xs"),
        (pl.col("Y") / POS_GRID).round(0).cast(pl.Int32).alias("grid_ys"),
        pl.col("side").alias("sides"),
    ]).sort(["round_num", "tick"])

    # Sample every 64th tick to keep it manageable
    sampled = round_ticks.gather_every(64)

    for row in sampled.to_dicts():
        alive_t = row["alive_t"] or 0
        alive_ct = row["alive_ct"] or 0
        equip_t = row["mean_equip_t"] or 0
        equip_ct = row["mean_equip_ct"] or 0

        # State bucket: map + alive config + economy tiers
        state = {
            "map": map_name,
            "alive": f"{alive_t}v{alive_ct}",
            "econ_t": classify_economy(equip_t),
            "econ_ct": classify_economy(equip_ct),
            "round_num": row["round_num"],
            "tick": row["tick"],
        }

        # Position signature: sorted grid cells per side
        grid_xs = row["grid_xs"] if row["grid_xs"] else []
        grid_ys = row["grid_ys"] if row["grid_ys"] else []
        sides = row["sides"] if row["sides"] else []

        t_positions = sorted([(x, y) for x, y, s in zip(grid_xs, grid_ys, sides) if s == "t"])
        ct_positions = sorted([(x, y) for x, y, s in zip(grid_xs, grid_ys, sides) if s == "ct"])
        state["t_pos_sig"] = str(t_positions)
        state["ct_pos_sig"] = str(ct_positions)

        states.append(state)

    print(f"    {map_name}: {len(states)} sampled states from {rounds_df.shape[0] if rounds_df is not None else 0} rounds")
    return states


def main():
    demo_dir = Path("data/extracted")

    # Use specific demos or all
    if len(sys.argv) > 1:
        dem_files = [Path(f) for f in sys.argv[1:]]
    else:
        dem_files = sorted(demo_dir.glob("*.dem"))[:10]

    print(f"Analyzing {len(dem_files)} demos for state overlap...\n")

    all_states = []
    demo_names = []
    for dem_path in dem_files:
        try:
            states = analyze_demo(dem_path)
            for s in states:
                s["demo"] = dem_path.stem
            all_states.extend(states)
            demo_names.append(dem_path.stem)
        except Exception as e:
            print(f"  ERROR: {e}")

    if not all_states:
        print("No states extracted.")
        return

    print(f"\n{'='*60}")
    print(f"Total sampled states: {len(all_states):,}")
    print(f"Demos: {len(demo_names)}")

    # --- Analysis 1: Alive configuration distribution ---
    alive_counts = Counter(s["alive"] for s in all_states)
    print(f"\n--- Alive Configuration Distribution ---")
    for config, count in alive_counts.most_common(15):
        pct = count / len(all_states) * 100
        bar = "#" * int(pct)
        print(f"  {config:>5s}: {count:>6,} ({pct:5.1f}%) {bar}")

    # --- Analysis 2: Economy tier combinations ---
    econ_counts = Counter((s["econ_t"], s["econ_ct"]) for s in all_states)
    print(f"\n--- Economy Tier Combinations (T vs CT) ---")
    for (et, ect), count in econ_counts.most_common():
        pct = count / len(all_states) * 100
        print(f"  {et:>8s} vs {ect:<8s}: {count:>6,} ({pct:5.1f}%)")

    # --- Analysis 3: Map distribution ---
    map_counts = Counter(s["map"] for s in all_states)
    print(f"\n--- Map Distribution ---")
    for m, count in map_counts.most_common():
        print(f"  {m}: {count:,} states")

    # --- Analysis 4: Coarse state bucket overlap ---
    # A "coarse state" = (map, alive_config, econ_t, econ_ct)
    coarse_keys = Counter(
        (s["map"], s["alive"], s["econ_t"], s["econ_ct"])
        for s in all_states
    )
    total_buckets = len(coarse_keys)
    multi_demo_buckets = 0
    for key, count in coarse_keys.items():
        demos_in_bucket = len(set(s["demo"] for s in all_states
                                  if (s["map"], s["alive"], s["econ_t"], s["econ_ct"]) == key))
        if demos_in_bucket > 1:
            multi_demo_buckets += 1

    print(f"\n--- Coarse State Overlap (map + alive + econ) ---")
    print(f"  Unique state buckets: {total_buckets}")
    print(f"  Buckets seen in 2+ demos: {multi_demo_buckets} ({multi_demo_buckets/total_buckets*100:.1f}%)")

    # Show top repeated buckets
    print(f"\n  Top 20 most common coarse states:")
    for (m, alive, et, ect), count in coarse_keys.most_common(20):
        demos_in = len(set(s["demo"] for s in all_states
                          if (s["map"], s["alive"], s["econ_t"], s["econ_ct"]) == (m, alive, et, ect)))
        print(f"    {m:>10s} {alive:>5s} {et:>8s}/{ect:<8s}: {count:>5,} ticks across {demos_in} demos")

    # --- Analysis 5: Position overlap (fine-grained) ---
    # How many unique grid positions appear across demos?
    # Use (map, grid_x, grid_y, side) as a position bucket
    t_positions = Counter()
    ct_positions = Counter()
    for s in all_states:
        # Parse position signatures back
        t_pos = s.get("t_pos_sig", "[]")
        ct_pos = s.get("ct_pos_sig", "[]")
        # Count unique grid cells occupied
        for pos_str in [t_pos, ct_pos]:
            try:
                positions = ast.literal_eval(pos_str)
                for p in positions:
                    if s["map"] not in ("unknown",):
                        key = (s["map"], p[0], p[1])
                        if "t" in pos_str[:10]:
                            t_positions[key] += 1
                        else:
                            ct_positions[key] += 1
            except (ValueError, SyntaxError):
                pass

    print(f"\n--- Position Grid Coverage ---")
    print(f"  Unique T grid cells visited: {len(t_positions)}")
    print(f"  Unique CT grid cells visited: {len(ct_positions)}")

    # How many grid cells are visited by multiple demos?
    t_multi = sum(1 for k, v in t_positions.items() if v > 10)
    ct_multi = sum(1 for k, v in ct_positions.items() if v > 10)
    print(f"  T cells with 10+ visits: {t_multi}")
    print(f"  CT cells with 10+ visits: {ct_multi}")

    # --- Analysis 6: Full state uniqueness ---
    # (map, alive, econ_t, econ_ct, t_pos_sig, ct_pos_sig)
    full_keys = Counter(
        (s["map"], s["alive"], s["econ_t"], s["econ_ct"], s["t_pos_sig"], s["ct_pos_sig"])
        for s in all_states
    )
    unique_full = len(full_keys)
    repeated_full = sum(1 for v in full_keys.values() if v > 1)
    print(f"\n--- Fine-grained State Overlap (map+alive+econ+positions) ---")
    print(f"  Total unique states: {unique_full:,}")
    print(f"  States seen 2+ times: {repeated_full:,} ({repeated_full/unique_full*100:.1f}%)")
    print(f"  Sparsity ratio: {unique_full/len(all_states)*100:.1f}% unique")


if __name__ == "__main__":
    main()
