"""Weapon classification for strategic state bucketing.

Maps CS2 weapon names (as they appear in awpy inventory data) to 6 strategic
classes based on how they affect decision-making, not just price or category.

See chimera decisions.md D023 for the design rationale.
"""

from enum import Enum


class WeaponClass(str, Enum):
    RIFLE = "rifle"
    AWP = "awp"
    FORCE_RIFLE = "force_rifle"
    SMG_SHOTGUN = "smg_shotgun"
    FORCE_ANGLE = "force_angle"
    PISTOL = "pistol"
    UNKNOWN = "unknown"


# Canonical weapon name -> class mapping.
# Names match awpy inventory strings (case-insensitive lookup used).
_WEAPON_MAP: dict[str, WeaponClass] = {
    # rifle: Full buy, standard engagement at any range
    "AK-47": WeaponClass.RIFLE,
    "M4A4": WeaponClass.RIFLE,
    "M4A1-S": WeaponClass.RIFLE,
    "AUG": WeaponClass.RIFLE,
    "SG 553": WeaponClass.RIFLE,

    # awp: Angle holding, one-shot body, team plays around
    "AWP": WeaponClass.AWP,

    # force_rifle: Force/half buy indicator, worse stats than full rifles
    "Galil AR": WeaponClass.FORCE_RIFLE,
    "FAMAS": WeaponClass.FORCE_RIFLE,

    # smg_shotgun: Close range, anti-eco, low economy
    "MAC-10": WeaponClass.SMG_SHOTGUN,
    "MP9": WeaponClass.SMG_SHOTGUN,
    "MP7": WeaponClass.SMG_SHOTGUN,
    "MP5-SD": WeaponClass.SMG_SHOTGUN,
    "UMP-45": WeaponClass.SMG_SHOTGUN,
    "PP-Bizon": WeaponClass.SMG_SHOTGUN,
    "P90": WeaponClass.SMG_SHOTGUN,
    "Nova": WeaponClass.SMG_SHOTGUN,
    "XM1014": WeaponClass.SMG_SHOTGUN,
    "Sawed-Off": WeaponClass.SMG_SHOTGUN,
    "MAG-7": WeaponClass.SMG_SHOTGUN,
    "M249": WeaponClass.SMG_SHOTGUN,
    "Negev": WeaponClass.SMG_SHOTGUN,

    # force_angle: Force-buy, headshot angles, effective at range
    "Desert Eagle": WeaponClass.FORCE_ANGLE,
    "R8 Revolver": WeaponClass.FORCE_ANGLE,
    "SSG 08": WeaponClass.FORCE_ANGLE,

    # pistol: Eco / default, limited range
    "USP-S": WeaponClass.PISTOL,
    "P2000": WeaponClass.PISTOL,
    "Glock-18": WeaponClass.PISTOL,
    "P250": WeaponClass.PISTOL,
    "Five-SeveN": WeaponClass.PISTOL,
    "Tec-9": WeaponClass.PISTOL,
    "CZ75-Auto": WeaponClass.PISTOL,
    "Dual Berettas": WeaponClass.PISTOL,
}

# Build case-insensitive lookup
_WEAPON_MAP_LOWER: dict[str, WeaponClass] = {
    k.lower(): v for k, v in _WEAPON_MAP.items()
}


def classify_weapon(weapon_name: str | None) -> WeaponClass:
    """Classify a weapon name into a strategic weapon class.

    Args:
        weapon_name: Weapon name as it appears in awpy inventory data.
            None or empty string returns UNKNOWN.

    Returns:
        The WeaponClass for strategic bucketing.
    """
    if not weapon_name:
        return WeaponClass.UNKNOWN
    return _WEAPON_MAP_LOWER.get(weapon_name.lower(), WeaponClass.UNKNOWN)


def classify_loadout(inventory: list[str] | None) -> WeaponClass:
    """Classify a player's loadout by their best weapon.

    Picks the highest-priority weapon from inventory for bucketing.
    Priority: AWP > rifle > force_angle > force_rifle > smg_shotgun > pistol.

    Args:
        inventory: List of weapon names from awpy inventory data.

    Returns:
        The WeaponClass of the best weapon in the loadout.
    """
    if not inventory:
        return WeaponClass.UNKNOWN

    _PRIORITY = {
        WeaponClass.AWP: 6,
        WeaponClass.RIFLE: 5,
        WeaponClass.FORCE_ANGLE: 4,
        WeaponClass.FORCE_RIFLE: 3,
        WeaponClass.SMG_SHOTGUN: 2,
        WeaponClass.PISTOL: 1,
        WeaponClass.UNKNOWN: 0,
    }

    best = WeaponClass.UNKNOWN
    best_priority = 0
    for item in inventory:
        cls = classify_weapon(item)
        p = _PRIORITY.get(cls, 0)
        if p > best_priority:
            best = cls
            best_priority = p
    return best
