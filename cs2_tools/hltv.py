"""Lightweight HLTV scraper for browsing events and downloading CS2 demos.

Uses curl_cffi with Chrome TLS impersonation to bypass Cloudflare.

Usage:
    # List recent events
    python -m cs2_tools.hltv events

    # List matches for an event
    python -m cs2_tools.hltv matches --event 8413

    # List matches filtered by map
    python -m cs2_tools.hltv matches --event 8413 --map de_mirage

    # Download demos for an event
    python -m cs2_tools.hltv download --event 8413 --output data/demos/

    # Download demos filtered by map
    python -m cs2_tools.hltv download --event 8413 --map de_mirage --output data/demos/
"""

import argparse
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

try:
    from curl_cffi import requests
except ImportError:
    print("curl_cffi is required: pip install curl_cffi")
    sys.exit(1)


BASE_URL = "https://www.hltv.org"

# Polite delay between requests (seconds)
REQUEST_DELAY = 1.5


def _get(path: str) -> str:
    """Fetch an HLTV page with Chrome TLS impersonation."""
    url = f"{BASE_URL}{path}"
    r = requests.get(url, impersonate="chrome", timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    time.sleep(REQUEST_DELAY)
    return r.text


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Event:
    id: int
    name: str
    url: str


@dataclass
class Match:
    id: int
    slug: str
    url: str
    maps: list[str] = field(default_factory=list)
    demo_id: int | None = None
    team1: str = ""
    team2: str = ""


# ---------------------------------------------------------------------------
# Scraping functions
# ---------------------------------------------------------------------------

def list_events(max_pages: int = 1) -> list[Event]:
    """List events from the HLTV events archive."""
    events = []
    seen = set()

    for page in range(max_pages):
        path = "/events/archive" if page == 0 else f"/events/archive?offset={page * 50}"
        html = _get(path)
        for eid, slug in re.findall(r'href="/events/(\d+)/([^"]+)"', html):
            eid = int(eid)
            if eid not in seen:
                seen.add(eid)
                name = slug.replace("-", " ").title()
                events.append(Event(id=eid, name=name, url=f"/events/{eid}/{slug}"))

    return events


def list_matches(event_id: int) -> list[Match]:
    """List all matches for an event from the results page."""
    html = _get(f"/results?event={event_id}")
    matches = []
    seen = set()

    for mid, slug in re.findall(r'href="/matches/(\d+)/([^"]+)"', html):
        mid = int(mid)
        if mid not in seen:
            seen.add(mid)
            # Extract team names from slug: "team1-vs-team2-event-name"
            parts = slug.split("-vs-")
            team1 = parts[0].replace("-", " ").title() if parts else ""
            team2 = parts[1].split("-")[0].replace("-", " ").title() if len(parts) > 1 else ""
            matches.append(Match(
                id=mid, slug=slug, url=f"/matches/{mid}/{slug}",
                team1=team1, team2=team2,
            ))

    return matches


def get_match_details(match: Match) -> Match:
    """Fetch match page to get maps and demo download link."""
    html = _get(match.url)

    # Extract played maps (from mapholder divs, excludes veto pool)
    raw_maps = re.findall(r'class="mapholder".*?class="mapname"[^>]*>([^<]+)', html, re.DOTALL)
    match.maps = [m for m in raw_maps if m != "TBA"]

    # Extract demo link
    demo_links = re.findall(r'href="/download/demo/(\d+)"', html)
    if demo_links:
        match.demo_id = int(demo_links[0])

    return match


def download_demo(match: Match, output_dir: Path) -> Path | None:
    """Download a demo file for a match.

    Returns the path to the downloaded file, or None if no demo available.
    """
    if match.demo_id is None:
        return None

    output_dir.mkdir(parents=True, exist_ok=True)

    # Follow the download redirect to get the actual file
    url = f"{BASE_URL}/download/demo/{match.demo_id}"
    r = requests.get(url, impersonate="chrome", timeout=120, allow_redirects=True)
    if r.status_code != 200:
        print(f"  Failed to download demo {match.demo_id}: HTTP {r.status_code}")
        return None

    # Determine filename from URL or content-disposition
    filename = None
    if "content-disposition" in r.headers:
        cd = r.headers["content-disposition"]
        fn_match = re.search(r'filename="?([^";\s]+)', cd)
        if fn_match:
            filename = fn_match.group(1)

    if not filename:
        # Extract from final URL
        filename = r.url.split("/")[-1].split("?")[0]

    if not filename:
        filename = f"demo_{match.demo_id}.rar"

    out_path = output_dir / filename
    out_path.write_bytes(r.content)
    return out_path


# Map name normalization: HLTV display name -> de_ prefix
_MAP_NORMALIZE = {
    "dust2": "de_dust2",
    "mirage": "de_mirage",
    "inferno": "de_inferno",
    "nuke": "de_nuke",
    "overpass": "de_overpass",
    "ancient": "de_ancient",
    "anubis": "de_anubis",
    "vertigo": "de_vertigo",
    "train": "de_train",
    "cache": "de_cache",
}


def normalize_map_name(name: str) -> str:
    """Normalize HLTV map display name to de_ format."""
    lower = name.lower().strip()
    return _MAP_NORMALIZE.get(lower, lower)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def cmd_events(args):
    events = list_events(max_pages=args.pages)
    print(f"Found {len(events)} events:\n")
    for e in events:
        print(f"  {e.id:>6}  {e.name}")


def cmd_matches(args):
    matches = list_matches(args.event)
    print(f"Found {len(matches)} matches for event {args.event}:\n")

    for m in matches:
        # Fetch details to get maps and demo availability
        get_match_details(m)
        maps_str = ", ".join(m.maps) if m.maps else "?"
        demo_str = f"demo:{m.demo_id}" if m.demo_id else "no demo"

        # Filter by map if specified
        if args.map:
            target = normalize_map_name(args.map)
            match_maps = [normalize_map_name(mp) for mp in m.maps]
            if target not in match_maps:
                continue

        print(f"  {m.id:>8}  {m.team1} vs {m.team2}  [{maps_str}]  ({demo_str})")


def cmd_download(args):
    matches = list_matches(args.event)
    output_dir = Path(args.output)
    print(f"Found {len(matches)} matches for event {args.event}")

    downloaded = 0
    skipped = 0

    for m in matches:
        get_match_details(m)

        # Filter by map if specified
        if args.map:
            target = normalize_map_name(args.map)
            match_maps = [normalize_map_name(mp) for mp in m.maps]
            if target not in match_maps:
                skipped += 1
                continue

        if m.demo_id is None:
            print(f"  {m.team1} vs {m.team2}: no demo available")
            skipped += 1
            continue

        # Check if already downloaded
        existing = list(output_dir.glob(f"*{m.demo_id}*"))
        if existing:
            print(f"  {m.team1} vs {m.team2}: already downloaded")
            skipped += 1
            continue

        maps_str = ", ".join(m.maps)
        print(f"  Downloading: {m.team1} vs {m.team2} [{maps_str}]...", end=" ", flush=True)
        path = download_demo(m, output_dir)
        if path:
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"{size_mb:.1f} MB -> {path.name}")
            downloaded += 1
        else:
            print("failed")
            skipped += 1

    print(f"\nDone: {downloaded} downloaded, {skipped} skipped")


def main():
    parser = argparse.ArgumentParser(description="Browse HLTV events and download CS2 demos")
    sub = parser.add_subparsers(dest="command", required=True)

    # events
    p_events = sub.add_parser("events", help="List recent events")
    p_events.add_argument("--pages", type=int, default=1, help="Number of archive pages to fetch")
    p_events.set_defaults(func=cmd_events)

    # matches
    p_matches = sub.add_parser("matches", help="List matches for an event")
    p_matches.add_argument("--event", type=int, required=True, help="HLTV event ID")
    p_matches.add_argument("--map", type=str, help="Filter by map (e.g., de_mirage, mirage)")
    p_matches.set_defaults(func=cmd_matches)

    # download
    p_dl = sub.add_parser("download", help="Download demos for an event")
    p_dl.add_argument("--event", type=int, required=True, help="HLTV event ID")
    p_dl.add_argument("--map", type=str, help="Filter by map (e.g., de_mirage, mirage)")
    p_dl.add_argument("--output", "-o", default="data/demos/", help="Output directory")
    p_dl.set_defaults(func=cmd_download)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
