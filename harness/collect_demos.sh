#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# CS2 Demo Collection Harness
#
# Downloads pro demos from HLTV, extracts .dem files, parses to Parquet,
# and cleans up raw files. Run on any machine with Python 3.11+ and ~200GB
# free disk space (temporarily; final Parquet output is ~10-20GB).
#
# Usage:
#   ./collect_demos.sh                    # default: all 8 major tournaments
#   ./collect_demos.sh --events 8047,8240 # specific events
#   ./collect_demos.sh --dry-run          # show what would be downloaded
#
# Output structure:
#   data/
#   ├── demos/          # downloaded archives (cleaned up after parsing)
#   ├── extracted/      # extracted .dem files (cleaned up after parsing)
#   └── parsed/         # final Parquet + metadata JSONs (keep this)
#
# After collection, push parsed/ to HuggingFace:
#   huggingface-cli upload <repo> data/parsed/ --repo-type dataset
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
DATA_DIR="${DATA_DIR:-$REPO_DIR/data}"
DEMO_DIR="$DATA_DIR/demos"
EXTRACT_DIR="$DATA_DIR/extracted"
PARSED_DIR="$DATA_DIR/parsed"
MAPS="de_mirage,de_inferno,de_nuke"
DRY_RUN=false
SKIP_DOWNLOAD=false
SKIP_PARSE=false
KEEP_RAW=false

# Default events: 8 major tier-1 tournaments
DEFAULT_EVENTS="8047,8240,8413,8241,8412,8246,8876,8575"
EVENTS="$DEFAULT_EVENTS"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --events)    EVENTS="$2"; shift 2 ;;
        --maps)      MAPS="$2"; shift 2 ;;
        --data-dir)  DATA_DIR="$2"; DEMO_DIR="$DATA_DIR/demos"; EXTRACT_DIR="$DATA_DIR/extracted"; PARSED_DIR="$DATA_DIR/parsed"; shift 2 ;;
        --dry-run)   DRY_RUN=true; shift ;;
        --skip-download) SKIP_DOWNLOAD=true; shift ;;
        --skip-parse)    SKIP_PARSE=true; shift ;;
        --keep-raw)      KEEP_RAW=true; shift ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --events IDS      Comma-separated HLTV event IDs (default: 8 major tournaments)"
            echo "  --maps MAPS       Comma-separated map names (default: de_mirage,de_inferno,de_nuke)"
            echo "  --data-dir DIR    Base output directory (default: ./data)"
            echo "  --dry-run         Show what would be downloaded without downloading"
            echo "  --skip-download   Skip download, only parse existing .dem files"
            echo "  --skip-parse      Skip parsing, only download demos"
            echo "  --keep-raw        Don't delete raw demos/archives after parsing"
            echo "  -h, --help        Show this help"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Step 0: Environment setup
# ---------------------------------------------------------------------------
echo "============================================"
echo "CS2 Demo Collection Harness"
echo "============================================"
echo "Data dir:  $DATA_DIR"
echo "Maps:      $MAPS"
echo "Events:    $EVENTS"
echo "Dry run:   $DRY_RUN"
echo ""

# Check Python version
PYTHON=""
for cmd in python3.11 python3.12 python3.13 python3; do
    if command -v "$cmd" &>/dev/null; then
        ver=$("$cmd" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        major=$(echo "$ver" | cut -d. -f1)
        minor=$(echo "$ver" | cut -d. -f2)
        if [[ "$major" -ge 3 && "$minor" -ge 11 ]]; then
            PYTHON="$cmd"
            break
        fi
    fi
done

if [[ -z "$PYTHON" ]]; then
    echo "ERROR: Python 3.11+ required. Install with: brew install python@3.11"
    exit 1
fi
echo "Python:    $PYTHON ($($PYTHON --version))"

# Install cs2-tools if needed
if ! $PYTHON -c "import cs2_tools" &>/dev/null; then
    echo "Installing cs2-tools..."
    $PYTHON -m pip install -e "$REPO_DIR[parse,scrape]" --quiet
fi

# Check for extraction tools
UNRAR=""
if command -v unrar &>/dev/null; then
    UNRAR="unrar"
elif command -v 7z &>/dev/null; then
    UNRAR="7z"
else
    echo "WARNING: Neither unrar nor 7z found. Install with: brew install unrar"
    echo "         Archives will be downloaded but not extracted."
fi

echo ""

# ---------------------------------------------------------------------------
# Step 1: Download demos
# ---------------------------------------------------------------------------
if [[ "$SKIP_DOWNLOAD" == false ]]; then
    echo "============================================"
    echo "Step 1: Downloading demos from HLTV"
    echo "============================================"

    mkdir -p "$DEMO_DIR"

    IFS=',' read -ra EVENT_LIST <<< "$EVENTS"
    IFS=',' read -ra MAP_LIST <<< "$MAPS"

    for event_id in "${EVENT_LIST[@]}"; do
        echo ""
        echo "--- Event $event_id ---"

        for map_name in "${MAP_LIST[@]}"; do
            # Strip de_ prefix for HLTV filter
            hltv_map="${map_name#de_}"

            if [[ "$DRY_RUN" == true ]]; then
                echo "  [dry-run] Would download: event=$event_id map=$hltv_map"
                $PYTHON -m cs2_tools.hltv matches --event "$event_id" --map "$hltv_map" 2>&1 | head -5
            else
                echo "  Downloading map=$hltv_map..."
                $PYTHON -m cs2_tools.hltv download \
                    --event "$event_id" \
                    --map "$hltv_map" \
                    --output "$DEMO_DIR"
            fi
        done
    done

    if [[ "$DRY_RUN" == true ]]; then
        echo ""
        echo "Dry run complete. No files downloaded."
        exit 0
    fi

    echo ""
    echo "Downloads complete. $(ls "$DEMO_DIR" 2>/dev/null | wc -l | tr -d ' ') archives in $DEMO_DIR"
fi

# ---------------------------------------------------------------------------
# Step 2: Extract archives
# ---------------------------------------------------------------------------
echo ""
echo "============================================"
echo "Step 2: Extracting .dem files"
echo "============================================"

mkdir -p "$EXTRACT_DIR"

if [[ -z "$UNRAR" ]]; then
    echo "ERROR: No extraction tool available. Install unrar or 7z."
    exit 1
fi

for archive in "$DEMO_DIR"/*; do
    [[ -f "$archive" ]] || continue
    basename=$(basename "$archive")
    echo "  Extracting $basename..."

    case "$archive" in
        *.rar)
            if [[ "$UNRAR" == "unrar" ]]; then
                unrar x -o- "$archive" "$EXTRACT_DIR/" >/dev/null 2>&1 || echo "    WARNING: Failed to extract $basename"
            else
                7z x -o"$EXTRACT_DIR" -aos "$archive" >/dev/null 2>&1 || echo "    WARNING: Failed to extract $basename"
            fi
            ;;
        *.zip)
            unzip -n "$archive" -d "$EXTRACT_DIR/" >/dev/null 2>&1 || echo "    WARNING: Failed to extract $basename"
            ;;
        *.gz)
            # Single .dem.gz file
            gunzip -k -c "$archive" > "$EXTRACT_DIR/$(basename "$archive" .gz)" 2>/dev/null || echo "    WARNING: Failed to extract $basename"
            ;;
        *)
            echo "    WARNING: Unknown archive format: $basename"
            ;;
    esac
done

dem_count=$(find "$EXTRACT_DIR" -name "*.dem" 2>/dev/null | wc -l | tr -d ' ')
echo ""
echo "Extracted $dem_count .dem files to $EXTRACT_DIR"

# ---------------------------------------------------------------------------
# Step 3: Parse to Parquet
# ---------------------------------------------------------------------------
if [[ "$SKIP_PARSE" == false ]]; then
    echo ""
    echo "============================================"
    echo "Step 3: Parsing .dem files to Parquet"
    echo "============================================"

    mkdir -p "$PARSED_DIR"

    $PYTHON -m cs2_tools.parse_demos "$EXTRACT_DIR" --output "$PARSED_DIR"

    parquet_count=$(find "$PARSED_DIR" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
    echo ""
    echo "Parsed $parquet_count demos to $PARSED_DIR"
fi

# ---------------------------------------------------------------------------
# Step 4: Cleanup
# ---------------------------------------------------------------------------
if [[ "$KEEP_RAW" == false ]]; then
    echo ""
    echo "============================================"
    echo "Step 4: Cleaning up raw files"
    echo "============================================"

    du_demos=$(du -sh "$DEMO_DIR" 2>/dev/null | cut -f1)
    du_extract=$(du -sh "$EXTRACT_DIR" 2>/dev/null | cut -f1)
    echo "  Removing $DEMO_DIR ($du_demos)..."
    rm -rf "$DEMO_DIR"
    echo "  Removing $EXTRACT_DIR ($du_extract)..."
    rm -rf "$EXTRACT_DIR"
    echo "  Cleaned up. Only parsed data remains."
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "============================================"
echo "Collection complete!"
echo "============================================"
du_parsed=$(du -sh "$PARSED_DIR" 2>/dev/null | cut -f1)
parquet_final=$(find "$PARSED_DIR" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
echo "  Parsed data: $PARSED_DIR ($du_parsed, $parquet_final demos)"
echo ""
echo "Next steps:"
echo "  1. Push to HuggingFace:"
echo "     huggingface-cli upload <repo> $PARSED_DIR/ --repo-type dataset"
echo "  2. Pull into chimera:"
echo "     python scripts/data.py pull"
