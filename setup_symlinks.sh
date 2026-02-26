#!/usr/bin/env bash
#
# setup_symlinks.sh
#
# Creates a symlink: nais_ps/data -> Box/nais_ps_data/data
# Move data to Box manually before running this.
#
# Usage:
#   bash setup_symlinks.sh                    # auto-detect Box location
#   bash setup_symlinks.sh /path/to/Box/root  # override Box root
#

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_LINK="$REPO_DIR/data"

# ── Detect Box root ──────────────────────────────────────────────────────────

if [[ -n "${1:-}" ]]; then
    BOX_ROOT="$1"
else
    CS="$HOME/Library/CloudStorage"
    if   [[ -d "$CS/Box-Box" ]]; then BOX_ROOT="$CS/Box-Box"
    elif [[ -d "$CS/Box" ]];     then BOX_ROOT="$CS/Box"
    elif [[ -d "$HOME/Box" ]];   then BOX_ROOT="$HOME/Box"
    else
        echo "ERROR: Could not find Box. Re-run with an explicit path:"
        echo "  bash setup_symlinks.sh /path/to/Box/root"
        exit 1
    fi
fi

BOX_DATA="$BOX_ROOT/nais_ps_data/data"

# ── Create symlink ───────────────────────────────────────────────────────────

if [[ -L "$DATA_LINK" ]]; then
    echo "Removing existing symlink at $DATA_LINK."
    rm "$DATA_LINK"
elif [[ -e "$DATA_LINK" ]]; then
    echo "ERROR: $DATA_LINK already exists and is not a symlink. Move or remove it first."
    exit 1
fi

ln -s "$BOX_DATA" "$DATA_LINK"
echo "OK  $DATA_LINK -> $BOX_DATA"
