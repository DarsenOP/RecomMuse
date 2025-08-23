#!/usr/bin/env bash
set -euo pipefail

MAIN_DIR="/home/senmrx/Documents/RecomMuse"
VENV_DIR="$MAIN_DIR/.venv"

# 1. Create virtual env if it doesn’t exist
if [[ ! -d "$VENV_DIR" ]]; then
  python3 -m venv "$VENV_DIR"
fi

# 2. Activate
source "$VENV_DIR/bin/activate"

# 3. Upgrade pip & install
pip install --upgrade pip wheel setuptools
pip install -r $MAIN_DIR/requirements.txt

echo "✅ Environment ready.  Activate with:"
echo "    source $VENV_DIR/bin/activate"
