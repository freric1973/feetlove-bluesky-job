#!/usr/bin/env bash
# BILDVORSCHLAG-20261114 – einmaliger erster Lauf auf der Box (kostet keine GitHub-Minuten).
# Voraussetzung: Dateien hochgeladen + _private/media-bridge.local.php auf dem Server angelegt.
# Das Geheimnis liegt NUR in einer Datei mit Rechten 600 (wird nie ausgegeben):
#   install -m 600 /dev/null ~/.fl-media-bridge-secret && <Wert hineinschreiben, ohne ihn anzuzeigen>
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
VENV="${FL_VENV:-/workspace/bildvorschlag-20261114/tmp/venv}"
export FL_MEDIA_BRIDGE_URL="${FL_MEDIA_BRIDGE_URL:-https://feetlove1973.de/neu/feetlove-cms-v2/media-gh-bridge.php}"
export FL_MEDIA_BRIDGE_SECRET_FILE="${FL_MEDIA_BRIDGE_SECRET_FILE:-$HOME/.fl-media-bridge-secret}"
export HF_HOME="${HF_HOME:-/workspace/bildvorschlag-20261114/tmp/hf}"
[ -f "$FL_MEDIA_BRIDGE_SECRET_FILE" ] || { echo "Geheimnis-Datei fehlt: $FL_MEDIA_BRIDGE_SECRET_FILE"; exit 2; }
[ "$(stat -c %a "$FL_MEDIA_BRIDGE_SECRET_FILE")" = "600" ] || { echo "Geheimnis-Datei muss chmod 600 haben."; exit 2; }
if [ ! -x "$VENV/bin/python" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q --index-url https://download.pytorch.org/whl/cpu torch==2.15.0 torchvision==0.29.2
  "$VENV/bin/pip" install -q -r "$HERE/job/requirements-media.txt"
fi
# Erst ein Probelauf mit 20 Medien, dann der Rest (Zeitbudget 90 Min., bricht sauber ab und macht beim nächsten Aufruf weiter).
"$VENV/bin/python" "$HERE/job/fl_media_index.py" bridge --limit "${1:-20}" --fetchers 4 --max-minutes 90
[ "${1:-20}" = "0" ] || echo "Probelauf fertig. Für alle: $0 0"
