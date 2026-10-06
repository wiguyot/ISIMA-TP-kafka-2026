#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

RATE_PER_SECOND="${SIMULPIX_RATE_PER_SECOND:-}"
TRAFFIC_MODEL="${SIMULPIX_TRAFFIC_MODEL:-}"
SHAPER_URL="${SIMULPIX_PIX_TRAFFIC_SHAPER_INTERNAL_URL:-http://pix-traffic-shaper:8089}"

body="{"
sep=""
if [ -n "$RATE_PER_SECOND" ]; then
  body="${body}${sep}\"rate_per_second\":${RATE_PER_SECOND}"
  sep=","
fi
if [ -n "$TRAFFIC_MODEL" ]; then
  body="${body}${sep}\"traffic_model\":\"${TRAFFIC_MODEL}\""
  sep=","
fi
body="${body}}"

if [ "$sep" = "" ]; then
  echo "[simulpix] update-scenario: rien à mettre à jour"
  exit 0
fi

echo "[simulpix] mise à jour en cours du traffic shaper : rate=${RATE_PER_SECOND:-inchangé} model=${TRAFFIC_MODEL:-inchangé}"
curl -sf -X POST -H "Content-Type: application/json" -d "$body" "${SHAPER_URL}/update" >/dev/null
echo "[simulpix] traffic shaper mis à jour"
