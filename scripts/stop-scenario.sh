#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

echo "[simulpix] stopping current pix-scenario-planner, pix-traffic-shaper and generator"
docker_compose stop pix-scenario-planner pix-traffic-shaper generator >/dev/null
echo "[simulpix] pix-scenario-planner, pix-traffic-shaper and generator stopped; consumers can continue draining the backlog"
