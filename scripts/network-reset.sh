#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

TARGETS="${SIMULPIX_NETWORK_TARGETS:-pix-scenario-planner pix-traffic-shaper generator pix-validator pix-decision-engine pix-outcome-publisher persister-valid persister-rejected}"
INTERFACE_NAME="${SIMULPIX_NETWORK_INTERFACE:-eth0}"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

for service_name in $TARGETS; do
  echo "[simulpix] clearing network perturbation on ${service_name}"
  docker_compose exec -T "$service_name" sh -lc "
    if command -v tc >/dev/null 2>&1; then
      tc qdisc del dev ${INTERFACE_NAME} root >/dev/null 2>&1 || true
    fi
  " >/dev/null 2>&1 || true
done

/bin/sh ./infra/scripts/write-network-state.sh 0 none none "$INTERFACE_NAME" 0 0 0 0 none
echo "[simulpix] network perturbation cleared"
