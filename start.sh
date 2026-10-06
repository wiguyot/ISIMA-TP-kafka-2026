#!/bin/sh
set -eu

# Se placer dans le répertoire réel de ce script, quel que soit le
# répertoire courant d'où il est appelé (cd .. && ./simul-pix/start.sh,
# lien symbolique, autre machine, etc.). Tous les chemins relatifs de ce
# script et de docker-compose.yml (./infra/..., ./runtime/...) sont ainsi
# toujours résolus contre le dépôt lui-même, jamais contre un autre
# répertoire qui porterait le même nom ailleurs sur la machine.
cd "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

docker_compose() {
  # Le nom de projet Compose (simulpix) est fixé par la clé `name:` de
  # docker-compose.yml : tous les scripts ciblent ainsi le même projet.
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

retry_command() {
  attempts="${1:-3}"
  shift
  while [ "$attempts" -gt 0 ]; do
    if "$@"; then
      return 0
    fi
    attempts=$((attempts - 1))
    if [ "$attempts" -le 0 ]; then
      return 1
    fi
    sleep 2
  done
  return 1
}

start_platform() {
  SIMULPIX_GENERATOR_IDLE=1 docker_compose up -d --build --remove-orphans
}

wait_for_http() {
  url="$1"
  attempts="${2:-60}"
  while [ "$attempts" -gt 0 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

echo "[simulpix] starting platform"
./infra/kafka/scripts/generate-secrets.sh
if [ ! -f runtime/current-run.env ]; then
  /bin/sh ./infra/scripts/write-run-state.sh default simulpix-persister-valid-v1 simulpix-persister-rejected-v1 simulpix-validator-v1 simulpix-decision-engine-v1 simulpix-outcome-publisher-v1
fi
if [ ! -f runtime/current-network.json ]; then
  /bin/sh ./infra/scripts/write-network-state.sh 0 none none eth0 0 0 0 0 none
fi
if ! retry_command 3 start_platform; then
  echo "[simulpix] platform startup failed after retries" >&2
  exit 1
fi
if ! wait_for_http "http://127.0.0.1:8082/health" 90; then
  echo "[simulpix] service health did not become ready in time" >&2
  exit 1
fi
echo "[simulpix] platform started"
echo "[simulpix] kafka sasl/plaintext brokers: localhost:9092, localhost:9093, localhost:9094"
echo "[simulpix] postgres: localhost:5432"
echo "[simulpix] service health: http://localhost:8082/health"
echo "[simulpix] influxdb: http://localhost:8086"
echo "[simulpix] grafana: http://localhost:3000 (admin / adminpass)"
printf '\033[32m[simulpix] atelier kafka: http://localhost:8083/\033[0m\n'
