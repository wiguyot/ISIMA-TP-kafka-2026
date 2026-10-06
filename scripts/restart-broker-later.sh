#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

BROKER_NAME="${1:?broker name required}"
DELAY_SECONDS="${2:?delay seconds required}"
PROJECT_DIR="${3:?project dir required}"
LOG_FILE="${SIMULPIX_RESTART_BROKER_LOG_FILE:-/tmp/simulpix-restart-broker-later.log}"

sleep "$DELAY_SECONDS"

{
  echo "[simulpix] restarting broker ${BROKER_NAME}"
  cd "$PROJECT_DIR"
  if docker compose version >/dev/null 2>&1; then
    docker compose start "$BROKER_NAME" >/dev/null
  else
    docker-compose start "$BROKER_NAME" >/dev/null
  fi
  echo "[simulpix] broker ${BROKER_NAME} restarted"
} >>"$LOG_FILE" 2>&1
