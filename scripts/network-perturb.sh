#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

PROFILE="${1:-kafka_latency}"
TARGET_SERVICE="${2:-pix-decision-engine}"
INTERFACE_NAME="${SIMULPIX_NETWORK_INTERFACE:-eth0}"
DELAY_MS="${SIMULPIX_NETWORK_DELAY_MS_OVERRIDE-}"
JITTER_MS="${SIMULPIX_NETWORK_JITTER_MS_OVERRIDE-}"
LOSS_PERCENT="${SIMULPIX_NETWORK_LOSS_PERCENT_OVERRIDE-}"
RATE_KBIT="${SIMULPIX_NETWORK_RATE_KBIT_OVERRIDE-}"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

case "$PROFILE" in
  kafka_latency)
    [ -z "$DELAY_MS" ] && DELAY_MS="250"
    [ -z "$JITTER_MS" ] && JITTER_MS="40"
    [ -z "$LOSS_PERCENT" ] && LOSS_PERCENT="0"
    [ -z "$RATE_KBIT" ] && RATE_KBIT="0"
    DESCRIPTION="latence vers Kafka"
    ;;
  kafka_loss)
    [ -z "$DELAY_MS" ] && DELAY_MS="120"
    [ -z "$JITTER_MS" ] && JITTER_MS="20"
    [ -z "$LOSS_PERCENT" ] && LOSS_PERCENT="8"
    [ -z "$RATE_KBIT" ] && RATE_KBIT="0"
    DESCRIPTION="perte reseau vers Kafka"
    ;;
  kafka_slow_link)
    [ -z "$DELAY_MS" ] && DELAY_MS="80"
    [ -z "$JITTER_MS" ] && JITTER_MS="20"
    [ -z "$LOSS_PERCENT" ] && LOSS_PERCENT="0"
    [ -z "$RATE_KBIT" ] && RATE_KBIT="256"
    DESCRIPTION="debit limite vers Kafka"
    ;;
  *)
    echo "[simulpix] unsupported network profile: $PROFILE" >&2
    exit 1
    ;;
esac

NETEM_ARGS="delay ${DELAY_MS}ms"
if [ "$JITTER_MS" -gt 0 ]; then
  NETEM_ARGS="$NETEM_ARGS ${JITTER_MS}ms distribution normal"
fi
if [ "$LOSS_PERCENT" != "0" ]; then
  NETEM_ARGS="$NETEM_ARGS loss ${LOSS_PERCENT}%"
fi
if [ "$RATE_KBIT" != "0" ]; then
  NETEM_ARGS="$NETEM_ARGS rate ${RATE_KBIT}kbit"
fi

echo "[simulpix] applying network profile=${PROFILE} target=${TARGET_SERVICE} interface=${INTERFACE_NAME} args=${NETEM_ARGS}"
docker_compose exec -T "$TARGET_SERVICE" sh -lc "
  command -v tc >/dev/null 2>&1 || { echo 'tc not available in container' >&2; exit 1; }
  tc qdisc replace dev ${INTERFACE_NAME} root netem ${NETEM_ARGS}
"

/bin/sh ./infra/scripts/write-network-state.sh 1 "$PROFILE" "$TARGET_SERVICE" "$INTERFACE_NAME" "$DELAY_MS" "$JITTER_MS" "$LOSS_PERCENT" "$RATE_KBIT" "$DESCRIPTION"
echo "[simulpix] network perturbation active"
