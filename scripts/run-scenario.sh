#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

SCENARIO="${1:-nominal}"
TOTAL_MESSAGES="${2-}"
RATE_PER_SECOND="${3-}"
# Délais de traitement : repris de l'environnement s'ils sont fournis (measure-tp.sh
# s'en sert pour élargir la fenêtre de panne), sinon nuls. consumer_lag les impose.
PROCESSING_DELAY_MS="${SIMULPIX_PROCESSING_DELAY_MS:-0}"
PERSIST_VALID_DELAY_MS="${SIMULPIX_PERSIST_VALID_DELAY_MS:-0}"
PERSIST_REJECTED_DELAY_MS="${SIMULPIX_PERSIST_REJECTED_DELAY_MS:-0}"
REPLICATION_OUTAGE_SECONDS="0"
REPLICATION_PAUSED_BROKER=""
MATCH_COUNT="10"
STADIUM_CAPACITY="44000"
PIX_USAGE_RATE="0.05"
PEAK_SHARE="0.30"
PEAK_WINDOW_MINUTES="15"
TOTAL_WINDOW_MINUTES="240"
TIME_COMPRESSION_FACTOR="10"
KEEP_HEALTH_RUNNING="${SIMULPIX_KEEP_HEALTH_RUNNING:-0}"
PROJECT_DIR="$(pwd)"

docker_compose() {
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

remove_matching_containers_by_name() {
  targets="$*"
  if ! command -v docker >/dev/null 2>&1; then
    return 0
  fi
  docker ps -a --format '{{.ID}} {{.Names}}' | while IFS= read -r line; do
    container_id="$(printf '%s' "$line" | awk '{print $1}')"
    container_name="$(printf '%s' "$line" | awk '{print $2}')"
    for target in $targets; do
      case "$container_name" in
        "$target"|*_"$target")
          if [ "$target" = "service-health" ] && [ "$KEEP_HEALTH_RUNNING" = "1" ]; then
            continue
          fi
          docker rm -f "$container_id" >/dev/null 2>&1 || true
          break
          ;;
      esac
    done
  done
}

cleanup_services() {
  docker_compose rm -f -s -v "$@" >/dev/null 2>&1 || true
  remove_matching_containers_by_name "$@"
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

case "$SCENARIO" in
  consumer_lag)
    PROCESSING_DELAY_MS="${SIMULPIX_PROCESSING_DELAY_MS_OVERRIDE:-350}"
    PERSIST_VALID_DELAY_MS="${SIMULPIX_PERSIST_VALID_DELAY_MS_OVERRIDE:-150}"
    PERSIST_REJECTED_DELAY_MS="${SIMULPIX_PERSIST_REJECTED_DELAY_MS_OVERRIDE:-250}"
    ;;
  replication_lag)
    REPLICATION_OUTAGE_SECONDS="${SIMULPIX_REPLICATION_OUTAGE_SECONDS_OVERRIDE:-20}"
    REPLICATION_PAUSED_BROKER="${SIMULPIX_REPLICATION_PAUSED_BROKER_OVERRIDE:-kafka-3}"
    ;;
  football_match_peak)
    MATCH_COUNT="${SIMULPIX_MATCH_COUNT_OVERRIDE:-10}"
    STADIUM_CAPACITY="${SIMULPIX_STADIUM_CAPACITY_OVERRIDE:-44000}"
    PIX_USAGE_RATE="${SIMULPIX_PIX_USAGE_RATE_OVERRIDE:-0.05}"
    PEAK_SHARE="${SIMULPIX_PEAK_SHARE_OVERRIDE:-0.30}"
    PEAK_WINDOW_MINUTES="${SIMULPIX_PEAK_WINDOW_MINUTES_OVERRIDE:-15}"
    TOTAL_WINDOW_MINUTES="${SIMULPIX_TOTAL_WINDOW_MINUTES_OVERRIDE:-240}"
    TIME_COMPRESSION_FACTOR="${SIMULPIX_TIME_COMPRESSION_FACTOR_OVERRIDE:-10}"
    TOTAL_MESSAGES="$(python3 -c "match_count=${MATCH_COUNT}; stadium_capacity=${STADIUM_CAPACITY}; pix_usage_rate=${PIX_USAGE_RATE}; print(max(int(round(match_count * stadium_capacity * pix_usage_rate)), 1))")"
    RATE_PER_SECOND="0"
    ;;
esac

RUN_ID="$(date +%s)"
PIX_VALIDATOR_GROUP="simulpix-validator-${RUN_ID}"
PIX_DECISION_ENGINE_GROUP="simulpix-decision-engine-${RUN_ID}"
PIX_OUTCOME_PUBLISHER_GROUP="simulpix-outcome-publisher-${RUN_ID}"
PERSISTER_VALID_GROUP="simulpix-persister-valid-${RUN_ID}"
PERSISTER_REJECTED_GROUP="simulpix-persister-rejected-${RUN_ID}"

SIMULPIX_KEEP_HEALTH_RUNNING="$KEEP_HEALTH_RUNNING" ./scripts/reset-scenario.sh

echo "[simulpix] persisting current run state"
/bin/sh ./infra/scripts/write-run-state.sh "$RUN_ID" "$PERSISTER_VALID_GROUP" "$PERSISTER_REJECTED_GROUP" "$PIX_VALIDATOR_GROUP" "$PIX_DECISION_ENGINE_GROUP" "$PIX_OUTCOME_PUBLISHER_GROUP"

echo "[simulpix] recreating consumers with run-specific groups"
ACTIVE_CONSUMER_SERVICES="pix-validator pix-decision-engine pix-outcome-publisher persister-valid persister-rejected"
cleanup_services $ACTIVE_CONSUMER_SERVICES
if [ "$KEEP_HEALTH_RUNNING" = "1" ]; then
  SIMULPIX_PROCESSING_DELAY_MS="$PROCESSING_DELAY_MS" \
  SIMULPIX_PERSIST_VALID_DELAY_MS="$PERSIST_VALID_DELAY_MS" \
  SIMULPIX_PERSIST_REJECTED_DELAY_MS="$PERSIST_REJECTED_DELAY_MS" \
  retry_command 5 docker_compose up -d --no-deps --remove-orphans $ACTIVE_CONSUMER_SERVICES >/dev/null
else
  cleanup_services $ACTIVE_CONSUMER_SERVICES service-health
  # --no-deps : sans lui, Compose démarrerait aussi pix-traffic-shaper, le generator
  # et le planner (dépendances de service-health) avec leurs réglages par défaut,
  # qui émettraient des Pix « nominal » parasites avant le vrai scénario.
  SIMULPIX_PROCESSING_DELAY_MS="$PROCESSING_DELAY_MS" \
  SIMULPIX_PERSIST_VALID_DELAY_MS="$PERSIST_VALID_DELAY_MS" \
  SIMULPIX_PERSIST_REJECTED_DELAY_MS="$PERSIST_REJECTED_DELAY_MS" \
  retry_command 5 docker_compose up -d --no-deps --remove-orphans $ACTIVE_CONSUMER_SERVICES service-health >/dev/null
fi

if [ "$SCENARIO" = "replication_lag" ]; then
  echo "[simulpix] replication lag will stop broker ${REPLICATION_PAUSED_BROKER} after generator startup for ${REPLICATION_OUTAGE_SECONDS}s"
fi

TOTAL_LABEL="${TOTAL_MESSAGES:-unlimited}"
RATE_LABEL="${RATE_PER_SECOND:-unlimited}"
echo "[simulpix] starting workshop flow scenario=${SCENARIO} total=${TOTAL_LABEL} rate=${RATE_LABEL}"
cleanup_services generator pix-scenario-planner pix-traffic-shaper
SIMULPIX_SCENARIO="$SCENARIO" \
SIMULPIX_TOTAL_MESSAGES="$TOTAL_MESSAGES" \
SIMULPIX_RATE_PER_SECOND="$RATE_PER_SECOND" \
SIMULPIX_MATCH_COUNT="$MATCH_COUNT" \
SIMULPIX_STADIUM_CAPACITY="$STADIUM_CAPACITY" \
SIMULPIX_PIX_USAGE_RATE="$PIX_USAGE_RATE" \
SIMULPIX_PEAK_SHARE="$PEAK_SHARE" \
SIMULPIX_PEAK_WINDOW_MINUTES="$PEAK_WINDOW_MINUTES" \
SIMULPIX_TOTAL_WINDOW_MINUTES="$TOTAL_WINDOW_MINUTES" \
SIMULPIX_TIME_COMPRESSION_FACTOR="$TIME_COMPRESSION_FACTOR" \
retry_command 5 docker_compose up -d --remove-orphans generator pix-scenario-planner pix-traffic-shaper >/dev/null

if [ "$SCENARIO" = "replication_lag" ]; then
  sleep 2
  echo "[simulpix] stopping broker ${REPLICATION_PAUSED_BROKER} for ${REPLICATION_OUTAGE_SECONDS}s to create replication lag"
  docker_compose stop "${REPLICATION_PAUSED_BROKER}" >/dev/null
  SIMULPIX_RESTART_BROKER_LOG_FILE="${TMPDIR:-/tmp}/simulpix-restart-${REPLICATION_PAUSED_BROKER}.log" \
    nohup /bin/sh ./scripts/restart-broker-later.sh \
    "${REPLICATION_PAUSED_BROKER}" \
    "${REPLICATION_OUTAGE_SECONDS}" \
    "${PROJECT_DIR}" >/dev/null 2>&1 &
fi

echo "[simulpix] generator publisher and pix-traffic-shaper launched"
echo "[simulpix] groups: ${PIX_VALIDATOR_GROUP} / ${PIX_DECISION_ENGINE_GROUP} / ${PIX_OUTCOME_PUBLISHER_GROUP} / ${PERSISTER_VALID_GROUP} / ${PERSISTER_REJECTED_GROUP}"
echo "[simulpix] delays(ms): decision_engine=${PROCESSING_DELAY_MS} valid=${PERSIST_VALID_DELAY_MS} rejected=${PERSIST_REJECTED_DELAY_MS}"
if [ "$SCENARIO" = "replication_lag" ]; then
  echo "[simulpix] replication outage: broker=${REPLICATION_PAUSED_BROKER} duration=${REPLICATION_OUTAGE_SECONDS}s"
fi
if [ "$SCENARIO" = "football_match_peak" ]; then
  echo "[simulpix] football_match_peak: matches=${MATCH_COUNT} stadium_capacity=${STADIUM_CAPACITY} pix_usage_rate=${PIX_USAGE_RATE} peak_share=${PEAK_SHARE} peak_window_minutes=${PEAK_WINDOW_MINUTES} total_window_minutes=${TOTAL_WINDOW_MINUTES} compression=${TIME_COMPRESSION_FACTOR}"
fi
if [ "$KEEP_HEALTH_RUNNING" != "1" ]; then
  if ! wait_for_http "http://127.0.0.1:8082/health" 60; then
    echo "[simulpix] service health did not become ready in time after scenario launch" >&2
    exit 1
  fi
fi
