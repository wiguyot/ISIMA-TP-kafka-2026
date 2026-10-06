#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

KEEP_HEALTH_RUNNING="${SIMULPIX_KEEP_HEALTH_RUNNING:-0}"
SERVICES_TO_STOP="generator pix-scenario-planner pix-traffic-shaper pix-validator pix-decision-engine pix-outcome-publisher persister-valid persister-rejected"
# Le generator repart en mode inactif (SIMULPIX_GENERATOR_IDLE=1), comme après start.sh :
# il est présent et sain, mais n'émet rien tant qu'aucun scénario n'est lancé.
SERVICES_TO_START="pix-validator pix-decision-engine pix-outcome-publisher persister-valid persister-rejected generator pix-scenario-planner pix-traffic-shaper"

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

kafka_exec_authenticated() {
  kafka_script="$1"
  shift
  docker_compose exec -T kafka-1 sh -lc '
    tmp_config="$(mktemp)"
    cat >"$tmp_config" <<'"'"'EOF'"'"'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="simulpix" password="simulpix-secret";
EOF
    export KAFKA_CLIENT_CONFIG="$tmp_config"
    trap '"'"'rm -f "$tmp_config"'"'"' EXIT
    sh -eu -c "$1"
  ' sh "$kafka_script"
}

remove_matching_containers_by_name() {
  if ! command -v docker >/dev/null 2>&1; then
    return 0
  fi
  docker ps -a --format '{{.ID}} {{.Names}}' | while IFS= read -r line; do
    container_id="$(printf '%s' "$line" | awk '{print $1}')"
    container_name="$(printf '%s' "$line" | awk '{print $2}')"
    case "$container_name" in
      service-health|*_service-health)
        if [ "$KEEP_HEALTH_RUNNING" = "1" ]; then
          continue
        fi
        docker rm -f "$container_id" >/dev/null 2>&1 || true
        ;;
      generator|pix-scenario-planner|pix-traffic-shaper|pix-validator|pix-decision-engine|pix-outcome-publisher|persister-valid|persister-rejected|service-health|topic-init|simulpix-topic-init-run-*|simul-pix-topic-init-run-*|*_generator|*_pix-scenario-planner|*_pix-traffic-shaper|*_pix-validator|*_pix-decision-engine|*_pix-outcome-publisher|*_persister-valid|*_persister-rejected|*_service-health|*_topic-init|*_topic-init-run-*)
        docker rm -f "$container_id" >/dev/null 2>&1 || true
        ;;
    esac
  done
}

wait_for_container_names_gone() {
  attempts="${1:-30}"
  if ! command -v docker >/dev/null 2>&1; then
    return 0
  fi
  while [ "$attempts" -gt 0 ]; do
    remaining="$(docker ps -a --format '{{.Names}}' | awk '
      /(^generator$|^pix-scenario-planner$|^pix-traffic-shaper$|^pix-validator$|^pix-decision-engine$|^pix-outcome-publisher$|^persister-valid$|^persister-rejected$|^service-health$|^topic-init$|^simulpix-topic-init-run-|^simul-pix-topic-init-run-)/ { print }
      /(_generator$|_pix-scenario-planner$|_pix-traffic-shaper$|_pix-validator$|_pix-decision-engine$|_pix-outcome-publisher$|_persister-valid$|_persister-rejected$|_service-health$|_topic-init$|_topic-init-run-)/ { print }
    ')"
    if [ "$KEEP_HEALTH_RUNNING" = "1" ]; then
      remaining="$(printf '%s\n' "$remaining" | awk '$0 !~ /(^service-health$|_service-health$)/ { print }')"
    fi
    if [ -z "$remaining" ]; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

remove_application_services() {
  docker_compose rm -f -s -v $SERVICES_TO_STOP >/dev/null 2>&1 || true
  remove_matching_containers_by_name
}

restart_application_services() {
  SIMULPIX_GENERATOR_IDLE=1 docker_compose up -d --no-deps --remove-orphans $SERVICES_TO_START >/dev/null
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

wait_for_postgres() {
  attempts="${1:-60}"
  while [ "$attempts" -gt 0 ]; do
    if docker_compose exec -T postgres pg_isready -U simulpix -d simulpix >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

wait_for_kafka() {
  attempts="${1:-240}"
  while [ "$attempts" -gt 0 ]; do
    if kafka_exec_authenticated '
      topics_bin="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"
      "$topics_bin" --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --command-config "$KAFKA_CLIENT_CONFIG" --list >/dev/null 2>&1
    ' >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

wait_for_service_containers_running() {
  attempts="${1:-90}"
  expected_count="$(printf '%s\n' $SERVICES_TO_START | wc -l | tr -d ' ')"
  while [ "$attempts" -gt 0 ]; do
    running_count="$(docker_compose ps --status running $SERVICES_TO_START 2>/dev/null | awk 'NR>1 {count++} END {print count+0}')"
    if [ "$running_count" -ge "$expected_count" ]; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

reset_kafka_topics() {
  kafka_exec_authenticated '
    bootstrap="kafka-1:29092,kafka-2:29092,kafka-3:29092"
    kafka_topics_bin="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"

    delete_topic() {
      topic="$1"
      "$kafka_topics_bin" --bootstrap-server "$bootstrap" --command-config "$KAFKA_CLIENT_CONFIG" --delete --if-exists --topic "$topic" >/dev/null 2>&1 || true
    }

    create_topic() {
      topic="$1"
      partitions="$2"
      "$kafka_topics_bin" --bootstrap-server "$bootstrap" --command-config "$KAFKA_CLIENT_CONFIG" --create --if-not-exists --topic "$topic" --partitions "$partitions" --replication-factor 3 >/dev/null
    }

    delete_topic simulpix.transactions.raw
    delete_topic simulpix.transactions.checked
    delete_topic simulpix.transactions.decision
    delete_topic simulpix.transactions.validated
    delete_topic simulpix.transactions.rejected
    delete_topic simulpix.transactions.retry
    delete_topic simulpix.transactions.outcome

    sleep 3

    create_topic simulpix.transactions.raw 6
    create_topic simulpix.transactions.checked 6
    create_topic simulpix.transactions.decision 6
    create_topic simulpix.transactions.validated 6
    create_topic simulpix.transactions.rejected 3
    create_topic simulpix.transactions.retry 3
    create_topic simulpix.transactions.outcome 6
  ' >/dev/null
}

truncate_postgres_tables() {
  docker_compose exec -T postgres psql -U simulpix -d simulpix -c "truncate table validated_transactions, rejected_transactions restart identity;" >/dev/null
}

wait_for_required_topics() {
  attempts="${1:-90}"
  while [ "$attempts" -gt 0 ]; do
    if kafka_exec_authenticated '
      topics_bin="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"
      topics="$($topics_bin --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --command-config "$KAFKA_CLIENT_CONFIG" --list 2>/dev/null || true)"
      for topic in \
        simulpix.transactions.raw \
        simulpix.transactions.checked \
        simulpix.transactions.decision \
        simulpix.transactions.validated \
        simulpix.transactions.rejected \
        simulpix.transactions.retry \
        simulpix.transactions.outcome
      do
        printf "%s\n" "$topics" | grep -Fx "$topic" >/dev/null || exit 1
      done
    ' >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

if [ "$KEEP_HEALTH_RUNNING" != "1" ]; then
  SERVICES_TO_STOP="${SERVICES_TO_STOP} service-health"
  SERVICES_TO_START="${SERVICES_TO_START} service-health"
fi

echo "[simulpix] stopping application services"
SIMULPIX_NETWORK_TARGETS="pix-scenario-planner pix-traffic-shaper generator pix-validator pix-decision-engine pix-outcome-publisher persister-valid persister-rejected" /bin/sh ./scripts/network-reset.sh >/dev/null 2>&1 || true
retry_command 3 docker_compose stop $SERVICES_TO_STOP >/dev/null
echo "[simulpix] removing stopped/orphaned application containers"
remove_application_services
if ! wait_for_container_names_gone 45; then
  echo "[simulpix] warning: some application containers are still being removed" >&2
fi

echo "[simulpix] resetting scenario state"
/bin/sh ./infra/scripts/write-run-state.sh default simulpix-persister-valid-v1 simulpix-persister-rejected-v1 simulpix-validator-v1 simulpix-decision-engine-v1 simulpix-outcome-publisher-v1

echo "[simulpix] ensuring dependencies are up"
if ! retry_command 5 docker_compose up -d --remove-orphans postgres kafka-1 kafka-2 kafka-3 >/dev/null; then
  echo "[simulpix] failed to start PostgreSQL/Kafka dependencies after retries" >&2
  exit 1
fi
if ! wait_for_postgres 60; then
  echo "[simulpix] PostgreSQL did not become ready in time" >&2
  exit 1
fi
if ! wait_for_kafka 240; then
  echo "[simulpix] Kafka did not become ready in time" >&2
  exit 1
fi
sleep 3

echo "[simulpix] truncating PostgreSQL tables"
if ! retry_command 5 truncate_postgres_tables; then
  echo "[simulpix] failed to truncate PostgreSQL tables after retries" >&2
  exit 1
fi

echo "[simulpix] resetting Kafka topics"
remove_matching_containers_by_name
if ! retry_command 5 reset_kafka_topics; then
  echo "[simulpix] failed to reset Kafka topics after retries" >&2
  exit 1
fi
if ! wait_for_kafka 120; then
  echo "[simulpix] Kafka did not become ready in time after topic reset" >&2
  exit 1
fi
if ! wait_for_required_topics 90; then
  echo "[simulpix] Kafka topics did not become visible in time after topic reset" >&2
  exit 1
fi
sleep 2

echo "[simulpix] restarting application services"
remove_application_services
if ! retry_command 5 restart_application_services; then
  echo "[simulpix] failed to restart application services after retries" >&2
  exit 1
fi
if ! wait_for_service_containers_running 90; then
  echo "[simulpix] application services did not become running in time after reset" >&2
  exit 1
fi
if [ "$KEEP_HEALTH_RUNNING" != "1" ]; then
  if ! wait_for_http "http://127.0.0.1:8082/health" 60; then
    echo "[simulpix] service health did not become ready in time after reset" >&2
    exit 1
  fi
fi

echo "[simulpix] scenario state reset"
echo "[simulpix] service health: http://localhost:8082/health"
