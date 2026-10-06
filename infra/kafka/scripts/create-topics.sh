#!/bin/sh
set -eu

bootstrap="kafka-1:29092,kafka-2:29092,kafka-3:29092"
kafka_topics_bin="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"
client_config="${KAFKA_CLIENT_CONFIG:-/config/client.properties}"

if [ ! -x "$kafka_topics_bin" ]; then
  kafka_topics_bin="$(command -v kafka-topics.sh || true)"
fi

if [ -z "$kafka_topics_bin" ]; then
  echo "[simulpix] kafka-topics.sh not found" >&2
  exit 127
fi

wait_for_cluster() {
  attempts="${1:-60}"
  while [ "$attempts" -gt 0 ]; do
    if "$kafka_topics_bin" --bootstrap-server "$bootstrap" --command-config "$client_config" --list >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 2
  done
  return 1
}

create_topic() {
  topic="$1"
  partitions="$2"
  "$kafka_topics_bin" --bootstrap-server "$bootstrap" --command-config "$client_config" --create --if-not-exists --topic "$topic" --partitions "$partitions" --replication-factor 3
}

if ! wait_for_cluster 60; then
  echo "[simulpix] kafka cluster not ready for authenticated topic creation" >&2
  exit 1
fi

echo "[simulpix] creating topics"
create_topic simulpix.transactions.raw 6
create_topic simulpix.transactions.checked 6
create_topic simulpix.transactions.decision 6
create_topic simulpix.transactions.validated 6
create_topic simulpix.transactions.rejected 3
create_topic simulpix.transactions.retry 3
create_topic simulpix.transactions.outcome 6
echo "[simulpix] topics ready"
