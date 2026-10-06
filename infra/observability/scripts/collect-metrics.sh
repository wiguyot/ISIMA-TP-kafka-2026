#!/bin/sh
set -eu

INFLUX_URL="${SIMULPIX_INFLUXDB_URL:-http://influxdb:8086}"
INFLUX_ORG="${SIMULPIX_INFLUXDB_ORG:-simulpix}"
INFLUX_BUCKET="${SIMULPIX_INFLUXDB_BUCKET:-simulpix_observability}"
INFLUX_TOKEN="${SIMULPIX_INFLUXDB_TOKEN:-simulpix-observability-token}"
HEALTH_URL="${SIMULPIX_SERVICE_HEALTH_URL:-http://service-health:8082/health/details}"
JMX_BROKERS="${SIMULPIX_JMX_BROKERS:-kafka-1:9991,kafka-2:9992,kafka-3:9993}"
INTERVAL="${SIMULPIX_OBS_INTERVAL_SECONDS:-10}"
RUNTIME_DIR="${SIMULPIX_RUNTIME_DIR:-/runtime/observability}"

mkdir -p "$RUNTIME_DIR"

escape_tag() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/,/\\,/g; s/=/\\=/g; s/ /\\ /g'
}

json_number() {
  key="$1"
  payload="$2"
  printf '%s' "$payload" | tr -d '\n' | sed -n "s/.*\"$key\"[[:space:]]*:[[:space:]]*\\([0-9.][0-9.]*\\).*/\\1/p" | head -n 1 || true
}

wait_for_http() {
  url="$1"
  attempts="${2:-90}"
  while [ "$attempts" -gt 0 ]; do
    if wget -q -O - "$url" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

write_influx() {
  body_file="$1"
  if [ ! -s "$body_file" ]; then
    return 0
  fi
  wget -q -O /dev/null \
    --header "Authorization: Token $INFLUX_TOKEN" \
    --header "Content-Type: text/plain; charset=utf-8" \
    --post-file="$body_file" \
    "$INFLUX_URL/api/v2/write?org=$INFLUX_ORG&bucket=$INFLUX_BUCKET&precision=ns" || true
}

collect_jmx_for_broker() {
  broker_host="$1"
  broker_port="$2"
  broker_name="$3"
  now_iso="$4"
  now_ns="$5"
  line_file="$6"
  json_file="$7"

  tmp_file="$(mktemp)"
  summary_file="$(mktemp)"

  /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.JmxTool \
    --jmx-url "service:jmx:rmi:///jndi/rmi://$broker_host:$broker_port/jmxrmi" \
    --report-format properties \
    --one-time \
    --object-name "kafka.server:type=FetcherLagMetrics,name=ConsumerLag,*" \
    --attributes Value >"$tmp_file" 2>/dev/null || true

  /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.JmxTool \
    --jmx-url "service:jmx:rmi:///jndi/rmi://$broker_host:$broker_port/jmxrmi" \
    --report-format properties \
    --one-time \
    --object-name "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions" \
    --object-name "kafka.controller:type=KafkaController,name=ActiveControllerCount" \
    --object-name "kafka.controller:type=KafkaController,name=OfflinePartitionsCount" \
    --attributes Value >"$summary_file" 2>/dev/null || true

  under_replicated="$(grep 'UnderReplicatedPartitions' "$summary_file" | tail -n 1 | awk -F= '{print $NF}' | tr -d '\r' || true)"
  active_controller="$(grep 'ActiveControllerCount' "$summary_file" | tail -n 1 | awk -F= '{print $NF}' | tr -d '\r' || true)"
  offline_partitions="$(grep 'OfflinePartitionsCount' "$summary_file" | tail -n 1 | awk -F= '{print $NF}' | tr -d '\r' || true)"
  under_replicated="${under_replicated:-0}"
  active_controller="${active_controller:-0}"
  offline_partitions="${offline_partitions:-0}"

  printf 'kafka_broker,broker=%s under_replicated_partitions=%si,active_controller_count=%si,offline_partitions_count=%si %s\n' \
    "$(escape_tag "$broker_name")" "$under_replicated" "$active_controller" "$offline_partitions" "$now_ns" >>"$line_file"

  first_json=1
  printf '{"broker":"%s","captured_at":"%s","under_replicated_partitions":%s,"active_controller_count":%s,"offline_partitions_count":%s,"replication_partitions":[' \
    "$broker_name" "$now_iso" "$under_replicated" "$active_controller" "$offline_partitions" >>"$json_file"

  grep 'FetcherLagMetrics' "$tmp_file" | while IFS= read -r line; do
    clean_line="$(printf '%s' "$line" | tr -d '\r')"
    topic="$(printf '%s' "$clean_line" | sed -n 's/.*topic=\([^,:"]*\).*/\1/p')"
    partition="$(printf '%s' "$clean_line" | sed -n 's/.*partition=\([0-9][0-9]*\).*/\1/p')"
    value="$(printf '%s' "$clean_line" | sed -n 's/.*:Value=\([0-9][0-9]*\).*/\1/p')"
    if [ -z "$topic" ] || [ -z "$partition" ] || [ -z "$value" ]; then
      continue
    fi
    if [ "$first_json" -eq 0 ]; then
      printf ',' >>"$json_file"
    fi
    first_json=0
    printf '{"topic":"%s","partition":%s,"lag_offsets":%s}' "$topic" "$partition" "$value" >>"$json_file"
    printf 'kafka_replication_partition,broker=%s,topic=%s,partition=%s lag_offsets=%si %s\n' \
      "$(escape_tag "$broker_name")" "$(escape_tag "$topic")" "$partition" "$value" "$now_ns" >>"$line_file"
  done

  printf ']}\n' >>"$json_file"
  rm -f "$tmp_file" "$summary_file"
}

collect_service_health() {
  now_ns="$1"
  line_file="$2"
  payload="$(wget -q -O - "$HEALTH_URL" 2>/dev/null || true)"
  if [ -z "$payload" ]; then
    return 0
  fi

  generated="$(json_number generated "$payload")"
  processed="$(json_number processed "$payload")"
  validated="$(json_number validated "$payload")"
  rejected="$(json_number rejected "$payload")"
  reject_rate_percent="$(json_number reject_rate_percent "$payload")"
  db_validated_count="$(json_number db_validated_count "$payload")"
  db_rejected_count="$(json_number db_rejected_count "$payload")"
  outcome_count="$(json_number outcome_count "$payload")"
  pending_result_count="$(json_number pending_result_count "$payload")"
  checked_topic_end_offsets="$(json_number checked_topic_end_offsets "$payload")"
  decision_topic_end_offsets="$(json_number decision_topic_end_offsets "$payload")"
  persisted_valid="$(json_number persisted_valid "$payload")"
  persisted_rejected="$(json_number persisted_rejected "$payload")"
  persistence_gap_valid="$(json_number persistence_gap_valid "$payload")"
  persistence_gap_rejected="$(json_number persistence_gap_rejected "$payload")"
  avg_validation_latency_ms="$(json_number avg_validation_latency_ms "$payload")"
  avg_rejection_latency_ms="$(json_number avg_rejection_latency_ms "$payload")"
  estimated_oldest_lag_seconds="$(json_number estimated_oldest_lag_seconds "$payload")"
  validated_sla_breaches="$(json_number validated_sla_breaches "$payload")"
  rejected_sla_breaches="$(json_number rejected_sla_breaches "$payload")"
  validated_within_sla_count="$(json_number validated_within_sla_count "$payload")"
  rejected_within_sla_count="$(json_number rejected_within_sla_count "$payload")"
  pix_validator_lag_total="$(json_number pix_validator_lag_total "$payload")"
  pix_decision_engine_lag_total="$(json_number pix_decision_engine_lag_total "$payload")"
  pix_outcome_publisher_lag_total="$(json_number pix_outcome_publisher_lag_total "$payload")"
  persister_valid_lag_total="$(json_number persister_valid_lag_total "$payload")"
  persister_rejected_lag_total="$(json_number persister_rejected_lag_total "$payload")"
  raw_topic_end_offsets="$(json_number raw_topic_end_offsets "$payload")"
  checked_topic_end_offsets="${checked_topic_end_offsets:-}"
  decision_topic_end_offsets="${decision_topic_end_offsets:-}"
  validated_topic_end_offsets="$(json_number validated_topic_end_offsets "$payload")"
  rejected_topic_end_offsets="$(json_number rejected_topic_end_offsets "$payload")"
  retry_topic_end_offsets="$(json_number retry_topic_end_offsets "$payload")"
  outcome_topic_end_offsets="$(json_number outcome_topic_end_offsets "$payload")"
  postgres_active_connections="$(json_number postgres_active_connections "$payload")"
  postgres_database_size_bytes="$(json_number postgres_database_size_bytes "$payload")"
  postgres_validated_table_size_bytes="$(json_number postgres_validated_table_size_bytes "$payload")"
  postgres_rejected_table_size_bytes="$(json_number postgres_rejected_table_size_bytes "$payload")"
  seconds_since_last_validated="$(json_number seconds_since_last_validated "$payload")"
  seconds_since_last_rejected="$(json_number seconds_since_last_rejected "$payload")"
  network_active="$(json_number network_active "$payload")"
  network_delay_ms="$(json_number network_delay_ms "$payload")"
  network_jitter_ms="$(json_number network_jitter_ms "$payload")"
  network_loss_percent="$(json_number network_loss_percent "$payload")"
  network_rate_kbit="$(json_number network_rate_kbit "$payload")"
  producer_semantics_code="$(json_number producer_semantics_code "$payload")"
  producer_risk_code="$(json_number producer_risk_code "$payload")"
  producer_acks_code="$(json_number producer_acks_code "$payload")"
  producer_retries="$(json_number producer_retries "$payload")"
  consumer_semantics_code="$(json_number consumer_semantics_code "$payload")"
  consumer_risk_code="$(json_number consumer_risk_code "$payload")"
  consumer_auto_commit_code="$(json_number consumer_auto_commit_code "$payload")"
  consumer_commit_strategy_code="$(json_number consumer_commit_strategy_code "$payload")"
  consumer_commit_batch_size="$(json_number consumer_commit_batch_size "$payload")"
  validator_transactional_code="$(json_number validator_transactional_code "$payload")"
  decision_transactional_code="$(json_number decision_transactional_code "$payload")"
  outcome_transactional_code="$(json_number outcome_transactional_code "$payload")"
  kafka_exactly_once_chain_count="$(json_number kafka_exactly_once_chain_count "$payload")"

  [ -n "$generated" ] || generated=0
  [ -n "$processed" ] || processed=0
  [ -n "$validated" ] || validated=0
  [ -n "$rejected" ] || rejected=0
  [ -n "$reject_rate_percent" ] || reject_rate_percent=0
  [ -n "$db_validated_count" ] || db_validated_count=0
  [ -n "$db_rejected_count" ] || db_rejected_count=0
  [ -n "$outcome_count" ] || outcome_count=0
  [ -n "$pending_result_count" ] || pending_result_count=0
  [ -n "$checked_topic_end_offsets" ] || checked_topic_end_offsets=0
  [ -n "$decision_topic_end_offsets" ] || decision_topic_end_offsets=0
  [ -n "$persisted_valid" ] || persisted_valid=0
  [ -n "$persisted_rejected" ] || persisted_rejected=0
  [ -n "$persistence_gap_valid" ] || persistence_gap_valid=0
  [ -n "$persistence_gap_rejected" ] || persistence_gap_rejected=0
  [ -n "$avg_validation_latency_ms" ] || avg_validation_latency_ms=0
  [ -n "$avg_rejection_latency_ms" ] || avg_rejection_latency_ms=0
  [ -n "$estimated_oldest_lag_seconds" ] || estimated_oldest_lag_seconds=0
  [ -n "$validated_sla_breaches" ] || validated_sla_breaches=0
  [ -n "$rejected_sla_breaches" ] || rejected_sla_breaches=0
  [ -n "$validated_within_sla_count" ] || validated_within_sla_count=0
  [ -n "$rejected_within_sla_count" ] || rejected_within_sla_count=0
  [ -n "$pix_decision_engine_lag_total" ] || pix_decision_engine_lag_total=0
  [ -n "$pix_validator_lag_total" ] || pix_validator_lag_total=0
  [ -n "$pix_decision_engine_lag_total" ] || pix_decision_engine_lag_total=0
  [ -n "$pix_outcome_publisher_lag_total" ] || pix_outcome_publisher_lag_total=0
  [ -n "$persister_valid_lag_total" ] || persister_valid_lag_total=0
  [ -n "$persister_rejected_lag_total" ] || persister_rejected_lag_total=0
  [ -n "$raw_topic_end_offsets" ] || raw_topic_end_offsets=0
  [ -n "$validated_topic_end_offsets" ] || validated_topic_end_offsets=0
  [ -n "$rejected_topic_end_offsets" ] || rejected_topic_end_offsets=0
  [ -n "$retry_topic_end_offsets" ] || retry_topic_end_offsets=0
  [ -n "$outcome_topic_end_offsets" ] || outcome_topic_end_offsets=0
  [ -n "$postgres_active_connections" ] || postgres_active_connections=0
  [ -n "$postgres_database_size_bytes" ] || postgres_database_size_bytes=0
  [ -n "$postgres_validated_table_size_bytes" ] || postgres_validated_table_size_bytes=0
  [ -n "$postgres_rejected_table_size_bytes" ] || postgres_rejected_table_size_bytes=0
  [ -n "$seconds_since_last_validated" ] || seconds_since_last_validated=0
  [ -n "$seconds_since_last_rejected" ] || seconds_since_last_rejected=0
  [ -n "$network_active" ] || network_active=0
  [ -n "$network_delay_ms" ] || network_delay_ms=0
  [ -n "$network_jitter_ms" ] || network_jitter_ms=0
  [ -n "$network_loss_percent" ] || network_loss_percent=0
  [ -n "$network_rate_kbit" ] || network_rate_kbit=0
  [ -n "$producer_semantics_code" ] || producer_semantics_code=2
  [ -n "$producer_risk_code" ] || producer_risk_code=2
  [ -n "$producer_acks_code" ] || producer_acks_code=2
  [ -n "$producer_retries" ] || producer_retries=0
  [ -n "$consumer_semantics_code" ] || consumer_semantics_code=2
  [ -n "$consumer_risk_code" ] || consumer_risk_code=2
  [ -n "$consumer_auto_commit_code" ] || consumer_auto_commit_code=0
  [ -n "$consumer_commit_strategy_code" ] || consumer_commit_strategy_code=2
  [ -n "$consumer_commit_batch_size" ] || consumer_commit_batch_size=1
  [ -n "$validator_transactional_code" ] || validator_transactional_code=0
  [ -n "$decision_transactional_code" ] || decision_transactional_code=0
  [ -n "$outcome_transactional_code" ] || outcome_transactional_code=0
  [ -n "$kafka_exactly_once_chain_count" ] || kafka_exactly_once_chain_count=0

  avg_validation_latency_ms_recent=0
  if [ "$validated" -gt 0 ] && [ -n "$avg_validation_latency_ms" ] && awk "BEGIN {exit !($seconds_since_last_validated <= 60.0)}"; then
    avg_validation_latency_ms_recent="$avg_validation_latency_ms"
  fi

  printf 'simulpix_platform generated=%si,processed=%si,validated=%si,rejected=%si,reject_rate_percent=%s,db_validated_count=%si,db_rejected_count=%si,outcome_count=%si,pending_result_count=%si,checked_topic_end_offsets=%si,decision_topic_end_offsets=%si,persisted_valid=%si,persisted_rejected=%si,persistence_gap_valid=%si,persistence_gap_rejected=%si,avg_validation_latency_ms=%s,avg_validation_latency_ms_recent=%s,avg_rejection_latency_ms=%s,estimated_oldest_lag_seconds=%s,validated_sla_breaches=%si,rejected_sla_breaches=%si,validated_within_sla_count=%si,rejected_within_sla_count=%si %s\n' \
    "$generated" "$processed" "$validated" "$rejected" "$reject_rate_percent" "$db_validated_count" "$db_rejected_count" "$outcome_count" "$pending_result_count" "$checked_topic_end_offsets" "$decision_topic_end_offsets" "$persisted_valid" "$persisted_rejected" "$persistence_gap_valid" "$persistence_gap_rejected" "$avg_validation_latency_ms" "$avg_validation_latency_ms_recent" "$avg_rejection_latency_ms" "$estimated_oldest_lag_seconds" "$validated_sla_breaches" "$rejected_sla_breaches" "$validated_within_sla_count" "$rejected_within_sla_count" "$now_ns" >>"$line_file"
  printf 'simulpix_network active=%si,delay_ms=%s,jitter_ms=%s,loss_percent=%s,rate_kbit=%si %s\n' \
    "$network_active" "$network_delay_ms" "$network_jitter_ms" "$network_loss_percent" "$network_rate_kbit" "$now_ns" >>"$line_file"
  printf 'simulpix_semantics producer_semantics_code=%si,producer_risk_code=%si,producer_acks_code=%si,producer_retries=%si,consumer_semantics_code=%si,consumer_risk_code=%si,consumer_commit_strategy_code=%si,consumer_auto_commit_code=%si,consumer_commit_batch_size=%si,validator_transactional_code=%si,decision_transactional_code=%si,outcome_transactional_code=%si,kafka_exactly_once_chain_count=%si %s\n' \
    "$producer_semantics_code" "$producer_risk_code" "$producer_acks_code" "$producer_retries" "$consumer_semantics_code" "$consumer_risk_code" "$consumer_commit_strategy_code" "$consumer_auto_commit_code" "$consumer_commit_batch_size" "$validator_transactional_code" "$decision_transactional_code" "$outcome_transactional_code" "$kafka_exactly_once_chain_count" "$now_ns" >>"$line_file"
  printf 'kafka_consumer_group,group=pix-validator lag_total=%si %s\n' "$pix_validator_lag_total" "$now_ns" >>"$line_file"
  printf 'kafka_consumer_group,group=pix-decision-engine lag_total=%si %s\n' "$pix_decision_engine_lag_total" "$now_ns" >>"$line_file"
  printf 'kafka_consumer_group,group=pix-outcome-publisher lag_total=%si %s\n' "$pix_outcome_publisher_lag_total" "$now_ns" >>"$line_file"
  printf 'kafka_consumer_group,group=persister-valid lag_total=%si %s\n' "$persister_valid_lag_total" "$now_ns" >>"$line_file"
  printf 'kafka_consumer_group,group=persister-rejected lag_total=%si %s\n' "$persister_rejected_lag_total" "$now_ns" >>"$line_file"
  printf 'kafka_topic,topic=simulpix.transactions.raw end_offsets_total=%si %s\n' "$raw_topic_end_offsets" "$now_ns" >>"$line_file"
  printf 'kafka_topic,topic=simulpix.transactions.checked end_offsets_total=%si %s\n' "$checked_topic_end_offsets" "$now_ns" >>"$line_file"
  printf 'kafka_topic,topic=simulpix.transactions.decision end_offsets_total=%si %s\n' "$decision_topic_end_offsets" "$now_ns" >>"$line_file"
  printf 'kafka_topic,topic=simulpix.transactions.validated end_offsets_total=%si %s\n' "$validated_topic_end_offsets" "$now_ns" >>"$line_file"
  printf 'kafka_topic,topic=simulpix.transactions.rejected end_offsets_total=%si %s\n' "$rejected_topic_end_offsets" "$now_ns" >>"$line_file"
  printf 'kafka_topic,topic=simulpix.transactions.retry end_offsets_total=%si %s\n' "$retry_topic_end_offsets" "$now_ns" >>"$line_file"
  printf 'kafka_topic,topic=simulpix.transactions.outcome end_offsets_total=%si %s\n' "$outcome_topic_end_offsets" "$now_ns" >>"$line_file"
  printf 'postgres_runtime validated_count=%si,rejected_count=%si,active_connections=%si,database_size_bytes=%si,validated_table_size_bytes=%si,rejected_table_size_bytes=%si,seconds_since_last_validated=%s,seconds_since_last_rejected=%s %s\n' \
    "$db_validated_count" "$db_rejected_count" "$postgres_active_connections" "$postgres_database_size_bytes" "$postgres_validated_table_size_bytes" "$postgres_rejected_table_size_bytes" "$seconds_since_last_validated" "$seconds_since_last_rejected" "$now_ns" >>"$line_file"
}

wait_for_http "$INFLUX_URL/health" 120
wait_for_http "$HEALTH_URL" 120 || true

while true; do
  now_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  now_ns="$(date +%s)000000000"
  line_file="$(mktemp)"
  json_tmp="$(mktemp)"

  printf '{"captured_at":"%s","brokers":[' "$now_iso" >"$json_tmp"
  first_broker=1

  OLD_IFS="$IFS"
  IFS=','
  for broker in $JMX_BROKERS; do
    host="${broker%%:*}"
    port="${broker##*:}"
    if [ "$first_broker" -eq 0 ]; then
      printf ',' >>"$json_tmp"
    fi
    first_broker=0
    collect_jmx_for_broker "$host" "$port" "$host" "$now_iso" "$now_ns" "$line_file" "$json_tmp"
  done
  IFS="$OLD_IFS"

  printf ']}\n' >>"$json_tmp"
  mv "$json_tmp" "$RUNTIME_DIR/latest-replication.json"

  collect_service_health "$now_ns" "$line_file"
  write_influx "$line_file"
  rm -f "$line_file"
  sleep "$INTERVAL"
done
