import json
import os
import socket
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient


def load_run_state(path: str) -> dict[str, str] | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return {
        "run_id": str(data.get("run_id", "")),
        "pix_validator_group": str(data.get("pix_validator_group", "")),
        "pix_decision_engine_group": str(data.get("pix_decision_engine_group", "")),
        "pix_outcome_publisher_group": str(data.get("pix_outcome_publisher_group", "")),
        "persister_valid_group": str(data.get("persister_valid_group", "")),
        "persister_rejected_group": str(data.get("persister_rejected_group", "")),
    }


def load_replication_snapshot(path: str) -> dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def load_network_state(path: str) -> dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def tcp_probe(host: str, port: int, timeout: float = 1.0) -> str:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return "ok"
    except OSError:
        return "down"


def build_kafka_client_config(group_id: str | None = None) -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": os.getenv("SIMULPIX_KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092"),
    }
    if group_id:
        config["group.id"] = group_id
        config["enable.auto.commit"] = False
        config["session.timeout.ms"] = 6000
    security_protocol = os.getenv("SIMULPIX_KAFKA_SECURITY_PROTOCOL", "")
    sasl_mechanism = os.getenv("SIMULPIX_KAFKA_SASL_MECHANISM", "")
    username = os.getenv("SIMULPIX_KAFKA_USERNAME", "")
    password = os.getenv("SIMULPIX_KAFKA_PASSWORD", "")
    if security_protocol:
        config["security.protocol"] = security_protocol
    if sasl_mechanism:
        config["sasl.mechanisms"] = sasl_mechanism
    if username:
        config["sasl.username"] = username
    if password:
        config["sasl.password"] = password
    return config


def http_probe(url: str, timeout: float = 1.0) -> str:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            if 200 <= response.status < 300:
                return "ok"
            return f"http_{response.status}"
    except (urllib.error.URLError, TimeoutError, ValueError):
        return "down"


def fetch_json(url: str, timeout: float = 1.0) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            if 200 <= response.status < 300:
                return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
        return None
    return None


def safe_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def query_postgres_metrics(database_url: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": "down",
        "validated_count": 0,
        "rejected_count": 0,
        "active_connections": 0,
        "database_size_bytes": 0,
        "validated_table_size_bytes": 0,
        "rejected_table_size_bytes": 0,
        "avg_validation_latency_ms": None,
        "avg_rejection_latency_ms": None,
        "validated_sla_breaches": 0,
        "rejected_sla_breaches": 0,
        "last_validated_at": None,
        "last_rejected_at": None,
        "seconds_since_last_validated": None,
        "seconds_since_last_rejected": None,
        "top_rejection_reasons": [],
        "rejections_by_scenario": [],
        "recent_rejections": [],
    }
    try:
        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("select count(*) from validated_transactions")
                result["validated_count"] = safe_int(cursor.fetchone()[0])

                cursor.execute("select count(*) from rejected_transactions")
                result["rejected_count"] = safe_int(cursor.fetchone()[0])

                cursor.execute("select count(*) from pg_stat_activity where datname = current_database()")
                result["active_connections"] = safe_int(cursor.fetchone()[0])

                cursor.execute("select pg_database_size(current_database())")
                result["database_size_bytes"] = safe_int(cursor.fetchone()[0])

                cursor.execute("select pg_total_relation_size('validated_transactions')")
                result["validated_table_size_bytes"] = safe_int(cursor.fetchone()[0])

                cursor.execute("select pg_total_relation_size('rejected_transactions')")
                result["rejected_table_size_bytes"] = safe_int(cursor.fetchone()[0])

                cursor.execute(
                    """
                    select round(avg(extract(epoch from (validated_at - event_time)) * 1000)::numeric, 2)
                    from validated_transactions
                    where validated_at is not null
                      and validated_at >= now() - interval '5 minutes'
                    """
                )
                avg_validation_latency = cursor.fetchone()[0]
                result["avg_validation_latency_ms"] = float(avg_validation_latency) if avg_validation_latency is not None else None

                cursor.execute(
                    """
                    select round(avg(extract(epoch from (rejected_at - event_time)) * 1000)::numeric, 2)
                    from rejected_transactions
                    where rejected_at is not null
                      and event_time is not null
                      and rejected_at >= now() - interval '5 minutes'
                    """
                )
                avg_rejection_latency = cursor.fetchone()[0]
                result["avg_rejection_latency_ms"] = float(avg_rejection_latency) if avg_rejection_latency is not None else None

                cursor.execute(
                    """
                    select count(*)
                    from validated_transactions
                    where coalesce(decision_within_sla, true) = false
                    """
                )
                result["validated_sla_breaches"] = safe_int(cursor.fetchone()[0])

                cursor.execute(
                    """
                    select count(*)
                    from rejected_transactions
                    where coalesce(decision_within_sla, true) = false
                    """
                )
                result["rejected_sla_breaches"] = safe_int(cursor.fetchone()[0])

                cursor.execute("select max(validated_at) from validated_transactions")
                last_validated_at = cursor.fetchone()[0]
                result["last_validated_at"] = last_validated_at.isoformat() if last_validated_at is not None else None

                cursor.execute("select max(rejected_at) from rejected_transactions")
                last_rejected_at = cursor.fetchone()[0]
                result["last_rejected_at"] = last_rejected_at.isoformat() if last_rejected_at is not None else None

                result["validated_within_sla_count"] = max(
                    0, safe_int(result["validated_count"]) - safe_int(result["validated_sla_breaches"])
                )
                result["rejected_within_sla_count"] = max(
                    0, safe_int(result["rejected_count"]) - safe_int(result["rejected_sla_breaches"])
                )

                now_utc = datetime.now(timezone.utc)
                if last_validated_at is not None:
                    validated_at = last_validated_at if last_validated_at.tzinfo is not None else last_validated_at.replace(tzinfo=timezone.utc)
                    result["seconds_since_last_validated"] = round((now_utc - validated_at).total_seconds(), 2)
                if last_rejected_at is not None:
                    rejected_at = last_rejected_at if last_rejected_at.tzinfo is not None else last_rejected_at.replace(tzinfo=timezone.utc)
                    result["seconds_since_last_rejected"] = round((now_utc - rejected_at).total_seconds(), 2)

                cursor.execute(
                    """
                    select rejection_reason, count(*) as occurrences
                    from rejected_transactions
                    group by rejection_reason
                    order by occurrences desc, rejection_reason asc
                    limit 5
                    """
                )
                result["top_rejection_reasons"] = [
                    {"rejection_reason": row[0], "occurrences": safe_int(row[1])}
                    for row in cursor.fetchall()
                ]

                cursor.execute(
                    """
                    select coalesce(scenario_type, 'unknown') as scenario_type, count(*) as occurrences
                    from rejected_transactions
                    group by scenario_type
                    order by occurrences desc, scenario_type asc
                    limit 5
                    """
                )
                result["rejections_by_scenario"] = [
                    {"scenario_type": row[0], "occurrences": safe_int(row[1])}
                    for row in cursor.fetchall()
                ]

                cursor.execute(
                    """
                    select
                      coalesce(transaction_id, 'unknown') as transaction_id,
                      rejection_reason,
                      rejected_at
                    from rejected_transactions
                    order by rejected_at desc, id desc
                    limit 5
                    """
                )
                result["recent_rejections"] = [
                    {
                        "transaction_id": row[0],
                        "rejection_reason": row[1],
                        "rejected_at": row[2].isoformat() if row[2] is not None else None,
                    }
                    for row in cursor.fetchall()
                ]
        result["status"] = "ok"
    except Exception as exc:
        result["status"] = "error"
        result["error"] = repr(exc)
    return result


def build_kafka_details(bootstrap_servers: str, topics: list[str], groups: dict[str, str]) -> dict[str, Any]:
    admin = AdminClient(build_kafka_client_config())
    metadata = admin.list_topics(timeout=5)
    broker_count = len(metadata.brokers)
    topic_details: dict[str, Any] = {}

    probe_consumer = Consumer(build_kafka_client_config("simulpix-health-probe"))

    try:
        for topic in topics:
            topic_meta = metadata.topics.get(topic)
            if topic_meta is None or topic_meta.error is not None:
                topic_details[topic] = {"status": "missing"}
                continue
            partitions = sorted(topic_meta.partitions.keys())
            end_offsets_total = 0
            partition_details = []
            for partition_id in partitions:
                low, high = probe_consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=5)
                end_offsets_total += high
                leader = topic_meta.partitions[partition_id].leader
                partition_details.append(
                    {
                        "partition": partition_id,
                        "leader": leader,
                        "low_offset": low,
                        "high_offset": high,
                    }
                )
            topic_details[topic] = {
                "status": "ok",
                "partitions": len(partitions),
                "end_offsets_total": end_offsets_total,
                "partition_details": partition_details,
            }

        group_details: dict[str, Any] = {}
        lag_alert = "ok"
        for label, group_id in groups.items():
            group_consumer = Consumer(build_kafka_client_config(group_id))
            try:
                lag_total = 0
                lag_topics: dict[str, Any] = {}
                for topic, info in topic_details.items():
                    if info.get("status") != "ok":
                        continue
                    requested = [TopicPartition(topic, item["partition"]) for item in info["partition_details"]]
                    committed = group_consumer.committed(requested, timeout=5)
                    topic_lag = 0
                    partitions_info = []
                    for committed_tp, partition_info in zip(committed, info["partition_details"], strict=False):
                        committed_offset = committed_tp.offset
                        high = partition_info["high_offset"]
                        lag = None if committed_offset is None or committed_offset < 0 else max(high - committed_offset, 0)
                        if lag is not None:
                            lag_total += lag
                            topic_lag += lag
                        partitions_info.append(
                            {
                                "partition": partition_info["partition"],
                                "committed_offset": committed_offset,
                                "high_offset": high,
                                "lag": lag,
                            }
                        )
                    lag_topics[topic] = {"lag_total": topic_lag, "partitions": partitions_info}
                if lag_total > 0:
                    lag_alert = "warning"
                group_details[label] = {"group_id": group_id, "lag_total": lag_total, "topics": lag_topics}
            finally:
                group_consumer.close()

        return {
            "bootstrap_servers": bootstrap_servers,
            "broker_count": broker_count,
            "lag_alert": lag_alert,
            "topics": topic_details,
            "consumer_groups": group_details,
        }
    finally:
        probe_consumer.close()


def topic_rate_per_second(topic_name: str, rate_metrics: dict[str, Any] | None) -> float:
    topic_key_map = {
        "simulpix.transactions.raw": "topic_raw_offsets_per_second",
        "simulpix.transactions.validated": "topic_validated_offsets_per_second",
        "simulpix.transactions.rejected": "topic_rejected_offsets_per_second",
        "simulpix.transactions.retry": "topic_retry_offsets_per_second",
        "simulpix.transactions.outcome": "topic_outcome_offsets_per_second",
    }
    field = topic_key_map.get(topic_name)
    if not field:
        return 0.0
    try:
        return float((rate_metrics or {}).get(field, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def enrich_kafka_replication(
    kafka_detail: dict[str, Any] | None,
    replication_snapshot: dict[str, Any] | None,
    rate_metrics: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not kafka_detail:
        return kafka_detail

    topic_details = kafka_detail.get("topics") or {}
    broker_entries = (replication_snapshot or {}).get("brokers") or []
    partition_lags: dict[tuple[str, int], dict[str, Any]] = {}
    broker_summaries: list[dict[str, Any]] = []

    for broker in broker_entries:
        broker_name = str(broker.get("broker", "unknown"))
        broker_summaries.append(
            {
                "broker": broker_name,
                "under_replicated_partitions": safe_int(broker.get("under_replicated_partitions")),
                "active_controller_count": safe_int(broker.get("active_controller_count")),
                "offline_partitions_count": safe_int(broker.get("offline_partitions_count")),
            }
        )
        for item in broker.get("replication_partitions") or []:
            topic = str(item.get("topic", ""))
            partition = safe_int(item.get("partition"))
            lag_offsets = safe_int(item.get("lag_offsets"))
            key = (topic, partition)
            current = partition_lags.get(key)
            if current is None or lag_offsets > safe_int(current.get("lag_offsets")):
                partition_lags[key] = {
                    "topic": topic,
                    "partition": partition,
                    "lag_offsets": lag_offsets,
                    "broker": broker_name,
                }

    replication_rows: list[dict[str, Any]] = []
    for topic, info in topic_details.items():
        partitions = info.get("partition_details") or []
        per_partition_rate = 0.0
        if partitions:
            per_partition_rate = topic_rate_per_second(topic, rate_metrics) / len(partitions)
        topic_max_offsets = 0
        topic_max_seconds = None
        for partition_info in partitions:
            key = (topic, safe_int(partition_info.get("partition")))
            lag_info = partition_lags.get(key, {"lag_offsets": 0, "broker": "n/a"})
            lag_offsets = safe_int(lag_info.get("lag_offsets"))
            lag_seconds_estimate = None
            if lag_offsets > 0 and per_partition_rate > 0.1:
                lag_seconds_estimate = round(lag_offsets / per_partition_rate, 2)
            partition_info["replication_lag_offsets"] = lag_offsets
            partition_info["replication_lag_seconds_estimate"] = lag_seconds_estimate
            partition_info["replication_lag_broker"] = lag_info.get("broker")
            topic_max_offsets = max(topic_max_offsets, lag_offsets)
            if lag_seconds_estimate is not None:
                topic_max_seconds = max(topic_max_seconds or 0.0, lag_seconds_estimate)
            replication_rows.append(
                {
                    "topic": topic,
                    "partition": safe_int(partition_info.get("partition")),
                    "leader": partition_info.get("leader"),
                    "broker": lag_info.get("broker"),
                    "lag_offsets": lag_offsets,
                    "lag_seconds_estimate": lag_seconds_estimate,
                }
            )
        info["replication_lag_max_offsets"] = topic_max_offsets
        info["replication_lag_max_seconds_estimate"] = topic_max_seconds

    kafka_detail["replication"] = {
        "captured_at": (replication_snapshot or {}).get("captured_at"),
        "brokers": broker_summaries,
        "partitions": sorted(
            replication_rows,
            key=lambda item: (-safe_int(item.get("lag_offsets")), str(item.get("topic")), safe_int(item.get("partition"))),
        ),
        "under_replicated_partitions_total": sum(
            safe_int(item.get("under_replicated_partitions")) for item in broker_summaries
        ),
    }
    return kafka_detail
