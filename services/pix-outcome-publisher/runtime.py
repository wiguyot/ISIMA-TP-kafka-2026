import json
import os
import threading
import time

from health import serve_http
from kafka_io import (
    get_consumer_group,
    get_consumer_settings,
    get_processing_arch,
    get_producer_semantics,
    get_transactional_id,
    load_existing_count,
    produce_in_transaction,
    produce_and_confirm,
    wait_for_kafka,
)
from state import STATE


def build_outcome_message(decision_message: dict[str, object], outcome_topic: str) -> dict[str, object]:
    return {
        "transaction_id": decision_message.get("transaction_id"),
        "source_transaction_id": decision_message.get("source_transaction_id"),
        "retry_attempt": decision_message.get("retry_attempt", 0),
        "event_time": decision_message.get("event_time"),
        "scenario_type": decision_message.get("scenario_type"),
        "producer_id": decision_message.get("producer_id"),
        "trace_id": decision_message.get("trace_id"),
        "decision_status": decision_message.get("decision_status"),
        "decision_reason_code": decision_message.get("decision_reason_code"),
        "decision_reason_label": decision_message.get("decision_reason_label"),
        "decision_origin": decision_message.get("decision_origin"),
        "decision_at": decision_message.get("decision_at"),
        "decision_deadline": decision_message.get("decision_deadline"),
        "decision_sla_seconds": decision_message.get("decision_sla_seconds"),
        "decision_latency_ms": decision_message.get("decision_latency_ms"),
        "decision_within_sla": decision_message.get("decision_within_sla"),
        "client_message": decision_message.get("client_message"),
        "final_topic": outcome_topic,
        "final_event_type": "PIX_TRANSACTION_OUTCOME",
        "processing_node": decision_message.get("processing_node") or os.getenv("SIMULPIX_SERVICE_NAME", "pix-outcome-publisher"),
    }


def run() -> int:
    bootstrap_servers = os.getenv("SIMULPIX_KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092")
    topic_decision = os.getenv("SIMULPIX_TOPIC_DECISION", "simulpix.transactions.decision")
    topic_outcome = os.getenv("SIMULPIX_TOPIC_OUTCOME", "simulpix.transactions.outcome")
    consumer_group = get_consumer_group()
    consumer_settings = get_consumer_settings()
    pending_commit_count = 0
    pending_commit_record = None

    STATE["arch"] = get_processing_arch()
    STATE["producer_semantics"] = get_producer_semantics()
    STATE["consumer_semantics"] = str(consumer_settings["semantics"])
    STATE["consumer_auto_commit"] = consumer_settings["auto_commit"]
    STATE["consumer_commit_strategy"] = consumer_settings["commit_strategy"]
    STATE["consumer_max_poll_interval_ms"] = consumer_settings["max_poll_interval_ms"]
    STATE["consumer_commit_batch_size"] = consumer_settings["commit_batch_size"]
    STATE["transactional_mode"] = "exactly_once_kafka" if consumer_settings["semantics"] == "exactly_once_kafka" else "disabled"
    STATE["transactional_id"] = get_transactional_id() if consumer_settings["semantics"] == "exactly_once_kafka" else None

    threading.Thread(target=serve_http, daemon=True).start()
    if STATE["arch"] != "split":
        STATE["status"] = "idle"
        while True:
            time.sleep(5)

    producer, consumer = wait_for_kafka(bootstrap_servers, consumer_group, [topic_decision])
    STATE["published"] = load_existing_count(bootstrap_servers, topic_outcome)
    STATE["status"] = "running"

    while True:
        record = consumer.poll(1.0)
        if record is None:
            if (
                not consumer_settings["auto_commit"]
                and consumer_settings["commit_strategy"] == "after"
                and pending_commit_count > 0
                and pending_commit_record is not None
            ):
                consumer.commit(message=pending_commit_record, asynchronous=False)
                pending_commit_count = 0
                pending_commit_record = None
            continue
        if record.error() is not None:
            STATE["last_error"] = str(record.error())
            continue
        try:
            decision_message = json.loads(record.value().decode("utf-8"))
            record_key = record.key().decode("utf-8") if record.key() else None
            transaction_id = decision_message.get("transaction_id")
            partition_key = decision_message.get("source_transaction_id") or record_key or transaction_id or "unknown"
            if not consumer_settings["auto_commit"] and consumer_settings["commit_strategy"] == "before":
                consumer.commit(message=record, asynchronous=False)
            outcome = build_outcome_message(decision_message, topic_outcome)
            if consumer_settings["semantics"] == "exactly_once_kafka":
                produce_in_transaction(producer, consumer, record, topic_outcome, str(partition_key), outcome)
            else:
                produce_and_confirm(producer, topic_outcome, str(partition_key), outcome)
            if not consumer_settings["auto_commit"] and consumer_settings["commit_strategy"] == "after":
                pending_commit_count += 1
                pending_commit_record = record
                if pending_commit_count >= int(consumer_settings["commit_batch_size"]):
                    consumer.commit(message=pending_commit_record, asynchronous=False)
                    pending_commit_count = 0
                    pending_commit_record = None
            STATE["published"] += 1
            STATE["last_transaction_id"] = transaction_id
            STATE["last_error"] = None
            print(json.dumps({"event": "outcome_published", "transaction_id": transaction_id}), flush=True)
        except Exception as exc:
            if consumer_settings["semantics"] == "exactly_once_kafka":
                try:
                    producer.abort_transaction(10.0)
                except Exception:
                    pass
            STATE["status"] = "error"
            STATE["ready_for_messages"] = False
            STATE["assigned_partitions"] = 0
            STATE["last_error"] = repr(exc)
            print(json.dumps({"event": "outcome_publish_failed", "error": repr(exc)}), flush=True)
            time.sleep(2)
            try:
                consumer.close()
            except Exception:
                pass
            producer, consumer = wait_for_kafka(bootstrap_servers, consumer_group, [topic_decision])
            STATE["published"] = load_existing_count(bootstrap_servers, topic_outcome)
            STATE["status"] = "running"
            pending_commit_count = 0
            pending_commit_record = None

    return 0
