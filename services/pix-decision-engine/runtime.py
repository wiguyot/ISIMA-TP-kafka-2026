import json
import os
import threading
import time
from typing import Any

from decision import build_timeout_error
from event_builder import build_decision_message, build_rejected_message, build_validated_message
from health import serve_http
from kafka_io import (
    get_consumer_group,
    get_consumer_settings,
    get_processing_arch,
    get_processing_delay_seconds,
    get_producer_semantics,
    get_transactional_id,
    load_existing_counts,
    produce_many_in_transaction,
    produce_and_confirm,
    wait_for_kafka,
)
from state import STATE


STATE_LOCK = threading.Lock()
WORKER_ASSIGNMENTS: dict[str, int] = {}


def get_worker_count() -> int:
    raw_value = os.getenv("SIMULPIX_DECISION_ENGINE_WORKERS", "1")
    try:
        return max(int(float(raw_value)), 1)
    except ValueError:
        return 1


def refresh_assignment_state() -> None:
    assigned_partitions = sum(WORKER_ASSIGNMENTS.values())
    STATE["assigned_partitions"] = assigned_partitions
    STATE["workers_ready"] = sum(1 for count in WORKER_ASSIGNMENTS.values() if count > 0)
    STATE["ready_for_messages"] = assigned_partitions > 0


def set_worker_assignment(worker_name: str, partition_count: int) -> None:
    with STATE_LOCK:
        WORKER_ASSIGNMENTS[worker_name] = max(partition_count, 0)
        refresh_assignment_state()


def update_counts(*, processed: int = 0, validated: int = 0, rejected: int = 0, decisions_published: int = 0) -> None:
    with STATE_LOCK:
        STATE["processed"] += processed
        STATE["validated"] += validated
        STATE["rejected"] += rejected
        STATE["decisions_published"] += decisions_published


def set_last_transaction(transaction_id: str | None) -> None:
    with STATE_LOCK:
        STATE["last_transaction_id"] = transaction_id
        STATE["last_error"] = None


def set_last_error(error: str | None) -> None:
    with STATE_LOCK:
        STATE["last_error"] = error


def worker_loop(
    worker_name: str,
    bootstrap_servers: str,
    consumer_group: str,
    processing_delay_seconds: float,
    consumer_settings: dict[str, Any],
    topic_checked: str,
    topic_validated: str,
    topic_rejected: str,
    topic_decision: str,
) -> None:
    pending_commit_count = 0
    pending_commit_record = None
    producer, consumer = wait_for_kafka(
        bootstrap_servers,
        consumer_group,
        [topic_checked],
        worker_name=worker_name,
        assignment_handler=set_worker_assignment,
    )

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
            set_last_error(str(record.error()))
            continue
        try:
            message = json.loads(record.value().decode("utf-8"))
            record_key = record.key().decode("utf-8") if record.key() else None
            transaction_id = message.get("transaction_id")
            partition_key = message.get("emitter_tax_id") or record_key or transaction_id or "unknown"
            if not consumer_settings["auto_commit"] and consumer_settings["commit_strategy"] == "before":
                consumer.commit(message=record, asynchronous=False)
            validation_errors = [str(error) for error in (message.get("validation_errors") or [])]
            errors = build_timeout_error(message) or validation_errors
            if errors:
                rejected = build_rejected_message(message, errors)
                decision = build_decision_message(message, rejected, topic_rejected)
                outputs = [
                    (topic_rejected, str(partition_key), rejected),
                    (topic_decision, str(partition_key), decision),
                ]
                if consumer_settings["semantics"] == "exactly_once_kafka":
                    produce_many_in_transaction(producer, consumer, record, outputs)
                else:
                    produce_and_confirm(producer, topic_rejected, str(partition_key), rejected)
                    produce_and_confirm(producer, topic_decision, str(partition_key), decision)
                update_counts(processed=1, rejected=1, decisions_published=1)
                print(
                    json.dumps(
                        {
                            "event": "message_rejected",
                            "worker": worker_name,
                            "transaction_id": transaction_id,
                            "errors": errors,
                        }
                    ),
                    flush=True,
                )
            else:
                validated = build_validated_message(message, str(partition_key))
                decision = build_decision_message(message, validated, topic_validated)
                outputs = [
                    (topic_validated, str(partition_key), validated),
                    (topic_decision, str(partition_key), decision),
                ]
                if consumer_settings["semantics"] == "exactly_once_kafka":
                    produce_many_in_transaction(producer, consumer, record, outputs)
                else:
                    produce_and_confirm(producer, topic_validated, str(partition_key), validated)
                    produce_and_confirm(producer, topic_decision, str(partition_key), decision)
                update_counts(processed=1, validated=1, decisions_published=1)
                print(
                    json.dumps(
                        {
                            "event": "message_validated",
                            "worker": worker_name,
                            "transaction_id": transaction_id,
                        }
                    ),
                    flush=True,
                )
            if not consumer_settings["auto_commit"] and consumer_settings["commit_strategy"] == "after":
                pending_commit_count += 1
                pending_commit_record = record
                if pending_commit_count >= int(consumer_settings["commit_batch_size"]):
                    consumer.commit(message=pending_commit_record, asynchronous=False)
                    pending_commit_count = 0
                    pending_commit_record = None
            set_last_transaction(transaction_id)
            if processing_delay_seconds > 0:
                time.sleep(processing_delay_seconds)
        except Exception as exc:
            if consumer_settings["semantics"] == "exactly_once_kafka":
                try:
                    producer.abort_transaction(10.0)
                except Exception:
                    pass
            set_worker_assignment(worker_name, 0)
            with STATE_LOCK:
                STATE["status"] = "error"
                STATE["last_error"] = f"{worker_name}: {repr(exc)}"
            print(json.dumps({"event": "decision_failed", "worker": worker_name, "error": repr(exc)}), flush=True)
            time.sleep(2)
            try:
                consumer.close()
            except Exception:
                pass
            producer, consumer = wait_for_kafka(
                bootstrap_servers,
                consumer_group,
                [topic_checked],
                worker_name=worker_name,
                assignment_handler=set_worker_assignment,
            )
            validated_count, rejected_count, decision_count = load_existing_counts(
                bootstrap_servers, topic_validated, topic_rejected, topic_decision
            )
            with STATE_LOCK:
                STATE["validated"] = validated_count
                STATE["rejected"] = rejected_count
                STATE["decisions_published"] = decision_count
                STATE["processed"] = validated_count + rejected_count
                STATE["status"] = "running"
            pending_commit_count = 0
            pending_commit_record = None


def run() -> int:
    bootstrap_servers = os.getenv("SIMULPIX_KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092")
    topic_checked = os.getenv("SIMULPIX_TOPIC_CHECKED", "simulpix.transactions.checked")
    topic_validated = os.getenv("SIMULPIX_TOPIC_VALIDATED", "simulpix.transactions.validated")
    topic_rejected = os.getenv("SIMULPIX_TOPIC_REJECTED", "simulpix.transactions.rejected")
    topic_decision = os.getenv("SIMULPIX_TOPIC_DECISION", "simulpix.transactions.decision")
    consumer_group = get_consumer_group()
    processing_delay_seconds = get_processing_delay_seconds()
    consumer_settings = get_consumer_settings()
    worker_count = get_worker_count()

    STATE["arch"] = get_processing_arch()
    STATE["producer_semantics"] = get_producer_semantics()
    STATE["consumer_semantics"] = str(consumer_settings["semantics"])
    STATE["consumer_auto_commit"] = consumer_settings["auto_commit"]
    STATE["consumer_commit_strategy"] = consumer_settings["commit_strategy"]
    STATE["consumer_max_poll_interval_ms"] = consumer_settings["max_poll_interval_ms"]
    STATE["consumer_commit_batch_size"] = consumer_settings["commit_batch_size"]
    STATE["transactional_mode"] = "exactly_once_kafka" if consumer_settings["semantics"] == "exactly_once_kafka" else "disabled"
    STATE["transactional_id"] = get_transactional_id("worker-1") if consumer_settings["semantics"] == "exactly_once_kafka" else None
    STATE["workers_configured"] = worker_count

    threading.Thread(target=serve_http, daemon=True).start()
    if STATE["arch"] != "split":
        STATE["status"] = "idle"
        while True:
            time.sleep(5)

    validated_count, rejected_count, decision_count = load_existing_counts(
        bootstrap_servers, topic_validated, topic_rejected, topic_decision
    )
    STATE["validated"] = validated_count
    STATE["rejected"] = rejected_count
    STATE["decisions_published"] = decision_count
    STATE["processed"] = validated_count + rejected_count
    STATE["status"] = "running"

    for worker_index in range(worker_count):
        worker_name = f"worker-{worker_index + 1}"
        set_worker_assignment(worker_name, 0)
        threading.Thread(
            target=worker_loop,
            args=(
                worker_name,
                bootstrap_servers,
                consumer_group,
                processing_delay_seconds,
                consumer_settings,
                topic_checked,
                topic_validated,
                topic_rejected,
                topic_decision,
            ),
            daemon=True,
        ).start()

    while True:
        time.sleep(60)

    return 0
