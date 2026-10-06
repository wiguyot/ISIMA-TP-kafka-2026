import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

from health import serve_http
from kafka_io import (
    get_consumer_group,
    get_consumer_settings,
    get_processing_arch,
    get_producer_semantics,
    get_transactional_id,
    produce_and_confirm,
    produce_in_transaction,
    wait_for_kafka,
)
from state import STATE
from validation import load_reference_clients, validate_message


STATE_LOCK = threading.Lock()
WORKER_ASSIGNMENTS: dict[str, int] = {}


def build_checked_message(message: dict[str, object], errors: list[str]) -> dict[str, object]:
    checked = dict(message)
    checked["validation_stage"] = "CHECKED"
    checked["validated_input_at"] = datetime.now(timezone.utc).isoformat()
    checked["validator_node"] = os.getenv("SIMULPIX_SERVICE_NAME", "pix-validator")
    checked["validation_ok"] = len(errors) == 0
    checked["validation_errors"] = errors
    return checked


def get_worker_count() -> int:
    raw_value = os.getenv("SIMULPIX_VALIDATOR_WORKERS", "1")
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


def update_counts(*, received: int = 0, checked: int = 0, validation_failed: int = 0) -> None:
    with STATE_LOCK:
        STATE["received"] += received
        STATE["checked"] += checked
        STATE["validation_failed"] += validation_failed


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
    consumer_settings: dict[str, Any],
    topic_raw: str,
    topic_retry: str,
    topic_checked: str,
    tax_to_pix: dict[str, str],
) -> None:
    pending_commit_count = 0
    pending_commit_record = None
    producer, consumer = wait_for_kafka(
        bootstrap_servers,
        consumer_group,
        [topic_raw, topic_retry],
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
            errors = validate_message(message, tax_to_pix)
            checked = build_checked_message(message, errors)
            if consumer_settings["semantics"] == "exactly_once_kafka":
                produce_in_transaction(
                    producer,
                    consumer,
                    topic_checked,
                    str(partition_key),
                    checked,
                    record,
                    consumer.consumer_group_metadata(),
                )
            else:
                produce_and_confirm(producer, topic_checked, str(partition_key), checked)
            if not consumer_settings["auto_commit"] and consumer_settings["commit_strategy"] == "after":
                pending_commit_count += 1
                pending_commit_record = record
                if pending_commit_count >= int(consumer_settings["commit_batch_size"]):
                    consumer.commit(message=pending_commit_record, asynchronous=False)
                    pending_commit_count = 0
                    pending_commit_record = None
            update_counts(received=1, checked=1, validation_failed=1 if errors else 0)
            set_last_transaction(transaction_id)
            print(
                json.dumps(
                    {
                        "event": "message_checked",
                        "worker": worker_name,
                        "transaction_id": transaction_id,
                        "validation_ok": len(errors) == 0,
                        "validation_errors": errors,
                    }
                ),
                flush=True,
            )
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
            print(json.dumps({"event": "validation_failed", "worker": worker_name, "error": repr(exc)}), flush=True)
            time.sleep(2)
            try:
                consumer.close()
            except Exception:
                pass
            producer, consumer = wait_for_kafka(
                bootstrap_servers,
                consumer_group,
                [topic_raw, topic_retry],
                worker_name=worker_name,
                assignment_handler=set_worker_assignment,
            )
            with STATE_LOCK:
                STATE["status"] = "running"
            pending_commit_count = 0
            pending_commit_record = None


def run() -> int:
    reference_data = os.getenv("SIMULPIX_REFERENCE_DATA", "/data/reference-clients.json")
    bootstrap_servers = os.getenv("SIMULPIX_KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092")
    topic_raw = os.getenv("SIMULPIX_TOPIC_RAW", "simulpix.transactions.raw")
    topic_retry = os.getenv("SIMULPIX_TOPIC_RETRY", "simulpix.transactions.retry")
    topic_checked = os.getenv("SIMULPIX_TOPIC_CHECKED", "simulpix.transactions.checked")
    consumer_group = get_consumer_group()
    consumer_settings = get_consumer_settings()
    worker_count = get_worker_count()

    STATE["arch"] = get_processing_arch()
    STATE["producer_semantics"] = get_producer_semantics()
    STATE["consumer_semantics"] = str(consumer_settings["semantics"])
    STATE["transactional_mode"] = "exactly_once_kafka" if consumer_settings["semantics"] == "exactly_once_kafka" else "disabled"
    STATE["transactional_id"] = get_transactional_id("worker-1") if consumer_settings["semantics"] == "exactly_once_kafka" else None
    STATE["consumer_auto_commit"] = consumer_settings["auto_commit"]
    STATE["consumer_commit_strategy"] = consumer_settings["commit_strategy"]
    STATE["consumer_max_poll_interval_ms"] = consumer_settings["max_poll_interval_ms"]
    STATE["consumer_commit_batch_size"] = consumer_settings["commit_batch_size"]
    STATE["workers_configured"] = worker_count

    threading.Thread(target=serve_http, daemon=True).start()
    if STATE["arch"] != "split":
        STATE["status"] = "idle"
        while True:
            time.sleep(5)

    tax_to_pix = load_reference_clients(reference_data)
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
                consumer_settings,
                topic_raw,
                topic_retry,
                topic_checked,
                tax_to_pix,
            ),
            daemon=True,
        ).start()

    while True:
        time.sleep(60)

    return 0
