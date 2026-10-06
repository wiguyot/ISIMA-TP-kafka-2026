import json
import os
import threading
import time
from itertools import count
from typing import Any

from health import register_control, serve_http
from kafka_io import (
    build_kafka_config,
    get_producer_semantics,
    make_delivery_callback,
    read_topic_end_offsets,
    wait_for_producer,
    wait_for_runtime_dependencies,
)
from message_factory import build_message, get_decision_sla_seconds, load_reference_clients
from state import STATE


PUBLISH_LOCK = threading.Lock()
PUBLISH_CONTEXT: dict[str, Any] = {
    "clients": None,
    "producer": None,
    "topic_raw": None,
    "bootstrap_servers": None,
    "counter": None,
    "ambiguous_ack_duplicate_percent": 0,
}


def parse_percent(raw_value: str | None) -> int:
    try:
        return min(max(int(float(str(raw_value or "0").strip() or "0")), 0), 100)
    except ValueError:
        return 0


def should_inject_ambiguous_ack_duplicate(index: int, percent: int) -> bool:
    if percent <= 0:
        return False
    if percent >= 100:
        return True
    interval = max(int(round(100 / percent)), 1)
    return index % interval == 0


def initialize_publisher() -> None:
    reference_data = os.getenv("SIMULPIX_REFERENCE_DATA", "/data/reference-clients.json")
    bootstrap_servers = os.getenv("SIMULPIX_KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092")
    topic_raw = os.getenv("SIMULPIX_TOPIC_RAW", "simulpix.transactions.raw")
    producer_config = build_kafka_config(os.getenv("SIMULPIX_SERVICE_NAME", "generator"))
    producer_semantics = get_producer_semantics()
    ambiguous_ack_duplicate_percent = parse_percent(os.getenv("SIMULPIX_KAFKA_AMBIGUOUS_ACK_DUPLICATE_PERCENT"))
    if producer_semantics != "at_least_once":
        ambiguous_ack_duplicate_percent = 0

    clients = load_reference_clients(reference_data)
    producer = wait_for_producer(bootstrap_servers)
    wait_for_runtime_dependencies(topic_raw)
    initial_offsets = read_topic_end_offsets(topic_raw)

    PUBLISH_CONTEXT["clients"] = clients
    PUBLISH_CONTEXT["producer"] = producer
    PUBLISH_CONTEXT["topic_raw"] = topic_raw
    PUBLISH_CONTEXT["bootstrap_servers"] = bootstrap_servers
    PUBLISH_CONTEXT["counter"] = count(initial_offsets + 1)
    PUBLISH_CONTEXT["ambiguous_ack_duplicate_percent"] = ambiguous_ack_duplicate_percent

    STATE["status"] = "running"
    STATE["ready_for_commands"] = True
    STATE["messages_sent"] = initial_offsets
    STATE["messages_enqueued"] = initial_offsets
    STATE["messages_delivered"] = initial_offsets
    STATE["messages_delivery_failed"] = 0
    STATE["producer_semantics"] = producer_semantics
    STATE["producer_acks"] = str(producer_config.get("acks", "all"))
    STATE["producer_retries"] = int(producer_config.get("retries", 0) or 0)
    STATE["ambiguous_ack_duplicate_percent"] = ambiguous_ack_duplicate_percent
    STATE["ambiguous_ack_duplicates_enqueued"] = 0
    STATE["decision_sla_seconds"] = get_decision_sla_seconds()
    STATE["traffic_model"] = str(os.getenv("SIMULPIX_TRAFFIC_MODEL", "bursty") or "bursty")
    STATE["scenario_type"] = "idle"
    STATE["current_phase"] = "ready"
    STATE["phase_progress"] = 0
    STATE["phase_total"] = 0
    STATE["last_requested_batch_count"] = 0
    STATE["last_error"] = None


def publisher_is_ready() -> bool:
    return bool(
        STATE["ready_for_commands"]
        and PUBLISH_CONTEXT["clients"] is not None
        and PUBLISH_CONTEXT["producer"] is not None
        and PUBLISH_CONTEXT["counter"] is not None
    )


def publish_batch(command: dict[str, Any]) -> dict[str, Any]:
    if not publisher_is_ready():
        raise RuntimeError("generator_not_ready_for_commands")

    requested_batch_count = max(int(command.get("batch_count", 0) or 0), 0)
    scenario = str(command.get("scenario") or "nominal")
    scenario_type = str(command.get("scenario_type") or scenario)
    phase_name = str(command.get("phase_name") or "default")
    phase_progress = max(int(command.get("phase_progress", 0) or 0), 0)
    phase_total = max(int(command.get("phase_total", 0) or 0), 0)

    if requested_batch_count <= 0:
        return {
            "batch_count": 0,
            "messages_sent": STATE["messages_sent"],
            "messages_enqueued": STATE["messages_enqueued"],
            "messages_delivered": STATE["messages_delivered"],
        }

    with PUBLISH_LOCK:
        producer = PUBLISH_CONTEXT["producer"]
        clients = PUBLISH_CONTEXT["clients"]
        counter = PUBLISH_CONTEXT["counter"]
        topic_raw = str(PUBLISH_CONTEXT["topic_raw"])
        bootstrap_servers = str(PUBLISH_CONTEXT["bootstrap_servers"])
        ambiguous_ack_duplicate_percent = int(PUBLISH_CONTEXT["ambiguous_ack_duplicate_percent"] or 0)

        STATE["scenario_type"] = scenario
        STATE["current_phase"] = phase_name
        STATE["phase_progress"] = phase_progress
        STATE["phase_total"] = phase_total
        STATE["last_requested_batch_count"] = requested_batch_count
        published = 0

        for _ in range(requested_batch_count):
            try:
                index = next(counter)
                message = build_message(index, clients, scenario)
                if scenario == "football_match_peak":
                    message["scenario_type"] = scenario_type
                    message["event_context"] = "brazilian_football_match"
                    message["traffic_phase"] = phase_name
                key = message.get("emitter_tax_id", f"key-{index}")
                key_bytes = key.encode("utf-8")
                value_bytes = json.dumps(message).encode("utf-8")
                producer.produce(
                    topic_raw,
                    key=key_bytes,
                    value=value_bytes,
                    on_delivery=make_delivery_callback(message["transaction_id"]),
                )
                if should_inject_ambiguous_ack_duplicate(index, ambiguous_ack_duplicate_percent):
                    producer.produce(topic_raw, key=key_bytes, value=value_bytes)
                    STATE["ambiguous_ack_duplicates_enqueued"] += 1
                    print(
                        json.dumps(
                            {
                                "event": "ambiguous_ack_retry_enqueued",
                                "transaction_id": message["transaction_id"],
                                "duplicate_percent": ambiguous_ack_duplicate_percent,
                            }
                        ),
                        flush=True,
                    )
                producer.poll(0)
                STATE["messages_enqueued"] += 1
                STATE["last_message_id"] = message["transaction_id"]
                STATE["last_error"] = None
                published += 1
            except Exception as exc:
                STATE["status"] = "error"
                STATE["ready_for_commands"] = False
                STATE["last_error"] = repr(exc)
                print(json.dumps({"event": "send_failed", "error": repr(exc)}), flush=True)
                time.sleep(2)
                producer = wait_for_producer(bootstrap_servers)
                PUBLISH_CONTEXT["producer"] = producer
                delivered_offsets = read_topic_end_offsets(topic_raw)
                STATE["messages_sent"] = delivered_offsets
                STATE["messages_delivered"] = delivered_offsets
                STATE["ready_for_commands"] = True
                STATE["status"] = "running"
                raise

        producer.flush(10)
        print(
            json.dumps(
                {
                    "event": "emit_batch_completed",
                    "scenario": scenario,
                    "scenario_type": scenario_type,
                    "phase": phase_name,
                    "batch_count": published,
                    "phase_progress": phase_progress,
                    "phase_total": phase_total,
                }
            ),
            flush=True,
        )
        return {
            "batch_count": published,
            "messages_sent": STATE["messages_sent"],
            "messages_enqueued": STATE["messages_enqueued"],
            "messages_delivered": STATE["messages_delivered"],
        }


def run() -> int:
    threading.Thread(target=serve_http, daemon=True).start()
    register_control(publish_batch, publisher_is_ready)
    initialize_publisher()
    while True:
        time.sleep(60)
