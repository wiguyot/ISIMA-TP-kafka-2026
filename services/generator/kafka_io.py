import json
import os
import time
from urllib.error import URLError
from urllib.request import urlopen
from typing import Any

from confluent_kafka import Consumer, Producer, TopicPartition

from state import STATE


ALLOWED_PRODUCER_SEMANTICS = {"at_most_once", "at_least_once", "exactly_once", "custom"}


def parse_non_negative_int(raw_value: str | None, default: int) -> int:
    value = str(raw_value or "").strip()
    if value == "":
        return default
    try:
        return max(int(float(value)), 0)
    except ValueError:
        return default


def get_producer_semantics() -> str:
    value = str(os.getenv("SIMULPIX_KAFKA_PRODUCER_SEMANTICS", "at_least_once") or "at_least_once").strip().lower()
    return value if value in ALLOWED_PRODUCER_SEMANTICS else "at_least_once"


def build_base_client_config(client_id: str) -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": os.getenv("SIMULPIX_KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092"),
        "client.id": client_id,
    }
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


def build_kafka_config(client_id: str) -> dict[str, Any]:
    raw_acks = str(os.getenv("SIMULPIX_KAFKA_ACKS", "all") or "all").strip() or "all"
    raw_retries = parse_non_negative_int(os.getenv("SIMULPIX_KAFKA_RETRIES", "0"), 0)
    request_timeout_ms = parse_non_negative_int(os.getenv("SIMULPIX_KAFKA_REQUEST_TIMEOUT_MS"), 0)
    message_timeout_ms = parse_non_negative_int(os.getenv("SIMULPIX_KAFKA_MESSAGE_TIMEOUT_MS"), 0)
    retry_backoff_ms = parse_non_negative_int(os.getenv("SIMULPIX_KAFKA_RETRY_BACKOFF_MS"), 0)
    producer_semantics = get_producer_semantics()
    acks = raw_acks
    retries = raw_retries
    config = build_base_client_config(client_id)
    if producer_semantics == "at_most_once":
        acks = "0"
        retries = 0
        config["enable.idempotence"] = False
    elif producer_semantics == "at_least_once":
        acks = "all"
        retries = max(raw_retries, 1)
        config["enable.idempotence"] = False
    elif producer_semantics == "exactly_once":
        acks = "all"
        retries = max(raw_retries, 1)
        config["enable.idempotence"] = True
    config["acks"] = acks
    config["retries"] = retries
    if request_timeout_ms > 0:
        config["request.timeout.ms"] = request_timeout_ms
    if message_timeout_ms > 0:
        config["message.timeout.ms"] = message_timeout_ms
    if retry_backoff_ms > 0:
        config["retry.backoff.ms"] = retry_backoff_ms
    return config


def build_consumer_kafka_config(group_id: str) -> dict[str, Any]:
    config = build_base_client_config(os.getenv("SIMULPIX_SERVICE_NAME", "generator"))
    config["group.id"] = group_id
    config["enable.auto.commit"] = False
    return config


def wait_for_producer(bootstrap_servers: str) -> Producer:
    while True:
        try:
            producer = Producer(build_kafka_config(os.getenv("SIMULPIX_SERVICE_NAME", "generator")))
            return producer
        except Exception as exc:
            STATE["status"] = "waiting_kafka"
            STATE["last_error"] = repr(exc)
            time.sleep(2)


def read_topic_end_offsets(topic: str) -> int:
    group_id = f"{os.getenv('SIMULPIX_SERVICE_NAME', 'generator')}-restore"
    consumer = Consumer(build_consumer_kafka_config(group_id))
    try:
        metadata = consumer.list_topics(topic=topic, timeout=5)
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            return 0
        total = 0
        for partition_id in sorted(topic_meta.partitions.keys()):
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=5)
            total += high
        return total
    except Exception:
        return 0
    finally:
        consumer.close()


def topic_is_ready(topic: str) -> bool:
    group_id = f"{os.getenv('SIMULPIX_SERVICE_NAME', 'generator')}-readiness"
    consumer = Consumer(build_consumer_kafka_config(group_id))
    try:
        metadata = consumer.list_topics(topic=topic, timeout=5)
        topic_meta = metadata.topics.get(topic)
        return topic_meta is not None and topic_meta.error is None and len(topic_meta.partitions) > 0
    except Exception:
        return False
    finally:
        consumer.close()


def fetch_json(url: str, timeout_seconds: float = 2.0) -> dict[str, Any] | None:
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            payload = response.read().decode("utf-8")
        data = json.loads(payload)
        return data if isinstance(data, dict) else None
    except (OSError, URLError, ValueError, json.JSONDecodeError):
        return None


def get_processing_arch() -> str:
    return "split"


def downstream_service_is_ready() -> bool:
    downstream_health_url = os.getenv("SIMULPIX_PIX_VALIDATOR_HEALTH_URL", "http://pix-validator:8086/health")
    payload = fetch_json(downstream_health_url)
    return (
        payload is not None
        and str(payload.get("status", "")).lower() == "running"
        and bool(payload.get("ready_for_messages"))
        and int(payload.get("assigned_partitions", 0) or 0) > 0
    )


def wait_for_runtime_dependencies(topic_raw: str) -> None:
    while True:
        if topic_is_ready(topic_raw) and downstream_service_is_ready():
            STATE["last_error"] = None
            return
        STATE["status"] = "waiting_dependencies"
        STATE["last_error"] = "waiting_for_kafka_topics_and_downstream_consumer"
        time.sleep(1)


def make_delivery_callback(transaction_id: str):
    def _delivery_callback(err: Any, msg: Any) -> None:
        if err is not None:
            STATE["messages_delivery_failed"] += 1
            STATE["last_delivery_error"] = repr(err)
            STATE["last_error"] = repr(err)
            print(
                json.dumps(
                    {
                        "event": "message_delivery_failed",
                        "transaction_id": transaction_id,
                        "error": repr(err),
                    }
                ),
                flush=True,
            )
            return
        STATE["messages_delivered"] += 1
        STATE["messages_sent"] = STATE["messages_delivered"]
        STATE["last_delivery_error"] = None
        print(
            json.dumps(
                {
                    "event": "message_delivered",
                    "transaction_id": transaction_id,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                }
            ),
            flush=True,
        )

    return _delivery_callback
