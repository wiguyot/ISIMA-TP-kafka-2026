import json
import os
import time
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, Producer, TopicPartition

from state import STATE


ALLOWED_PRODUCER_SEMANTICS = {"at_most_once", "at_least_once", "exactly_once", "custom"}
ALLOWED_CONSUMER_SEMANTICS = {"at_most_once", "at_least_once", "exactly_once_kafka", "custom"}


def get_processing_arch() -> str:
    return "split"


def get_producer_semantics() -> str:
    value = str(os.getenv("SIMULPIX_KAFKA_PRODUCER_SEMANTICS", "at_least_once") or "at_least_once").strip().lower()
    return value if value in ALLOWED_PRODUCER_SEMANTICS else "at_least_once"


def get_consumer_semantics() -> str:
    value = str(os.getenv("SIMULPIX_KAFKA_CONSUMER_SEMANTICS", "at_least_once") or "at_least_once").strip().lower()
    return value if value in ALLOWED_CONSUMER_SEMANTICS else "at_least_once"


def get_consumer_settings() -> dict[str, Any]:
    semantics = get_consumer_semantics()
    commit_strategy = str(os.getenv("SIMULPIX_KAFKA_CONSUMER_COMMIT_STRATEGY", "after") or "after").strip().lower()
    if commit_strategy not in {"before", "after"}:
        commit_strategy = "after"
    auto_commit = str(os.getenv("SIMULPIX_KAFKA_CONSUMER_AUTO_COMMIT", "off") or "off").strip().lower() in {"on", "1", "true", "yes"}
    try:
        max_poll_interval_ms = max(int(float(os.getenv("SIMULPIX_KAFKA_MAX_POLL_INTERVAL_MS", "300000") or "300000")), 1000)
    except ValueError:
        max_poll_interval_ms = 300000
    try:
        commit_batch_size = max(int(float(os.getenv("SIMULPIX_KAFKA_COMMIT_BATCH_SIZE", "1") or "1")), 1)
    except ValueError:
        commit_batch_size = 1
    if semantics == "at_most_once":
        auto_commit = False
        commit_strategy = "before"
        commit_batch_size = 1
    elif semantics == "at_least_once":
        auto_commit = False
        commit_strategy = "after"
        commit_batch_size = 1
    elif semantics == "exactly_once_kafka":
        auto_commit = False
        commit_strategy = "transaction"
        commit_batch_size = 1
    return {
        "semantics": semantics,
        "auto_commit": auto_commit,
        "commit_strategy": commit_strategy,
        "max_poll_interval_ms": max_poll_interval_ms,
        "commit_batch_size": commit_batch_size,
    }


def build_kafka_client_config(group_id: str | None = None, client_id: str | None = None) -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": os.getenv("SIMULPIX_KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092"),
    }
    if group_id:
        config["group.id"] = group_id
        config["auto.offset.reset"] = "earliest"
        consumer_settings = get_consumer_settings()
        config["enable.auto.commit"] = consumer_settings["auto_commit"]
        config["max.poll.interval.ms"] = consumer_settings["max_poll_interval_ms"]
        if consumer_settings["semantics"] == "exactly_once_kafka":
            config["isolation.level"] = "read_committed"
    if client_id:
        config["client.id"] = client_id
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


def build_kafka_producer_config(client_id: str | None = None, worker_suffix: str | None = None) -> dict[str, Any]:
    raw_acks = str(os.getenv("SIMULPIX_KAFKA_ACKS", "all") or "all").strip() or "all"
    try:
        raw_retries = max(int(float(os.getenv("SIMULPIX_KAFKA_RETRIES", "0") or "0")), 0)
    except ValueError:
        raw_retries = 0
    producer_semantics = get_producer_semantics()
    consumer_semantics = get_consumer_semantics()
    config = build_kafka_client_config(client_id=client_id)
    if producer_semantics == "at_most_once":
        config["acks"] = "0"
        config["retries"] = 0
        config["enable.idempotence"] = False
    elif producer_semantics == "at_least_once":
        config["acks"] = "all"
        config["retries"] = max(raw_retries, 1)
        config["enable.idempotence"] = False
    elif producer_semantics == "exactly_once":
        config["acks"] = "all"
        config["retries"] = max(raw_retries, 1)
        config["enable.idempotence"] = True
    else:
        config["acks"] = raw_acks
        config["retries"] = raw_retries
    if producer_semantics == "exactly_once" or consumer_semantics == "exactly_once_kafka":
        config["acks"] = "all"
        config["retries"] = max(raw_retries, 1)
        config["enable.idempotence"] = True
    if consumer_semantics == "exactly_once_kafka":
        config["transactional.id"] = get_transactional_id(worker_suffix)
    return config


def get_transactional_id(worker_suffix: str | None = None) -> str:
    base = str(
        os.getenv("SIMULPIX_KAFKA_TRANSACTIONAL_ID")
        or os.getenv("SIMULPIX_PIX_VALIDATOR_TRANSACTIONAL_ID", "pix-validator-tx-v1")
    ).strip()
    if not worker_suffix:
        return base
    return f"{base}-{worker_suffix}"


def get_consumer_group() -> str:
    return os.getenv("SIMULPIX_CONSUMER_GROUP") or os.getenv("SIMULPIX_PIX_VALIDATOR_GROUP", "simulpix-validator-v1")


def wait_for_kafka(
    bootstrap_servers: str,
    group_id: str,
    topics: list[str],
    worker_name: str = "worker-1",
    assignment_handler: Callable[[str, int], None] | None = None,
) -> tuple[Producer, Consumer]:
    while True:
        try:
            client_id = f"{os.getenv('SIMULPIX_SERVICE_NAME', 'pix-validator')}-{worker_name}"
            producer = Producer(
                build_kafka_producer_config(
                    client_id=client_id,
                    worker_suffix=worker_name,
                )
            )
            if get_consumer_semantics() == "exactly_once_kafka":
                producer.init_transactions(10.0)
            consumer = Consumer(build_kafka_client_config(group_id=group_id, client_id=client_id))
            if assignment_handler is not None:
                assignment_handler(worker_name, 0)

            def on_assign(local_consumer: Consumer, partitions: list[TopicPartition]) -> None:
                if assignment_handler is not None:
                    assignment_handler(worker_name, len(partitions))
                local_consumer.assign(partitions)

            def on_revoke(local_consumer: Consumer, partitions: list[TopicPartition]) -> None:
                if assignment_handler is not None:
                    assignment_handler(worker_name, 0)
                local_consumer.unassign()

            consumer.subscribe(topics, on_assign=on_assign, on_revoke=on_revoke)
            return producer, consumer
        except Exception as exc:
            STATE["status"] = "waiting_kafka"
            STATE["last_error"] = repr(exc)
            time.sleep(2)


def produce_and_confirm(producer: Producer, topic: str, key: str, value: dict[str, Any]) -> None:
    producer.produce(topic, key=key.encode("utf-8"), value=json.dumps(value).encode("utf-8"))
    pending = producer.flush(10.0)
    if pending != 0:
        raise RuntimeError(f"failed_to_publish_to_{topic}")


def produce_in_transaction(
    producer: Producer,
    consumer: Consumer,
    topic: str,
    key: str,
    value: dict[str, Any],
    record: Any,
    consumer_group_metadata: Any,
) -> None:
    producer.begin_transaction()
    producer.produce(topic, key=key.encode("utf-8"), value=json.dumps(value).encode("utf-8"))
    pending = producer.flush(10.0)
    if pending != 0:
        raise RuntimeError(f"failed_to_publish_to_{topic}")
    offset = record.offset()
    if offset is None:
        raise RuntimeError("missing_record_offset")
    topic_partition = TopicPartition(record.topic(), record.partition(), offset + 1)
    producer.send_offsets_to_transaction([topic_partition], consumer_group_metadata, 10.0)
    producer.commit_transaction(10.0)
