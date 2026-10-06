import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import psycopg
from confluent_kafka import Consumer


STATE: dict[str, Any] = {
    "status": "starting",
    "persisted": 0,
    "last_transaction_id": None,
    "last_error": None,
    "consumer_semantics": None,
    "consumer_auto_commit": None,
    "consumer_commit_strategy": None,
    "consumer_max_poll_interval_ms": None,
    "consumer_commit_batch_size": None,
}


ALLOWED_CONSUMER_SEMANTICS = {"at_most_once", "at_least_once", "custom"}


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
    return {
        "semantics": semantics,
        "auto_commit": auto_commit,
        "commit_strategy": commit_strategy,
        "max_poll_interval_ms": max_poll_interval_ms,
        "commit_batch_size": commit_batch_size,
    }


def has_transient_topic_error() -> bool:
    last_error = str(STATE.get("last_error") or "")
    return "Unknown topic or partition" in last_error or "Subscribed topic not available" in last_error


def build_kafka_client_config(group_id: str) -> dict[str, Any]:
    consumer_settings = get_consumer_settings()
    config: dict[str, Any] = {
        "bootstrap.servers": os.getenv("SIMULPIX_KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092"),
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": consumer_settings["auto_commit"],
        "max.poll.interval.ms": consumer_settings["max_poll_interval_ms"],
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


def get_consumer_group() -> str:
    return os.getenv("SIMULPIX_CONSUMER_GROUP") or os.getenv("SIMULPIX_PERSISTER_REJECTED_GROUP", "simulpix-persister-rejected-v1")


def get_persist_delay_seconds() -> float:
    value = os.getenv("SIMULPIX_PERSIST_DELAY_MS", "0")
    try:
        return max(float(value), 0.0) / 1000.0
    except ValueError:
        return 0.0


def wait_for_dependencies(database_url: str, bootstrap_servers: str, group_id: str, topic: str) -> tuple[psycopg.Connection, Consumer]:
    while True:
        try:
            connection = psycopg.connect(database_url)
            consumer = Consumer(build_kafka_client_config(group_id))
            consumer.subscribe([topic])
            return connection, consumer
        except Exception as exc:
            STATE["status"] = "waiting_dependencies"
            STATE["last_error"] = repr(exc)
            time.sleep(2)


def load_existing_persisted_count(connection: psycopg.Connection) -> int:
    with connection.cursor() as cursor:
        cursor.execute("select count(*) from rejected_transactions")
        return int(cursor.fetchone()[0])


def ensure_rejected_schema(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("alter table rejected_transactions add column if not exists source_transaction_id text")
        cursor.execute("alter table rejected_transactions add column if not exists retry_attempt integer not null default 0")
        cursor.execute("alter table rejected_transactions add column if not exists rejection_fingerprint text")
        cursor.execute("alter table rejected_transactions add column if not exists decision_deadline timestamptz")
        cursor.execute("alter table rejected_transactions add column if not exists decision_sla_seconds integer not null default 5")
        cursor.execute("alter table rejected_transactions add column if not exists decision_status text not null default 'REJECTED'")
        cursor.execute("alter table rejected_transactions add column if not exists decision_reason_code text not null default 'TECHNICAL_REJECTED'")
        cursor.execute("alter table rejected_transactions add column if not exists decision_reason_label text not null default 'Paiement Pix rejete'")
        cursor.execute("alter table rejected_transactions add column if not exists decision_origin text not null default 'SYSTEM'")
        cursor.execute("alter table rejected_transactions add column if not exists decision_latency_ms integer")
        cursor.execute("alter table rejected_transactions add column if not exists decision_within_sla boolean not null default true")
        cursor.execute("alter table rejected_transactions add column if not exists client_message text")
        cursor.execute(
            """
            update rejected_transactions
            set source_transaction_id = coalesce(source_transaction_id, transaction_id),
                retry_attempt = coalesce(retry_attempt, 0),
                rejection_fingerprint = coalesce(
                  rejection_fingerprint,
                  concat(coalesce(source_transaction_id, transaction_id, 'unknown'), ':', coalesce(retry_attempt, 0), ':', rejection_reason)
                ),
                decision_deadline = coalesce(decision_deadline, event_time + make_interval(secs => coalesce(decision_sla_seconds, 5))),
                decision_sla_seconds = coalesce(decision_sla_seconds, 5),
                decision_status = coalesce(decision_status, 'REJECTED'),
                decision_reason_code = coalesce(decision_reason_code, 'TECHNICAL_REJECTED'),
                decision_reason_label = coalesce(decision_reason_label, 'Paiement Pix rejete'),
                decision_origin = coalesce(decision_origin, 'SYSTEM'),
                decision_latency_ms = coalesce(
                  decision_latency_ms,
                  case
                    when rejected_at is not null and event_time is not null then greatest((extract(epoch from (rejected_at - event_time)) * 1000)::integer, 0)
                    else null
                  end
                ),
                decision_within_sla = coalesce(
                  decision_within_sla,
                  case
                    when rejected_at is not null and decision_deadline is not null then rejected_at <= decision_deadline
                    else true
                  end
                ),
                client_message = coalesce(client_message, decision_reason_label, 'Paiement Pix rejete')
            where source_transaction_id is null
               or rejection_fingerprint is null
               or decision_deadline is null
               or client_message is null
            """
        )
        cursor.execute("alter table rejected_transactions alter column rejection_fingerprint set not null")
        cursor.execute("alter table rejected_transactions alter column decision_deadline set not null")
        cursor.execute(
            """
            create unique index if not exists rejected_transactions_rejection_fingerprint_idx
            on rejected_transactions (rejection_fingerprint)
            """
        )
    connection.commit()


def insert_rejected(connection: psycopg.Connection, message: dict[str, Any]) -> int:
    source_transaction_id = message.get("source_transaction_id") or message.get("transaction_id")
    retry_attempt = int(message.get("retry_attempt", 0) or 0)
    rejection_reason = message.get("rejection_reason", "unknown")
    rejection_fingerprint = message.get("rejection_fingerprint") or f"{source_transaction_id}:{retry_attempt}:{rejection_reason}"
    with connection.cursor() as cursor:
        cursor.execute(
            """
            insert into rejected_transactions (
              transaction_id,
              source_transaction_id,
              retry_attempt,
              event_time,
              scenario_type,
              producer_id,
              trace_id,
              rejection_type,
              rejection_reason,
              rejection_fingerprint,
              decision_deadline,
              decision_sla_seconds,
              decision_status,
              decision_reason_code,
              decision_reason_label,
              decision_origin,
              decision_latency_ms,
              decision_within_sla,
              client_message,
              rejected_at,
              processing_node,
              original_payload
            ) values (
              %(transaction_id)s,
              %(source_transaction_id)s,
              %(retry_attempt)s,
              %(event_time)s,
              %(scenario_type)s,
              %(producer_id)s,
              %(trace_id)s,
              %(rejection_type)s,
              %(rejection_reason)s,
              %(rejection_fingerprint)s,
              %(decision_deadline)s,
              %(decision_sla_seconds)s,
              %(decision_status)s,
              %(decision_reason_code)s,
              %(decision_reason_label)s,
              %(decision_origin)s,
              %(decision_latency_ms)s,
              %(decision_within_sla)s,
              %(client_message)s,
              %(rejected_at)s,
              %(processing_node)s,
              %(original_payload)s
            )
            on conflict (rejection_fingerprint) do update set
              transaction_id = excluded.transaction_id,
              source_transaction_id = excluded.source_transaction_id,
              retry_attempt = excluded.retry_attempt,
              event_time = excluded.event_time,
              scenario_type = excluded.scenario_type,
              producer_id = excluded.producer_id,
              trace_id = excluded.trace_id,
              rejection_type = excluded.rejection_type,
              rejection_reason = excluded.rejection_reason,
              decision_deadline = excluded.decision_deadline,
              decision_sla_seconds = excluded.decision_sla_seconds,
              decision_status = excluded.decision_status,
              decision_reason_code = excluded.decision_reason_code,
              decision_reason_label = excluded.decision_reason_label,
              decision_origin = excluded.decision_origin,
              decision_latency_ms = excluded.decision_latency_ms,
              decision_within_sla = excluded.decision_within_sla,
              client_message = excluded.client_message,
              rejected_at = excluded.rejected_at,
              processing_node = excluded.processing_node,
              original_payload = excluded.original_payload
            """,
            {
                **message,
                "source_transaction_id": source_transaction_id,
                "retry_attempt": retry_attempt,
                "rejection_fingerprint": rejection_fingerprint,
                "original_payload": json.dumps(message.get("original_payload", {})),
            },
        )
    connection.commit()
    return load_existing_persisted_count(connection)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(
            {
                "status": STATE["status"],
                "service": os.getenv("SIMULPIX_SERVICE_NAME", "persister-rejected"),
                "consumer_group": get_consumer_group(),
                "persisted": STATE["persisted"],
                "persist_delay_ms": int(get_persist_delay_seconds() * 1000),
                "last_transaction_id": STATE["last_transaction_id"],
                "last_error": STATE["last_error"],
                "consumer_semantics": STATE["consumer_semantics"],
                "consumer_auto_commit": STATE["consumer_auto_commit"],
                "consumer_commit_strategy": STATE["consumer_commit_strategy"],
                "consumer_max_poll_interval_ms": STATE["consumer_max_poll_interval_ms"],
                "consumer_commit_batch_size": STATE["consumer_commit_batch_size"],
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        return


def serve_http() -> None:
    port = int(os.getenv("SIMULPIX_PORT", "8085"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()


def main() -> int:
    database_url = os.getenv("SIMULPIX_DATABASE_URL", "postgresql://simulpix:simulpix@postgres:5432/simulpix")
    bootstrap_servers = os.getenv("SIMULPIX_KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092")
    topic = os.getenv("SIMULPIX_TOPIC_REJECTED", "simulpix.transactions.rejected")
    group_id = get_consumer_group()
    persist_delay_seconds = get_persist_delay_seconds()

    threading.Thread(target=serve_http, daemon=True).start()
    connection, consumer = wait_for_dependencies(database_url, bootstrap_servers, group_id, topic)
    ensure_rejected_schema(connection)
    STATE["persisted"] = load_existing_persisted_count(connection)
    STATE["status"] = "running"
    consumer_settings = get_consumer_settings()
    consumer_semantics = str(consumer_settings["semantics"])
    STATE["consumer_semantics"] = consumer_semantics
    STATE["consumer_auto_commit"] = consumer_settings["auto_commit"]
    STATE["consumer_commit_strategy"] = consumer_settings["commit_strategy"]
    STATE["consumer_max_poll_interval_ms"] = consumer_settings["max_poll_interval_ms"]
    STATE["consumer_commit_batch_size"] = consumer_settings["commit_batch_size"]
    pending_commit_count = 0
    pending_commit_record = None

    while True:
        record = consumer.poll(1.0)
        if record is None:
            if not consumer_settings["auto_commit"] and consumer_settings["commit_strategy"] == "after" and pending_commit_count > 0 and pending_commit_record is not None:
                consumer.commit(message=pending_commit_record, asynchronous=False)
                pending_commit_count = 0
                pending_commit_record = None
            if has_transient_topic_error():
                STATE["last_error"] = None
            continue
        if record.error() is not None:
            STATE["last_error"] = str(record.error())
            continue
        try:
            message = json.loads(record.value().decode("utf-8"))
            if not consumer_settings["auto_commit"] and consumer_settings["commit_strategy"] == "before":
                consumer.commit(message=record, asynchronous=False)
            STATE["persisted"] = insert_rejected(connection, message)
            if not consumer_settings["auto_commit"] and consumer_settings["commit_strategy"] == "after":
                pending_commit_count += 1
                pending_commit_record = record
                if pending_commit_count >= int(consumer_settings["commit_batch_size"]):
                    consumer.commit(message=pending_commit_record, asynchronous=False)
                    pending_commit_count = 0
                    pending_commit_record = None
            STATE["last_transaction_id"] = message.get("transaction_id")
            STATE["last_error"] = None
            if persist_delay_seconds > 0:
                time.sleep(persist_delay_seconds)
            print(json.dumps({"event": "persisted_rejected", "transaction_id": message.get("transaction_id")}), flush=True)
        except Exception as exc:
            STATE["status"] = "error"
            STATE["last_error"] = repr(exc)
            print(json.dumps({"event": "persist_failed", "error": repr(exc)}), flush=True)
            time.sleep(2)
            try:
                consumer.close()
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass
            connection, consumer = wait_for_dependencies(database_url, bootstrap_servers, group_id, topic)
            ensure_rejected_schema(connection)
            STATE["persisted"] = load_existing_persisted_count(connection)
            STATE["status"] = "running"
            consumer_settings = get_consumer_settings()
            consumer_semantics = str(consumer_settings["semantics"])
            STATE["consumer_semantics"] = consumer_semantics
            STATE["consumer_auto_commit"] = consumer_settings["auto_commit"]
            STATE["consumer_commit_strategy"] = consumer_settings["commit_strategy"]
            STATE["consumer_max_poll_interval_ms"] = consumer_settings["max_poll_interval_ms"]
            STATE["consumer_commit_batch_size"] = consumer_settings["commit_batch_size"]
            pending_commit_count = 0
            pending_commit_record = None


if __name__ == "__main__":
    sys.exit(main())
