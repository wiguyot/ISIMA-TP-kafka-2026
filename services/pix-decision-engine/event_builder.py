import os
from datetime import datetime, timezone
from typing import Any

from decision import build_client_decision, map_rejection_reason


def build_validated_message(message: dict[str, Any], partition_key: str) -> dict[str, Any]:
    validated = dict(message)
    source_transaction_id = message.get("source_transaction_id") or message.get("transaction_id")
    retry_attempt = int(message.get("retry_attempt", 0) or 0)
    decision_time = datetime.now(timezone.utc)
    validated["status"] = "ENRICHED"
    validated["validation_status"] = "VALID"
    validated["validated_at"] = decision_time.isoformat()
    validated["enriched_at"] = decision_time.isoformat()
    validated["partition_key"] = partition_key
    validated["processing_node"] = os.getenv("SIMULPIX_SERVICE_NAME", "pix-decision-engine")
    validated["source_transaction_id"] = source_transaction_id
    validated["retry_attempt"] = retry_attempt
    validated.update(
        build_client_decision(
            decision_status="ACCEPTED",
            decision_reason_code="ACCEPTED",
            decision_reason_label="Paiement Pix accepté",
            decision_origin="BANK",
            decision_time=decision_time,
            message=message,
        )
    )
    return validated


def build_rejected_message(message: dict[str, Any], errors: list[str]) -> dict[str, Any]:
    source_transaction_id = message.get("source_transaction_id") or message.get("transaction_id")
    retry_attempt = int(message.get("retry_attempt", 0) or 0)
    decision_time = datetime.now(timezone.utc)
    rejection_reason = ",".join(errors)
    rejection_fingerprint = f"{source_transaction_id}:{retry_attempt}:{rejection_reason}"
    rejection_type, decision_reason_code, decision_reason_label, decision_origin = map_rejection_reason(errors)
    return {
        "transaction_id": message.get("transaction_id"),
        "source_transaction_id": source_transaction_id,
        "retry_attempt": retry_attempt,
        "event_time": message.get("event_time"),
        "scenario_type": message.get("scenario_type", "unknown"),
        "producer_id": message.get("producer_id", "unknown"),
        "trace_id": message.get("trace_id"),
        "original_payload": message,
        "rejection_type": rejection_type,
        "rejection_reason": rejection_reason,
        "rejection_fingerprint": rejection_fingerprint,
        "rejected_at": decision_time.isoformat(),
        "processing_node": os.getenv("SIMULPIX_SERVICE_NAME", "pix-decision-engine"),
        **build_client_decision(
            decision_status="REJECTED",
            decision_reason_code=decision_reason_code,
            decision_reason_label=decision_reason_label,
            decision_origin=decision_origin,
            decision_time=decision_time,
            message=message,
        ),
    }


def build_decision_message(message: dict[str, Any], final_message: dict[str, Any], final_topic: str) -> dict[str, Any]:
    return {
        "transaction_id": final_message.get("transaction_id"),
        "source_transaction_id": final_message.get("source_transaction_id") or message.get("transaction_id"),
        "retry_attempt": final_message.get("retry_attempt", 0),
        "event_time": final_message.get("event_time") or message.get("event_time"),
        "scenario_type": final_message.get("scenario_type") or message.get("scenario_type", "unknown"),
        "producer_id": final_message.get("producer_id") or message.get("producer_id", "unknown"),
        "trace_id": final_message.get("trace_id") or message.get("trace_id"),
        "decision_status": final_message.get("decision_status"),
        "decision_reason_code": final_message.get("decision_reason_code"),
        "decision_reason_label": final_message.get("decision_reason_label"),
        "decision_origin": final_message.get("decision_origin"),
        "decision_at": final_message.get("decision_at"),
        "decision_deadline": final_message.get("decision_deadline"),
        "decision_sla_seconds": final_message.get("decision_sla_seconds"),
        "decision_latency_ms": final_message.get("decision_latency_ms"),
        "decision_within_sla": final_message.get("decision_within_sla"),
        "client_message": final_message.get("client_message"),
        "final_topic": final_topic,
        "decision_event_type": "PIX_TRANSACTION_DECISION",
        "processing_node": final_message.get("processing_node") or os.getenv("SIMULPIX_SERVICE_NAME", "pix-decision-engine"),
    }
