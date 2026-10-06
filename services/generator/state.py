from typing import Any


STATE: dict[str, Any] = {
    "status": "starting",
    "ready_for_commands": False,
    "messages_sent": 0,
    "messages_enqueued": 0,
    "messages_delivered": 0,
    "messages_delivery_failed": 0,
    "last_error": None,
    "last_message_id": None,
    "last_delivery_error": None,
    "scenario_type": None,
    "current_phase": None,
    "phase_progress": 0,
    "phase_total": 0,
    "last_requested_batch_count": 0,
    "producer_acks": None,
    "producer_retries": None,
    "producer_semantics": None,
    "ambiguous_ack_duplicate_percent": 0,
    "ambiguous_ack_duplicates_enqueued": 0,
    "decision_sla_seconds": None,
    "traffic_model": None,
}
