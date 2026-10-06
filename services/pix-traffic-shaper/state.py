from typing import Any


STATE: dict[str, Any] = {
    "status": "starting",
    "ready_to_shape": False,
    "scenario_type": None,
    "current_phase": None,
    "phase_progress": 0,
    "phase_total": 0,
    "traffic_model": None,
    "target_rate_per_second": 0.0,
    "bucket_seconds": 0.0,
    "last_bucket_count": 0,
    "batches_sent": 0,
    "messages_requested": 0,
    "last_error": None,
    "live_rate_override": None,
    "live_model_override": None,
}
