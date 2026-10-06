from typing import Any


STATE: dict[str, Any] = {
    "status": "starting",
    "ready_to_plan": False,
    "scenario_type": None,
    "plan_name": None,
    "phase_count": 0,
    "total_messages": 0,
    "default_rate_per_second": 0.0,
    "last_error": None,
}
