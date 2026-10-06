import os
import threading
import time

from health import serve_http
from planning import build_plan
from state import STATE


def run() -> int:
    idle_mode = str(os.getenv("SIMULPIX_GENERATOR_IDLE", "0")).strip().lower() in {"1", "true", "yes", "on"}
    threading.Thread(target=serve_http, daemon=True).start()

    if idle_mode:
        STATE["status"] = "idle"
        STATE["ready_to_plan"] = False
        STATE["scenario_type"] = "idle"
        STATE["plan_name"] = "idle"
        while True:
            time.sleep(60)

    plan = build_plan()
    STATE["status"] = "running"
    STATE["ready_to_plan"] = True
    STATE["scenario_type"] = str(plan.get("scenario_type") or "nominal")
    STATE["plan_name"] = str(plan.get("plan_name") or "single_phase")
    STATE["phase_count"] = len(plan.get("phases") or [])
    STATE["total_messages"] = int(plan.get("total_messages") or 0)
    STATE["default_rate_per_second"] = float(plan.get("default_rate_per_second") or 0.0)
    STATE["last_error"] = None
    while True:
        time.sleep(60)
