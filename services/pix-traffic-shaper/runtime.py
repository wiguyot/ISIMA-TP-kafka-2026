import json
import os
import random
import threading
import time
from urllib.error import URLError
from urllib.request import Request, urlopen

from health import serve_http
from state import STATE
from traffic_model import build_traffic_profile, clamp, compute_bucket_target_count, get_configured_traffic_model


def parse_non_negative_int(raw_value: str | None, default: int) -> int:
    value = str(raw_value or "").strip()
    if value == "":
        return 0
    try:
        return max(int(float(value)), 0)
    except ValueError:
        return default


def parse_non_negative_float(raw_value: str | None, default: float) -> float:
    value = str(raw_value or "").strip()
    if value == "":
        return 0.0
    try:
        return max(float(value), 0.0)
    except ValueError:
        return default


def fetch_json(url: str, timeout_seconds: float = 2.0) -> dict | None:
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            payload = response.read().decode("utf-8")
        data = json.loads(payload)
        return data if isinstance(data, dict) else None
    except (OSError, URLError, ValueError, json.JSONDecodeError):
        return None


def post_emit_batch(url: str, payload: dict) -> dict:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))
    if not isinstance(data, dict) or str(data.get("status")) != "ok":
        raise RuntimeError(f"invalid_emit_response:{data!r}")
    return data


def wait_for_publisher(publisher_url: str) -> None:
    while True:
        payload = fetch_json(publisher_url)
        if payload is not None and bool(payload.get("ready_for_commands")):
            STATE["ready_to_shape"] = True
            STATE["last_error"] = None
            return
        STATE["status"] = "waiting_publisher"
        STATE["last_error"] = "waiting_for_generator_publisher"
        time.sleep(1)


def wait_for_plan(planner_health_url: str, planner_plan_url: str) -> dict:
    while True:
        planner_health = fetch_json(planner_health_url)
        if planner_health is not None and bool(planner_health.get("ready_to_plan")):
            planner_plan = fetch_json(planner_plan_url)
            if planner_plan is not None and isinstance(planner_plan.get("phases"), list):
                STATE["last_error"] = None
                return planner_plan
        STATE["status"] = "waiting_planner"
        STATE["last_error"] = "waiting_for_pix_scenario_planner"
        time.sleep(1)


def run() -> int:
    idle_mode = str(os.getenv("SIMULPIX_GENERATOR_IDLE", "0")).strip().lower() in {"1", "true", "yes", "on"}
    seed = int(os.getenv("SIMULPIX_SEED", "42"))
    planner_health_url = os.getenv("SIMULPIX_SCENARIO_PLANNER_HEALTH_URL", "http://pix-scenario-planner:8090/health")
    planner_plan_url = os.getenv("SIMULPIX_SCENARIO_PLANNER_PLAN_URL", "http://pix-scenario-planner:8090/plan")
    publisher_health_url = os.getenv("SIMULPIX_GENERATOR_HEALTH_URL", "http://generator:8081/health")
    publisher_emit_url = os.getenv("SIMULPIX_GENERATOR_EMIT_URL", "http://generator:8081/emit-batch")

    random.seed(seed)
    threading.Thread(target=serve_http, daemon=True).start()
    STATE["traffic_model"] = get_configured_traffic_model()

    if idle_mode:
        STATE["status"] = "idle"
        STATE["scenario_type"] = "idle"
        STATE["current_phase"] = "idle"
        while True:
            time.sleep(60)

    wait_for_publisher(publisher_health_url)
    plan = wait_for_plan(planner_health_url, planner_plan_url)
    STATE["status"] = "running"
    scenario = str(plan.get("scenario_type") or "nominal")
    phases = list(plan.get("phases") or [])
    total_messages = parse_non_negative_int(str(plan.get("total_messages") or "0"), 0)
    STATE["scenario_type"] = scenario

    for phase in phases:
        phase_name = str(phase["name"])
        phase_count = int(phase["count"])
        phase_rate = float(phase["rate"])
        phase_scenario_type = str(phase.get("scenario_type") or scenario)
        traffic_profile = build_traffic_profile(phase_rate, scenario)
        bucket_seconds = float(traffic_profile["bucket_seconds"])
        STATE["traffic_model"] = str(traffic_profile["model"])
        STATE["target_rate_per_second"] = round(phase_rate, 3)
        STATE["bucket_seconds"] = bucket_seconds
        STATE["current_phase"] = phase_name
        STATE["phase_progress"] = 0
        STATE["phase_total"] = phase_count
        print(
            json.dumps(
                {
                    "event": "traffic_phase_start",
                    "phase": phase_name,
                    "count": phase_count,
                    "rate_per_second": round(phase_rate, 3),
                    "traffic_model": traffic_profile["model"],
                    "bucket_seconds": round(bucket_seconds, 3),
                }
            ),
            flush=True,
        )
        generated = 0
        phase_started_at = time.monotonic()
        while phase_count <= 0 or generated < phase_count:
            bucket_started_at = time.monotonic()
            live_rate = STATE["live_rate_override"]
            live_model = STATE["live_model_override"]
            if live_rate is not None or live_model is not None:
                current_rate = float(live_rate) if live_rate is not None else phase_rate
                current_profile = build_traffic_profile(current_rate, str(live_model or traffic_profile["model"]))
                STATE["target_rate_per_second"] = round(current_rate, 3)
                STATE["traffic_model"] = str(current_profile["model"])
                STATE["bucket_seconds"] = float(current_profile["bucket_seconds"])
            else:
                current_rate = phase_rate
                current_profile = traffic_profile
            bucket_count = compute_bucket_target_count(current_profile, bucket_started_at - phase_started_at, current_rate, phase_count, generated)
            STATE["last_bucket_count"] = bucket_count
            open_loop = current_rate <= 0
            if bucket_count > 0:
                phase_progress = min(generated + bucket_count, phase_count) if phase_count > 0 else generated + bucket_count
                post_emit_batch(
                    publisher_emit_url,
                    {
                        "batch_count": bucket_count,
                        "scenario": scenario,
                        "scenario_type": phase_scenario_type,
                        "phase_name": phase_name,
                        "phase_progress": phase_progress,
                        "phase_total": phase_count,
                    },
                )
                generated += bucket_count
                STATE["batches_sent"] += 1
                STATE["messages_requested"] += bucket_count
                STATE["phase_progress"] = generated
            print(
                json.dumps(
                    {
                        "event": "traffic_bucket_requested",
                        "phase": phase_name,
                        "bucket_messages": bucket_count,
                        "phase_progress": generated,
                        "phase_total": phase_count,
                        "open_loop": open_loop,
                    }
                ),
                flush=True,
            )
            remaining_bucket_sleep = bucket_started_at + bucket_seconds - time.monotonic()
            if not open_loop and remaining_bucket_sleep > 0:
                time.sleep(remaining_bucket_sleep)
            elif open_loop:
                open_loop_pause = parse_non_negative_float(os.getenv("SIMULPIX_OPEN_LOOP_PAUSE_SECONDS", "0.01"), 0.01)
                open_loop_pause = clamp(open_loop_pause, 0.0, 1.0)
                if open_loop_pause > 0:
                    time.sleep(open_loop_pause)

    STATE["current_phase"] = "completed"
    STATE["phase_progress"] = STATE["phase_total"]
    while True:
        time.sleep(60)
