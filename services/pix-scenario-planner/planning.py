import json
import os


def get_float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def get_int_env(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except ValueError:
        return default


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


def compute_football_match_peak_plan() -> tuple[list[dict[str, float | int | str]], int]:
    match_count = max(get_int_env("SIMULPIX_MATCH_COUNT", 10), 1)
    stadium_capacity = max(get_int_env("SIMULPIX_STADIUM_CAPACITY", 44000), 1)
    pix_usage_rate = min(max(get_float_env("SIMULPIX_PIX_USAGE_RATE", 0.05), 0.0), 1.0)
    peak_share = min(max(get_float_env("SIMULPIX_PEAK_SHARE", 0.30), 0.0), 1.0)
    peak_window_minutes = max(get_float_env("SIMULPIX_PEAK_WINDOW_MINUTES", 15.0), 1.0)
    total_window_minutes = max(get_float_env("SIMULPIX_TOTAL_WINDOW_MINUTES", 240.0), peak_window_minutes)
    time_compression_factor = max(get_float_env("SIMULPIX_TIME_COMPRESSION_FACTOR", 10.0), 1.0)

    spectators_total = match_count * stadium_capacity
    total_transactions = max(int(round(spectators_total * pix_usage_rate)), 1)
    peak_transactions = int(round(total_transactions * peak_share))
    baseline_transactions = max(total_transactions - peak_transactions, 0)

    baseline_window_seconds = max(((total_window_minutes - peak_window_minutes) * 60.0) / time_compression_factor, 1.0)
    peak_window_seconds = max((peak_window_minutes * 60.0) / time_compression_factor, 1.0)
    baseline_rate = max(baseline_transactions / baseline_window_seconds, 0.1) if baseline_transactions > 0 else 0.0
    peak_rate = max(peak_transactions / peak_window_seconds, 0.1) if peak_transactions > 0 else 0.0

    plan: list[dict[str, float | int | str]] = []
    if baseline_transactions > 0:
        plan.append({"name": "baseline", "count": baseline_transactions, "rate": baseline_rate, "scenario_type": "football_match_peak_baseline"})
    if peak_transactions > 0:
        plan.append({"name": "peak", "count": peak_transactions, "rate": peak_rate, "scenario_type": "football_match_peak_peak"})

    print(
        json.dumps(
            {
                "event": "football_match_peak_plan",
                "match_count": match_count,
                "stadium_capacity": stadium_capacity,
                "spectators_total": spectators_total,
                "pix_usage_rate": pix_usage_rate,
                "total_transactions": total_transactions,
                "peak_share": peak_share,
                "peak_transactions": peak_transactions,
                "baseline_transactions": baseline_transactions,
                "peak_window_minutes": peak_window_minutes,
                "total_window_minutes": total_window_minutes,
                "time_compression_factor": time_compression_factor,
                "baseline_rate_per_second": round(baseline_rate, 3),
                "peak_rate_per_second": round(peak_rate, 3),
            }
        ),
        flush=True,
    )
    return plan, total_transactions


def build_plan() -> dict[str, object]:
    scenario = os.getenv("SIMULPIX_SCENARIO", "nominal")
    rate = parse_non_negative_float(os.getenv("SIMULPIX_RATE_PER_SECOND", "10"), 10.0)
    total_messages = parse_non_negative_int(os.getenv("SIMULPIX_TOTAL_MESSAGES", "100"), 100)

    if scenario == "football_match_peak":
        phases, total_messages = compute_football_match_peak_plan()
        plan_name = "football_match_peak"
    else:
        phases = [{"name": "flux nominal", "count": total_messages, "rate": rate, "scenario_type": scenario}]
        plan_name = "single_phase"

    payload = {
        "scenario_type": scenario,
        "plan_name": plan_name,
        "total_messages": total_messages,
        "default_rate_per_second": rate,
        "phases": phases,
    }
    print(json.dumps({"event": "scenario_plan_built", **payload}), flush=True)
    return payload
