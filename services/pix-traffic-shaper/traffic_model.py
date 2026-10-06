import json
import math
import os
import random
from typing import Any


DEFAULT_TRAFFIC_MODELS = {"linear", "poisson", "bursty", "profiled"}


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


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(value, maximum))


def poisson_sample(mean: float) -> int:
    if mean <= 0:
        return 0
    if mean < 30:
        threshold = math.exp(-mean)
        product = 1.0
        count_value = 0
        while product > threshold:
            count_value += 1
            product *= random.random()
        return max(count_value - 1, 0)
    return max(int(round(random.gauss(mean, math.sqrt(mean)))), 0)


def build_traffic_profile(base_rate: float, scenario: str) -> dict[str, Any]:
    traffic_model = str(os.getenv("SIMULPIX_TRAFFIC_MODEL", "bursty") or "bursty").strip().lower() or "bursty"
    if traffic_model not in DEFAULT_TRAFFIC_MODELS:
        traffic_model = "bursty"
    bucket_seconds = clamp(get_float_env("SIMULPIX_TRAFFIC_BUCKET_SECONDS", 1.0), 0.25, 5.0)
    compression_factor = max(get_float_env("SIMULPIX_TRAFFIC_TIME_COMPRESSION_FACTOR", 1.0), 1.0)
    if scenario == "football_match_peak":
        compression_factor = max(compression_factor, get_float_env("SIMULPIX_TIME_COMPRESSION_FACTOR", 10.0))
    event_enabled = traffic_model in {"bursty", "profiled"}
    profile = {
        "model": traffic_model,
        "base_rate": max(base_rate, 0.0),
        "bucket_seconds": bucket_seconds,
        "compression_factor": compression_factor,
        "second_amplitude": clamp(get_float_env("SIMULPIX_TRAFFIC_SECOND_AMPLITUDE", 0.18), 0.0, 0.9),
        "minute_amplitude": clamp(get_float_env("SIMULPIX_TRAFFIC_MINUTE_AMPLITUDE", 0.12), 0.0, 0.9),
        "ten_minute_amplitude": clamp(get_float_env("SIMULPIX_TRAFFIC_TEN_MINUTE_AMPLITUDE", 0.08), 0.0, 0.9),
        "hour_amplitude": clamp(get_float_env("SIMULPIX_TRAFFIC_HOUR_AMPLITUDE", 0.06), 0.0, 0.9),
        "noise_amplitude": clamp(get_float_env("SIMULPIX_TRAFFIC_NOISE_AMPLITUDE", 0.08), 0.0, 0.5),
        "burst_probability": clamp(get_float_env("SIMULPIX_TRAFFIC_BURST_PROBABILITY", 0.08), 0.0, 1.0) if event_enabled else 0.0,
        "burst_amplitude": clamp(get_float_env("SIMULPIX_TRAFFIC_BURST_AMPLITUDE", 1.8), 0.0, 10.0),
        "burst_duration_seconds": max(get_float_env("SIMULPIX_TRAFFIC_BURST_DURATION_SECONDS", 6.0), bucket_seconds),
        "lull_probability": clamp(get_float_env("SIMULPIX_TRAFFIC_LULL_PROBABILITY", 0.05), 0.0, 1.0) if event_enabled else 0.0,
        "lull_amplitude": clamp(get_float_env("SIMULPIX_TRAFFIC_LULL_AMPLITUDE", 0.6), 0.0, 0.95),
        "lull_duration_seconds": max(get_float_env("SIMULPIX_TRAFFIC_LULL_DURATION_SECONDS", 10.0), bucket_seconds),
        "second_period_seconds": max(get_float_env("SIMULPIX_TRAFFIC_SECOND_PERIOD_SECONDS", 7.0) / compression_factor, bucket_seconds),
        "minute_period_seconds": max(get_float_env("SIMULPIX_TRAFFIC_MINUTE_PERIOD_SECONDS", 75.0) / compression_factor, bucket_seconds),
        "ten_minute_period_seconds": max(get_float_env("SIMULPIX_TRAFFIC_TEN_MINUTE_PERIOD_SECONDS", 720.0) / compression_factor, bucket_seconds),
        "hour_period_seconds": max(get_float_env("SIMULPIX_TRAFFIC_HOUR_PERIOD_SECONDS", 3600.0) / compression_factor, bucket_seconds),
        "phase_offsets": {
            "second": random.uniform(0.0, math.tau),
            "minute": random.uniform(0.0, math.tau),
            "ten_minute": random.uniform(0.0, math.tau),
            "hour": random.uniform(0.0, math.tau),
        },
        "smoothed_noise": 1.0,
        "burst_state": None,
        "lull_state": None,
    }
    print(
        json.dumps(
            {
                "event": "traffic_profile_configured",
                "scenario_type": scenario,
                "traffic_model": traffic_model,
                "bucket_seconds": round(bucket_seconds, 3),
                "base_rate_per_second": round(max(base_rate, 0.0), 3),
                "compression_factor": round(compression_factor, 3),
                "burst_probability": profile["burst_probability"],
                "lull_probability": profile["lull_probability"],
            }
        ),
        flush=True,
    )
    return profile


def wave_component(elapsed_seconds: float, period_seconds: float, amplitude: float, offset_radians: float) -> float:
    if amplitude <= 0 or period_seconds <= 0:
        return 1.0
    return 1.0 + amplitude * math.sin((elapsed_seconds / period_seconds) * math.tau + offset_radians)


def maybe_start_traffic_event(profile: dict[str, Any], event_key: str, probability_key: str, amplitude_key: str, duration_key: str) -> None:
    if profile[event_key] is not None:
        return
    if random.random() > float(profile[probability_key]) * float(profile["bucket_seconds"]):
        return
    profile[event_key] = {
        "remaining_seconds": max(float(profile[duration_key]), float(profile["bucket_seconds"])),
        "duration_seconds": max(float(profile[duration_key]), float(profile["bucket_seconds"])),
        "strength": random.uniform(float(profile[amplitude_key]) * 0.6, float(profile[amplitude_key]) * 1.1),
    }


def event_factor(state: dict[str, float] | None, bucket_seconds: float, kind: str) -> float:
    if state is None:
        return 1.0
    progress = 1.0 - max(state["remaining_seconds"], 0.0) / max(state["duration_seconds"], bucket_seconds)
    shape = 1.0 - abs(1.0 - 2.0 * progress)
    if kind == "burst":
        return 1.0 + state["strength"] * max(shape, 0.15)
    return clamp(1.0 - state["strength"] * max(shape, 0.15), 0.05, 1.0)


def compute_bucket_target_count(
    profile: dict[str, Any],
    elapsed_seconds: float,
    phase_rate: float,
    phase_count: int,
    generated: int,
) -> int:
    bucket_seconds = float(profile["bucket_seconds"])
    remaining_messages = max(phase_count - generated, 0) if phase_count > 0 else 0
    if phase_rate <= 0:
        batch_size = max(get_int_env("SIMULPIX_OPEN_LOOP_BATCH_SIZE", 500), 1)
        if phase_count > 0:
            return min(batch_size, remaining_messages)
        return batch_size
    if str(profile["model"]) == "linear":
        return max(int(round(max(phase_rate, 0.0) * bucket_seconds)), 0)

    maybe_start_traffic_event(profile, "burst_state", "burst_probability", "burst_amplitude", "burst_duration_seconds")
    maybe_start_traffic_event(profile, "lull_state", "lull_probability", "lull_amplitude", "lull_duration_seconds")
    for event_key in ("burst_state", "lull_state"):
        state = profile.get(event_key)
        if state is not None:
            state["remaining_seconds"] = max(float(state["remaining_seconds"]) - bucket_seconds, 0.0)
            if state["remaining_seconds"] <= 0:
                profile[event_key] = None

    smoothed_noise = float(profile["smoothed_noise"])
    smoothed_noise = 0.7 * smoothed_noise + 0.3 * (
        1.0 + random.uniform(-float(profile["noise_amplitude"]), float(profile["noise_amplitude"]))
    )
    profile["smoothed_noise"] = clamp(smoothed_noise, 0.6, 1.4)

    modulation = (
        wave_component(elapsed_seconds, float(profile["second_period_seconds"]), float(profile["second_amplitude"]), float(profile["phase_offsets"]["second"]))
        * wave_component(elapsed_seconds, float(profile["minute_period_seconds"]), float(profile["minute_amplitude"]), float(profile["phase_offsets"]["minute"]))
        * wave_component(elapsed_seconds, float(profile["ten_minute_period_seconds"]), float(profile["ten_minute_amplitude"]), float(profile["phase_offsets"]["ten_minute"]))
        * wave_component(elapsed_seconds, float(profile["hour_period_seconds"]), float(profile["hour_amplitude"]), float(profile["phase_offsets"]["hour"]))
        * float(profile["smoothed_noise"])
        * event_factor(profile.get("burst_state"), bucket_seconds, "burst")
        * event_factor(profile.get("lull_state"), bucket_seconds, "lull")
    )
    modulation = clamp(modulation, 0.05, 8.0)
    expected_count = max(phase_rate, 0.0) * modulation * bucket_seconds

    if phase_count > 0 and phase_rate > 0:
        target_after_bucket = min(float(phase_count), max(elapsed_seconds + bucket_seconds, bucket_seconds) * phase_rate)
        catch_up_expected = max(target_after_bucket - generated, 0.0)
        expected_count = 0.6 * expected_count + 0.4 * catch_up_expected
        expected_count = min(expected_count, float(remaining_messages))

    bucket_count = poisson_sample(expected_count)
    if phase_count > 0:
        bucket_count = min(bucket_count, max(phase_count - generated, 0))
    return max(bucket_count, 0)


def get_configured_traffic_model() -> str:
    traffic_model = str(os.getenv("SIMULPIX_TRAFFIC_MODEL", "bursty") or "bursty").strip().lower() or "bursty"
    return traffic_model if traffic_model in DEFAULT_TRAFFIC_MODELS else "bursty"
