#!/usr/bin/env python3
"""
Test protocol for network perturbation forms.
Run from outside Docker, targeting http://localhost:8083

Usage: python3 scripts/test-network-perturb.py
"""
import sys
import time
import urllib.parse
import urllib.request
import json

BASE = "http://localhost:8083"
CONTROL = BASE + "/control"
HEALTH = BASE + "/health"
SEPARATOR = "-" * 60


def post_form(fields: dict[str, str]) -> tuple[int, str]:
    data = urllib.parse.urlencode(fields).encode("utf-8")
    req = urllib.request.Request(CONTROL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.url
    except urllib.error.HTTPError as e:
        return e.code, ""
    except urllib.error.URLError as e:
        return 0, str(e)


def get_health() -> dict:
    try:
        with urllib.request.urlopen(HEALTH, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def get_control_state() -> dict:
    return get_health().get("control", {})


def wait_for_control_done(max_seconds: int = 45) -> str:
    for _ in range(max_seconds):
        state = get_control_state()
        status = state.get("status", "?")
        if status not in ("running",):
            return status
        time.sleep(1)
    return "timeout"


def check(label: str, expect_result: str) -> bool:
    final = wait_for_control_done()
    state = get_control_state()
    last_result = state.get("last_result", "?")
    last_action = state.get("last_action", "?")
    last_output = state.get("last_output", [])
    ok = (expect_result == "any") or (last_result == expect_result)
    symbol = "OK" if ok else "FAIL"
    print(f"  [{symbol}] {label}")
    print(f"       control: status={final!r}  last_result={last_result!r}  last_action={last_action!r}")
    if last_output:
        print(f"       output: {last_output[-1]!r}")
    if not ok:
        print(f"       ATTENDU: {expect_result!r}  OBTENU: {last_result!r}")
    return ok


failures = 0


def run(label: str, fields: dict[str, str], expect: str) -> None:
    global failures
    print()
    print(SEPARATOR)
    print(f"TEST : {label}")
    print(SEPARATOR)
    code, url = post_form(fields)
    print(f"  POST -> HTTP {code}  redirect: ...{url[-80:] if url else ''}")
    if not check(label, expect):
        failures += 1


# -- T1: apply sans confirmation → erreur --
run(
    "apply_network_profile SANS confirmation → error",
    {
        "action": "apply_network_profile",
        "network_profile": "kafka_latency",
        "network_target_service": "pix-decision-engine",
        "network_delay_ms": "250",
        "network_jitter_ms": "40",
        "network_loss_percent": "0",
        "network_rate_kbit": "0",
        "return_view": "scenario",
    },
    "error",
)

# -- T2: apply AVEC confirmation → ok --
run(
    "apply_network_profile AVEC confirmation → ok",
    {
        "action": "apply_network_profile",
        "network_profile": "kafka_latency",
        "network_target_service": "pix-decision-engine",
        "network_delay_ms": "250",
        "network_jitter_ms": "40",
        "network_loss_percent": "0",
        "network_rate_kbit": "0",
        "confirm_network_apply": "yes",
        "return_view": "scenario",
    },
    "ok",
)

# -- T3: apply SECONDE FOIS avec confirmation → ok (pas de blocage après premier) --
run(
    "apply SECONDE FOIS avec confirmation → ok (était bug 2)",
    {
        "action": "apply_network_profile",
        "network_profile": "kafka_latency",
        "network_target_service": "pix-validator",
        "network_delay_ms": "100",
        "network_jitter_ms": "20",
        "network_loss_percent": "0",
        "network_rate_kbit": "0",
        "confirm_network_apply": "yes",
        "return_view": "scenario",
    },
    "ok",
)

# -- T4: reset_network_profile → ok --
run(
    "reset_network_profile → ok",
    {
        "action": "reset_network_profile",
        "return_view": "scenario",
    },
    "ok",
)

# -- T5: update_scenario (Modifier l'atelier) → ok, PAS network_confirmation_missing --
run(
    "update_scenario (Modifier l'atelier) → ok (était bug 1)",
    {
        "action": "update_scenario",
        "rate_per_second": "5",
        "traffic_model": "linear",
        "return_view": "scenario",
    },
    "ok",
)

# -- T6: stop_platform → accepted (action async) --
# (on ne stoppe pas vraiment, on teste juste que l'action est reconnue)
# Skipped in automated test to avoid stopping the platform.

print()
print(SEPARATOR)
if failures == 0:
    print("RÉSULTAT : tous les tests passent.")
else:
    print(f"RÉSULTAT : {failures} test(s) en échec.")
print(SEPARATOR)
sys.exit(failures)
