import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any


def get_decision_sla_seconds() -> int:
    try:
        return max(int(float(os.getenv("SIMULPIX_DECISION_SLA_SECONDS", "10"))), 1)
    except ValueError:
        return 10


def load_reference_clients(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_nominal_message(index: int, clients: list[dict[str, Any]], scenario_type: str) -> dict[str, Any]:
    emitter = clients[index % len(clients)]
    beneficiary = clients[(index + 1) % len(clients)]
    event_time = datetime.now(timezone.utc)
    decision_sla_seconds = get_decision_sla_seconds()
    return {
        "transaction_id": f"tx-{index:08d}",
        "event_time": event_time.isoformat(),
        "emitter_account_id": emitter["accounts"][0],
        "beneficiary_account_id": beneficiary["accounts"][0],
        "emitter_pix_client_id": emitter["pix_client_id"],
        "beneficiary_pix_client_id": beneficiary["pix_client_id"],
        "emitter_tax_id": emitter["tax_id"],
        "beneficiary_tax_id": beneficiary["tax_id"],
        "amount": round(10 + (index % 50) * 3.75, 2),
        "currency": "BRL",
        "status": "RECEIVED",
        "scenario_type": scenario_type,
        "producer_id": os.getenv("SIMULPIX_SERVICE_NAME", "generator"),
        "trace_id": f"trace-tx-{index:08d}",
        "decision_sla_seconds": decision_sla_seconds,
        "decision_deadline": (event_time + timedelta(seconds=decision_sla_seconds)).isoformat(),
    }


def apply_error_variant(message: dict[str, Any], index: int, clients: list[dict[str, Any]]) -> dict[str, Any]:
    variant = index % 4
    if variant == 0:
        message["amount"] = -10
        message["scenario_type"] = "invalid_amount"
    elif variant == 1:
        message.pop("beneficiary_tax_id", None)
        message["scenario_type"] = "missing_field"
    elif variant == 2:
        wrong_client = clients[(index + 2) % len(clients)]
        message["beneficiary_pix_client_id"] = wrong_client["pix_client_id"]
        message["scenario_type"] = "tax_pix_mismatch"
    else:
        message["status"] = "UNKNOWN"
        message["scenario_type"] = "invalid_status"
    return message


def build_message(index: int, clients: list[dict[str, Any]], scenario: str) -> dict[str, Any]:
    if scenario == "errors_simple":
        return apply_error_variant(build_nominal_message(index, clients, "errors_simple"), index, clients)
    if scenario == "mixed":
        base = build_nominal_message(index, clients, "mixed")
        if index % 5 == 0:
            return apply_error_variant(base, index, clients)
        return base
    if scenario == "football_match_peak":
        return build_nominal_message(index, clients, "football_match_peak")
    return build_nominal_message(index, clients, scenario)
