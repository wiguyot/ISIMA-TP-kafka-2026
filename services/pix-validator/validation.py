import json
from typing import Any


def load_reference_clients(path: str) -> dict[str, str]:
    with open(path, "r", encoding="utf-8") as handle:
        clients = json.load(handle)
    return {client["tax_id"]: client["pix_client_id"] for client in clients}


def validate_message(message: dict[str, Any], tax_to_pix: dict[str, str]) -> list[str]:
    required_fields = [
        "transaction_id",
        "event_time",
        "emitter_account_id",
        "beneficiary_account_id",
        "emitter_pix_client_id",
        "beneficiary_pix_client_id",
        "emitter_tax_id",
        "beneficiary_tax_id",
        "amount",
        "currency",
        "status",
        "scenario_type",
        "producer_id",
    ]
    errors: list[str] = []
    for field in required_fields:
        if field not in message or message[field] in (None, ""):
            errors.append(f"missing_{field}")
    if errors:
        return errors
    if float(message["amount"]) <= 0:
        errors.append("amount_must_be_positive")
    if message["emitter_account_id"] == message["beneficiary_account_id"]:
        errors.append("accounts_must_differ")
    if message["status"] != "RECEIVED":
        errors.append("invalid_initial_status")
    emitter_expected = tax_to_pix.get(message["emitter_tax_id"])
    beneficiary_expected = tax_to_pix.get(message["beneficiary_tax_id"])
    if emitter_expected != message["emitter_pix_client_id"]:
        errors.append("emitter_tax_pix_mismatch")
    if beneficiary_expected != message["beneficiary_pix_client_id"]:
        errors.append("beneficiary_tax_pix_mismatch")
    return errors
