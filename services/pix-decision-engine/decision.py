import os
from datetime import datetime, timedelta, timezone
from typing import Any


def parse_iso_datetime(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    candidate = str(raw_value).strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def get_decision_sla_seconds(message: dict[str, Any]) -> int:
    raw_value = message.get("decision_sla_seconds", os.getenv("SIMULPIX_DECISION_SLA_SECONDS", "5"))
    try:
        return max(int(float(raw_value)), 1)
    except (TypeError, ValueError):
        return 5


def get_decision_deadline(message: dict[str, Any]) -> datetime:
    event_time = parse_iso_datetime(message.get("event_time")) or datetime.now(timezone.utc)
    decision_deadline = parse_iso_datetime(message.get("decision_deadline"))
    if decision_deadline is not None:
        return decision_deadline
    return event_time + timedelta(seconds=get_decision_sla_seconds(message))


def compute_decision_latency_ms(message: dict[str, Any], decision_time: datetime) -> int | None:
    event_time = parse_iso_datetime(message.get("event_time"))
    if event_time is None:
        return None
    return max(int((decision_time - event_time).total_seconds() * 1000), 0)


def is_decision_within_sla(message: dict[str, Any], decision_time: datetime) -> bool:
    return decision_time <= get_decision_deadline(message)


def build_client_decision(
    *,
    decision_status: str,
    decision_reason_code: str,
    decision_reason_label: str,
    decision_origin: str,
    decision_time: datetime,
    message: dict[str, Any],
) -> dict[str, Any]:
    decision_latency_ms = compute_decision_latency_ms(message, decision_time)
    return {
        "decision_status": decision_status,
        "decision_reason_code": decision_reason_code,
        "decision_reason_label": decision_reason_label,
        "decision_origin": decision_origin,
        "decision_at": decision_time.isoformat(),
        "decision_deadline": get_decision_deadline(message).isoformat(),
        "decision_sla_seconds": get_decision_sla_seconds(message),
        "decision_latency_ms": decision_latency_ms,
        "decision_within_sla": is_decision_within_sla(message, decision_time),
        "client_message": decision_reason_label,
    }


def build_timeout_error(message: dict[str, Any]) -> list[str] | None:
    decision_time = datetime.now(timezone.utc)
    if is_decision_within_sla(message, decision_time):
        return None
    return ["processing_timeout"]


def map_rejection_reason(errors: list[str]) -> tuple[str, str, str, str]:
    if "processing_timeout" in errors:
        return (
            "TECHNICAL_TIMEOUT",
            "PROCESSING_TIMEOUT",
            "Paiement Pix rejeté : délai de décision dépassé",
            "SYSTEM",
        )
    business_to_client = {
        "amount_must_be_positive": ("BUSINESS_VALIDATION", "BANK_REJECTED", "Paiement Pix rejeté par la banque : montant invalide", "BANK"),
        "accounts_must_differ": ("BUSINESS_VALIDATION", "BANK_REJECTED", "Paiement Pix rejeté par la banque : comptes incompatibles", "BANK"),
        "invalid_initial_status": ("BUSINESS_VALIDATION", "BANK_REJECTED", "Paiement Pix rejeté par la banque : statut initial invalide", "BANK"),
        "emitter_tax_pix_mismatch": ("BUSINESS_VALIDATION", "BANK_REJECTED", "Paiement Pix rejeté par la banque : identité émetteur incohérente", "BANK"),
        "beneficiary_tax_pix_mismatch": ("BUSINESS_VALIDATION", "BANK_REJECTED", "Paiement Pix rejeté par la banque : identité bénéficiaire incohérente", "BANK"),
    }
    for error in errors:
        if error.startswith("missing_"):
            return (
                "BUSINESS_VALIDATION",
                "BANK_REJECTED",
                "Paiement Pix rejeté par la banque : données obligatoires manquantes",
                "BANK",
            )
        if error in business_to_client:
            return business_to_client[error]
    return ("TECHNICAL_FAILURE", "TECHNICAL_REJECTED", "Paiement Pix rejeté : incident technique", "SYSTEM")
