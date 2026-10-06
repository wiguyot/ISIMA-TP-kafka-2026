import os
from typing import Any

from probes import safe_int


def build_alerts(
    services: dict[str, str],
    kafka_detail: dict[str, Any] | None,
    business_metrics: dict[str, Any],
    network_detail: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    lag_warning_threshold = int(os.getenv("SIMULPIX_WARNING_LAG_TOTAL", "1"))
    reject_warning_threshold = float(os.getenv("SIMULPIX_WARNING_REJECT_RATE_PERCENT", "30"))
    reject_min_processed = int(os.getenv("SIMULPIX_WARNING_REJECT_MIN_PROCESSED", "10"))
    persistence_gap_threshold = int(os.getenv("SIMULPIX_WARNING_PERSISTENCE_GAP", "1"))
    oldest_lag_threshold = float(os.getenv("SIMULPIX_WARNING_OLDEST_LAG_SECONDS", "20"))

    for service_name, service_status in services.items():
        if service_status not in ("ok", "not_probed"):
            alerts.append(
                {
                    "level": "error" if service_status == "down" else "warning",
                    "kind": "service_status",
                    "service": service_name,
                    "message": f"service {service_name} status={service_status}",
                }
            )

    consumer_groups = (kafka_detail or {}).get("consumer_groups", {})
    for label, group_info in consumer_groups.items():
        lag_total = safe_int(group_info.get("lag_total"))
        if lag_total >= lag_warning_threshold:
            alerts.append(
                {
                    "level": "warning",
                    "kind": "kafka_lag",
                    "service": label,
                    "group_id": group_info.get("group_id"),
                    "message": f"lag Kafka detecte pour {label}: {lag_total}",
                    "lag_total": lag_total,
                }
            )

    replication_detail = (kafka_detail or {}).get("replication") or {}
    under_replicated_total = safe_int(replication_detail.get("under_replicated_partitions_total"))
    if under_replicated_total > 0:
        alerts.append(
            {
                "level": "warning",
                "kind": "kafka_replication",
                "message": f"partitions sous-repliquees detectees: {under_replicated_total}",
                "under_replicated_partitions_total": under_replicated_total,
            }
        )

    counts = business_metrics.get("counts", {})
    ratios = business_metrics.get("ratios", {})
    gaps = business_metrics.get("gaps", {})
    rates = business_metrics.get("rates") or {}

    processed = safe_int(counts.get("processed"))
    validated_sla_breaches = safe_int(counts.get("validated_sla_breaches"))
    rejected_sla_breaches = safe_int(counts.get("rejected_sla_breaches"))
    reject_rate_percent = float(ratios.get("reject_rate_percent", 0.0))
    processed_rate = float(rates.get("processed_per_second", 0.0) or 0.0)
    rejected_rate = float(rates.get("rejected_per_second", 0.0) or 0.0)
    recent_reject_rate_percent = round((rejected_rate / processed_rate) * 100.0, 2) if processed_rate > 0 else 0.0
    if processed_rate > 0 and processed >= reject_min_processed and recent_reject_rate_percent >= reject_warning_threshold:
        alerts.append(
            {
                "level": "warning",
                "kind": "reject_rate",
                "message": f"taux de rejet recent eleve: {recent_reject_rate_percent}%",
                "processed": processed,
                "reject_rate_percent": reject_rate_percent,
                "recent_reject_rate_percent": recent_reject_rate_percent,
            }
        )

    if safe_int(gaps.get("persistence_gap_valid")) >= persistence_gap_threshold:
        alerts.append(
            {
                "level": "warning",
                "kind": "persistence_gap_valid",
                "message": f"ecart de persistance validated: {safe_int(gaps.get('persistence_gap_valid'))}",
            }
        )
    if safe_int(gaps.get("persistence_gap_rejected")) >= persistence_gap_threshold:
        alerts.append(
            {
                "level": "warning",
                "kind": "persistence_gap_rejected",
                "message": f"ecart de persistance rejected: {safe_int(gaps.get('persistence_gap_rejected'))}",
            }
        )

    validated_sla_breaches_rate = float(rates.get("validated_sla_breaches_per_second", 0.0) or 0.0)
    rejected_sla_breaches_rate = float(rates.get("rejected_sla_breaches_per_second", 0.0) or 0.0)
    if validated_sla_breaches_rate > 0 or rejected_sla_breaches_rate > 0:
        alerts.append(
            {
                "level": "warning",
                "kind": "decision_sla",
                "message": (
                    f"decisions hors SLA recentes detectees: "
                    f"accepted_rate={validated_sla_breaches_rate}/s rejected_rate={rejected_sla_breaches_rate}/s"
                ),
                "validated_sla_breaches": validated_sla_breaches,
                "rejected_sla_breaches": rejected_sla_breaches,
                "validated_sla_breaches_rate": validated_sla_breaches_rate,
                "rejected_sla_breaches_rate": rejected_sla_breaches_rate,
            }
        )

    timings = business_metrics.get("timings", {})
    oldest_lag_seconds = timings.get("estimated_oldest_lag_seconds")
    if oldest_lag_seconds is not None and float(oldest_lag_seconds) >= oldest_lag_threshold:
        alerts.append(
            {
                "level": "warning",
                "kind": "oldest_lag",
                "message": f"anciennete estimee du backlog: {oldest_lag_seconds}s",
                "estimated_oldest_lag_seconds": oldest_lag_seconds,
            }
        )

    if safe_int((network_detail or {}).get("active")):
        alerts.append(
            {
                "level": "warning",
                "kind": "network_perturbation",
                "message": (
                    "perturbation reseau active: "
                    f"profile={(network_detail or {}).get('profile', 'unknown')} "
                    f"target={(network_detail or {}).get('target_service', 'unknown')}"
                ),
            }
        )

    return alerts


def compute_global_status(services: dict[str, str], alerts: list[dict[str, Any]]) -> str:
    if any(value == "down" for value in services.values()):
        return "degraded"
    if any(value == "degraded" for value in services.values()):
        return "degraded"
    if any(alert.get("level") == "error" for alert in alerts):
        return "degraded"
    if alerts:
        return "warning"
    return "ok"
