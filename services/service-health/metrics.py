import time
from typing import Any

from app_state import HISTORY_CACHE, HISTORY_MAX_POINTS, SNAPSHOT_CACHE
from probes import safe_int


def build_snapshot_sample(
    generator_detail: dict[str, Any] | None,
    pix_validator_detail: dict[str, Any] | None,
    processing_detail: dict[str, Any] | None,
    pix_outcome_publisher_detail: dict[str, Any] | None,
    persister_valid_detail: dict[str, Any] | None,
    persister_rejected_detail: dict[str, Any] | None,
    kafka_detail: dict[str, Any] | None,
    postgres_detail: dict[str, Any] | None,
) -> dict[str, int]:
    kafka_topics = (kafka_detail or {}).get("topics", {})
    validator_is_transactional = str((pix_validator_detail or {}).get("transactional_mode") or "") == "exactly_once_kafka"
    decision_is_transactional = str((processing_detail or {}).get("transactional_mode") or "") == "exactly_once_kafka"
    outcome_is_transactional = str((pix_outcome_publisher_detail or {}).get("transactional_mode") or "") == "exactly_once_kafka"
    logical_checked_count = (
        safe_int((pix_validator_detail or {}).get("checked"))
        if validator_is_transactional
        else safe_int((kafka_topics.get("simulpix.transactions.checked") or {}).get("end_offsets_total"))
    )
    logical_validated_count = (
        safe_int((processing_detail or {}).get("validated"))
        if decision_is_transactional
        else safe_int((kafka_topics.get("simulpix.transactions.validated") or {}).get("end_offsets_total"))
    )
    logical_rejected_count = (
        safe_int((processing_detail or {}).get("rejected"))
        if decision_is_transactional
        else safe_int((kafka_topics.get("simulpix.transactions.rejected") or {}).get("end_offsets_total"))
    )
    logical_decision_count = (
        safe_int((processing_detail or {}).get("decisions_published"))
        if decision_is_transactional
        else safe_int((kafka_topics.get("simulpix.transactions.decision") or {}).get("end_offsets_total"))
    )
    logical_outcome_count = (
        safe_int((pix_outcome_publisher_detail or {}).get("published"))
        if outcome_is_transactional
        else safe_int((kafka_topics.get("simulpix.transactions.outcome") or {}).get("end_offsets_total"))
    )
    return {
        "generated": safe_int((generator_detail or {}).get("messages_sent")),
        "processed": safe_int((processing_detail or {}).get("processed")),
        "validated": safe_int((processing_detail or {}).get("validated")),
        "rejected": safe_int((processing_detail or {}).get("rejected")),
        "validated_sla_breaches": safe_int((postgres_detail or {}).get("validated_sla_breaches")),
        "rejected_sla_breaches": safe_int((postgres_detail or {}).get("rejected_sla_breaches")),
        "persisted_valid": safe_int((persister_valid_detail or {}).get("persisted")),
        "persisted_rejected": safe_int((persister_rejected_detail or {}).get("persisted")),
        "db_validated_count": safe_int((postgres_detail or {}).get("validated_count")),
        "db_rejected_count": safe_int((postgres_detail or {}).get("rejected_count")),
        "outcome_count": logical_outcome_count,
        "topic_checked_offsets": logical_checked_count,
        "topic_decision_offsets": logical_decision_count,
        "topic_raw_offsets": safe_int((kafka_topics.get("simulpix.transactions.raw") or {}).get("end_offsets_total")),
        "topic_validated_offsets": logical_validated_count,
        "topic_rejected_offsets": logical_rejected_count,
        "topic_retry_offsets": safe_int((kafka_topics.get("simulpix.transactions.retry") or {}).get("end_offsets_total")),
        "topic_outcome_offsets": safe_int((kafka_topics.get("simulpix.transactions.outcome") or {}).get("end_offsets_total")),
    }


def build_rate_metrics(sample: dict[str, int]) -> dict[str, float] | None:
    now = time.time()
    previous_time = SNAPSHOT_CACHE.get("captured_at")
    previous_sample = SNAPSHOT_CACHE.get("sample")
    SNAPSHOT_CACHE["captured_at"] = now
    SNAPSHOT_CACHE["sample"] = sample

    if previous_time is None or previous_sample is None:
        return None

    delta_seconds = now - previous_time
    if delta_seconds <= 0.25:
        return None

    rate_metrics: dict[str, float] = {"window_seconds": round(delta_seconds, 3)}
    for key, current_value in sample.items():
        previous_value = safe_int(previous_sample.get(key))
        delta_value = current_value - previous_value
        if delta_value < 0:
            continue
        rate_metrics[f"{key}_per_second"] = round(delta_value / delta_seconds, 3)
    return rate_metrics


def append_history_snapshot(
    timestamp: str,
    business_metrics: dict[str, Any],
    kafka_detail: dict[str, Any] | None,
    alerts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    counts = business_metrics.get("counts", {})
    ratios = business_metrics.get("ratios", {})
    rates = business_metrics.get("rates") or {}
    consumer_groups = (kafka_detail or {}).get("consumer_groups", {})
    history_values = {
        "generated": safe_int(counts.get("generated")),
        "processed": safe_int(counts.get("processed")),
        "validated": safe_int(counts.get("validated")),
        "rejected": safe_int(counts.get("rejected")),
        "db_validated_count": safe_int(counts.get("db_validated_count")),
        "db_rejected_count": safe_int(counts.get("db_rejected_count")),
        "reject_rate_percent": float(ratios.get("reject_rate_percent", 0.0)),
        "raw_rate_per_second": float(rates.get("topic_raw_offsets_per_second", 0.0)),
        "lag_total": sum(safe_int(group.get("lag_total")) for group in consumer_groups.values()),
        "alert_count": len(alerts),
    }
    if HISTORY_CACHE:
        previous = dict(HISTORY_CACHE[-1])
        previous.pop("timestamp", None)
        if previous == history_values:
            return HISTORY_CACHE[-HISTORY_MAX_POINTS:]
    history_point = {"timestamp": timestamp, **history_values}
    if HISTORY_CACHE and HISTORY_CACHE[-1] == history_point:
        return HISTORY_CACHE[-HISTORY_MAX_POINTS:]
    HISTORY_CACHE.append(history_point)
    if len(HISTORY_CACHE) > HISTORY_MAX_POINTS:
        del HISTORY_CACHE[:-HISTORY_MAX_POINTS]
    return HISTORY_CACHE[-HISTORY_MAX_POINTS:]


def estimate_oldest_lag_seconds(kafka_detail: dict[str, Any] | None, rate_metrics: dict[str, Any] | None) -> float | None:
    consumer_groups = (kafka_detail or {}).get("consumer_groups", {})
    lag_total = sum(safe_int(group.get("lag_total")) for group in consumer_groups.values())
    raw_rate = float((rate_metrics or {}).get("topic_raw_offsets_per_second", 0.0))
    if lag_total <= 0 or raw_rate <= 0:
        return None
    return round(lag_total / raw_rate, 2)


def build_business_metrics(
    generator_detail: dict[str, Any] | None,
    pix_validator_detail: dict[str, Any] | None,
    pix_decision_engine_detail: dict[str, Any] | None,
    pix_outcome_publisher_detail: dict[str, Any] | None,
    persister_valid_detail: dict[str, Any] | None,
    persister_rejected_detail: dict[str, Any] | None,
    kafka_detail: dict[str, Any] | None,
    postgres_detail: dict[str, Any] | None,
) -> dict[str, Any]:
    active_processing_detail = pix_decision_engine_detail or {}
    generated = safe_int((generator_detail or {}).get("messages_sent"))
    processed = safe_int((active_processing_detail or {}).get("processed"))
    validated = safe_int((active_processing_detail or {}).get("validated"))
    rejected = safe_int((active_processing_detail or {}).get("rejected"))
    consumer_groups = (kafka_detail or {}).get("consumer_groups", {})
    topics = (kafka_detail or {}).get("topics", {})
    persisted_valid = safe_int((persister_valid_detail or {}).get("persisted"))
    persisted_rejected = safe_int((persister_rejected_detail or {}).get("persisted"))
    db_validated_count = safe_int((postgres_detail or {}).get("validated_count"))
    db_rejected_count = safe_int((postgres_detail or {}).get("rejected_count"))
    validator_is_transactional = str((pix_validator_detail or {}).get("transactional_mode") or "") == "exactly_once_kafka"
    decision_is_transactional = str((active_processing_detail or {}).get("transactional_mode") or "") == "exactly_once_kafka"
    outcome_is_transactional = str((pix_outcome_publisher_detail or {}).get("transactional_mode") or "") == "exactly_once_kafka"

    def topic_end_offsets(topic_name: str) -> int:
        return safe_int((topics.get(topic_name) or {}).get("end_offsets_total"))

    logical_outcome_count = (
        safe_int((pix_outcome_publisher_detail or {}).get("published"))
        if outcome_is_transactional
        else topic_end_offsets("simulpix.transactions.outcome")
    )
    logical_decision_count = (
        safe_int((active_processing_detail or {}).get("decisions_published"))
        if decision_is_transactional
        else topic_end_offsets("simulpix.transactions.decision")
    )
    logical_checked_count = (
        safe_int((pix_validator_detail or {}).get("checked"))
        if validator_is_transactional
        else topic_end_offsets("simulpix.transactions.checked")
    )
    logical_validated_count = (
        safe_int((active_processing_detail or {}).get("validated"))
        if decision_is_transactional
        else topic_end_offsets("simulpix.transactions.validated")
    )
    logical_rejected_count = (
        safe_int((active_processing_detail or {}).get("rejected"))
        if decision_is_transactional
        else topic_end_offsets("simulpix.transactions.rejected")
    )
    pending_result_count = max(topic_end_offsets("simulpix.transactions.raw") - logical_validated_count - logical_rejected_count, 0)

    producer_semantics = str((generator_detail or {}).get("producer_semantics") or "at_least_once")
    consumer_semantics = str((active_processing_detail or {}).get("consumer_semantics") or "at_least_once")
    producer_acks = str((generator_detail or {}).get("producer_acks") or "all")
    producer_retries = safe_int((generator_detail or {}).get("producer_retries"))
    consumer_commit_strategy = str((active_processing_detail or {}).get("consumer_commit_strategy") or "after")
    consumer_auto_commit = bool((active_processing_detail or {}).get("consumer_auto_commit"))
    consumer_commit_batch_size = safe_int((active_processing_detail or {}).get("consumer_commit_batch_size") or 1)

    producer_semantics_code = {"at_most_once": 1, "at_least_once": 2, "exactly_once": 3}.get(producer_semantics, 4)
    producer_risk_code = {"at_most_once": 1, "at_least_once": 2, "exactly_once": 3}.get(producer_semantics, 4)
    consumer_semantics_code = {"at_most_once": 1, "at_least_once": 2, "exactly_once_kafka": 3}.get(consumer_semantics, 4)
    consumer_risk_code = {"at_most_once": 1, "at_least_once": 2, "exactly_once_kafka": 3}.get(consumer_semantics, 4)
    producer_acks_code = {"0": 0, "1": 1, "all": 2}.get(producer_acks, 2)
    consumer_commit_strategy_code = {"before": 1, "after": 2}.get(consumer_commit_strategy, 3)

    reject_rate_percent = round((rejected / processed) * 100, 2) if processed > 0 else 0.0
    validation_rate_percent = round((validated / processed) * 100, 2) if processed > 0 else 0.0
    persistence_gap_valid = max(validated - persisted_valid, 0)
    persistence_gap_rejected = max(rejected - persisted_rejected, 0)

    sample = build_snapshot_sample(
        generator_detail,
        pix_validator_detail,
        active_processing_detail,
        pix_outcome_publisher_detail,
        persister_valid_detail,
        persister_rejected_detail,
        kafka_detail,
        postgres_detail,
    )

    rate_metrics = build_rate_metrics(sample)
    oldest_lag_seconds = estimate_oldest_lag_seconds(kafka_detail, rate_metrics)

    return {
        "counts": {
            "generated": generated,
            "processed": processed,
            "validated": validated,
            "rejected": rejected,
            "persisted_valid": persisted_valid,
            "persisted_rejected": persisted_rejected,
            "db_validated_count": db_validated_count,
            "db_rejected_count": db_rejected_count,
            "outcome_count": logical_outcome_count,
            "pending_result_count": pending_result_count,
            "raw_topic_end_offsets": topic_end_offsets("simulpix.transactions.raw"),
            "checked_topic_end_offsets": logical_checked_count,
            "decision_topic_end_offsets": logical_decision_count,
            "validated_topic_end_offsets": logical_validated_count,
            "rejected_topic_end_offsets": logical_rejected_count,
            "retry_topic_end_offsets": topic_end_offsets("simulpix.transactions.retry"),
            "outcome_topic_end_offsets": topic_end_offsets("simulpix.transactions.outcome"),
            "pix_validator_lag_total": safe_int((consumer_groups.get("pix-validator") or {}).get("lag_total")),
            "pix_decision_engine_lag_total": safe_int((consumer_groups.get("pix-decision-engine") or {}).get("lag_total")),
            "pix_outcome_publisher_lag_total": safe_int((consumer_groups.get("pix-outcome-publisher") or {}).get("lag_total")),
            "persister_valid_lag_total": safe_int((consumer_groups.get("persister-valid") or {}).get("lag_total")),
            "persister_rejected_lag_total": safe_int((consumer_groups.get("persister-rejected") or {}).get("lag_total")),
            "broker_count": safe_int((kafka_detail or {}).get("broker_count")),
            "validated_sla_breaches": safe_int((postgres_detail or {}).get("validated_sla_breaches")),
            "rejected_sla_breaches": safe_int((postgres_detail or {}).get("rejected_sla_breaches")),
            "validated_within_sla_count": safe_int((postgres_detail or {}).get("validated_within_sla_count")),
            "rejected_within_sla_count": safe_int((postgres_detail or {}).get("rejected_within_sla_count")),
            "postgres_active_connections": safe_int((postgres_detail or {}).get("active_connections")),
            "postgres_database_size_bytes": safe_int((postgres_detail or {}).get("database_size_bytes")),
            "postgres_validated_table_size_bytes": safe_int((postgres_detail or {}).get("validated_table_size_bytes")),
            "postgres_rejected_table_size_bytes": safe_int((postgres_detail or {}).get("rejected_table_size_bytes")),
            "producer_semantics_code": producer_semantics_code,
            "producer_risk_code": producer_risk_code,
            "producer_acks_code": producer_acks_code,
            "producer_retries": producer_retries,
            "consumer_semantics_code": consumer_semantics_code,
            "consumer_risk_code": consumer_risk_code,
            "consumer_commit_strategy_code": consumer_commit_strategy_code,
            "consumer_auto_commit_code": 1 if consumer_auto_commit else 0,
            "consumer_commit_batch_size": consumer_commit_batch_size,
            "validator_transactional_code": 1 if validator_is_transactional else 0,
            "decision_transactional_code": 1 if decision_is_transactional else 0,
            "outcome_transactional_code": 1 if outcome_is_transactional else 0,
            "kafka_exactly_once_chain_count": (1 if validator_is_transactional else 0) + (1 if decision_is_transactional else 0) + (1 if outcome_is_transactional else 0),
        },
        "ratios": {
            "reject_rate_percent": reject_rate_percent,
            "validation_rate_percent": validation_rate_percent,
        },
        "gaps": {
            "persistence_gap_valid": persistence_gap_valid,
            "persistence_gap_rejected": persistence_gap_rejected,
        },
        "timings": {
            "avg_validation_latency_ms": (postgres_detail or {}).get("avg_validation_latency_ms"),
            "avg_rejection_latency_ms": (postgres_detail or {}).get("avg_rejection_latency_ms"),
            "estimated_oldest_lag_seconds": oldest_lag_seconds,
            "seconds_since_last_validated": (postgres_detail or {}).get("seconds_since_last_validated"),
            "seconds_since_last_rejected": (postgres_detail or {}).get("seconds_since_last_rejected"),
        },
        "rates": rate_metrics,
    }
