import json
import os
import time
from datetime import datetime, timezone
from typing import Any

from app_state import CONTROL_STATE, HISTORY_CACHE, HISTORY_MAX_POINTS
from alerts import build_alerts, compute_global_status
from metrics import append_history_snapshot, build_business_metrics
from probes import (
    build_kafka_details,
    enrich_kafka_replication,
    fetch_json,
    http_probe,
    load_network_state,
    load_replication_snapshot,
    load_run_state,
    query_postgres_metrics,
    safe_int,
    tcp_probe,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_health_payload() -> dict[str, Any]:
    kafka_host = os.getenv("SIMULPIX_KAFKA_HOST", "kafka")
    kafka_port = int(os.getenv("SIMULPIX_KAFKA_PORT", "29092"))
    kafka_bootstrap_servers = os.getenv("SIMULPIX_KAFKA_BOOTSTRAP_SERVERS", f"{kafka_host}:{kafka_port}")
    postgres_host = os.getenv("SIMULPIX_POSTGRES_HOST", "postgres")
    postgres_port = int(os.getenv("SIMULPIX_POSTGRES_PORT", "5432"))
    postgres_database_url = os.getenv("SIMULPIX_DATABASE_URL", "postgresql://simulpix:simulpix@postgres:5432/simulpix")
    influxdb_url = os.getenv("SIMULPIX_INFLUXDB_HEALTH_URL", "http://influxdb:8086/health")
    grafana_url = os.getenv("SIMULPIX_GRAFANA_HEALTH_URL", "http://grafana:3000/api/health")
    generator_url = os.getenv("SIMULPIX_GENERATOR_URL", "http://generator:8081/health")
    pix_scenario_planner_url = os.getenv("SIMULPIX_PIX_SCENARIO_PLANNER_URL", "http://pix-scenario-planner:8090/health")
    pix_traffic_shaper_url = os.getenv("SIMULPIX_PIX_TRAFFIC_SHAPER_URL", "http://pix-traffic-shaper:8089/health")
    pix_validator_url = os.getenv("SIMULPIX_PIX_VALIDATOR_URL", "http://pix-validator:8086/health")
    pix_decision_engine_url = os.getenv("SIMULPIX_PIX_DECISION_ENGINE_URL", "http://pix-decision-engine:8087/health")
    pix_outcome_publisher_url = os.getenv("SIMULPIX_PIX_OUTCOME_PUBLISHER_URL", "http://pix-outcome-publisher:8088/health")
    persister_valid_url = os.getenv("SIMULPIX_PERSISTER_VALID_URL", "http://persister-valid:8084/health")
    persister_rejected_url = os.getenv("SIMULPIX_PERSISTER_REJECTED_URL", "http://persister-rejected:8085/health")
    run_state_path = os.getenv("SIMULPIX_RUN_STATE_PATH", "/runtime/current-run.json")
    network_state_path = os.getenv("SIMULPIX_NETWORK_STATE_PATH", "/runtime/current-network.json")
    replication_snapshot_path = os.getenv("SIMULPIX_OBSERVABILITY_REPLICATION_FILE", "/runtime/observability/latest-replication.json")
    topics = [
        os.getenv("SIMULPIX_TOPIC_RAW", "simulpix.transactions.raw"),
        os.getenv("SIMULPIX_TOPIC_CHECKED", "simulpix.transactions.checked"),
        os.getenv("SIMULPIX_TOPIC_DECISION", "simulpix.transactions.decision"),
        os.getenv("SIMULPIX_TOPIC_VALIDATED", "simulpix.transactions.validated"),
        os.getenv("SIMULPIX_TOPIC_REJECTED", "simulpix.transactions.rejected"),
        os.getenv("SIMULPIX_TOPIC_RETRY", "simulpix.transactions.retry"),
        os.getenv("SIMULPIX_TOPIC_OUTCOME", "simulpix.transactions.outcome"),
    ]
    run_state = load_run_state(run_state_path) or {}

    generator_detail = fetch_json(generator_url)
    pix_scenario_planner_detail = fetch_json(pix_scenario_planner_url)
    pix_traffic_shaper_detail = fetch_json(pix_traffic_shaper_url)
    pix_validator_detail = fetch_json(pix_validator_url)
    pix_decision_engine_detail = fetch_json(pix_decision_engine_url)
    pix_outcome_publisher_detail = fetch_json(pix_outcome_publisher_url)
    persister_valid_detail = fetch_json(persister_valid_url)
    persister_rejected_detail = fetch_json(persister_rejected_url)
    postgres_detail = query_postgres_metrics(postgres_database_url)
    influxdb_detail = fetch_json(influxdb_url)
    grafana_detail = fetch_json(grafana_url)
    network_detail = load_network_state(network_state_path) or {
        "active": 0,
        "profile": "none",
        "target_service": "none",
        "interface": "eth0",
        "delay_ms": 0,
        "jitter_ms": 0,
        "loss_percent": 0,
        "rate_kbit": 0,
        "description": "none",
        "applied_at": None,
    }
    network_detail["network_active"] = safe_int(network_detail.get("active"))
    network_detail["network_delay_ms"] = safe_int(network_detail.get("delay_ms"))
    network_detail["network_jitter_ms"] = safe_int(network_detail.get("jitter_ms"))
    network_detail["network_loss_percent"] = safe_int(network_detail.get("loss_percent"))
    network_detail["network_rate_kbit"] = safe_int(network_detail.get("rate_kbit"))
    replication_snapshot = load_replication_snapshot(replication_snapshot_path)
    groups = {
        "pix-validator": (pix_validator_detail or {}).get("consumer_group")
        or run_state.get("pix_validator_group")
        or os.getenv("SIMULPIX_PIX_VALIDATOR_GROUP", "simulpix-validator-v1"),
        "pix-decision-engine": (pix_decision_engine_detail or {}).get("consumer_group")
        or run_state.get("pix_decision_engine_group")
        or os.getenv("SIMULPIX_PIX_DECISION_ENGINE_GROUP", "simulpix-decision-engine-v1"),
        "pix-outcome-publisher": (pix_outcome_publisher_detail or {}).get("consumer_group")
        or run_state.get("pix_outcome_publisher_group")
        or os.getenv("SIMULPIX_PIX_OUTCOME_PUBLISHER_GROUP", "simulpix-outcome-publisher-v1"),
        "persister-valid": (persister_valid_detail or {}).get("consumer_group")
        or run_state.get("persister_valid_group")
        or os.getenv("SIMULPIX_PERSISTER_VALID_GROUP", "simulpix-persister-valid-v1"),
        "persister-rejected": (persister_rejected_detail or {}).get("consumer_group")
        or run_state.get("persister_rejected_group")
        or os.getenv("SIMULPIX_PERSISTER_REJECTED_GROUP", "simulpix-persister-rejected-v1"),
    }
    kafka_status = tcp_probe(kafka_host, kafka_port)
    kafka_detail = None
    if kafka_status == "ok":
        try:
            kafka_detail = build_kafka_details(kafka_bootstrap_servers, topics, groups)
        except Exception as exc:
            kafka_status = "degraded"
            kafka_detail = {"status": "error", "error": repr(exc)}

    services = {
        "kafka": kafka_status,
        "pix-scenario-planner": http_probe(pix_scenario_planner_url),
        "pix-traffic-shaper": http_probe(pix_traffic_shaper_url),
        "generator": http_probe(generator_url),
    }
    services["pix-validator"] = http_probe(pix_validator_url)
    services["pix-decision-engine"] = http_probe(pix_decision_engine_url)
    services["pix-outcome-publisher"] = http_probe(pix_outcome_publisher_url)
    services["persister-valid"] = http_probe(persister_valid_url)
    services["persister-rejected"] = http_probe(persister_rejected_url)
    services["postgres"] = tcp_probe(postgres_host, postgres_port)
    services["influxdb"] = http_probe(influxdb_url)
    services["grafana"] = http_probe(grafana_url)
    business_metrics = build_business_metrics(
        generator_detail,
        pix_validator_detail,
        pix_decision_engine_detail,
        pix_outcome_publisher_detail,
        persister_valid_detail,
        persister_rejected_detail,
        kafka_detail,
        postgres_detail,
    )
    kafka_detail = enrich_kafka_replication(kafka_detail, replication_snapshot, business_metrics.get("rates"))
    alerts = build_alerts(services, kafka_detail, business_metrics, network_detail)
    global_status = compute_global_status(services, alerts)
    snapshot_timestamp = datetime.now(timezone.utc).isoformat()
    history = append_history_snapshot(snapshot_timestamp, business_metrics, kafka_detail, alerts)

    return {
        "status": global_status,
        "service": os.getenv("SIMULPIX_SERVICE_NAME", "service-health"),
        "timestamp": snapshot_timestamp,
        "run": {
            "run_id": run_state.get("run_id") or os.getenv("SIMULPIX_RUN_ID", "default"),
            "groups": groups,
        },
        "alerts": alerts,
        "control": CONTROL_STATE,
        "history": history,
        "metrics": business_metrics,
        "services": services,
        "details": {
            "kafka": kafka_detail,
            "postgres": postgres_detail,
            "generator": generator_detail,
            "pix-scenario-planner": pix_scenario_planner_detail,
            "pix-traffic-shaper": pix_traffic_shaper_detail,
            "pix-validator": pix_validator_detail,
            "pix-decision-engine": pix_decision_engine_detail,
            "pix-outcome-publisher": pix_outcome_publisher_detail,
            "persister-valid": persister_valid_detail,
            "persister-rejected": persister_rejected_detail,
            "influxdb": influxdb_detail,
            "grafana": grafana_detail,
            "network": network_detail,
        },
    }


def build_fallback_payload(error: Exception, route: str) -> dict[str, Any]:
    timestamp = utc_now_iso()
    return {
        "status": "degraded",
        "service": os.getenv("SIMULPIX_SERVICE_NAME", "service-health"),
        "timestamp": timestamp,
        "run": {
            "run_id": os.getenv("SIMULPIX_RUN_ID", "default"),
            "groups": {},
        },
        "alerts": [
            {
                "level": "error",
                "kind": "service_health_exception",
                "message": f"service-health request failed on {route}: {repr(error)}",
            }
        ],
        "control": CONTROL_STATE,
        "history": HISTORY_CACHE[-HISTORY_MAX_POINTS:],
        "metrics": {"counts": {}, "ratios": {}, "gaps": {}, "rates": None},
        "services": {
            "kafka": "unknown",
            "generator": "unknown",
            "pix-validator": "unknown",
            "pix-decision-engine": "unknown",
            "pix-outcome-publisher": "unknown",
            "persister-valid": "unknown",
            "persister-rejected": "unknown",
            "postgres": "unknown",
            "influxdb": "unknown",
            "grafana": "unknown",
        },
        "details": {
            "kafka": {"status": "error", "error": repr(error)},
            "postgres": {"status": "error", "error": repr(error)},
            "generator": None,
            "pix-validator": None,
            "pix-decision-engine": None,
            "pix-outcome-publisher": None,
            "persister-valid": None,
            "persister-rejected": None,
            "influxdb": None,
            "grafana": None,
            "network": {"active": 0, "network_active": 0, "profile": "none", "target_service": "none"},
        },
        "ui": {"message": "error", "action": route},
    }


def get_log_interval_seconds() -> float:
    value = os.getenv("SIMULPIX_HEALTH_LOG_INTERVAL_SECONDS", "60")
    try:
        return max(float(value), 5.0)
    except ValueError:
        return 60.0


def build_log_summary(payload: dict[str, Any]) -> dict[str, Any]:
    counts = (payload.get("metrics") or {}).get("counts", {})
    ratios = (payload.get("metrics") or {}).get("ratios", {})
    services = payload.get("services") or {}
    network = (payload.get("details") or {}).get("network") or {}
    return {
        "timestamp": payload.get("timestamp"),
        "event": "service_health_summary",
        "status": payload.get("status"),
        "alerts": len(payload.get("alerts") or []),
        "services": services,
        "generated": counts.get("generated", 0),
        "processed": counts.get("processed", 0),
        "validated": counts.get("validated", 0),
        "rejected": counts.get("rejected", 0),
        "reject_rate_percent": ratios.get("reject_rate_percent", 0.0),
        "control_status": (payload.get("control") or {}).get("status"),
        "network_active": safe_int(network.get("active")),
        "network_profile": network.get("profile"),
        "network_target_service": network.get("target_service"),
    }


def emit_periodic_health_logs() -> None:
    interval_seconds = get_log_interval_seconds()
    while True:
        try:
            payload = build_health_payload()
            print(json.dumps(build_log_summary(payload)), flush=True)
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "timestamp": utc_now_iso(),
                        "event": "service_health_summary_failed",
                        "error": repr(exc),
                    }
                ),
                flush=True,
            )
        time.sleep(interval_seconds)
