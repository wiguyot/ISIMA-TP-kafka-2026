#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

BASE_URL="${SIMULPIX_SERVICE_HEALTH_BASE_URL:-http://127.0.0.1:8082}"
DETAILS_URL="${BASE_URL%/}/health/details"
TMP_JSON="${TMPDIR:-/tmp}/simulpix-tp-parts-details.json"

ensure_stack_running() {
  echo "[simulpix] ensuring stack is running for TP verification"
  if ! wait_for_http "${BASE_URL%/}/health" 5; then
    ./start.sh >/dev/null
    wait_for_http "${BASE_URL%/}/health" 120
  fi
}

wait_for_http() {
  url="$1"
  attempts="${2:-90}"
  while [ "$attempts" -gt 0 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

wait_for_predicate() {
  mode="$1"
  attempts="${2:-120}"
  while [ "$attempts" -gt 0 ]; do
    if curl -fsS "$DETAILS_URL" >"$TMP_JSON" 2>/dev/null && python3 - "$TMP_JSON" "$mode" <<'PY'
import json
import sys
from pathlib import Path

raw_payload = Path(sys.argv[1]).read_text(encoding="utf-8")
payload, _ = json.JSONDecoder().raw_decode(raw_payload)
mode = sys.argv[2]
metrics = payload.get("metrics") or {}
counts = metrics.get("counts") or {}
ratios = metrics.get("ratios") or {}
timings = metrics.get("timings") or {}
details = payload.get("details") or {}
generator = details.get("generator") or {}
kafka = details.get("kafka") or {}
network = details.get("network") or {}
postgres = details.get("postgres") or {}
services = payload.get("services") or {}

def iv(name):
    return int(counts.get(name, 0) or 0)

def fv(name):
    value = timings.get(name)
    return None if value in (None, "") else float(value)

ok = False

if mode == "part1":
    ok = (
        iv("generated") == 100
        and iv("processed") == 100
        and iv("validated") == 100
        and iv("rejected") == 0
        and iv("db_validated_count") == 100
        and iv("outcome_count") == 100
        and str(kafka.get("lag_alert")) == "ok"
    )
elif mode == "part2":
    ok = (
        iv("generated") == 20
        and iv("processed") == 20
        and iv("validated") == 0
        and iv("rejected") == 20
        and iv("db_rejected_count") == 20
        and float(ratios.get("reject_rate_percent", 0.0)) >= 99.0
    )
elif mode == "part3_active":
    ok = (
        iv("generated") == 120
        and 0 < iv("processed") < 120
        and fv("estimated_oldest_lag_seconds") not in (None, 0.0)
        and int((((details.get("kafka") or {}).get("consumer_groups") or {}).get("pix-decision-engine") or {}).get("lag_total", 0) or 0) > 0
    )
elif mode == "part3_final":
    ok = (
        iv("generated") == 120
        and iv("processed") == 120
        and iv("rejected") >= 100
        and iv("rejected_sla_breaches") >= 100
        and str(kafka.get("lag_alert")) == "ok"
    )
elif mode == "part4":
    ok = (
        iv("generated") == 80
        and iv("processed") == 80
        and iv("validated") == 80
        and iv("rejected") == 0
        and str(services.get("kafka")) == "ok"
        and int((kafka.get("broker_count") or 0)) == 3
        and int((((kafka.get("replication") or {}).get("under_replicated_partitions_total")) or 0) == 0)
    )
elif mode == "part5":
    ok = (
        str(generator.get("scenario_type")) == "football_match_peak"
        and str(generator.get("current_phase")) in {"baseline", "peak"}
        and int(generator.get("messages_sent", 0) or 0) > 0
        and iv("generated") > 0
        and iv("processed") == iv("generated")
        and iv("validated") == iv("generated")
        and iv("rejected") == 0
    )
elif mode == "part6":
    pix_decision_engine_lag_total = int((((details.get("kafka") or {}).get("consumer_groups") or {}).get("pix-decision-engine") or {}).get("lag_total", 0) or 0)
    ok = (
        int(network.get("active", 0) or 0) == 1
        and str(network.get("profile")) == "kafka_latency"
        and str(network.get("target_service")) == "pix-decision-engine"
        and pix_decision_engine_lag_total > 0
        and (
            iv("rejected") > 0
            or iv("rejected_sla_breaches") > 0
            or fv("estimated_oldest_lag_seconds") not in (None, 0.0)
        )
    )
else:
    raise SystemExit(2)

raise SystemExit(0 if ok else 1)
PY
    then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 2
  done
  return 1
}

reset_platform() {
  SIMULPIX_KEEP_HEALTH_RUNNING=1 ./scripts/reset-scenario.sh >/dev/null
  /bin/sh ./scripts/network-reset.sh >/dev/null 2>&1 || true
  wait_for_http "${BASE_URL%/}/health" 60
}

echo "[simulpix] verifying TP parts 1 to 6"
ensure_stack_running

echo "[simulpix] part 1"
reset_platform
SIMULPIX_KEEP_HEALTH_RUNNING=1 ./scripts/run-scenario.sh nominal 100 10 >/dev/null
wait_for_predicate part1 120

echo "[simulpix] part 2"
SIMULPIX_KEEP_HEALTH_RUNNING=1 ./scripts/run-scenario.sh errors_simple 20 20 >/dev/null
wait_for_predicate part2 120

echo "[simulpix] part 3"
SIMULPIX_KEEP_HEALTH_RUNNING=1 ./scripts/run-scenario.sh consumer_lag 120 80 >/dev/null
wait_for_predicate part3_active 90
wait_for_predicate part3_final 180

echo "[simulpix] part 4"
SIMULPIX_KEEP_HEALTH_RUNNING=1 ./scripts/run-scenario.sh replication_lag 80 80 >/dev/null
sleep 10
wait_for_predicate part4 120

echo "[simulpix] part 5"
SIMULPIX_KEEP_HEALTH_RUNNING=1 \
SIMULPIX_MATCH_COUNT_OVERRIDE=1 \
SIMULPIX_STADIUM_CAPACITY_OVERRIDE=1000 \
SIMULPIX_PIX_USAGE_RATE_OVERRIDE=0.05 \
SIMULPIX_PEAK_SHARE_OVERRIDE=0.30 \
SIMULPIX_PEAK_WINDOW_MINUTES_OVERRIDE=15 \
SIMULPIX_TOTAL_WINDOW_MINUTES_OVERRIDE=240 \
SIMULPIX_TIME_COMPRESSION_FACTOR_OVERRIDE=10 \
./scripts/run-scenario.sh football_match_peak >/dev/null
wait_for_predicate part5 120

echo "[simulpix] part 6"
SIMULPIX_KEEP_HEALTH_RUNNING=1 ./scripts/run-scenario.sh nominal 200 40 >/dev/null
sleep 5
/bin/sh ./scripts/network-perturb.sh kafka_latency pix-decision-engine >/dev/null
wait_for_predicate part6 120
/bin/sh ./scripts/network-reset.sh >/dev/null

echo "[simulpix] TP validation complete"
