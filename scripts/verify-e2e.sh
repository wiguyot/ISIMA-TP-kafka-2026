#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

BASE_URL="${SIMULPIX_SERVICE_HEALTH_BASE_URL:-http://127.0.0.1:8082}"
DETAILS_URL="${BASE_URL%/}/health/details"
TMP_JSON="${TMPDIR:-/tmp}/simulpix-e2e-details.json"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
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

wait_for_metrics() {
  mode="$1"
  expected="$2"
  attempts="${3:-120}"
  while [ "$attempts" -gt 0 ]; do
    if curl -fsS "$DETAILS_URL" >"$TMP_JSON" 2>/dev/null && python3 - "$TMP_JSON" "$mode" "$expected" <<'PY'
import json
import sys
from pathlib import Path

raw_payload = Path(sys.argv[1]).read_text(encoding="utf-8")
payload, _ = json.JSONDecoder().raw_decode(raw_payload)
mode = sys.argv[2]
expected = int(sys.argv[3])
services = payload.get("services", {})
if any(str(services.get(name)) != "ok" for name in ("kafka", "pix-validator", "pix-decision-engine", "pix-outcome-publisher", "persister-valid", "persister-rejected", "postgres")):
    raise SystemExit(1)
counts = ((payload.get("metrics") or {}).get("counts") or {})
topics = (((payload.get("details") or {}).get("kafka") or {}).get("topics") or {})

def value(name: str) -> int:
    raw = counts.get(name, 0)
    return int(raw or 0)

def topic_total(name: str) -> int:
    topic = topics.get(name) or {}
    raw = topic.get("end_offsets_total", 0)
    return int(raw or 0)

if mode == "nominal":
    ok = (
        value("generated") == expected
        and value("processed") == expected
        and value("validated") == expected
        and value("rejected") == 0
        and value("db_validated_count") == expected
        and value("db_rejected_count") == 0
        and topic_total("simulpix.transactions.outcome") == expected
    )
elif mode == "errors":
    ok = (
        value("generated") == expected
        and value("processed") == expected
        and value("validated") == 0
        and value("rejected") == expected
        and value("db_validated_count") == 0
        and value("db_rejected_count") == expected
        and topic_total("simulpix.transactions.outcome") == expected
    )
elif mode == "replay":
    ok = (
        value("db_validated_count") == expected
        and value("db_rejected_count") == expected
        and topic_total("simulpix.transactions.outcome") == expected * 2
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

assert_postgres_invariants() {
  result="$(docker_compose exec -T postgres psql -U simulpix -d simulpix -At -F $'\t' -c "
select
  (select count(*) from validated_transactions),
  (select count(*) from rejected_transactions),
  (select count(*) - count(distinct rejection_fingerprint) from rejected_transactions);
")"
  validated="$(printf '%s' "$result" | cut -f1)"
  rejected="$(printf '%s' "$result" | cut -f2)"
  duplicate_fingerprints="$(printf '%s' "$result" | cut -f3)"

  if [ "$validated" != "8" ] || [ "$rejected" != "8" ] || [ "$duplicate_fingerprints" != "0" ]; then
    echo "[simulpix] unexpected PostgreSQL invariants: validated=$validated rejected=$rejected duplicate_fingerprints=$duplicate_fingerprints" >&2
    exit 1
  fi
}

echo "[simulpix] ensuring stack is running"
if ! wait_for_http "${BASE_URL%/}/health" 5; then
  ./start.sh >/dev/null
  wait_for_http "${BASE_URL%/}/health" 120
fi

echo "[simulpix] resetting platform through Docker scripts"
SIMULPIX_KEEP_HEALTH_RUNNING=1 ./scripts/reset-scenario.sh >/dev/null
wait_for_http "${BASE_URL%/}/health" 60

echo "[simulpix] running nominal end-to-end scenario"
SIMULPIX_KEEP_HEALTH_RUNNING=1 ./scripts/run-scenario.sh nominal 12 12 >/dev/null
wait_for_metrics "nominal" 12 120

echo "[simulpix] running rejection scenario"
SIMULPIX_KEEP_HEALTH_RUNNING=1 ./scripts/run-scenario.sh errors_simple 8 20 >/dev/null
wait_for_metrics "errors" 8 120

echo "[simulpix] replaying corrected rejected events"
./scripts/replay-corrected.sh 8 >/dev/null
wait_for_metrics "replay" 8 120

echo "[simulpix] checking PostgreSQL invariants"
assert_postgres_invariants

echo "[simulpix] end-to-end Docker verification complete"
