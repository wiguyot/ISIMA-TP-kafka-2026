#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

LIMIT="${1:-5}"
REFERENCE_FILE="${SIMULPIX_REFERENCE_FILE:-infra/reference/reference-clients.json}"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

tmp_db="$(mktemp)"
tmp_fixed="$(mktemp)"

cleanup() {
  rm -f "$tmp_db" "$tmp_fixed"
}
trap cleanup EXIT

docker_compose exec -T postgres psql -U simulpix -d simulpix -At -F $'\t' -c "
select rejection_reason, original_payload::text
from rejected_transactions
order by id desc
limit ${LIMIT};
" > "$tmp_db"

if [ ! -s "$tmp_db" ]; then
  echo "[simulpix] no rejected payload available for corrected replay"
  exit 0
fi

python3 - "$tmp_db" "$tmp_fixed" "$REFERENCE_FILE" <<'PY'
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

source_path = Path(sys.argv[1])
target_path = Path(sys.argv[2])
reference_path = Path(sys.argv[3])

clients = json.loads(reference_path.read_text(encoding="utf-8"))
pix_to_tax = {client["pix_client_id"]: client["tax_id"] for client in clients}
tax_to_pix = {client["tax_id"]: client["pix_client_id"] for client in clients}

fixed = []
attempts: dict[str, int] = {}
for raw_line in source_path.read_text(encoding="utf-8").splitlines():
    if not raw_line.strip():
        continue
    reason, payload_raw = raw_line.split("\t", 1)
    message = json.loads(payload_raw)
    source_transaction_id = message.get("source_transaction_id") or message.get("transaction_id")
    attempts[source_transaction_id] = attempts.get(source_transaction_id, int(message.get("retry_attempt", 0) or 0)) + 1
    retry_attempt = attempts[source_transaction_id]
    reasons = [item.strip() for item in reason.split(",") if item.strip()]
    changed = False

    for item in reasons:
        if item == "amount_must_be_positive":
            amount = float(message.get("amount", 0) or 0)
            message["amount"] = abs(amount) if amount != 0 else 10.0
            changed = True
        elif item == "missing_beneficiary_tax_id":
            beneficiary_pix = message.get("beneficiary_pix_client_id")
            if beneficiary_pix in pix_to_tax:
                message["beneficiary_tax_id"] = pix_to_tax[beneficiary_pix]
                changed = True
        elif item == "beneficiary_tax_pix_mismatch":
            beneficiary_tax = message.get("beneficiary_tax_id")
            if beneficiary_tax in tax_to_pix:
                message["beneficiary_pix_client_id"] = tax_to_pix[beneficiary_tax]
                changed = True
        elif item == "emitter_tax_pix_mismatch":
            emitter_tax = message.get("emitter_tax_id")
            if emitter_tax in tax_to_pix:
                message["emitter_pix_client_id"] = tax_to_pix[emitter_tax]
                changed = True
        elif item == "invalid_initial_status":
            message["status"] = "RECEIVED"
            changed = True
        elif item == "accounts_must_differ":
            beneficiary = message.get("beneficiary_account_id")
            emitter = message.get("emitter_account_id")
            if beneficiary and emitter and beneficiary == emitter:
                message["beneficiary_account_id"] = f"{beneficiary}-retry"
                changed = True

    if changed:
        decision_sla_seconds = max(int(message.get("decision_sla_seconds", 5) or 5), 1)
        event_time = datetime.now(timezone.utc)
        message["source_transaction_id"] = source_transaction_id
        message["retry_attempt"] = retry_attempt
        message["event_time"] = event_time.isoformat()
        message["decision_sla_seconds"] = decision_sla_seconds
        message["decision_deadline"] = (event_time + timedelta(seconds=decision_sla_seconds)).isoformat()
        message["transaction_id"] = f"{source_transaction_id}-retry-{retry_attempt:03d}"
        message["trace_id"] = f"{message.get('trace_id', source_transaction_id)}-retry-{retry_attempt:03d}"
        message["scenario_type"] = f"{message.get('scenario_type', 'unknown')}_replayed_corrected"
        fixed.append(json.dumps(message, ensure_ascii=False))

target_path.write_text("\n".join(fixed) + ("\n" if fixed else ""), encoding="utf-8")
print(len(fixed))
PY

count="$(wc -l < "$tmp_fixed" | tr -d ' ')"
if [ "$count" = "0" ]; then
  echo "[simulpix] no rejected payload could be corrected automatically"
  exit 0
fi

docker_compose exec -T kafka-1 sh -lc '
  producer_bin="${KAFKA_PRODUCER_BIN:-/opt/kafka/bin/kafka-console-producer.sh}"
  client_config="${KAFKA_CLIENT_CONFIG:-/config/client.properties}"
  "$producer_bin" --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --producer.config "$client_config" --topic simulpix.transactions.retry > /dev/null
' < "$tmp_fixed"

echo "[simulpix] replayed ${count} corrected payload(s) to simulpix.transactions.retry"
