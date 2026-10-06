#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

LIMIT="${1:-10}"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

tmp_file="$(mktemp)"
cleanup() {
  rm -f "$tmp_file"
}
trap cleanup EXIT

docker_compose exec -T postgres psql -U simulpix -d simulpix -At -c "
select original_payload::text
from rejected_transactions
order by id desc
limit ${LIMIT};
" > "$tmp_file"

if [ ! -s "$tmp_file" ]; then
  echo "[simulpix] no rejected payload available for replay"
  exit 0
fi

python3 - "$tmp_file" <<'PY'
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

target = Path(sys.argv[1])
rewritten: list[str] = []
for raw_line in target.read_text(encoding="utf-8").splitlines():
    if not raw_line.strip():
        continue
    message = json.loads(raw_line)
    decision_sla_seconds = max(int(message.get("decision_sla_seconds", 5) or 5), 1)
    event_time = datetime.now(timezone.utc)
    message["event_time"] = event_time.isoformat()
    message["decision_sla_seconds"] = decision_sla_seconds
    message["decision_deadline"] = (event_time + timedelta(seconds=decision_sla_seconds)).isoformat()
    rewritten.append(json.dumps(message, ensure_ascii=False))

target.write_text("\n".join(rewritten) + ("\n" if rewritten else ""), encoding="utf-8")
PY

docker_compose exec -T kafka-1 sh -lc '
  producer_bin="${KAFKA_PRODUCER_BIN:-/opt/kafka/bin/kafka-console-producer.sh}"
  client_config="${KAFKA_CLIENT_CONFIG:-/config/client.properties}"
  "$producer_bin" --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --producer.config "$client_config" --topic simulpix.transactions.retry > /dev/null
' < "$tmp_file"

echo "[simulpix] replayed $(wc -l < "$tmp_file" | tr -d " ") rejected payload(s) to simulpix.transactions.retry"
