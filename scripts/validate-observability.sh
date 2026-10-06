#!/bin/zsh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

wait_for_http() {
  url="$1"
  attempts="${2:-60}"
  auth_header="${3:-}"
  while [ "$attempts" -gt 0 ]; do
    if python3 - "$url" "$auth_header" <<'PY'
import base64
import sys
import urllib.request

url = sys.argv[1]
auth = sys.argv[2]
request = urllib.request.Request(url)
if auth:
    token = base64.b64encode(auth.encode("utf-8")).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
with urllib.request.urlopen(request, timeout=3) as response:
    payload = response.read()
    raise SystemExit(0 if payload else 1)
PY
    then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

wait_for_measurement() {
  measurement="$1"
  attempts="${2:-24}"
  while [ "$attempts" -gt 0 ]; do
    if docker_compose exec -T influxdb influx query "from(bucket:\"simulpix_observability\") |> range(start: -10m) |> filter(fn: (r) => r._measurement == \"$measurement\") |> limit(n: 1)" >"/tmp/${measurement}.txt" 2>/dev/null; then
      if python3 -c 'from pathlib import Path; import sys; raw=Path(sys.argv[1]).read_text(); measurement=sys.argv[2]; raise SystemExit(0 if measurement in raw else 1)' "/tmp/${measurement}.txt" "$measurement"; then
        return 0
      fi
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

echo "[simulpix] validating service-health"
wait_for_http http://127.0.0.1:8082/health 60

echo "[simulpix] validating InfluxDB"
if ! wait_for_http http://127.0.0.1:8086/health 10; then
  docker_compose exec -T influxdb sh -lc 'curl -fsS http://127.0.0.1:8086/health >/dev/null'
fi

echo "[simulpix] validating Grafana"
if ! wait_for_http http://127.0.0.1:3000/api/health 10 admin:adminpass; then
  docker_compose exec -T grafana sh -lc 'wget -qO- http://127.0.0.1:3000/api/health --header="Authorization: Basic YWRtaW46YWRtaW5wYXNz" >/dev/null || curl -fsS -u admin:adminpass http://127.0.0.1:3000/api/health >/dev/null'
fi

echo "[simulpix] validating provisioned dashboards"
if ! curl -fsS -u admin:adminpass 'http://127.0.0.1:3000/api/search?query=Simul-Pix' >/tmp/simulpix-grafana-dashboards.json 2>/dev/null; then
  docker_compose exec -T grafana sh -lc 'wget -qO- "http://127.0.0.1:3000/api/search?query=Simul-Pix" --header="Authorization: Basic YWRtaW46YWRtaW5wYXNz" || curl -fsS -u admin:adminpass "http://127.0.0.1:3000/api/search?query=Simul-Pix"' >/tmp/simulpix-grafana-dashboards.json
fi
cat /tmp/simulpix-grafana-dashboards.json
python3 -c 'import json,sys; data=json.load(open("/tmp/simulpix-grafana-dashboards.json")); titles={item["title"] for item in data}; expected={"Simul-Pix - General Dashboard","Simul-Pix - Kafka Dashboard","Simul-Pix - Persistence Dashboard","Simul-Pix Incidents"}; missing=sorted(expected-titles); print("dashboards=", sorted(titles)); raise SystemExit(1 if missing else 0)'

echo "[simulpix] validating InfluxDB measurements"
docker_compose exec -T influxdb influx query 'from(bucket:"simulpix_observability") |> range(start: -5m) |> limit(n: 5)' >/tmp/simulpix-influx-sample.txt
sed -n '1,80p' /tmp/simulpix-influx-sample.txt
for measurement in simulpix_platform simulpix_semantics kafka_broker kafka_replication_partition kafka_consumer_group kafka_topic postgres_runtime; do
  wait_for_measurement "$measurement" 24
  sed -n '1,20p' "/tmp/${measurement}.txt"
  python3 -c 'from pathlib import Path; import sys; raw=Path(sys.argv[1]).read_text(); measurement=sys.argv[2]; print("measurement=", measurement); raise SystemExit(0 if measurement in raw else 1)' "/tmp/${measurement}.txt" "$measurement"
done

echo "[simulpix] observability validation complete"
