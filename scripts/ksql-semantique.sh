#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

# TP 04 — garantie de traitement de ksqlDB face à un crash.
#
# Usage : ./scripts/ksql-semantique.sh <at_least_once|exactly_once_v2> [TOTAL] [RATE] [KILL_APRES_S] [ARRET_S]
#
# Lance un flux de Pix, fait compter les Pix validés par ksqlDB (COUNT par émetteur),
# tue brutalement ksqldb-server en plein flux, le redémarre, puis compare le total
# calculé par ksqlDB au nombre de Pix réellement en base (vérité terrain).

GUARANTEE="${1:-at_least_once}"
TOTAL="${2:-1200}"
RATE="${3:-20}"
KILL_AFTER="${4:-25}"
DOWNTIME="${5:-10}"
case "$GUARANTEE" in
  at_least_once|exactly_once_v2) ;;
  *) echo "Garantie inconnue : $GUARANTEE (at_least_once | exactly_once_v2)" >&2; exit 1 ;;
esac

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose --profile ksqldb "$@"
  else
    docker-compose --profile ksqldb "$@"
  fi
}

ksql() {
  docker_compose exec -T ksqldb-server ksql http://localhost:8088 "$@" 2>/dev/null
}

wait_ksql() {
  attempts=120
  until curl -fsS http://127.0.0.1:8088/info >/dev/null 2>&1; do
    attempts=$((attempts - 1))
    [ "$attempts" -gt 0 ] || { echo "[simulpix] ksqldb-server ne répond pas" >&2; exit 1; }
    sleep 2
  done
}

ksql_total() {
  python3 - <<'PY'
import json
import time
import urllib.error
import urllib.request

total = 0
for number in range(1, 7):
    emitter = f"TAX-{number:04d}"
    statement = (
        "SELECT NB_PIX FROM PIX_PAR_EMETTEUR "
        f"WHERE EMITTER_TAX_ID = '{emitter}';"
    )
    payload = json.dumps({"ksql": statement, "streamsProperties": {}}).encode()
    request = urllib.request.Request(
        "http://127.0.0.1:8088/query",
        data=payload,
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
    )
    for attempt in range(30):
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                body = response.read().decode()
        except urllib.error.HTTPError as error:
            body = error.read().decode()
            try:
                error_message = json.loads(body).get("message", body)
            except json.JSONDecodeError:
                error_message = body
            if "Materialized data for key" in error_message and attempt < 29:
                import time

                time.sleep(1)
                continue
            if "Materialized data for key" in error_message:
                print("UNAVAILABLE")
                raise SystemExit(0)
            raise SystemExit(f"requête pull ksqlDB refusée pour {emitter}: {error_message}")
        except (OSError, urllib.error.URLError) as error:
            raise SystemExit(f"lecture pull ksqlDB impossible pour {emitter}: {error}")

        try:
            decoded = json.loads(body)
        except json.JSONDecodeError as error:
            raise SystemExit(f"réponse JSON ksqlDB invalide pour {emitter}: {error}")
        records = decoded if isinstance(decoded, list) else [decoded]
        errors = [
            record.get("message")
            for record in records
            if isinstance(record, dict) and record.get("@type") == "statement_error"
        ]
        if errors:
            if "Materialized data for key" in errors[0]:
                print("UNAVAILABLE")
                raise SystemExit(0)
            raise SystemExit(f"requête pull ksqlDB refusée pour {emitter}: {errors[0]}")
        rows = [
            record["row"]["columns"]
            for record in records
            if isinstance(record, dict)
            and isinstance(record.get("row"), dict)
            and isinstance(record["row"].get("columns"), list)
        ]
        if len(rows) > 1 or (rows and not rows[0]):
            raise SystemExit(f"réponse pull ksqlDB inattendue pour {emitter}: {rows}")
        if rows:
            total += int(rows[0][0])
            break
        print("UNAVAILABLE")
        raise SystemExit(0)
    else:
        raise SystemExit(f"état ksqlDB toujours indisponible pour {emitter} après 30 s")

print(total)
PY
}

pg_total() {
  docker_compose exec -T postgres psql -U simulpix -d simulpix -At -c "select count(*) from validated_transactions" 2>/dev/null | tr -d '[:space:]'
}

wait_for_ksql_total() {
  attempts=60
  while [ "$attempts" -gt 0 ]; do
    value="$(ksql_total)"
    case "$value" in
      ''|*[!0-9]*) ;;
      *) printf '%s\n' "$value"; return 0 ;;
    esac
    attempts=$((attempts - 1))
    sleep 1
  done
  echo "[simulpix] table d'agrégats ksqlDB non disponible avant le crash" >&2
  return 1
}

echo "[simulpix] TP 04 — garantie ksqlDB : $GUARANTEE"
echo "[simulpix] (re)création de ksqldb-server avec processing.guarantee=$GUARANTEE"
if curl -fsS http://127.0.0.1:8088/info >/dev/null 2>&1; then
  reset_output="$(ksql --file /sql/tp14-reset.sql || true)"
  if printf '%s' "$reset_output" | grep -qiE 'could not|timeout|exception'; then
    echo "[simulpix] impossible de nettoyer les requêtes ksqlDB existantes :" >&2
    printf '%s\n' "$reset_output" | grep -iE 'could not|timeout|exception' >&2
    exit 1
  fi
fi
SIMULPIX_KSQL_PROCESSING_GUARANTEE="$GUARANTEE" SIMULPIX_KSQL_COMMIT_INTERVAL_MS=10000 \
  docker_compose up -d --no-deps --force-recreate ksqldb-server >/dev/null 2>&1
wait_ksql

docker_compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --command-config /config/client.properties \
  --delete --if-exists --topic simulpix.ksql.pix_par_emetteur >/dev/null

echo "[simulpix] flux de $TOTAL Pix à $RATE Pix/s"
./scripts/run-scenario.sh nominal "$TOTAL" "$RATE" >/dev/null 2>&1
out="$(ksql --file /sql/tp14-semantique.sql || true)"
if printf '%s' "$out" | grep -qiE 'could not|error|exception'; then
  echo "[simulpix] échec de création des requêtes ksqlDB :" >&2
  printf '%s\n' "$out" | grep -iE 'could not|error|exception|caused' >&2
  exit 1
fi

echo "[simulpix] ${KILL_AFTER} s de traitement, puis SIGKILL sur ksqldb-server pendant ${DOWNTIME} s"
sleep "$KILL_AFTER"
before_ksql="$(wait_for_ksql_total)"
echo "[simulpix] avant le crash : ksqlDB=$before_ksql base=$(pg_total)"
docker_compose kill -s SIGKILL ksqldb-server >/dev/null 2>&1
sleep "$DOWNTIME"
SIMULPIX_KSQL_PROCESSING_GUARANTEE="$GUARANTEE" SIMULPIX_KSQL_COMMIT_INTERVAL_MS=10000 \
  docker_compose up -d --no-deps ksqldb-server >/dev/null 2>&1
wait_ksql

echo "[simulpix] attente de la fin du flux et du rattrapage"
last=""; stable=0; budget=300
while [ "$budget" -gt 0 ]; do
  ksql_now="$(ksql_total)"
  pg_now="$(pg_total)"
  case "$ksql_now" in
    ''|*[!0-9]*)
      echo "[simulpix] ksqlDB restaure son état; nouvelle vérification dans 5 s"
      stable=0
      last=""
      sleep 5
      budget=$((budget - 5))
      continue
      ;;
  esac
  now="$ksql_now/$pg_now"
  if [ "$now" = "$last" ] && [ "$pg_now" = "$TOTAL" ] && [ "$ksql_now" -ge "$TOTAL" ]; then
    stable=$((stable + 1)); [ "$stable" -ge 3 ] && break
  else
    stable=0
  fi
  last="$now"; sleep 5; budget=$((budget - 5))
done

k="$(ksql_total)"; p="$(pg_total)"
case "$k" in
  ''|*[!0-9]*)
    echo "[simulpix] résultat indisponible : l'état ksqlDB n'a pas été restauré" >&2
    exit 1
    ;;
esac
echo
echo "=========================================================="
echo "  BILAN TP 04 ($GUARANTEE)"
echo "=========================================================="
printf "  %-38s : %6s\n" "Pix validés en base (vérité terrain)" "$p"
printf "  %-38s : %6s\n" "Pix comptés par ksqlDB (somme COUNT)" "$k"
printf "  %-38s : %+6d\n" "Écart ksqlDB - base" "$((k - p))"
echo "=========================================================="

if [ "$k" -lt "$p" ]; then
  echo "[simulpix] résultat incomplet : ksqlDB n'a pas compté tous les Pix validés" >&2
  exit 1
fi
if [ "$GUARANTEE" = "exactly_once_v2" ] && [ "$k" -ne "$p" ]; then
  echo "[simulpix] écart inattendu en exactly_once_v2" >&2
  exit 1
fi
