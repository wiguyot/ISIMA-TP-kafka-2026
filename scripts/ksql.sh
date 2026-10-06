#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

# TP 04 — ouvre la CLI ksqlDB et prépare son topic source de référence.
#
# Usage :
#   ./scripts/ksql.sh                          CLI interactive
#   ./scripts/ksql.sh --file /sql/tp14-pix.sql exécuter un script (dossier infra/ksqldb/ monté sur /sql)
#   ./scripts/ksql.sh -e "SHOW STREAMS;"       exécuter une instruction
#   ./scripts/ksql.sh stop                     arrêter ksqldb-server
#
# Variables utiles au démarrage :
#   SIMULPIX_KSQL_PROCESSING_GUARANTEE  at_least_once (défaut) | exactly_once_v2
#   SIMULPIX_KSQL_COMMIT_INTERVAL_MS    intervalle de commit (défaut 2000)

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose --profile ksqldb "$@"
  else
    docker-compose --profile ksqldb "$@"
  fi
}

if [ "${1:-}" = "stop" ]; then
  docker_compose stop ksqldb-server
  exit 0
fi

if ! curl -fsS http://127.0.0.1:8088/info >/dev/null 2>&1; then
  echo "[simulpix] démarrage de ksqldb-server (premier lancement : téléchargement de l'image, environ 1 Go)"
  docker_compose up -d ksqldb-server >/dev/null 2>&1
  attempts=120
  until curl -fsS http://127.0.0.1:8088/info >/dev/null 2>&1; do
    attempts=$((attempts - 1))
    if [ "$attempts" -le 0 ]; then
      echo "[simulpix] ksqldb-server ne répond pas : voir 'docker compose logs ksqldb-server'" >&2
      exit 1
    fi
    sleep 2
  done
  echo "[simulpix] ksqldb-server prêt sur http://localhost:8088"
fi

docker_compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --command-config /config/client.properties \
  --create --if-not-exists \
  --topic simulpix.ksql.clients --partitions 6 --replication-factor 3 >/dev/null

if [ -t 0 ]; then
  docker_compose exec ksqldb-server ksql http://localhost:8088 "$@"
else
  docker_compose exec -T ksqldb-server ksql http://localhost:8088 "$@"
fi
