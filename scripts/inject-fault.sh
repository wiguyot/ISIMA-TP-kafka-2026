#!/bin/sh
#
# inject-fault.sh — orchestrateur d'injection de fautes pour les TP 01 à 03
#
# Le but pédagogique est de provoquer concrètement les failles théoriques des
# sémantiques Kafka, plutôt que de se contenter d'en parler. Les leviers
# utilisés sont uniquement externes au code applicatif :
#
#   - SIGKILL sur un conteneur (pas de flush du buffer producteur, fenêtre
#     commit / traitement brutalement interrompue côté consommateur) ;
#   - injection de perturbation réseau ciblée pour amplifier les conditions
#     d'apparition des doublons producteur ;
#   - élargissement des fenêtres de vulnérabilité via les variables d'env
#     existantes SIMULPIX_PERSIST_VALID_DELAY_MS et SIMULPIX_PROCESSING_DELAY_MS.
#
# Usage :
#   ./scripts/inject-fault.sh producer_drop
#   ./scripts/inject-fault.sh consumer_kill <persister-valid|persister-rejected>
#   ./scripts/inject-fault.sh chain_kill <pix-validator|pix-decision-engine|pix-outcome-publisher>
#   ./scripts/inject-fault.sh producer_ack_loss [loss_percent]
#   ./scripts/inject-fault.sh producer_ack_loss_off
#   ./scripts/inject-fault.sh broker_outage  <kafka-1|kafka-2|kafka-3>   arrêt propre du broker
#   ./scripts/inject-fault.sh broker_kill    <kafka-1|kafka-2|kafka-3>   arrêt brutal (SIGKILL)
#   ./scripts/inject-fault.sh broker_isolate <kafka-1|kafka-2|kafka-3>   isolement réseau (panne silencieuse)
#   (durée de la panne : SIMULPIX_FAULT_DOWNTIME, 60 s par défaut)
#
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

ACTION="${1:-}"

if [ -z "$ACTION" ]; then
  echo "Usage : $0 <producer_drop|consumer_kill|chain_kill|producer_ack_loss|producer_ack_loss_off|broker_outage|broker_kill|broker_isolate> [args]" >&2
  exit 1
fi

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

health_counters() {
  curl -fsS "http://127.0.0.1:8082/health/details" 2>/dev/null \
    | python3 -c '
import json, sys
data = json.load(sys.stdin)
counts = data.get("metrics", {}).get("counts", {})
keys = [
    "generated", "raw_topic_end_offsets",
    "validated", "validated_topic_end_offsets",
    "rejected", "rejected_topic_end_offsets",
    "outcome_count", "outcome_topic_end_offsets",
    "db_validated_count", "db_rejected_count",
]
for k in keys:
    print(f"  {k:35s} = {counts.get(k, 0)}")
' || echo "  (service-health indisponible)"
}

print_snapshot() {
  label="$1"
  echo "--- $label ---"
  health_counters
  echo
}

case "$ACTION" in
  producer_drop)
    print_snapshot "compteurs AVANT kill du generator"
    echo "[simulpix] SIGKILL sur generator (le buffer producteur non flushé est perdu)"
    docker_compose kill -s SIGKILL generator >/dev/null 2>&1 || true
    sleep 2
    print_snapshot "compteurs APRÈS kill"
    echo "[simulpix] le generator reste arrêté. Pour reprendre : ./scripts/run-scenario.sh <scenario>"
    ;;

  consumer_kill)
    TARGET="${2:-persister-valid}"
    DOWNTIME="${SIMULPIX_FAULT_DOWNTIME:-60}"
    case "$TARGET" in
      persister-valid|persister-rejected) ;;
      *)
        echo "Cible non supportée: $TARGET (attendu : persister-valid | persister-rejected)" >&2
        exit 1
        ;;
    esac
    print_snapshot "compteurs AVANT kill de $TARGET"
    echo "[simulpix] SIGKILL sur $TARGET au milieu de son travail"
    docker_compose kill -s SIGKILL "$TARGET" >/dev/null 2>&1 || true
    echo "[simulpix] $TARGET arrêté pendant ${DOWNTIME}s (laissez Grafana capturer la panne)"
    sleep "$DOWNTIME"
    echo "[simulpix] redémarrage de $TARGET (Kafka rejoue les messages non commités)"
    docker_compose up -d "$TARGET" >/dev/null
    sleep 4
    print_snapshot "compteurs APRÈS redémarrage"
    ;;

  chain_kill)
    TARGET="${2:-pix-decision-engine}"
    DOWNTIME="${SIMULPIX_FAULT_DOWNTIME:-60}"
    case "$TARGET" in
      pix-validator|pix-decision-engine|pix-outcome-publisher) ;;
      *)
        echo "Cible non supportée: $TARGET (attendu : pix-validator | pix-decision-engine | pix-outcome-publisher)" >&2
        exit 1
        ;;
    esac
    print_snapshot "compteurs AVANT kill de $TARGET"
    echo "[simulpix] SIGKILL sur $TARGET"
    docker_compose kill -s SIGKILL "$TARGET" >/dev/null 2>&1 || true
    echo "[simulpix] $TARGET arrêté pendant ${DOWNTIME}s (laissez Grafana capturer la panne)"
    sleep "$DOWNTIME"
    echo "[simulpix] redémarrage de $TARGET"
    docker_compose up -d "$TARGET" >/dev/null
    sleep 4
    print_snapshot "compteurs APRÈS redémarrage"
    ;;

  producer_ack_loss)
    LOSS_PERCENT="${2:-35}"
    DELAY_MS="${3:-1500}"
    JITTER_MS="${4:-500}"
    print_snapshot "compteurs AVANT injection kafka_loss sur generator"
    echo "[simulpix] perturbation kafka_loss loss=${LOSS_PERCENT}% delay=${DELAY_MS}ms jitter=${JITTER_MS}ms sur generator"
    echo "           (force des timeouts de requêtes Kafka → retries → doublons PUB at-least-once)"
    SIMULPIX_NETWORK_LOSS_PERCENT_OVERRIDE="$LOSS_PERCENT" \
    SIMULPIX_NETWORK_DELAY_MS_OVERRIDE="$DELAY_MS" \
    SIMULPIX_NETWORK_JITTER_MS_OVERRIDE="$JITTER_MS" \
      ./scripts/network-perturb.sh kafka_loss generator
    echo "[simulpix] pour retirer la perturbation : ./scripts/inject-fault.sh producer_ack_loss_off"
    ;;

  broker_outage)
    TARGET="${2:-kafka-3}"
    DOWNTIME="${SIMULPIX_FAULT_DOWNTIME:-60}"
    case "$TARGET" in
      kafka-1|kafka-2|kafka-3) ;;
      *)
        echo "Cible non supportée: $TARGET (attendu : kafka-1 | kafka-2 | kafka-3)" >&2
        exit 1
        ;;
    esac
    print_snapshot "compteurs AVANT outage de $TARGET"
    echo "[simulpix] arrêt du broker $TARGET pendant ${DOWNTIME}s"
    echo "           (provoque une réélection de leader → retries producteur → doublons probables)"
    docker_compose stop "$TARGET" >/dev/null 2>&1 || true
    sleep "$DOWNTIME"
    echo "[simulpix] redémarrage du broker $TARGET"
    docker_compose start "$TARGET" >/dev/null
    sleep 4
    print_snapshot "compteurs APRÈS retour du broker"
    ;;

  broker_kill)
    TARGET="${2:-kafka-3}"
    DOWNTIME="${SIMULPIX_FAULT_DOWNTIME:-60}"
    case "$TARGET" in
      kafka-1|kafka-2|kafka-3) ;;
      *)
        echo "Cible non supportée: $TARGET (attendu : kafka-1 | kafka-2 | kafka-3)" >&2
        exit 1
        ;;
    esac
    print_snapshot "compteurs AVANT panne brutale de $TARGET"
    echo "[simulpix] SIGKILL sur le broker $TARGET, arrêté pendant ${DOWNTIME}s"
    echo "           (panne brutale : les messages envoyés sans accusé à ce broker peuvent être perdus)"
    docker_compose kill -s SIGKILL "$TARGET" >/dev/null 2>&1 || true
    sleep "$DOWNTIME"
    echo "[simulpix] redémarrage du broker $TARGET"
    docker_compose start "$TARGET" >/dev/null
    sleep 4
    print_snapshot "compteurs APRÈS retour du broker"
    ;;

  broker_isolate)
    TARGET="${2:-kafka-3}"
    DOWNTIME="${SIMULPIX_FAULT_DOWNTIME:-60}"
    case "$TARGET" in
      kafka-1|kafka-2|kafka-3) ;;
      *)
        echo "Cible non supportée: $TARGET (attendu : kafka-1 | kafka-2 | kafka-3)" >&2
        exit 1
        ;;
    esac
    network="$(docker inspect "$TARGET" --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
    print_snapshot "compteurs AVANT isolement de $TARGET"
    echo "[simulpix] isolement réseau du broker $TARGET pendant ${DOWNTIME}s (réseau $network)"
    echo "           (panne silencieuse : les paquets partent dans le vide, sans fermeture de connexion)"
    docker network disconnect "$network" "$TARGET" >/dev/null
    sleep "$DOWNTIME"
    echo "[simulpix] reconnexion du broker $TARGET"
    docker network connect --alias "$TARGET" "$network" "$TARGET" >/dev/null
    sleep 4
    print_snapshot "compteurs APRÈS reconnexion du broker"
    ;;

  producer_ack_loss_off)
    echo "[simulpix] retrait de la perturbation réseau"
    ./scripts/network-reset.sh
    sleep 1
    print_snapshot "compteurs APRÈS retrait"
    ;;

  *)
    echo "Action inconnue: $ACTION" >&2
    echo "Voir l'en-tête du script pour les actions disponibles." >&2
    exit 1
    ;;
esac
