#!/bin/sh
#
# measure-tp.sh — orchestrateur de mesure pour TP 01 / 02 / 03
#
# Pilote un scénario complet, injecte la faute prévue par le TP au bon moment,
# attend le drainage puis affiche un bilan : nombre de Pix ATTENDUS vs nombre
# de Pix réellement GÉRÉS de bout en bout.
#
# Usage :
#   ./scripts/measure-tp.sh <action> [TOTAL] [RATE] [INJECTION_DELAY] [DOWNTIME] [OBSERVATION] [PERSIST_DELAY]
#
# Actions :
#   11_perte_pub          11_perte_sub          11_arret_generator
#   12_doublon_pub        12_doublon_sub
#   13_protection_pub     13_protection_sub     13_limite_pg
#
# Arguments positionnels :
#   TOTAL            nombre total de Pix à émettre par le scénario (défaut 6000)
#   RATE             débit cible en Pix/s (défaut 50)
#   INJECTION_DELAY  secondes de régime stable AVANT injection (défaut 90)
#   DOWNTIME         durée d'arrêt du composant pour les actions kill (défaut 60)
#   OBSERVATION      secondes minimum après redémarrage (défaut 60)
#   PERSIST_DELAY    délai ms d'écriture du persister (défaut 0 ; 80 pour 11_perte_sub, pour élargir la fenêtre commit/écriture)
#
# Variables d'env spécifiques aux scénarios PUB (12_doublon_pub / 13_protection_pub) :
#   SIMULPIX_TP_LOSS_PERCENT     perte réseau % sur generator (défaut 35)
#   SIMULPIX_TP_NET_DELAY_MS     latence réseau ms (défaut 1500)
#   SIMULPIX_TP_NET_JITTER_MS    jitter réseau ms (défaut 500)
#   SIMULPIX_TP_REQUEST_TIMEOUT_MS timeout requête producteur Kafka (défaut 700)
#   SIMULPIX_TP_MESSAGE_TIMEOUT_MS timeout global message producteur Kafka (défaut 120000)
#   SIMULPIX_TP_RETRY_BACKOFF_MS  pause entre retries producteur Kafka (défaut 100)
#   SIMULPIX_TP_PRODUCER_RETRIES  nombre de retries producteur Kafka (défaut 20)
#   SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT
#                                % de retries PUB ambigus injectés pour rendre le TP reproductible (défaut 5 pour 12_doublon_pub)
#   SIMULPIX_TP_PUB_SEMANTICS    sémantique PUB de 11_perte_pub (défaut at_most_once ; at_least_once pour comparer)
#   SIMULPIX_TP_BROKER_OUTAGE    1=tuer kafka-3 pendant DOWNTIME (défaut 1)
#                                Mettre à 0 pour s'en passer (effet souvent insuffisant).
#
# Exemples :
#   ./scripts/measure-tp.sh 11_perte_pub
#   ./scripts/measure-tp.sh 11_perte_pub 12000 100
#   ./scripts/measure-tp.sh 11_perte_sub 6000 50 90 90 120
#
# Les durées sont calibrées pour qu'un dashboard Grafana actualisé toutes les
# 5 s capture au moins une vingtaine de points : régime stable / panne /
# rattrapage. Sur une machine très performante, augmenter TOTAL et RATE pour
# rester dans la zone où la sémantique se manifeste.
#
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

ACTION="${1:-}"
if [ -z "$ACTION" ]; then
  cat >&2 <<EOF
Usage : $0 <action> [TOTAL] [RATE] [INJECTION_DELAY] [DOWNTIME] [OBSERVATION] [PERSIST_DELAY]
Actions : 11_perte_pub | 11_perte_sub | 11_arret_generator | 12_doublon_pub | 12_doublon_sub
          | 13_protection_pub | 13_protection_sub | 13_limite_pg
EOF
  exit 1
fi

TOTAL="${2:-${SIMULPIX_TP_TOTAL_MESSAGES:-6000}}"
RATE="${3:-${SIMULPIX_TP_RATE_PER_SECOND:-50}}"
INJECTION_DELAY="${4:-${SIMULPIX_TP_INJECTION_DELAY:-90}}"
DOWNTIME="${5:-${SIMULPIX_TP_DOWNTIME:-60}}"
OBSERVATION="${6:-${SIMULPIX_TP_OBSERVATION:-60}}"
PERSIST_DELAY="${7:-${SIMULPIX_TP_PERSIST_DELAY:-0}}"
LOSS_PERCENT="${SIMULPIX_TP_LOSS_PERCENT:-35}"
NET_DELAY_MS="${SIMULPIX_TP_NET_DELAY_MS:-1500}"
NET_JITTER_MS="${SIMULPIX_TP_NET_JITTER_MS:-500}"
USE_BROKER_OUTAGE="${SIMULPIX_TP_BROKER_OUTAGE:-1}"
BROKER_OUTAGE_DURATION="${SIMULPIX_TP_BROKER_OUTAGE_DURATION:-$DOWNTIME}"
PRODUCER_REQUEST_TIMEOUT_MS="${SIMULPIX_TP_REQUEST_TIMEOUT_MS:-700}"
PRODUCER_MESSAGE_TIMEOUT_MS="${SIMULPIX_TP_MESSAGE_TIMEOUT_MS:-120000}"
PRODUCER_RETRY_BACKOFF_MS="${SIMULPIX_TP_RETRY_BACKOFF_MS:-100}"
PRODUCER_RETRIES="${SIMULPIX_TP_PRODUCER_RETRIES:-20}"
AMBIGUOUS_ACK_DUPLICATE_PERCENT="${SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT:-0}"
PERSIST_VALID_CRASH_AFTER_INSERT_ONCE="${SIMULPIX_PERSIST_VALID_CRASH_AFTER_INSERT_ONCE:-0}"
GENERATOR_MAX_WAIT="${SIMULPIX_TP_GENERATOR_MAX_WAIT:-600}"
DRAIN_MAX_SECONDS="${SIMULPIX_TP_DRAIN_MAX:-180}"
DRAIN_THRESHOLD="${SIMULPIX_TP_DRAIN_THRESHOLD:-3}"
DECISION_SLA_SECONDS="${SIMULPIX_TP_DECISION_SLA_SECONDS:-${SIMULPIX_DECISION_SLA_SECONDS:-10}}"

export SIMULPIX_FAULT_DOWNTIME="$DOWNTIME"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

# Renvoie une ligne pipe-séparée : generated|raw|validated|valid_off|outcome|out_off|db_valid|rejected|db_rejected
read_counters() {
  curl -fsS "http://127.0.0.1:8082/health/details" 2>/dev/null \
    | python3 -c '
import json, sys
data = json.load(sys.stdin)
c = data.get("metrics", {}).get("counts", {})
print("|".join(str(c.get(k, 0)) for k in [
    "generated",
    "raw_topic_end_offsets",
    "validated",
    "validated_topic_end_offsets",
    "outcome_count",
    "outcome_topic_end_offsets",
    "db_validated_count",
    "rejected",
    "db_rejected_count",
]))
' || echo "0|0|0|0|0|0|0|0|0"
}

field() {
  echo "$1" | awk -F'|' -v n="$2" '{print $n}'
}

# Attente du drainage : Kafka raw doit être couvert par validated + rejected,
# et le topic outcome doit avoir rattrapé les résultats finaux logiques.
wait_for_drain() {
  budget="$DRAIN_MAX_SECONDS"
  while [ "$budget" -gt 0 ]; do
    counters=$(read_counters)
    raw=$(field "$counters" 2)
    validated=$(field "$counters" 3)
    outcome=$(field "$counters" 5)
    db_valid=$(field "$counters" 7)
    rejected=$(field "$counters" 8)
    db_rejected=$(field "$counters" 9)
    db_total=$((db_valid + db_rejected))
    final_logical=$((validated + rejected))
    if [ "$ACTION" = "12_doublon_pub" ] \
      || { [ "$ACTION" = "11_perte_pub" ] && [ "${SIMULPIX_TP_PUB_SEMANTICS:-at_most_once}" = "at_least_once" ]; }; then
      target=$(field "$counters" 1)
    else
      target="$raw"
    fi
    gap=$((target - db_total))
    if [ "$gap" -lt 0 ]; then gap=$((-gap)); fi
    outcome_gap=$((final_logical - outcome))
    if [ "$outcome_gap" -lt 0 ]; then outcome_gap=0; fi
    if [ "$gap" -le "$DRAIN_THRESHOLD" ] && [ "$outcome_gap" -le "$DRAIN_THRESHOLD" ]; then
      echo "[simulpix] backlog résorbé : raw=$raw db_total=$db_total outcome=$outcome final_logical=$final_logical target=$target (delta_db=$gap delta_outcome=$outcome_gap)"
      return 0
    fi
    echo "[simulpix] drainage en cours : raw=$raw db_total=$db_total outcome=$outcome final_logical=$final_logical target=$target (delta_db=$gap delta_outcome=$outcome_gap, budget=${budget}s)"
    sleep 10
    budget=$((budget - 10))
  done
  echo "[simulpix] drainage non terminé après ${DRAIN_MAX_SECONDS}s — mesure prise telle quelle"
}

# Bilan formaté avec valeurs avant injection (pour récupérer generated)
print_bilan() {
  before="$1"
  after="$2"
  expected="$3"
  faille_message="$4"

  gen_before=$(field "$before" 1)
  gen_after=$(field "$after" 1)
  # Si le generator a été tué, gen_after = 0 → utiliser la dernière valeur connue.
  emis=$gen_before
  if [ "$gen_after" -gt "$gen_before" ]; then
    emis=$gen_after
  fi

  raw_after=$(field "$after" 2)
  validated_after=$(field "$after" 3)
  outcome_after=$(field "$after" 5)
  out_off_after=$(field "$after" 6)
  db_valid_after=$(field "$after" 7)
  rejected_after=$(field "$after" 8)
  db_rejected_after=$(field "$after" 9)
  db_total_after=$((db_valid_after + db_rejected_after))
  final_logical_after=$((validated_after + rejected_after))

  # Trois écarts distincts. Chacun ne peut être que perte OU doublon, pas les deux.
  delta_emis_attendu=$((expected - emis))     # > 0 : Pix attendus non émis (generator coupé OU pas eu le temps)
  delta_emis_kafka=$((emis - raw_after))      # > 0 : perte PUB (buffer non flushé) ;  < 0 : doublon PUB (retries écrits en plus)
  delta_kafka_db=$((raw_after - db_total_after)) # > 0 : perte SUB OU backlog non drainé, validés et rejetés inclus

  non_emission=$delta_emis_attendu
  if [ "$non_emission" -lt 0 ]; then non_emission=0; fi

  perte_pub=0
  doublon_pub=0
  if [ "$delta_emis_kafka" -gt 0 ]; then
    perte_pub=$delta_emis_kafka
  elif [ "$delta_emis_kafka" -lt 0 ]; then
    doublon_pub=$((-delta_emis_kafka))
  fi
  if [ "$ACTION" = "12_doublon_pub" ] \
    || { [ "$ACTION" = "11_perte_pub" ] && [ "${SIMULPIX_TP_PUB_SEMANTICS:-at_most_once}" = "at_least_once" ]; }; then
    doublon_pub=$((raw_after - emis))
    if [ "$doublon_pub" -lt 0 ]; then doublon_pub=0; fi
  elif [ "$ACTION" = "13_protection_pub" ]; then
    doublon_pub=$((raw_after - db_total_after))
    if [ "$doublon_pub" -lt 0 ]; then doublon_pub=0; fi
  fi

  perte_sub=$((delta_kafka_db - doublon_pub))
  if [ "$perte_sub" -lt 0 ]; then perte_sub=0; fi
  doublon_pub_marker=""
  perte_pub_marker=""
  non_emission_marker=""
  case "$ACTION" in
    12_doublon_pub) doublon_pub_marker="   <-- TP 02 PUB vise cette ligne" ;;
    13_protection_pub) doublon_pub_marker="   <-- TP 03 PUB doit garder cette ligne à 0" ;;
    11_perte_pub) perte_pub_marker="   <-- TP 01 PUB vise cette ligne" ;;
    11_arret_generator) non_emission_marker="   <-- non-émission, pas une perte at-most-once" ;;
  esac

  echo
  echo "=========================================================================="
  echo "                       BILAN DU TP ($ACTION)"
  echo "=========================================================================="
  printf "  %-44s : %10s\n" "Pix ATTENDUS (paramètre TOTAL)" "$expected"
  printf "  %-44s : %10s   (delta vs attendu : %+d)\n" "Pix ÉMIS par le generator" "$emis" "$((emis - expected))"
  printf "  %-44s : %10s   (delta vs émis    : %+d)\n" "Pix ARRIVÉS dans Kafka (raw)" "$raw_after" "$((raw_after - emis))"
  printf "  %-44s : %10s   (validated + rejected)\n" "Pix avec RÉSULTAT FINAL logique" "$final_logical_after"
  printf "  %-44s : %10s   (réponses finales publiées)\n" "Outcome count observé" "$outcome_after"
  printf "  %-44s : %10s   (offset brut, marqueurs transactions inclus)\n" "Topic outcome technique" "$out_off_after"
  printf "  %-44s : %10s   (delta vs Kafka raw : %+d)\n" "Pix PERSISTÉS en base (validated)" "$db_valid_after" "$((db_valid_after - raw_after))"
  printf "  %-44s : %10s\n" "Pix REJETÉS métier (compteur)" "$rejected_after"
  printf "  %-44s : %10s\n" "Pix rejetés en base (db_rejected)" "$db_rejected_after"
  printf "  %-44s : %10s   (delta vs Kafka raw : %+d)\n" "Pix persistés TOTAL (valid + rejected)" "$db_total_after" "$((db_total_after - raw_after))"
  echo "  ----------------------------------------------------------------------"
  echo "  ÉCART entre ÉMIS et ARRIVÉS DANS KAFKA :"
  printf "    - %-42s : %d%s\n" "PERTE PUB (émis mais absents, AMO PUB)" "$perte_pub" "$perte_pub_marker"
  printf "    - %-42s : %d%s\n" "DOUBLONS PUB (retries acceptés, ALO PUB)" "$doublon_pub" "$doublon_pub_marker"
  echo "  ÉCART entre ARRIVÉS DANS KAFKA et BASE PG :"
  printf "    - %-42s : %d\n" "PERTE SUB / backlog résiduel" "$perte_sub"
  echo "  ----------------------------------------------------------------------"
  printf "  %-44s : %10d%s\n" "Pix attendus mais NON ÉMIS par le generator" "$non_emission" "$non_emission_marker"
  echo "    (cause : generator coupé OU pas eu le temps de finir)"
  echo "=========================================================================="
  echo
  echo "Lecture pédagogique : $faille_message"
  echo
  echo "Rappel — que signifie outcome ?"
  echo "  outcome est le topic Kafka des réponses finales publiées par pix-outcome-publisher."
  echo "  outcome_count compte les vrais résultats métier finaux :"
  echo "    - paiement accepté ;"
  echo "    - paiement rejeté métier ;"
  echo "    - paiement rejeté pour délai dépassé ;"
  echo "    - autre rejet technique ou fonctionnel."
  echo "  À distinguer de outcome_topic_end_offsets, qui mesure l'offset technique Kafka"
  echo "  et peut inclure des marqueurs de fin de transaction COMMIT / ABORT."
  echo "  Les tentatives de publication dans une transaction abortée ne sont pas des outcomes métier ;"
  echo "  elles peuvent seulement laisser des traces dans les offsets techniques."
  if [ "$final_logical_after" -gt 0 ] && [ "$out_off_after" -eq $((final_logical_after * 2)) ]; then
    echo "  Ici, outcome_topic_end_offsets vaut exactement 2 × le résultat final logique :"
    echo "  cela correspond typiquement à un message métier + un marqueur de transaction par réponse publiée."
  fi
  if [ "$outcome_after" -lt "$final_logical_after" ]; then
    echo "  Dans ce bilan, outcome_count observé (${outcome_after}) est inférieur au résultat final logique (${final_logical_after})."
    echo "  Cela signifie que pix-outcome-publisher n'a pas encore publié toutes les réponses finales."
    echo "  Les Pix sont bien décidés/persistés (${final_logical_after}), mais le topic outcome est encore en rattrapage."
  fi
  if [ "$outcome_after" -gt "$final_logical_after" ]; then
    echo "  Dans ce bilan, outcome_count observé (${outcome_after}) est supérieur au résultat final logique (${final_logical_after})."
    echo "  Cela signale une lecture technique du topic outcome ou un compteur transitoire gonflé."
    echo "  Pour conclure côté métier, privilégier validated + rejected et la persistance DB."
  fi
  if [ "$ACTION" = "13_protection_pub" ]; then
    echo
    echo "Lecture attendue TP 03 — protection PUB exactly-once :"
    if [ "$emis" -eq "$raw_after" ]; then
      printf "  [OK] generated == raw_topic_end_offsets : %s == %s\n" "$emis" "$raw_after"
    elif [ "$raw_after" -gt "$emis" ] && [ "$db_total_after" -eq "$raw_after" ] && [ "$doublon_pub" -eq 0 ]; then
      printf "  [INFO] raw_topic_end_offsets > generated : %s > %s\n" "$raw_after" "$emis"
      echo "         Sous forte perturbation réseau, le compteur generator peut sous-estimer les messages"
      echo "         dont le callback de livraison n'a pas été observé, alors qu'ils sont bien arrivés dans Kafka."
      printf "         Lecture métier : raw == persistés TOTAL = %s, donc pas de doublon PUB durable.\n" "$raw_after"
    else
      printf "  [ALERTE] generated != raw_topic_end_offsets : %s != %s\n" "$emis" "$raw_after"
    fi
    if [ "$doublon_pub" -eq 0 ]; then
      printf "  [OK] DOUBLONS PUB = 0\n"
    else
      printf "  [ALERTE] DOUBLONS PUB = %s\n" "$doublon_pub"
    fi
    echo "  [OK] Réglage producteur attendu : enable.idempotence=true, acks=all, retries > 0"
    if [ "$emis" -eq "$raw_after" ]; then
      echo "  Comparaison TP 02 : dans 12_doublon_pub, raw_topic_end_offsets > generated ; ici l'écart disparaît."
    else
      echo "  Comparaison TP 02 : sous perturbation réseau forte, ne pas conclure uniquement avec raw-generated ;"
      echo "  privilégier raw vs persistés TOTAL et la ligne DOUBLONS PUB."
    fi
  fi
  if [ "$ACTION" = "13_protection_sub" ]; then
    echo
    echo "Lecture attendue TP 03 — protection SUB exactly-once-kafka :"
    echo "  [OK] Protection Kafka : pas de perte ou doublon logique sur la chaîne transactionnelle."
    echo "  [INFO] TTL métier du Pix : ${DECISION_SLA_SECONDS}s ; arrêt pix-decision-engine : ${DOWNTIME}s."
    echo "  [INFO] Des rejets pour délai dépassé restent possibles :"
    echo "         Kafka protège la cohérence technique, mais le métier refuse un Pix traité trop tard."
  fi
  echo
}

# Attendre que le generator ait atteint target Pix, ou qu'il se soit
# durablement arrêté (stagnation 30s).
wait_for_generator_done() {
  target="$1"
  budget="${2:-300}"
  last_value=-1
  stable_count=0
  while [ "$budget" -gt 0 ]; do
    counters=$(read_counters)
    current=$(field "$counters" 1)
    if [ -z "$current" ]; then current=0; fi
    if [ "$current" -ge "$target" ]; then
      echo "[simulpix] generator a atteint $current/$target"
      return 0
    fi
    if [ "$current" = "$last_value" ]; then
      stable_count=$((stable_count + 1))
      if [ "$stable_count" -ge 6 ]; then
        echo "[simulpix] generator stagne à $current/$target depuis 30s — abandon de l'attente"
        return 1
      fi
    else
      stable_count=0
    fi
    echo "[simulpix] generator : $current/$target (budget restant ${budget}s)"
    last_value="$current"
    sleep 5
    budget=$((budget - 5))
  done
  echo "[simulpix] timeout d'attente du generator (a émis $current/$target)"
  return 1
}

wait_for_health() {
  attempts=30
  while [ "$attempts" -gt 0 ]; do
    if curl -fsS "http://127.0.0.1:8082/health" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  echo "[simulpix] service-health indisponible" >&2
  return 1
}

run_with_semantics() {
  pub="$1"
  sub="$2"
  echo "[simulpix] lancement nominal — PUB=$pub SUB=$sub total=$TOTAL rate=$RATE ttl=${DECISION_SLA_SECONDS}s persist_delay=${PERSIST_DELAY}ms"
  SIMULPIX_KAFKA_PRODUCER_SEMANTICS="$pub" \
  SIMULPIX_KAFKA_CONSUMER_SEMANTICS="$sub" \
  SIMULPIX_DECISION_SLA_SECONDS="$DECISION_SLA_SECONDS" \
  SIMULPIX_KAFKA_RETRIES="$PRODUCER_RETRIES" \
  SIMULPIX_KAFKA_REQUEST_TIMEOUT_MS="$PRODUCER_REQUEST_TIMEOUT_MS" \
  SIMULPIX_KAFKA_MESSAGE_TIMEOUT_MS="$PRODUCER_MESSAGE_TIMEOUT_MS" \
  SIMULPIX_KAFKA_RETRY_BACKOFF_MS="$PRODUCER_RETRY_BACKOFF_MS" \
  SIMULPIX_KAFKA_AMBIGUOUS_ACK_DUPLICATE_PERCENT="$AMBIGUOUS_ACK_DUPLICATE_PERCENT" \
  SIMULPIX_PERSIST_VALID_CRASH_AFTER_INSERT_ONCE="$PERSIST_VALID_CRASH_AFTER_INSERT_ONCE" \
  SIMULPIX_PERSIST_VALID_DELAY_MS="$PERSIST_DELAY" \
  SIMULPIX_PERSIST_REJECTED_DELAY_MS="$PERSIST_DELAY" \
  ./scripts/run-scenario.sh nominal "$TOTAL" "$RATE"
  wait_for_health
  sleep 3
}

wait_for_health || exit 1

case "$ACTION" in
  11_perte_pub)
    # SIMULPIX_TP_PUB_SEMANTICS=at_least_once rejoue la même panne avec acks=all, pour comparaison.
    pub_semantics="${SIMULPIX_TP_PUB_SEMANTICS:-at_most_once}"
    if [ "$pub_semantics" = "at_least_once" ]; then
      pub_failure_message="Avec acks=all, le producteur attend un accusé puis réessaie en cas de doute. La perte PUB disparaît, mais un premier envoi déjà reçu peut être écrit à nouveau lors du retry : les doublons apparaissent dans raw."
    else
      pub_failure_message="Avec acks=0, le generator compte un Pix comme émis dès qu'il est écrit dans la connexion réseau. Pendant l'isolement de kafka-3, les Pix destinés à ses partitions partent dans le vide : comptés comme émis, ils n'arrivent jamais dans le topic. C'est la ligne PERTE PUB, et personne n'est prévenu. Avec acks=all, le producteur aurait attendu un accusé, puis réessayé."
    fi
    echo "[simulpix] TP 01 — provoquer la PERTE côté PUB (PUB=$pub_semantics, broker isolé du réseau)"
    run_with_semantics "$pub_semantics" at_least_once
    echo "[simulpix] $INJECTION_DELAY s de régime stable avant l'isolement réseau de kafka-3"
    sleep "$INJECTION_DELAY"
    before=$(read_counters)
    echo "[simulpix] snapshot juste avant la panne : generated=$(field "$before" 1) raw=$(field "$before" 2)"
    SIMULPIX_FAULT_DOWNTIME="$DOWNTIME" ./scripts/inject-fault.sh broker_isolate kafka-3 >/dev/null
    echo "[simulpix] attente fin d'émission du generator (max ${GENERATOR_MAX_WAIT}s)"
    wait_for_generator_done "$TOTAL" "$GENERATOR_MAX_WAIT" || true
    echo "[simulpix] $OBSERVATION s d'observation puis attente du drainage"
    sleep "$OBSERVATION"
    wait_for_drain
    after=$(read_counters)
    print_bilan "$before" "$after" "$TOTAL" "$pub_failure_message"
    ;;

  11_arret_generator)
    echo "[simulpix] TP 01 — comparaison : arrêter le generator (non-émission, pas une perte at-most-once)"
    run_with_semantics at_most_once at_most_once
    echo "[simulpix] $INJECTION_DELAY s de régime stable avant le kill du generator"
    sleep "$INJECTION_DELAY"
    before=$(read_counters)
    echo "[simulpix] snapshot juste avant kill : generated=$(field "$before" 1) raw=$(field "$before" 2)"
    ./scripts/inject-fault.sh producer_drop >/dev/null
    echo "[simulpix] $OBSERVATION s d'observation post-kill puis attente du drainage"
    sleep "$OBSERVATION"
    wait_for_drain
    after=$(read_counters)
    print_bilan "$before" "$after" "$TOTAL" \
      "Le generator a été tué en plein flux : les Pix restants n'ont jamais été produits. C'est une NON-ÉMISSION, visible quelle que soit la sémantique (même avec acks=all). Elle ne démontre pas at-most-once : comparer avec 11_perte_pub."
    ;;

  11_perte_sub)
    if [ -z "${7:-}" ] && [ -z "${SIMULPIX_TP_PERSIST_DELAY:-}" ]; then
      PERSIST_DELAY=80
    fi
    echo "[simulpix] TP 01 — provoquer la PERTE côté SUB (at-most-once)"
    run_with_semantics at_least_once at_most_once
    echo "[simulpix] $INJECTION_DELAY s de régime stable avant le kill du persister-valid"
    sleep "$INJECTION_DELAY"
    before=$(read_counters)
    echo "[simulpix] snapshot juste avant kill : raw=$(field "$before" 2) db_valid=$(field "$before" 7)"
    ./scripts/inject-fault.sh consumer_kill persister-valid >/dev/null
    echo "[simulpix] $OBSERVATION s d'observation post-restart puis attente du drainage"
    sleep "$OBSERVATION"
    wait_for_drain
    after=$(read_counters)
    print_bilan "$before" "$after" "$TOTAL" \
      "En at-most-once SUB, le commit a précédé l'INSERT. Les messages dont l'offset était commité mais l'INSERT pas terminé au moment du kill sont définitivement perdus. La perte par message est faible (~ 1 par kill) mais réelle et irréversible."
    ;;

  12_doublon_pub)
    echo "[simulpix] TP 02 — provoquer le DOUBLON côté PUB (at-least-once)"
    AMBIGUOUS_ACK_DUPLICATE_PERCENT="${SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT:-5}"
    run_with_semantics at_least_once at_least_once
    if [ "$LOSS_PERCENT" -gt 0 ] || [ "$NET_DELAY_MS" -gt 0 ]; then
      echo "[simulpix] perturbation réseau agressive loss=${LOSS_PERCENT}% delay=${NET_DELAY_MS}ms jitter=${NET_JITTER_MS}ms"
      echo "[simulpix] timeouts producteur Kafka request=${PRODUCER_REQUEST_TIMEOUT_MS}ms message=${PRODUCER_MESSAGE_TIMEOUT_MS}ms retries=${PRODUCER_RETRIES}"
      echo "[simulpix] injection ack ambigu reproductible=${AMBIGUOUS_ACK_DUPLICATE_PERCENT}% (retries écrits sans incrémenter les Pix logiques)"
      ./scripts/inject-fault.sh producer_ack_loss "$LOSS_PERCENT" "$NET_DELAY_MS" "$NET_JITTER_MS" >/dev/null
      perturbation_active=1
    else
      perturbation_active=0
    fi
    echo "[simulpix] $INJECTION_DELAY s de régime stable"
    sleep "$INJECTION_DELAY"
    before=$(read_counters)
    if [ "$USE_BROKER_OUTAGE" = "1" ]; then
      echo "[simulpix] outage BREF du broker kafka-3 pendant ${BROKER_OUTAGE_DURATION}s (réélection → retries)"
      SIMULPIX_FAULT_DOWNTIME="$BROKER_OUTAGE_DURATION" \
        ./scripts/inject-fault.sh broker_outage kafka-3 >/dev/null
    fi
    echo "[simulpix] attente fin d'émission du generator (max ${GENERATOR_MAX_WAIT}s)"
    wait_for_generator_done "$TOTAL" "$GENERATOR_MAX_WAIT" || true
    echo "[simulpix] $OBSERVATION s d'observation puis drainage"
    sleep "$OBSERVATION"
    wait_for_drain
    if [ "$perturbation_active" = "1" ]; then
      ./scripts/inject-fault.sh producer_ack_loss_off >/dev/null
    fi
    after=$(read_counters)
    print_bilan "$before" "$after" "$TOTAL" \
      "Sans idempotence producteur, les retries provoqués par la réélection de leader créent des doublons dans le topic. La ligne 'DOUBLONS PUB' doit être > 0 ; les autres lignes (perte PUB, non-émission) doivent rester proches de 0."
    ;;

  12_doublon_sub)
    echo "[simulpix] TP 02 — provoquer le DOUBLON côté SUB (at-least-once) avec audit"
    PERSIST_VALID_CRASH_AFTER_INSERT_ONCE=1
    echo "[simulpix] installation du trigger d'audit"
    ./scripts/toggle-unique.sh off >/dev/null
    run_with_semantics at_least_once at_least_once
    echo "[simulpix] $INJECTION_DELAY s de régime stable avant le kill du persister-valid"
    sleep "$INJECTION_DELAY"
    before=$(read_counters)
    ./scripts/inject-fault.sh consumer_kill persister-valid >/dev/null
    echo "[simulpix] $OBSERVATION s d'observation post-restart puis attente du drainage"
    sleep "$OBSERVATION"
    wait_for_drain
    after=$(read_counters)
    print_bilan "$before" "$after" "$TOTAL" \
      "L'arrêt APRÈS INSERT PostgreSQL mais AVANT commit Kafka fait rejouer le message au restart. La PK absorbe le doublon → db_valid reste cohérent avec raw_topic. La PROTECTION applicative est documentée par le compteur d'audit ci-dessous."
    echo
    ./scripts/toggle-unique.sh count
    echo
    echo "[simulpix] nettoyage : retrait du trigger d'audit"
    ./scripts/reset-scenario.sh >/dev/null 2>&1 || true
    ./scripts/toggle-unique.sh on >/dev/null 2>&1 || true
    ;;

  13_protection_pub)
    echo "[simulpix] TP 03 — démontrer la PROTECTION côté PUB (exactly_once)"
    AMBIGUOUS_ACK_DUPLICATE_PERCENT=0
    if [ "$LOSS_PERCENT" -eq 0 ] && [ "$NET_DELAY_MS" -eq 0 ] && [ "$USE_BROKER_OUTAGE" = "0" ]; then
      echo "[simulpix] mode déterministe : perte réseau, latence et outage broker désactivés"
      echo "[simulpix] cette mesure vérifie l'alignement generated/raw et l'absence de doublon PUB"
    fi
    run_with_semantics exactly_once at_least_once
    if [ "$LOSS_PERCENT" -gt 0 ] || [ "$NET_DELAY_MS" -gt 0 ]; then
      echo "[simulpix] perturbation réseau identique à 12_doublon_pub (loss=${LOSS_PERCENT}%, delay=${NET_DELAY_MS}ms)"
      echo "[simulpix] timeouts producteur Kafka request=${PRODUCER_REQUEST_TIMEOUT_MS}ms message=${PRODUCER_MESSAGE_TIMEOUT_MS}ms retries=${PRODUCER_RETRIES}"
      ./scripts/inject-fault.sh producer_ack_loss "$LOSS_PERCENT" "$NET_DELAY_MS" "$NET_JITTER_MS" >/dev/null
      perturbation_active=1
    else
      perturbation_active=0
    fi
    echo "[simulpix] $INJECTION_DELAY s de régime stable"
    sleep "$INJECTION_DELAY"
    before=$(read_counters)
    if [ "$USE_BROKER_OUTAGE" = "1" ]; then
      echo "[simulpix] outage bref du broker kafka-3 pendant ${BROKER_OUTAGE_DURATION}s"
      SIMULPIX_FAULT_DOWNTIME="$BROKER_OUTAGE_DURATION" \
        ./scripts/inject-fault.sh broker_outage kafka-3 >/dev/null
    fi
    echo "[simulpix] attente fin d'émission du generator (max ${GENERATOR_MAX_WAIT}s)"
    wait_for_generator_done "$TOTAL" "$GENERATOR_MAX_WAIT" || true
    echo "[simulpix] $OBSERVATION s d'observation puis drainage"
    sleep "$OBSERVATION"
    wait_for_drain
    if [ "$perturbation_active" = "1" ]; then
      ./scripts/inject-fault.sh producer_ack_loss_off >/dev/null
    fi
    after=$(read_counters)
    print_bilan "$before" "$after" "$TOTAL" \
      "L'idempotence producteur protège Kafka des doublons de retry. En mode réseau très dégradé, le compteur generator peut sous-estimer les livraisons confirmées ; la lecture robuste est raw vs persistés TOTAL et la ligne 'DOUBLONS PUB'."
    ;;

  13_protection_sub)
    echo "[simulpix] TP 03 — démontrer la PROTECTION côté SUB (exactly_once_kafka)"
    run_with_semantics exactly_once exactly_once_kafka
    echo "[simulpix] $INJECTION_DELAY s de régime stable avant le kill de pix-decision-engine"
    sleep "$INJECTION_DELAY"
    before=$(read_counters)
    ./scripts/inject-fault.sh chain_kill pix-decision-engine >/dev/null
    echo "[simulpix] $OBSERVATION s d'observation post-restart puis attente du drainage"
    sleep "$OBSERVATION"
    wait_for_drain
    after=$(read_counters)
    print_bilan "$before" "$after" "$TOTAL" \
      "La transaction Kafka annule les publications interrompues. outcome_count reste cohérent. L'offset technique du topic outcome peut être supérieur (marqueurs de transaction) — c'est attendu."
    ;;

  13_limite_pg)
    echo "[simulpix] TP 03 — démontrer la LIMITE côté PG"
    PERSIST_VALID_CRASH_AFTER_INSERT_ONCE=1
    echo "[simulpix] installation du trigger d'audit PG"
    ./scripts/toggle-unique.sh off >/dev/null
    run_with_semantics exactly_once exactly_once_kafka
    echo "[simulpix] $INJECTION_DELAY s de régime stable avant l'arrêt contrôlé du persister-valid"
    sleep "$INJECTION_DELAY"
    before=$(read_counters)
    ./scripts/inject-fault.sh consumer_kill persister-valid >/dev/null
    echo "[simulpix] $OBSERVATION s d'observation post-restart puis attente du drainage"
    sleep "$OBSERVATION"
    wait_for_drain
    after=$(read_counters)
    print_bilan "$before" "$after" "$TOTAL" \
      "Exactly_once_kafka NE COUVRE PAS l'écriture PostgreSQL. Le persister peut écrire en base puis redémarrer avant commit Kafka ; Kafka rejoue alors le message, et la PK/UPSERT absorbe la deuxième tentative."
    echo
    ./scripts/toggle-unique.sh count
    echo
    echo "[simulpix] nettoyage : retrait du trigger d'audit PG"
    ./scripts/reset-scenario.sh >/dev/null 2>&1 || true
    ./scripts/toggle-unique.sh on >/dev/null 2>&1 || true
    ;;

  *)
    echo "Action inconnue : $ACTION" >&2
    exit 1
    ;;
esac
