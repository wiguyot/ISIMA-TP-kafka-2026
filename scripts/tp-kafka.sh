#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

RUNTIME_ENV_FILE="${SIMULPIX_RUNTIME_ENV_FILE:-./runtime/current-run.env}"

PIX_VALIDATOR_GROUP="simulpix-validator-v1"
PIX_DECISION_ENGINE_GROUP="simulpix-decision-engine-v1"
PIX_OUTCOME_PUBLISHER_GROUP="simulpix-outcome-publisher-v1"
PERSISTER_VALID_GROUP="simulpix-persister-valid-v1"
PERSISTER_REJECTED_GROUP="simulpix-persister-rejected-v1"

if [ -f "$RUNTIME_ENV_FILE" ]; then
  # shellcheck disable=SC1090
  . "$RUNTIME_ENV_FILE"
  PIX_VALIDATOR_GROUP="${SIMULPIX_PIX_VALIDATOR_GROUP:-$PIX_VALIDATOR_GROUP}"
  PIX_DECISION_ENGINE_GROUP="${SIMULPIX_PIX_DECISION_ENGINE_GROUP:-$PIX_DECISION_ENGINE_GROUP}"
  PIX_OUTCOME_PUBLISHER_GROUP="${SIMULPIX_PIX_OUTCOME_PUBLISHER_GROUP:-$PIX_OUTCOME_PUBLISHER_GROUP}"
  PERSISTER_VALID_GROUP="${SIMULPIX_PERSISTER_VALID_GROUP:-$PERSISTER_VALID_GROUP}"
  PERSISTER_REJECTED_GROUP="${SIMULPIX_PERSISTER_REJECTED_GROUP:-$PERSISTER_REJECTED_GROUP}"
fi

docker compose exec kafka-1 sh -lc '
  topics_bin="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"
  groups_bin="${KAFKA_GROUPS_BIN:-/opt/kafka/bin/kafka-consumer-groups.sh}"
  client_config="${KAFKA_CLIENT_CONFIG:-/config/client.properties}"

  bootstrap="kafka-1:29092,kafka-2:29092,kafka-3:29092"

  topic_summary() {
    topic="$1"
    role="$2"
    desc="$("$topics_bin" --bootstrap-server "$bootstrap" --command-config "$client_config" --describe --topic "$topic" 2>/dev/null || true)"
    if [ -z "$desc" ]; then
      printf "  %-36s  absent ou pas encore créé\n" "$topic"
      return
    fi
    summary="$(printf "%s\n" "$desc" | awk '"'"'NR == 1 {
      partition_count = "?"
      replication_factor = "?"
      for (i = 1; i <= NF; i++) {
        if ($i == "PartitionCount:") partition_count = $(i + 1)
        if ($i == "ReplicationFactor:") replication_factor = $(i + 1)
      }
      print partition_count " " replication_factor
    }'"'"')"
    partitions="$(printf "%s" "$summary" | awk "{print \$1}")"
    replication="$(printf "%s" "$summary" | awk "{print \$2}")"
    printf "  %-36s  partitions=%-2s replication=%-2s  %s\n" "$topic" "$partitions" "$replication" "$role"
  }

  group_summary() {
    group="$1"
    label="$2"
    echo
    echo "Groupe: $group"
    echo "Rôle  : $label"
    echo "Lecture: CURRENT-OFFSET = position déjà lue, LOG-END-OFFSET = fin du topic, LAG = messages en retard."
    result="$("$groups_bin" --bootstrap-server "$bootstrap" --command-config "$client_config" --describe --group "$group" 2>/dev/null || true)"
    if [ -z "$result" ]; then
      echo "  Aucune information disponible pour ce groupe."
      return
    fi
    printf "%s\n" "$result" | awk '"'"'
      BEGIN {
        found = 0
      }
      /^GROUP[[:space:]]/ { next }
      /^[[:space:]]*$/ { next }
      /does not exist/ {
        print "  Groupe non encore connu par Kafka."
        found = 1
        next
      }
      {
        topic = $2
        partitions[topic] += 1
        found = 1
        if ($5 ~ /^[0-9]+$/) {
          end_offsets[topic] += $5
        }
        if ($6 ~ /^[0-9]+$/) {
          lags[topic] += $6
        } else {
          unknown_offsets[topic] += 1
        }
      }
      END {
        if (!found) {
          print "  Aucun topic assigné pour le moment."
          next
        }
        printf "  %-34s %-10s %-14s %-10s %s\n", "topic", "partitions", "fin-topic", "lag", "lecture"
        for (topic in partitions) {
          note = "à jour"
          if (unknown_offsets[topic] > 0) {
            note = "offset non initialisé sur " unknown_offsets[topic] " partition(s)"
          } else if (lags[topic] > 0) {
            note = "retard à traiter"
          }
          printf "  %-34s %-10d %-14d %-10d %s\n", topic, partitions[topic], end_offsets[topic], lags[topic], note
        }
      }
    '"'"'
    if [ "${SIMULPIX_TP_KAFKA_VERBOSE:-0}" = "1" ]; then
      echo
      echo "Détail Kafka brut pour $group:"
      printf "%s\n" "$result"
    fi
  }

  echo "============================================================"
  echo "TP Kafka - lecture guidée de la plateforme simul-pix"
  echo "============================================================"
  echo
  echo "À retenir :"
  echo "- un topic est un journal logique de messages ;"
  echo "- une partition est une tranche ordonnée de ce journal ;"
  echo "- un consumer group garde sa propre position de lecture ;"
  echo "- un lag positif signifie que le groupe a encore des messages à traiter."
  echo

  echo "1) Topics Kafka applicatifs"
  echo "---------------------------"
  echo "Ordre logique du parcours d'\''une transaction Pix :"
  echo "  raw -> checked -> decision -> outcome"
  echo "  validated / rejected sont les sorties métier ; retry sert au rejeu."
  echo
  topic_summary simulpix.transactions.raw "Pix émis par generator"
  topic_summary simulpix.transactions.checked "Pix contrôlés par pix-validator"
  topic_summary simulpix.transactions.decision "Décisions publiées par pix-decision-engine"
  topic_summary simulpix.transactions.validated "Pix acceptés"
  topic_summary simulpix.transactions.rejected "Pix rejetés / topic de rejet métier"
  topic_summary simulpix.transactions.retry "Messages republiés pour rejeu"
  topic_summary simulpix.transactions.outcome "Réponses finales client"

  echo
  echo "2) Tous les topics visibles dans le cluster"
  echo "------------------------------------------"
  "$topics_bin" --bootstrap-server "$bootstrap" --command-config "$client_config" --list | sort | sed "s/^/  - /"

  echo
  echo "3) Consumer groups applicatifs"
  echo "------------------------------"
  echo "Un groupe correspond à une application qui lit un ou plusieurs topics."
  echo "Deux groupes différents peuvent lire les mêmes messages indépendamment."
  echo "Dans un même groupe, Kafka répartit les partitions entre les membres."

  group_summary "'"$PIX_VALIDATOR_GROUP"'" "lit raw et retry, publie checked"
  group_summary "'"$PIX_DECISION_ENGINE_GROUP"'" "lit checked, publie validated/rejected/decision"
  group_summary "'"$PIX_OUTCOME_PUBLISHER_GROUP"'" "lit decision, publie outcome"
  group_summary "'"$PERSISTER_VALID_GROUP"'" "lit validated et persiste les Pix acceptés"
  group_summary "'"$PERSISTER_REJECTED_GROUP"'" "lit rejected et persiste les Pix rejetés"

  echo
  echo "4) Comment lire cette sortie"
  echo "---------------------------"
  echo "- partitions=6 signifie que jusqu'\''à 6 consommateurs du même groupe peuvent travailler utilement en parallèle sur ce topic."
  echo "- replication=3 signifie que chaque partition est répliquée sur les 3 brokers Kafka."
  echo "- LAG=0 signifie que le groupe a rattrapé la fin du topic."
  echo "- LAG>0 signifie que des messages sont encore en attente pour ce groupe."
  echo "- offset non initialisé signifie souvent que cette partition est vide ou pas encore committée."
  echo "- Si un groupe affiche peu ou pas de lignes juste après un reset, lancez un scénario puis relancez ce script."
  echo "- Pour afficher le détail brut Kafka : SIMULPIX_TP_KAFKA_VERBOSE=1 ./scripts/tp-kafka.sh"
'
