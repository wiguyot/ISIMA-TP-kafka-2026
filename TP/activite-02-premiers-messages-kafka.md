# Activité 02 — Première publication de transactions

## Objectif

Vous allez publier vos premières transactions Pix simulées et vérifier concrètement ce qu'un producteur Kafka écrit dans un topic : une clé, une valeur JSON, une partition et un offset.

## Prérequis

- Activité 01 réalisé.
- Plateforme démarrée avec `./start.sh`.

## Étapes

### 1. Produire quelques transactions

```sh
./scripts/run-scenario.sh nominal 10 2
```

Cette commande réinitialise d'abord la plateforme (topics recréés, tables vidées), puis demande au générateur de produire 10 transactions au rythme cible de 2 messages par seconde. Elle rend la main au bout d'une minute environ. C'est vrai dans tous les TP : inutile de lancer `reset-scenario.sh` avant `run-scenario.sh`.

### 2. Observer les topics et groupes

```sh
./scripts/tp-kafka.sh
```

### 3. Lire quelques messages du topic `raw`

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --consumer.config /config/client.properties --topic simulpix.transactions.raw --from-beginning --timeout-ms 5000 --max-messages 5 --property print.key=true --property print.partition=true --property print.offset=true --property key.separator=" | "'
```

Le consommateur de console affiche chaque message sur une ligne. L'option `--timeout-ms 5000` provoque la sortie automatique après 5 secondes sans nouveau message. La partition et l'offset sont affichés en premier, puis la clé Kafka, puis la valeur JSON, séparés par ` | `.

Si le topic contient moins de messages que `--max-messages`, la commande s'arrête au bout des 5 secondes en affichant `ERROR Error processing message, terminating consumer process` suivi de `TimeoutException`. **Ce n'est pas une panne** : c'est la façon dont l'outil signale qu'il a attendu 5 secondes sans nouveau message. Vous retrouverez ce message dans les TP suivants.

## Ce que vous devez observer

- Chaque ligne a la forme : `Partition:<n> | Offset:<n> | <clé> | <JSON de la transaction>`, par exemple `Partition:4 | Offset:0 | TAX-0004 | {"transaction_id": "tx-00000003", ...}`.
- La clé est l'identifiant fiscal de l'émetteur (ex. `TAX-0003`) : c'est le champ `emitter_tax_id`.
- `Partition` indique sur quelle tranche du topic le message a atterri, et `Offset` sa position dans cette partition. Les offsets repartent de 0 dans chaque partition.
- Le producteur publie dans `simulpix.transactions.raw` sans savoir qui consomme.
- Le producteur ne connaît pas les consommateurs.

## Questions

- Quelle partie du message représente la clé Kafka ?
- Quelle partie représente la valeur ?
- Pourquoi une transaction Pix simulée contient-elle un `trace_id` ?

## Critères de réussite

- Vous voyez au moins un message dans `simulpix.transactions.raw`.
- Vous identifiez la clé Kafka.
- Vous savez expliquer la différence entre clé et valeur.
