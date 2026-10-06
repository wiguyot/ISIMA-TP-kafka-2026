# Activité 06 — Offsets, arrêt, reprise et rejeu

## Objectif

Vous allez arrêter puis reprendre un consommateur, rejouer des messages et rembobiner un groupe, pour comprendre que la position de lecture appartient au groupe et qu'un message n'est pas supprimé parce qu'il a été lu.

## Prérequis

- Activité 03 réalisé.
- Plateforme démarrée.

## Étapes

Dans ce TP, les commandes de lecture n'affichent que la partition, l'offset et la clé (`print.value=false`) : les lignes sont courtes et faciles à compter.

### 1. Produire un flux court

```sh
./scripts/run-scenario.sh nominal 20 4
```

La commande réinitialise la plateforme puis publie 20 Pix.

### 2. Lire 5 messages avec un groupe

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --consumer.config /config/client.properties --topic simulpix.transactions.raw --property print.partition=true --property print.offset=true --property print.key=true --property print.value=false --property key.separator=" | " --from-beginning --group tp06-reprise --timeout-ms 5000 --max-messages 5'
```

### 3. Décrire la position du groupe

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --command-config /config/client.properties --describe --group tp06-reprise'
```

Le groupe affiche « no active members » car le consommateur s'est terminé : c'est attendu (voir Activité 03). La colonne `CURRENT-OFFSET` indique la position mémorisée par Kafka pour ce groupe ; `LAG` indique ce qu'il lui reste à lire. Seules les partitions déjà lues apparaissent avec une position.

### 4. Reprendre là où le groupe s'était arrêté

Relancez la lecture avec **le même groupe**, sans `--from-beginning` et sans limite de nombre :

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --consumer.config /config/client.properties --topic simulpix.transactions.raw --property print.partition=true --property print.offset=true --property print.key=true --property print.value=false --property key.separator=" | " --group tp06-reprise --consumer-property auto.offset.reset=earliest --timeout-ms 5000'
```

Le groupe lit les **15 messages restants** et aucun des 5 déjà lus : il reprend à sa position. La commande s'arrête au bout de 5 secondes sans nouveau message (le `ERROR ... TimeoutException` est normal, voir Activité 02). Décrivez de nouveau le groupe : le `LAG` est à 0 sur toutes les partitions.

**Pourquoi `auto.offset.reset=earliest` ?** Ce réglage dit quoi faire sur une partition où le groupe n'a **encore aucune position** mémorisée. La console utilise par défaut `latest` : elle saute directement à la fin de ces partitions, **sans lire les messages qui s'y trouvent**. Essayez sans ce réglage avec un nouveau groupe : vous verrez qu'une partie des messages est sautée. C'est un piège classique en production, où un nouveau consommateur démarré en `latest` ignore tout l'historique. Les services de simul-pix sont configurés en `earliest`.

### 5. Rejouer avec un nouveau groupe

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --consumer.config /config/client.properties --topic simulpix.transactions.raw --property print.partition=true --property print.offset=true --property print.key=true --property print.value=false --property key.separator=" | " --from-beginning --group tp06-rejeu --timeout-ms 5000'
```

Le nouveau groupe relit les **20 messages** depuis le début : lire un message ne le supprime pas.

### 6. Rembobiner un groupe existant

Un groupe peut aussi être ramené en arrière, par exemple pour retraiter des messages après la correction d'un bogue :

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --command-config /config/client.properties --group tp06-reprise --topic simulpix.transactions.raw --reset-offsets --to-earliest --execute'
```

Relancez la commande de l'étape 4 : `tp06-reprise` relit à nouveau les 20 messages. Le groupe doit être inactif pendant le rembobinage, sinon Kafka refuse l'opération.

### 7. Afficher la rétention du topic

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --command-config /config/client.properties --describe --all --entity-type topics --entity-name simulpix.transactions.raw' | grep -E '^ *retention\.(ms|bytes)='
```

Sortie réelle :

```
  retention.bytes=-1 ...
  retention.ms=604800000 ...
```

`604800000` ms = 7 jours ; `-1` = pas de limite de taille.

## Ce que vous devez observer

- Une position (offset) est mémorisée **par groupe et par partition** ; elle survit à l'arrêt du consommateur.
- Le même groupe **reprend** là où il s'était arrêté ; un nouveau groupe **rejoue** tout depuis le début ; un groupe existant peut être **rembobiné**.
- Sur une partition sans position mémorisée, `auto.offset.reset` décide : `earliest` lit tout, `latest` saute à la fin.
- Kafka conserve les messages selon la **rétention** du topic (ici 7 jours), qu'ils aient été lus ou non. Tant que la rétention n'est pas expirée, n'importe quel groupe peut rejouer.

## Questions

- Qu'est-ce qu'un offset ?
- Pourquoi l'offset dépend-il du groupe consommateur ?
- Quelle différence existe-t-il entre "message consommé" et "message supprimé" ?

## Critères de réussite

- Vous savez expliquer la reprise, le rejeu avec un nouveau `group.id` et le rembobinage d'un groupe.
- Vous savez lire l'offset courant d'un groupe.
