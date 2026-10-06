# Activité 03 — Comprendre la sémantique PUB/SUB

## Objectif

Vous allez constater par vous-même qu'un producteur publie dans un topic sans connaître les consommateurs, et que plusieurs groupes peuvent relire les mêmes messages indépendamment.

## Prérequis

- Activité 02 réalisé.
- Messages présents dans `simulpix.transactions.raw`.

## Étapes

### 1. Produire un petit flux

```sh
./scripts/run-scenario.sh nominal 12 3
```

### 2. Lire avec un premier groupe indépendant

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --consumer.config /config/client.properties --topic simulpix.transactions.raw --from-beginning --group tp03-a --timeout-ms 5000 --property print.partition=true --property print.key=true --property print.offset=true --property key.separator=" | "'
```

### 3. Lire avec un second groupe indépendant

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --consumer.config /config/client.properties --topic simulpix.transactions.raw --from-beginning --group tp03-b --timeout-ms 5000 --property print.partition=true --property print.key=true --property print.offset=true --property key.separator=" | "'
```

### 4. Comparer les offsets des groupes

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --command-config /config/client.properties --describe --group tp03-a; /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --command-config /config/client.properties --describe --group tp03-b'
```

La sortie affiche un tableau par groupe. Voici le sens de chaque colonne :

| Colonne | Signification |
|---------|---------------|
| `CURRENT-OFFSET` | Prochain offset que ce groupe lira. C'est la position validée (committée) après la dernière lecture. |
| `LOG-END-OFFSET` | Dernier offset disponible dans cette partition (fin du journal). |
| `LAG` | `LOG-END-OFFSET − CURRENT-OFFSET` : nombre de messages que ce groupe n'a pas encore lus. 0 = à jour. |
| `CONSUMER-ID` | Identifiant du membre actif du groupe assigné à cette partition. Vide si personne n'est connecté. |
| `HOST` | Adresse réseau du consommateur actif. Vide si inactif. |
| `CLIENT-ID` | Nom applicatif du client Kafka (configurable dans le code ou la commande). |

**"Consumer group has no active members"** est normal ici : `kafka-console-consumer.sh` s'est terminé après avoir lu ses messages (fin du `--timeout-ms 5000`). Kafka a gardé les offsets commités du groupe même après la déconnexion — c'est précisément ce qui permet la reprise et le rejeu.

## Ce que vous devez observer

- Les deux groupes lisent indépendamment tout le petit flux publié dans le topic ; Kafka ne supprime pas un message quand il est lu.
- Comme les deux consommateurs partent de `--from-beginning` et lisent jusqu'à cinq secondes sans nouveau message, `CURRENT-OFFSET` doit être identique pour `tp03-a` et `tp03-b` sur chaque partition.
- `print.partition=true` permet de relier les messages affichés aux offsets décrits par `kafka-consumer-groups.sh`.
- Le message `TimeoutException` après cinq secondes sans nouveau message est attendu : il marque la fin de la lecture de ce flux fini.
- "No active members" n'est pas une erreur : le processus a fini, mais Kafka conserve les offsets du groupe. C'est ce qui permettra la reprise au Activité 06.
- Un topic Kafka ressemble à un **journal d'événements** plutôt qu'à une file de tâches à consommer une seule fois.

## Questions

- Pourquoi `tp03-a` et `tp03-b` peuvent-ils lire les mêmes messages ?
- En quoi Kafka diffère-t-il d'une file classique où un message disparaît après lecture ?
- Que représente un topic dans cette manipulation ?

## Critères de réussite

- Vous avez fait lire tout le flux `simulpix.transactions.raw` par deux groupes différents et comparé leurs offsets par partition.
- Vous savez expliquer la phrase : "Kafka est un journal d'événements".
