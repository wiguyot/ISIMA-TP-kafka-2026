# Activité 05 — Partitionnement par clé métier

## Objectif

Vous allez vérifier quelle clé métier est utilisée pour partitionner les paiements, puis comprendre pourquoi cette clé protège l'ordre relatif d'un même émetteur.

## Prérequis

- Activité 02 réalisé.
- Savoir lire un message Kafka avec sa clé.

## Étapes

### 1. Vérifier le nombre de partitions

```sh
./scripts/tp-kafka.sh
```

Repérer la description de `simulpix.transactions.raw`.

### 2. Vérifier la clé utilisée par le producteur

Dans `services/generator/runtime.py` (cherchez `emitter_tax_id`), le producteur utilise :

```python
key = message.get("emitter_tax_id", f"key-{index}")
```

La clé Kafka est donc le numéro fiscal de l'émetteur quand il est présent. Si le message est invalide (champ manquant), Kafka reçoit un fallback `key-<index>`. La clé voyage séparément de la valeur JSON dans l'en-tête Kafka, mais le même champ `emitter_tax_id` est aussi présent dans le corps du message : les deux portent la même information.

### 3. Produire un flux nominal

```sh
./scripts/run-scenario.sh nominal 30 5
```

### 4. Lire les clés, partitions et offsets

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --consumer.config /config/client.properties --topic simulpix.transactions.raw --from-beginning --timeout-ms 5000 --max-messages 10 --property print.partition=true --property print.offset=true --property print.key=true --property key.separator=" | "'
```

## Ce que vous devez observer

La commande affiche une ligne par message au format :

```
Partition:<n> | Offset:<n> | <clé> | <valeur JSON>
```

Le troisième champ — entre le deuxième et le troisième `|` — est la clé Kafka. Sur un scénario nominal, elle ressemble à `TAX-0003`. C'est l'`emitter_tax_id` de l'émetteur, pas un préfixe `key-` : le fallback `key-<index>` du code ne se déclenche que si ce champ est absent, ce qui n'arrive pas en nominal.

Ce que vous devez vérifier ligne par ligne :

- Le champ clé (`TAX-XXXX`) correspond exactement au champ `emitter_tax_id` dans le JSON.
- Tous les messages du même émetteur (même clé) tombent dans la même partition.
- Deux émetteurs différents peuvent tomber dans des partitions différentes, **ou dans la même**.
- L'ordre des offsets dans une partition est cohérent pour un émetteur donné, pas entre partitions.

Sur la plateforme, les 6 émetteurs du référentiel se répartissent toujours ainsi (topic `raw`, 6 partitions) :

| Partition | Émetteurs |
|---|---|
| 0 | `TAX-0006` |
| 1 | aucun |
| 2 | aucun |
| 3 | `TAX-0003` |
| 4 | `TAX-0004`, `TAX-0005` |
| 5 | `TAX-0001`, `TAX-0002` |

**Comment le producteur choisit-il la partition ?**

Le producteur calcule un **hachage** des octets de la clé, puis `hachage(clé) % nombre_de_partitions`. Les services de simul-pix sont écrits en Python avec la bibliothèque librdkafka, dont le partitionneur par défaut utilise l'algorithme **CRC32**. Les clients Java de Kafka utilisent un autre algorithme, **Murmur2** : pour une même clé, un producteur Python et un producteur Java ne choisissent donc pas forcément la même partition.

Conséquences pratiques :
- La partition d'une clé donnée est **déterministe** : même clé → même partition, toujours, sur le même topic avec le même nombre de partitions.
- Le numéro de partition **n'a aucun rapport** avec le contenu de la clé : que `TAX-0003` tombe en partition 3 est une coïncidence. C'est le résultat du hachage qui décide.
- Avec peu de clés, la répartition est **inégale** : ici, 2 partitions sur 6 restent vides et 2 partitions reçoivent deux émetteurs. Un consumer group ne peut donc employer utilement que 4 consommateurs sur ce topic (voir Activité 04).
- Si le nombre de partitions change (redimensionnement du topic), le modulo change et les clés sont redistribuées : l'ordre par clé n'est plus garanti entre anciens et nouveaux messages.

## Questions

- Pourquoi l'émetteur est-il une bonne clé métier pour un paiement ?
- Quel problème pourrait apparaître si deux paiements du même émetteur étaient traités dans un ordre incohérent ?
- Pourquoi Kafka ne garantit-il pas un ordre global sur toutes les partitions ?
- Avec 6 émetteurs et 6 partitions, pourquoi 2 partitions restent-elles vides ? Que faudrait-il pour mieux répartir la charge ?

## Critères de réussite

- Vous savez identifier la clé `emitter_tax_id`.
- Vous savez relier clé, partition et ordre relatif.
