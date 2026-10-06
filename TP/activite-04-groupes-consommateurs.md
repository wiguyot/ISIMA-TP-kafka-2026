# Activité 04 — Consumer groups Kafka

## Objectif

Vous allez faire varier le nombre de membres d'un consumer group et observer comment Kafka leur répartit les partitions.

## Prérequis

- Activité 03 réalisé.
- Plateforme démarrée.

## Étapes

Le service `pix-validator` lit le topic `raw` au sein d'un consumer group. Il peut lancer plusieurs **workers** : chacun est un membre distinct du groupe. Vous allez faire varier ce nombre de membres et observer comment Kafka répartit les partitions entre eux.

### 1. Un seul membre

```sh
./scripts/run-scenario.sh nominal 300 10
```

Cette commande réinitialise la plateforme puis lance 300 Pix à 10 Pix/s (environ 30 secondes). Relevez le nom du groupe du validateur, qui change à chaque exécution :

```sh
grep -o 'simulpix-validator-[0-9]*' runtime/current-run.env
```

Décrivez ce groupe (remplacez `<groupe>` par le nom relevé) :

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --command-config /config/client.properties --describe --group <groupe>'
```

Un seul `CONSUMER-ID` apparaît (`pix-validator-worker-1-...`) : ce membre détient à lui seul les 6 partitions de `raw` et les 3 partitions de `retry`. Les rappels de colonnes vus au Activité 03 s'appliquent.

Le `LAG` reste probablement à 0 : à 10 Pix/s, le validateur suit sans peine. Et `CONSUMER-ID` reste rempli même après la fin du flux, car un service applicatif reste abonné en permanence, contrairement à la console du Activité 03 qui s'arrêtait.

### 2. Trois membres dans le même groupe

```sh
SIMULPIX_VALIDATOR_WORKERS=3 ./scripts/run-scenario.sh nominal 300 10
```

Relevez le nouveau nom de groupe, puis décrivez-le comme à l'étape 1. Ajoutez l'option `--members` pour obtenir le nombre de partitions par membre :

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --command-config /config/client.properties --describe --group <groupe> --members'
```

Exemple de sortie réelle :

```
CONSUMER-ID                                   #PARTITIONS
pix-validator-worker-1-bedaeb70-...           3
pix-validator-worker-2-2ae7500f-...           3
pix-validator-worker-3-034a08f4-...           3
```

Les 9 partitions (6 de `raw`, 3 de `retry`) sont réparties entre les 3 membres : chaque partition est lue par **un seul** membre du groupe.

### 3. Plus de membres que de partitions utiles

```sh
SIMULPIX_VALIDATOR_WORKERS=8 ./scripts/run-scenario.sh nominal 300 10
```

Décrivez de nouveau le groupe avec `--members`. Exemple de sortie réelle :

```
CONSUMER-ID                                   #PARTITIONS
pix-validator-worker-1-...                    2
pix-validator-worker-2-...                    2
pix-validator-worker-3-...                    2
pix-validator-worker-4-...                    1
pix-validator-worker-5-...                    1
pix-validator-worker-6-...                    1
pix-validator-worker-7-...                    0
pix-validator-worker-8-...                    0
```

Deux membres n'ont **aucune** partition : ils sont connectés mais inactifs. Et avec l'option `--describe` sans `--members`, vous verrez que deux autres membres ne détiennent que les partitions 1 et 2 de `raw`, qui sont **vides** (le Activité 05 explique pourquoi) : ils ne traitent rien non plus.

## Ce que vous devez observer

- Chaque partition est assignée à **un seul** membre du groupe à la fois : deux membres d'un même groupe ne lisent jamais la même partition.
- Quand on ajoute des membres, Kafka **redistribue** les partitions entre eux (rééquilibrage).
- Le nombre de membres **utiles** est borné par le nombre de partitions, et même par le nombre de partitions qui reçoivent des messages : ici 4 sur 6 pour `raw`.
- Deux **groupes** différents lisent chacun tous les messages (Activité 03) ; deux **membres** d'un même groupe se **partagent** les messages.
- Les topics `raw`, `checked`, `decision`, `validated` et `outcome` ont 6 partitions ; `rejected` et `retry` en ont 3.

## Questions

- Pourquoi ajouter plus de consommateurs que de partitions n'augmente-t-il pas toujours le débit ?
- Quelle différence existe-t-il entre deux groupes différents et deux membres du même groupe ?
- Quel service applicatif consomme le topic `raw` ?

## Critères de réussite

- Vous savez lire une sortie `kafka-consumer-groups.sh`.
- Vous savez expliquer le rôle de `group.id`.
- Vous savez expliquer pourquoi des membres supplémentaires peuvent rester inactifs.
