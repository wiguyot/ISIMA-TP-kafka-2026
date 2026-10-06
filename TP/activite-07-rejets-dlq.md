# Activité 07 — Rejets, erreurs et DLQ (file de rejets)

## Qu'est-ce qu'une DLQ ?

Une **Dead Letter Queue** (littéralement « file des lettres mortes ») est un topic ou une file réservé à recevoir les messages qu'un système n'a pas pu traiter normalement. Au lieu de bloquer le flux principal en attendant qu'un message problématique soit corrigé, ou de le supprimer silencieusement, le pipeline le dévie vers cette destination secondaire où il est conservé pour diagnostic ou rejeu ultérieur.

Dans ce projet, la DLQ s'appelle **`simulpix.transactions.rejected`**. Voici comment le pipeline se bifurque :

```
raw ──► pix-validator ──► checked ──► pix-decision-engine ──► validated ──► persister-valid ──► PostgreSQL (acceptés)
                                                          │
                                                          └──► rejected  ──► persister-rejected ──► PostgreSQL (rejets)
                                                               (DLQ)
```

Concrètement sur la plateforme, quand un message invalide est injecté :
- la carte **Pix rejetés** (`http://localhost:8082/`) monte ;
- la carte **Taux de rejet** monte en parallèle ;
- la carte **Pix valides** reste à zéro (avec `errors_simple`, tous les messages sont invalides) ;
- `simulpix.transactions.rejected` accumule des messages visibles via `tp-kafka.sh` ;
- `persister-rejected` vide ce topic vers PostgreSQL — visible via `tp-db.sh`.

Une DLQ peut recevoir des erreurs **techniques** (timeout, panne réseau, désérialisation impossible) ou des erreurs **métier** (règle violée, champ manquant). Dans ce TP, vous allez illustrer les erreurs métier : `pix-validator` détecte le problème et l'inscrit dans le message, `pix-decision-engine` publie alors le Pix dans `rejected`, et le pipeline continue sans interruption.

Attention : dans simul-pix, seule la file des **rejets métier** existe. Un message techniquement illisible (JSON invalide, par exemple) n'est dévié vers aucune file : c'est une limite connue du projet, et un sujet de SAé.

## Objectif

Vous allez injecter des messages invalides, suivre leur bifurcation vers `rejected`, et comprendre comment une DLQ évite de bloquer tout le flux.

## Prérequis

- Activité 02 réalisé.
- Plateforme démarrée.

## Étapes

### 1. Produire des messages invalides

```sh
./scripts/run-scenario.sh errors_simple 20 10
```

Le scénario `errors_simple` demande au générateur de produire des messages avec des défauts intentionnels, à tour de rôle : montant négatif, champ obligatoire absent, identité du bénéficiaire incohérente avec le référentiel, statut initial inattendu. Ces messages passent par `raw`, sont contrôlés par `pix-validator`, puis rejetés par `pix-decision-engine`.

### 2. Observer les topics et groupes

```sh
./scripts/tp-kafka.sh
```

### 3. Observer la persistance des rejets

```sh
./scripts/tp-db.sh
```

### 4. Lire le topic de rejet

```sh
docker compose exec kafka-1 sh -lc '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --consumer.config /config/client.properties --topic simulpix.transactions.rejected --from-beginning --timeout-ms 5000 --max-messages 5 --property print.key=true --property print.offset=true --property key.separator=" | "'
```

## Ce que vous devez observer

**Dashboard `http://localhost:8082/`** — la DLQ en chiffres :
- **Pix rejetés** monte ; **Pix valides** reste à zéro avec `errors_simple`.
- **Taux de rejet** atteint 100 % : tous les messages ont bifurqué vers la DLQ.
- La sparkline **Reject %** dans la section Tendances trace l'évolution en temps réel.

**`tp-kafka.sh` (section 1)** — la DLQ comme topic Kafka :
- `simulpix.transactions.rejected` affiche un `fin-topic` non nul : la DLQ contient bien des messages.
- `simulpix.transactions.validated` reste à zéro : le chemin nominal est intact mais vide.
- Le consumer group `persister-rejected` apparaît avec un lag qui revient à 0 une fois que `persister-rejected` a fini de consommer.

**Console consumer (étape 4)** — le contenu de la DLQ :
- Chaque message affiche un `rejection_reason` dans sa valeur JSON : `amount_must_be_positive` (montant négatif), `missing_beneficiary_tax_id` (champ absent), `beneficiary_tax_pix_mismatch` (identité incohérente avec le référentiel) ou `invalid_initial_status` (statut inattendu).
- La clé est l'`emitter_tax_id` — le routage par clé est préservé même dans la DLQ.

**`tp-db.sh`** — la DLQ vidée vers PostgreSQL :
- `rejected_count` vaut 20 : autant que le `fin-topic` de `simulpix.transactions.rejected` dans `tp-kafka.sh`. Aucun rejet n'a été perdu entre le topic et la base.
- Le tableau `rejection_reason` montre les 4 causes, 5 fois chacune : le scénario `errors_simple` fait tourner les 4 défauts.
- Cet écart topic / base est aussi calculé par `service-health` : champ `persistence_gap_rejected` dans `http://localhost:8082/health/details` (0 quand tout est persisté).

**Ce que ce TP démontre en deux phrases** : la DLQ (`rejected`) isole les erreurs sans bloquer le flux principal ; `persister-rejected` garantit que chaque rejet est tracé durablement, ce qui permet un diagnostic et un éventuel rejeu.

## Questions

- Pourquoi isoler les messages invalides ?
- Quelle différence faites-vous entre rejet métier et panne technique ?
- Pourquoi une DLQ aide-t-elle le diagnostic ?

## Critères de réussite

- Vous voyez des messages dans `simulpix.transactions.rejected`.
- Vous voyez les rejets persistés via `./scripts/tp-db.sh`.
- Vous savez expliquer l'intérêt d'un topic de rejet.
