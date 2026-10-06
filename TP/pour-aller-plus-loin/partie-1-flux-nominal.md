# Partie 1 : flux nominal

## Objectif

Dans cette fiche avancée, vous allez suivre le trajet nominal d'un paiement Pix :

1. `generator` produit un message valide dans `simulpix.transactions.raw`.
2. `pix-validator` lit `raw`, applique les contrôles d'entrée et publie dans `simulpix.transactions.checked`.
3. `pix-decision-engine` lit `checked`, décide l'acceptation et publie dans `validated` et `decision`.
4. `pix-outcome-publisher` lit `decision` et publie dans `simulpix.transactions.outcome`.
5. `persister-valid` consomme `validated` et persiste la transaction dans PostgreSQL.
6. `service-health` consolide les états des services, les compteurs, les lags Kafka et les métriques exposées à Grafana.

Dans cette partie, vous cherchez un pipeline sans rejet métier et sans incident Kafka. Toute divergence devient donc immédiatement visible, y compris dans le topic final `outcome`.

Important : au démarrage complet de la plateforme, le `generator` reste maintenant en mode `idle`. Les compteurs ne doivent donc plus bouger tant qu'aucun scénario n'a été lancé explicitement.

## Dashboards Grafana à lire

Dashboards utiles :

- `Simul-Pix - General Dashboard`
- `Simul-Pix - Kafka Dashboard`
- `Simul-Pix - Persistence Dashboard`

Ces dashboards remplacent désormais l'ancien dashboard dédié de la partie 1. Ils doivent être lus avec leur équivalent dans la météo des services.

## Scénario recommandé

```sh
./scripts/run-scenario.sh nominal 100 10
```

Signification des arguments :

- `nominal` : nom du scénario à exécuter ;
- `100` : nombre total de messages Pix à produire ;
- `10` : débit visé de production, en messages par seconde.

Autrement dit, cette commande demande au `generator` de produire `100` messages valides au total, à un rythme de `10 msg/s`.

## Lecture du dashboard général

### `messages produits`

Ce panneau affiche le dernier compteur `generated`.

Ce qu'il signifie :

- nombre total de messages émis par `generator` depuis le démarrage de la partie ;
- dans un flux nominal de 100 messages, la valeur cible finale est `100`.

À rapprocher dans la météo des services :

- carte `Pix générés` sur `http://localhost:8082/` ;
- `metrics.counts.generated` dans `GET /health` ou `GET /health/details` ;
- carte du service `generator`.

Lecture attendue :

- la valeur monte jusqu'au volume demandé ;
- si elle reste bloquée, regarder d'abord l'état `generator` puis `control.last_output`.
- juste après `./start.sh`, la valeur normale est `0` tant qu'aucun scénario n'a été lancé.

### `Étapes visibles de la chaîne pédagogique`

Ce panneau affiche la traversée de la chaîne split :

- `checked` : Pix contrôlés par `pix-validator` ;
- `decision` : décisions produites par `pix-decision-engine` ;
- `pix acceptés` : messages validés ;
- `outcomes publiés` : décisions finales publiées par `pix-outcome-publisher`.

À rapprocher dans la météo des services :

- `details.pix-validator.received` et `details.pix-validator.checked` ;
- `details.pix-decision-engine.processed` et `details.pix-decision-engine.validated` ;
- `details.pix-outcome-publisher.published` ;
- `metrics.counts.checked_topic_end_offsets` et `metrics.counts.decision_topic_end_offsets`.

Lecture attendue :

- en nominal, `checked`, `decision`, `validated` et `outcome` rejoignent `generated` ;
- si `generated` augmente mais pas `checked`, on regarde `pix-validator` ;
- si `checked` monte mais pas `decision`, on regarde `pix-decision-engine` ;
- si `decision` monte mais pas `outcome`, on regarde `pix-outcome-publisher`.

### `Sortie finale : décisions finales / pix acceptés / pix rejetés`

Ce panneau affiche simultanément `décisions finales`, `pix acceptés` et `pix rejetés`.

Ce qu'il signifie :

- `décisions finales` = nombre total de décisions finales publiées dans le topic `outcome` ;
- `pix acceptés` = messages acceptés par les règles métier ;
- `pix rejetés` = messages refusés par les règles métier.

À rapprocher dans la météo des services :

- cartes `Pix valides` et `Pix rejetés` ;
- `metrics.counts.validated` et `metrics.counts.rejected` ;
- `metrics.counts.outcome_count` ;
- les compteurs de persistance et d'alertes dans `GET /health/details`.

Lecture attendue :

- en partie 1, `pix acceptés = décisions produites = décisions finales = messages produits` à la fin ;
- `pix rejetés = 0` ;
- `décisions finales = pix acceptés + pix rejetés` ;
- si `pix rejetés` n'est pas nul, ce n'est plus un flux nominal.

### `Compteurs pipeline`

Ce panneau montre l'évolution cumulée de `messages produits`, `pix contrôlés`, `décisions produites`, `pix acceptés`, `pix rejetés` et `décisions finales`.

Ce qu'il signifie :

- il permet de voir la séquence temporelle du pipeline ;
- `messages produits` doit monter en premier ;
- `pix contrôlés` suit ;
- `décisions produites` suit à son tour ;
- `pix acceptés` suit à son tour ;
- `pix rejetés` doit rester plat à `0` ;
- `décisions finales` rejoint la sortie finale du pipeline.

À rapprocher dans la météo des services :

- les cartes `Lag Kafka`, `Débit raw/s` et `Reject %` ;
- `metrics.rates.topic_raw_offsets_per_second` dans `/health/details`.

Lecture attendue :

- les courbes restent cohérentes ;
- l'écart entre `messages produits` et `pix contrôlés`, puis entre `pix contrôlés` et `décisions produites`, ne doit pas s'installer durablement ;
- `Reject %` doit rester à `0%`.

## Lecture du dashboard Kafka

### `État du cluster Kafka`

Ce panneau affiche les indicateurs du cluster, ici surtout `active_controller_count` et `offline_partitions_count`.

Ce qu'il signifie :

- le cluster Kafka reste sain pendant l'exercice ;
- un seul broker doit porter le contrôleur actif ;
- aucune partition ne doit être hors ligne.

À rapprocher dans la météo des services :

- `details.kafka.replication.brokers` ;
- `details.kafka.replication.under_replicated_partitions_total` ;
- `details.kafka.lag_alert` ;
- carte du service `kafka`.

Lecture attendue :

- un `active_controller_count = 1` sur un broker et `0` sur les autres ;
- `offline_partitions_count = 0` partout ;
- `under_replicated_partitions_total = 0`.

## Indicateurs de météo à surveiller en priorité

Sur `http://localhost:8082/`, les indicateurs les plus utiles pour la partie 1 sont :

- `Pix générés`, `Pix traités`, `Pix valides`, `Pix rejetés` ;
- `services ok` et `services en défaut` ;
- `lag total` ;
- `latence validation` ;
- `anciennete backlog` ;
- `alertes actives` ;
- `metrics.counts.db_validated_count` et `metrics.counts.db_rejected_count` dans `GET /health/details`.

L'interprétation attendue est simple :

- tous les services métier doivent être `ok` ;
- `lag total` doit revenir à `0` ;
- `anciennete backlog` doit rester nulle ou très faible ;
- `db_validated_count` doit rejoindre `validated` ;
- `db_rejected_count` doit rester à `0`.

## Résultat attendu en fin de partie

Pour une exécution nominale de `100` messages :

- `generated = 100`
- `processed = 100`
- `validated = 100`
- `rejected = 0`
- `db_validated_count = 100`
- `db_rejected_count = 0`
- `lag total = 0`
- aucune alerte critique

Si l'un de ces points n'est pas vrai, la première question à se poser est : le problème vient-il de la production, du traitement, de la persistance ou de l'état Kafka ?

Cette même grille de lecture vaut pour tout scénario nominal lancé explicitement depuis `8083` ou via `run-scenario.sh`.
