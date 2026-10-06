# Réalisation De L'Atelier

## Objet du document

Ce document structure la réalisation de l'atelier autour de Simul-Pix.

L'idée directrice est la suivante :

- utiliser un scénario métier crédible ;
- observer ses effets sur le pipeline Kafka ;
- relier ces effets aux sémantiques d'écriture et de lecture ;
- montrer pourquoi la persistance PostgreSQL reste le point de vérité final.

Le scénario principal retenu est celui des paiements Pix émis pendant des matchs de football.

## Intention pédagogique générale

Le dépôt ne sert pas seulement à montrer un flux Kafka nominal. Il sert à faire comprendre :

- ce qui est vu côté métier ;
- ce qui se passe dans la couche de transport Kafka ;
- ce qui est réellement durable en sortie du pipeline ;
- pourquoi certaines garanties techniques deviennent critiques quand la charge augmente ou quand le réseau se dégrade.

L'atelier doit toujours articuler trois vues :

1. la vue métier avec le `General Dashboard` ;
2. la vue transport avec le `Kafka Dashboard` ;
3. la vue persistance avec le `Persistence Dashboard`.

Repères utiles sur les dashboards actuels :

- le `Kafka Dashboard` sert aussi à lire le lag consommateur par groupe, l'ancienneté estimée du backlog et le volume observé par topic ;
- le `Persistence Dashboard` sert aussi à lire le débit d'écriture PostgreSQL, la fraîcheur des dernières écritures et quelques indicateurs simples de capacité de base ;
- la météo sur `8082` reste la vue rapide, tandis que `8083` sert au pilotage de l'atelier et aux repères pédagogiques.

## Chaine Pedagogique A Montrer

Depuis la séparation du traitement, l'atelier doit faire lire explicitement ces trois étapes :

1. `pix-validator`
   Il lit `raw` et `retry`, contrôle l'entrée, puis publie dans `checked`.
2. `pix-decision-engine`
   Il lit `checked`, prend la décision métier, puis publie dans `validated`, `rejected` et `decision`.
3. `pix-outcome-publisher`
   Il lit `decision` et publie la décision finale dans `outcome`.

La lecture attendue du dashboard général devient donc :

- `messages produits` : volume injecté dans `raw` ;
- `Étapes visibles de la chaîne pédagogique` : passage par `checked`, `decision`, `validated`, `outcome` ;
- `Débit par étape pédagogique (msg/s)` : vitesse de traversée des trois étapes visibles ;
- `Sortie finale` : résultat métier observable côté client et côté persistance.

## Scénario principal : paiements Pix pendant des matchs de football

Le scénario `football_match_peak` est le scénario central du parcours.

Pour comprendre la logique de génération non linéaire des flux Pix, voir aussi [traffic-models.md](../../docs/architecture/traffic-models.md).

Pourquoi ce scénario est central :

- il donne une justification métier compréhensible à la montée en charge ;
- il produit une charge non plate, avec une phase de base puis un pic ;
- il permet de discuter à la fois le débit, le lag, la stabilité et la persistance ;
- il permet de montrer qu'un pipeline peut sembler "vivant" côté Kafka tout en posant des questions de durabilité ou de délai de décision.

Ce scénario doit servir de fil rouge pour discuter :

- des sémantiques d'écriture Kafka ;
- des sémantiques de lecture Kafka ;
- de la gestion du backlog ;
- des délais de décision ;
- de l'intérêt d'une base PostgreSQL comme point de consolidation final.

## Ce Que Chaque Partie De L'Atelier Doit Montrer

### 1. Vue métier

On cherche à répondre à des questions simples :

- combien de messages ont été produits ;
- combien ont été traités ;
- combien ont été acceptés ;
- combien ont été rejetés ;
- combien de décisions finales ont été publiées ;
- le taux de rejet reste-t-il acceptable ;
- les délais de décision restent-ils compatibles avec le SLA.

Outils principaux :

- météo des services sur `http://localhost:8082/`
- `Simul-Pix - General Dashboard`

### 2. Vue transport Kafka

On cherche à montrer que la charge ou le réseau peuvent dégrader le transport, même si l'application reste en apparence "up".

Questions clés :

- le lag consommateur monte-t-il ;
- la réplication Kafka reste-t-elle saine ;
- des partitions deviennent-elles sous-répliquées ;
- l'ordre, le commit et les garanties de lecture deviennent-ils visibles dans le comportement du pipeline.

Outils principaux :

- `Simul-Pix - Kafka Dashboard`
- `./scripts/tp-kafka.sh`

### 3. Vue persistance

On cherche à montrer que le point d'arrivée pédagogique n'est pas seulement Kafka, mais la donnée durable.

Questions clés :

- les lignes attendues sont-elles réellement en base ;
- y a-t-il un écart entre ce que le pipeline annonce et ce qui est persisté ;
- les latences de validation et de rejet restent-elles raisonnables ;
- les décisions hors SLA apparaissent-elles ;
- que vaut un pipeline "rapide" si la persistance finale ne suit pas.

Outils principaux :

- `Simul-Pix - Persistence Dashboard`
- `./scripts/tp-db.sh`

## Scripts Utiles Dans L'Atelier Et Rôle Des Arguments

### `./scripts/run-scenario.sh`

Script principal de lancement d'un scénario.

Forme générale :

```sh
./scripts/run-scenario.sh <scenario> <total_messages> <rate_per_second>
```

Rôle des arguments :

- `<scenario>` : nom du scénario à exécuter.
  Valeurs utilisées dans l'atelier :
  `nominal`, `errors_simple`, `mixed`, `consumer_lag`, `replication_lag`, `football_match_peak`.
- `<total_messages>` : nombre total de messages Pix à produire pendant le scénario.
- `<rate_per_second>` : débit théorique de génération en messages par seconde.

Exemples :

```sh
./scripts/run-scenario.sh nominal 100 10
./scripts/run-scenario.sh errors_simple 20 20
./scripts/run-scenario.sh consumer_lag 120 80
./scripts/run-scenario.sh replication_lag 400 120
./scripts/run-scenario.sh football_match_peak
```

Remarque :

- pour `football_match_peak`, le script calcule surtout un plan en plusieurs phases à partir des paramètres métier.
  Le troisième argument joue donc un rôle secondaire.

### `./scripts/reset-scenario.sh`

Script de remise à zéro de l'état pédagogique.

Forme générale :

```sh
./scripts/reset-scenario.sh
```

Arguments :

- aucun argument positionnel.

Ce que fait le script :

- arrête les services applicatifs du pipeline ;
- supprime les perturbations réseau en cours ;
- réinitialise les groupes consommateurs ;
- vide les tables PostgreSQL ;
- recrée les topics de travail ;
- redémarre les services applicatifs.

### `./scripts/stop-scenario.sh`

Script d'arrêt du scénario en cours.

Forme générale :

```sh
./scripts/stop-scenario.sh
```

Arguments :

- aucun argument positionnel.

Ce que fait le script :

- arrête uniquement le `generator` ;
- laisse les consommateurs finir de drainer le backlog éventuel.

### `./scripts/replay-rejected.sh`

Script de rejeu brut des messages rejetés.

Forme générale :

```sh
./scripts/replay-rejected.sh <limit>
```

Rôle des arguments :

- `<limit>` : nombre maximum de rejets à relire depuis PostgreSQL puis à republier dans `simulpix.transactions.retry`.

Exemple :

```sh
./scripts/replay-rejected.sh 10
```

### `./scripts/replay-corrected.sh`

Script de rejeu corrigé automatique.

Forme générale :

```sh
./scripts/replay-corrected.sh <limit>
```

Rôle des arguments :

- `<limit>` : nombre maximum de rejets récents à examiner pour tenter une correction automatique avant rejeu.

Exemple :

```sh
./scripts/replay-corrected.sh 5
```

Ce script corrige seulement certains cas simples, par exemple :

- montant négatif ;
- champ obligatoire manquant ;
- incohérence simple entre identifiant fiscal et client Pix ;
- statut initial invalide.

### `./scripts/network-perturb.sh`

Script d'injection d'une perturbation réseau contrôlée.

Forme générale :

```sh
./scripts/network-perturb.sh <profile> <target_service>
```

Rôle des arguments :

- `<profile>` : type de perturbation à appliquer.
  Profils disponibles :
  `kafka_latency`, `kafka_loss`, `kafka_slow_link`.
- `<target_service>` : service applicatif sur lequel appliquer la perturbation.
  Valeurs usuelles :
  `generator`, `pix-validator`, `pix-decision-engine`, `pix-outcome-publisher`, `persister-valid`, `persister-rejected`.

Exemple :

```sh
./scripts/network-perturb.sh kafka_latency pix-decision-engine
```

Lecture des profils :

- `kafka_latency` : ajoute de la latence ;
- `kafka_loss` : ajoute de la perte réseau ;
- `kafka_slow_link` : limite le débit réseau.

### `./scripts/network-reset.sh`

Script de suppression des perturbations réseau.

Forme générale :

```sh
./scripts/network-reset.sh
```

Arguments :

- aucun argument positionnel.

Ce que fait le script :

- retire la configuration `tc` appliquée sur les services cibles ;
- remet l'état réseau courant à `none`.

### `./scripts/tp-status.sh`

Script d'affichage rapide de la météo détaillée.

Forme générale :

```sh
./scripts/tp-status.sh
```

Arguments :

- aucun argument positionnel.

### `./scripts/tp-db.sh`

Script d'inspection rapide de la base PostgreSQL.

Forme générale :

```sh
./scripts/tp-db.sh
```

Arguments :

- aucun argument positionnel.

Ce que montre le script :

- nombre de lignes validées ;
- nombre de lignes rejetées ;
- motifs de rejet et occurrences.

### `./scripts/tp-kafka.sh`

Script d'inspection rapide de Kafka.

Forme générale :

```sh
./scripts/tp-kafka.sh
```

Arguments :

- aucun argument positionnel.

Ce que montre le script :

- la liste des topics ;
- la description des topics applicatifs ;
- l'état des consumer groups du run courant.

## Parcours Recommandé De L'Atelier

### Partie 1. Lecture nominale du pipeline

Objectif pour vous :

- comprendre le trajet complet d'un Pix valide ;
- apprendre à lire la météo et le dashboard général ;
- vérifier qu'un flux simple traverse correctement tout le pipeline.

Scénario recommandé :

```sh
./scripts/run-scenario.sh nominal 100 10
```

Ce que vous devez observer :

- `messages produits = pix controles = decisions produites = pix acceptes = decisions finales`
- `pix rejetes = 0`
- pas d'écart de persistance

Conclusion attendue :

- en situation nominale, le pipeline est cohérent de bout en bout.

### Partie 2. Rejets métier simples

Objectif pour vous :

- distinguer validation métier et incident technique ;
- lire les rejets côté pipeline et côté persistance ;
- comprendre la différence entre `accepted`, `rejected` et `outcome`.

Scénario recommandé :

```sh
./scripts/run-scenario.sh errors_simple 20 20
```

Ce que vous devez observer :

- hausse de `pix rejetes`
- motifs de rejet compréhensibles
- cohérence entre Kafka et PostgreSQL

Conclusion attendue :

- un rejet fonctionnel doit être explicable, visible et durablement tracé.

### Partie 3. Charge et lag consommateur

Objectif pour vous :

- montrer qu'un pipeline peut continuer à recevoir des messages tout en prenant du retard ;
- relier le backlog à la vitesse réelle de traitement ;
- discuter l'effet des sémantiques de lecture et des commits.

Scénario recommandé :

```sh
./scripts/run-scenario.sh consumer_lag 120 80
```

Ce que vous devez observer :

- montée du lag
- divergence temporaire entre production et traitement
- rattrapage progressif si le système reste stable

Conclusion attendue :

- sous charge, la lecture Kafka et le commit consommateur deviennent des choix critiques.

### Partie 4. Incident de réplication Kafka

Objectif pour vous :

- montrer que la santé du cluster Kafka n'est pas un sujet abstrait ;
- observer l'effet d'un incident broker sur la réplication ;
- faire le lien entre cluster sain et garanties de transport.

Scénario recommandé :

```sh
./scripts/run-scenario.sh replication_lag 400 120
```

Ce que vous devez observer :

- partitions sous-répliquées
- retard de réplication par partition
- stabilité ou dégradation du pipeline métier selon la durée et l'intensité de l'incident

Conclusion attendue :

- la robustesse applicative dépend aussi de la santé du cluster Kafka.

### Partie 5. Scénario central : pic de paiements football

Objectif pour vous :

- montrer un cas de stress crédible ;
- observer le comportement combiné du pipeline, de Kafka et de PostgreSQL ;
- discuter les garanties utiles quand l'activité réelle devient intense.

Scénario recommandé :

```sh
./scripts/run-scenario.sh football_match_peak
```

Ce que vous devez observer :

- variation du débit entre baseline et peak ;
- apparition possible d'écarts ou de tensions sur les consommateurs ;
- tenue du pipeline sur la décision finale ;
- tenue de la persistance finale.

Conclusion attendue :

- le point important n'est pas seulement "Kafka tient-il", mais "la décision métier et la donnée persistée restent-elles fiables pendant le pic".

## Rôle des perturbations réseau

Les outils réseau ne sont pas une séance autonome principale. Ils servent à enrichir les scénarios précédents.

Leur rôle pédagogique est de montrer :

- qu'une charge élevée et un réseau imparfait révèlent les vrais choix d'architecture ;
- qu'un pipeline peut rester disponible tout en perdant en qualité de service ;
- que les garanties d'écriture, de lecture et de persistance se discutent concrètement.

Exemples d'utilisation :

```sh
./scripts/network-perturb.sh kafka_latency pix-decision-engine
./scripts/network-reset.sh
```

Ces perturbations sont particulièrement utiles sur :

- `consumer_lag`
- `replication_lag`
- `football_match_peak`

## Questions Pédagogiques À Poser Pendant L'Atelier

Questions transverses utiles :

- à partir de quel moment le lag devient-il un risque métier ;
- un message visible dans Kafka est-il déjà un résultat fiable pour le métier ;
- quelle différence fait-on entre traitement, décision finale et persistance ;
- que protège réellement `acks=all` ;
- pourquoi un commit consommateur trop tôt est-il dangereux ;
- en quoi PostgreSQL joue-t-il un rôle différent de Kafka ;
- que signifie un pipeline "sain" en période de pic.

## Résultat pédagogique attendu

À la fin de l'atelier, l'étudiant doit pouvoir expliquer clairement :

- le rôle respectif de Kafka et de PostgreSQL ;
- la différence entre flux observé, flux traité et flux durablement persisté ;
- les effets d'une charge forte sur les consommateurs et sur le cluster ;
- pourquoi certaines sémantiques d'écriture et de lecture sont critiques ;
- pourquoi un scénario métier réaliste, comme les paiements Pix pendant des matchs de football, est plus formateur qu'un simple benchmark abstrait.
