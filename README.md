# Simul-Pix

Plateforme pédagogique locale autour de Kafka, construite autour d'un pipeline de paiements Pix simulés.

## Parcours étudiant

Commencez par le [parcours Kafka](TP/README.md). Il vous guide dans les activités 01 à 10, puis les TP 01 à 04. Vous y trouverez les étapes, les commandes et les observations attendues.

Avant de démarrer, vérifiez les [prérequis](#prérequis). Les activités utilisent la plateforme locale et ne nécessitent pas de modifier le code.

## Ressources pour prolonger le parcours

- [SAÉ Kafka](TP/README-SAé.md) : choisir et développer un sujet de projet.
- [Documentation complémentaire](docs/README.md) : parcours de lecture de 30 minutes et ressources facultatives.

Le présent README décrit la plateforme disponible. Les ressources utiles aux activités, aux TP et aux SAÉ sont classées dans la [documentation complémentaire](docs/README.md).

## Vue d'ensemble

Le pipeline applicatif est le suivant :

1. `generator` produit des transactions JSON et publie les `Pix émis` dans `simulpix.transactions.raw`.
2. `pix-validator` consomme les `Pix émis` (`raw`) et `retry`, applique les contrôles d'entrée puis publie les `Pix contrôlés` dans `simulpix.transactions.checked`.
3. `pix-decision-engine` consomme les `Pix contrôlés` (`checked`), prend la décision métier puis publie vers `validated`, `rejected` et `decision`.
4. `pix-outcome-publisher` consomme les `Décisions publiées` (`decision`) et publie les `Réponses finales` dans `outcome`.
5. `persister-valid` persiste les transactions validées dans PostgreSQL.
6. `persister-rejected` persiste les rejets métier dans PostgreSQL.
7. `service-health` agrège l'état de la plateforme, expose une météo HTML/JSON et pilote les scénarios.
8. `metrics-collector` collecte des métriques Kafka et métier vers InfluxDB.
9. Grafana affiche les dashboards transverses d'exploitation et d'observabilité.

L'architecture standard de la plateforme repose désormais uniquement sur la chaîne pédagogique suivante :

- `raw` : Pix émis
- `checked` : Pix contrôlés
- `decision` : Décisions publiées
- `outcome` : Réponses finales

## Parcours pédagogique Kafka / PUB-SUB

Le parcours détaillé, ses objectifs et les liens vers chaque activité et TP sont regroupés dans [TP/README.md](TP/README.md). Les [ateliers complémentaires](TP/pour-aller-plus-loin/README.md) sont facultatifs et s'adressent aux personnes qui souhaitent approfondir les incidents et l'exploitation.

## Stack locale

- Kafka : cluster local à 3 brokers
- PostgreSQL : 1 base `simulpix`
- InfluxDB : bucket `simulpix_observability`
- Grafana : dashboards provisionnés
- Services Python actifs par défaut : `pix-scenario-planner`, `pix-traffic-shaper`, `generator`, `pix-validator`, `pix-decision-engine`, `pix-outcome-publisher`, `persister-valid`, `persister-rejected`, `service-health`

Kafka est aujourd'hui utilisé en `SASL_PLAINTEXT` côté flux applicatifs. Les secrets TLS du dossier `infra/kafka/secrets/` sont générés pour préparer une activation ultérieure du chiffrement, mais ils ne sont pas encore utilisés par les services applicatifs.

Les images Docker utilisées par la plateforme sont figées sur une version explicite et un digest `sha256` afin de favoriser la reproductibilité des démonstrations et du parcours TP.

## Arborescence utile

- `docker-compose.yml` : définition complète de la plateforme
- `start.sh`, `stop.sh` : exploitation locale
- `services/` : implémentations Python des services métier
- `scripts/` : scénarios, rejeux, outils pédagogiques et perturbations réseau
- `TP/` : parcours étudiant, ateliers complémentaires et sujets de SAÉ
- `docs/` : ressources complémentaires pour les activités, les TP et les SAÉ
- `infra/postgres/init/` : schéma et migrations PostgreSQL
- `infra/observability/` : collecte InfluxDB et dashboards Grafana
- `runtime/` : état courant du run et des perturbations réseau

## Prérequis

### Communs à tous les systèmes

- **Docker** avec **Docker Compose v2** : la commande `docker compose version` doit répondre (avec un espace, pas `docker-compose`).
- **Mémoire** : allouer au moins **4 Go** à Docker ; **6 Go** pour le TP 04 (ksqlDB). Au repos, la plateforme occupe environ 1,4 Go.
- **Disque** : environ **6 Go** pour les images (10 Go avec ksqlDB).
- **Outils** : `git`, `sh`, `curl` et `python3` (bibliothèque standard seulement), utilisés par les scripts de `scripts/`.
- **Ports libres** sur la machine : `3000`, `5432`, `8082`, `8083`, `8086`, `9092`, `9093`, `9094` (et `8088` pour ksqlDB).
- **Aucun autre conteneur** portant les mêmes noms : `kafka-1`, `kafka-2`, `kafka-3`, `postgres`, `influxdb`, `grafana`, `generator`, etc. Arrêtez les autres piles Docker (Kafka, Grafana, PostgreSQL...) avant `./start.sh`.
- Le premier `./start.sh` télécharge les images et construit les services : comptez **5 à 10 minutes** selon le réseau ; les suivants prennent environ 1 minute.

### macOS (Apple Silicon)

- Docker Desktop récent, avec au moins 4 Go de mémoire dans *Settings > Resources*.

### Debian 13

- Docker Engine en mode **rootful** (le service `service-health` pilote Docker par `/var/run/docker.sock`), avec le plugin `docker compose`.
- L'utilisateur doit appartenir au groupe `docker`.

## Démarrage

```sh
chmod +x start.sh stop.sh scripts/*.sh
./start.sh
```

Le script :

- génère les secrets Kafka s'ils sont absents ;
- initialise `runtime/current-run.env` et `runtime/current-run.json` si nécessaire ;
- initialise `runtime/current-network.json` ;
- construit les images locales ;
- démarre toute la plateforme avec un `generator` initialement en mode inactif ;
- attend la disponibilité de `service-health`.

### Démarrage sans flux automatique

Au démarrage, la plateforme ne lance plus automatiquement de lot nominal.

Le `generator` est monté en mode `idle`, ce qui permet :

- de stabiliser la stack avant toute émission ;
- de laisser l'enseignant ou l'étudiant choisir explicitement le scénario à lancer ;
- d'éviter qu'un flux de démonstration perturbe la lecture initiale des dashboards.

La documentation et les dashboards doivent donc être lus en gardant cette distinction :

- `./start.sh` prépare la plateforme et les vues d'observabilité ;
- les scénarios lancés ensuite via `run-scenario.sh` ou `service-health` correspondent aux démonstrations pédagogiques proprement dites.

### Endpoints locaux

- `http://localhost:8082/` : météo HTML
- `http://localhost:8082/health` : synthèse JSON
- `http://localhost:8082/health/details` : détail JSON
- `http://localhost:8083/` : pilotage du parcours TP Kafka
- `http://localhost:8086` : InfluxDB
- `http://localhost:3000` : Grafana
- `localhost:5432` : PostgreSQL
- `localhost:9092`, `localhost:9093`, `localhost:9094` : brokers Kafka

### Identifiants locaux

- Grafana : `admin / adminpass`
- InfluxDB : `admin / adminpass`
- PostgreSQL : `simulpix / simulpix`

## Dashboards Grafana

Dashboards provisionnés :

- `Simul-Pix - General Dashboard`
- `Simul-Pix - Kafka Dashboard`
- `Simul-Pix - Persistence Dashboard`
- `Simul-Pix Incidents`

Répartition des vues :

- `General Dashboard` : vue métier et pipeline ;
- `Kafka Dashboard` : vue transport et cluster Kafka, avec réplication, lag consommateur, ancienneté estimée du backlog et volume observé par topic ;
- `Persistence Dashboard` : vue persistance, écarts de persistance, latences, débit d'écriture PostgreSQL, fraîcheur d'écriture et capacité de base ;
- `Incidents` : vue orientée alertes et situations dégradées.

Les mesures avancées sont facultatives ; les références sont classées dans la [documentation complémentaire](docs/README.md).

Après un `./start.sh`, les compteurs Grafana restent normalement à `0` tant qu'aucun scénario n'est lancé.

## Contrats et persistance

Topics applicatifs :

- `simulpix.transactions.raw`
- `simulpix.transactions.checked`
- `simulpix.transactions.decision`
- `simulpix.transactions.validated`
- `simulpix.transactions.rejected`
- `simulpix.transactions.retry`
- `simulpix.transactions.outcome`

Tables PostgreSQL :

- `validated_transactions`
- `rejected_transactions`

Points importants de l'implémentation actuelle :

- en mode `split`, la décision métier n'est plus concentrée dans un seul conteneur mais dans la chaîne `pix-validator -> pix-decision-engine -> pix-outcome-publisher` ;
- les transactions validées sont idempotentes sur `transaction_id` ;
- les rejets persistés sont idempotents via `rejection_fingerprint` ;
- les tentatives de rejeu transportent `source_transaction_id` et `retry_attempt` ;
- les consommateurs Kafka fonctionnent selon la sémantique choisie :
  - commit explicite après succès en `at_least_once` ;
  - commit anticipé en `at_most_once` ;
  - offsets envoyés dans la transaction Kafka en `exactly_once_kafka`.

## Pilotage des scénarios

### Réinitialiser l'état

```sh
./scripts/reset-scenario.sh
```

Le reset :

- arrête les services consommateurs ;
- efface les perturbations réseau en cours ;
- réinitialise les groupes consommateurs par défaut ;
- vide les tables PostgreSQL ;
- supprime et recrée les topics de travail ;
- redémarre les services applicatifs.

### Lancer un scénario

```sh
./scripts/run-scenario.sh nominal 100 10
./scripts/run-scenario.sh errors_simple 20 20
./scripts/run-scenario.sh mixed 60 20
./scripts/run-scenario.sh consumer_lag 120 80
./scripts/run-scenario.sh replication_lag 400 120
./scripts/run-scenario.sh football_match_peak
```

Arguments :

- nom du scénario ;
- nombre total de messages ;
- débit d'émission par seconde.

Les arguments `total_messages` et `rate_per_second` peuvent aussi être laissés vides depuis l'interface `8083` :

- `total_messages` vide : pas de borne de volume imposée par le formulaire ;
- `rate_per_second` vide ou `0` : le générateur émet aussi vite que possible ;
- `decision_sla_seconds` permet de régler le TTL métier d'une transaction Pix ;
- `producer_acks` et `producer_retries` permettent de discuter la sémantique d'écriture Kafka dès le lancement du scénario.
- `traffic_model` permet de choisir le profil d'émission du générateur :
  - `linear` : débit fixe ;
  - `poisson` : volume tiré stochastiquement autour d'une moyenne ;
  - `bursty` : trafic fluctuant avec bursts, creux et bruit lissé.

Scénarios disponibles :

- `nominal` : flux valide de bout en bout ;

### Visualiser les groupes de conteneurs

```sh
./scripts/docker-groups.sh
```

Ce script affiche les conteneurs Docker regroupés par rôle pédagogique :

- génération
- traitement
- persistance
- observabilité
- middleware
- `errors_simple` : erreurs métier simples ;
- `mixed` : mélange de succès et d'erreurs ;
- `consumer_lag` : ralentissement applicatif pour faire monter le lag ;
- `replication_lag` : arrêt temporaire d'un broker pendant l'émission ;
- `football_match_peak` : charge compressée dans le temps avec phase de base puis pic.

Chaque lancement crée aussi des consumer groups dédiés au run courant et les persiste dans `runtime/current-run.env`.

### Arrêter un scénario

```sh
./scripts/stop-scenario.sh
```

Ce script arrête uniquement `generator`. Les consommateurs continuent à drainer le backlog éventuel.

À noter : `./start.sh` ne lance plus de scénario nominal automatiquement. Les scénarios lancés via `./scripts/run-scenario.sh ...` ou via `service-health` servent désormais de point d'entrée explicite pour les démonstrations pédagogiques.

## Variables utiles

Variables courantes :

- `SIMULPIX_SCENARIO`
- `SIMULPIX_TOTAL_MESSAGES`
- `SIMULPIX_RATE_PER_SECOND`
- `SIMULPIX_TRAFFIC_MODEL`
- `SIMULPIX_SEED`
- `SIMULPIX_PROCESSING_DELAY_MS`
- `SIMULPIX_PERSIST_VALID_DELAY_MS`
- `SIMULPIX_PERSIST_REJECTED_DELAY_MS`

Variables utiles pour le modèle de trafic :

- `SIMULPIX_TRAFFIC_BUCKET_SECONDS`
- `SIMULPIX_TRAFFIC_SECOND_AMPLITUDE`
- `SIMULPIX_TRAFFIC_MINUTE_AMPLITUDE`
- `SIMULPIX_TRAFFIC_TEN_MINUTE_AMPLITUDE`
- `SIMULPIX_TRAFFIC_HOUR_AMPLITUDE`
- `SIMULPIX_TRAFFIC_NOISE_AMPLITUDE`
- `SIMULPIX_TRAFFIC_BURST_PROBABILITY`
- `SIMULPIX_TRAFFIC_BURST_AMPLITUDE`
- `SIMULPIX_TRAFFIC_BURST_DURATION_SECONDS`
- `SIMULPIX_TRAFFIC_LULL_PROBABILITY`
- `SIMULPIX_TRAFFIC_LULL_AMPLITUDE`
- `SIMULPIX_TRAFFIC_LULL_DURATION_SECONDS`

Le générateur ne fonctionne plus comme un simple métronome par milliseconde. En mode `poisson` ou `bursty`, il calcule un taux instantané variable, tire le volume à produire par bucket avec un modèle stochastique, puis répartit ce volume en microbursts à l'intérieur du bucket. Un correctif de budget ramène progressivement le volume émis vers la moyenne attendue sur la durée du scénario.

Variables utiles pour `football_match_peak` :

- `SIMULPIX_MATCH_COUNT_OVERRIDE`
- `SIMULPIX_STADIUM_CAPACITY_OVERRIDE`
- `SIMULPIX_PIX_USAGE_RATE_OVERRIDE`
- `SIMULPIX_PEAK_SHARE_OVERRIDE`
- `SIMULPIX_PEAK_WINDOW_MINUTES_OVERRIDE`
- `SIMULPIX_TOTAL_WINDOW_MINUTES_OVERRIDE`
- `SIMULPIX_TIME_COMPRESSION_FACTOR_OVERRIDE`

Exemple :

```sh
SIMULPIX_TIME_COMPRESSION_FACTOR_OVERRIDE=4 ./scripts/run-scenario.sh football_match_peak
```

## Outils TP

```sh
./scripts/tp-status.sh
./scripts/tp-db.sh
./scripts/tp-kafka.sh
./scripts/validate-observability.sh
./scripts/replay-rejected.sh 10
./scripts/replay-corrected.sh 5
./scripts/network-perturb.sh kafka_latency pix-decision-engine
./scripts/network-reset.sh
./scripts/verify-e2e.sh
```

Ces scripts permettent :

- d'inspecter la météo détaillée ;
- de consulter rapidement les volumes persistés ;
- de décrire les topics et les consumer groups du run courant ;
- de vérifier InfluxDB, Grafana et les dashboards provisionnés ;
- de republier les rejets bruts vers `retry` ;
- de corriger automatiquement certains rejets simples avant rejeu ;
- d'activer et de retirer une perturbation réseau contrôlée ;
- d'exécuter une vérification d'intégration bout en bout sur la stack Docker.

Le parcours pédagogique principal est documenté dans [TP/README.md](TP/README.md).
Les anciennes fiches d'atelier sont conservées dans [TP/pour-aller-plus-loin/](TP/pour-aller-plus-loin/README.md).

## Documentation technique complémentaire

Ces références ne sont pas nécessaires pour réaliser les activités. Leur index est dans [docs/README.md](docs/README.md) ; consultez-les lorsqu'un TP ou un projet en a besoin.

## Perturbations réseau

Profils actuellement supportés :

- `kafka_latency`
- `kafka_loss`
- `kafka_slow_link`

Services applicables par défaut :

- `generator`
- `pix-decision-engine`
- `persister-valid`
- `persister-rejected`

L'état courant de la perturbation est persisté dans `runtime/current-network.json`.

## Observabilité exposée

`service-health` expose notamment :

- l'état des services et dépendances ;
- les offsets et lags Kafka par groupe consommateur ;
- le retard de réplication par topic/partition ;
- une estimation de l'ancienneté du backlog ;
- des compteurs métier consolidés ;
- des latences moyennes de validation et de rejet depuis PostgreSQL ;
- un historique court des débits, lags et taux de rejet ;
- l'état d'InfluxDB, Grafana et de la perturbation réseau active.

Le collecteur InfluxDB écrit aussi :

- l'état des brokers ;
- les partitions sous-répliquées ;
- le lag de réplication ;
- les métriques métier issues de `service-health`.

## Arrêt et nettoyage

```sh
./stop.sh
```

- `stop.sh` arrête la plateforme, supprime ses conteneurs et volumes, puis réinitialise les fichiers d'état de `runtime/`. Les données persistées, notamment PostgreSQL, sont effacées.

## Tests

Tests unitaires légers sans Docker :

```sh
python3 -m unittest discover -s tests
```

Ils vérifient en particulier :

- la validation métier de la chaîne `pix-validator -> pix-decision-engine` ;
- la présence du commit manuel Kafka dans les configs consommateur ;
- les métadonnées de rejeu et d'idempotence sur les rejets.

Vérification d'intégration Docker :

```sh
./scripts/verify-e2e.sh
```

Ce script démarre la stack si nécessaire, exécute un scénario nominal, un scénario de rejet et un rejeu corrigé via les scripts Docker du dépôt, puis contrôle l'état et les métriques exposés par `service-health` avant de vérifier les invariants PostgreSQL.

## Limites actuelles

- pas de TLS applicatif actif sur Kafka ;
- pas de suite d'intégration automatisée de bout en bout dans le dépôt ;
- observabilité essentiellement orientée démonstration locale ;
- topologie mono-machine, non destinée à un usage de production.
