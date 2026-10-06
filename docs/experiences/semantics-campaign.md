# Campagne Combinatoire Trafics et Sémantiques

Ce document décrit la campagne qui croise les modèles de trafic et les sémantiques Kafka de publication et de souscription.

Objectif :

- vérifier que chaque combinaison produit bien des données ;
- mesurer les effets applicatifs visibles dans la plateforme ;
- générer un export `.xlsx` exploitable en TP.

## Matrice standard

La matrice standard comprend :

- trafics : `linear`, `poisson`, `bursty`
- sémantiques PUB : `at_most_once`, `at_least_once`, `exactly_once`
- sémantiques SUB : `at_most_once`, `at_least_once`, `exactly_once_kafka`

Soit `3 x 3 x 3 = 27` cas.

## Estimation de durée

Pour des lots de `5000` Pix à `200 Pix/s` :

- temps théorique d'injection par cas : `5000 / 200 = 25 s`
- attente post-injection par défaut du script : `30 s`

Le coût total dépend ensuite de la stratégie de préparation avant chaque cas.

### Mode `full_restart`

Ce mode exécute à chaque cas :

- `./stop.sh` (arrête et nettoie les conteneurs, volumes et fichiers d'état)
- `./start.sh`

Estimation raisonnable :

- préparation Docker et santé plateforme : environ `50 s`
- stabilisation explicite : `5 s`
- injection : `25 s`
- observation post-injection : `30 s`

Soit environ `110 s` par cas.

Pour `27` cas :

- `27 x 110 s = 2970 s`
- soit environ `49 min 30 s`

Cette valeur est une estimation optimiste. En pratique, il faut plutôt prévoir autour de `50 à 65 minutes` selon la machine.

### Mode `reset_only`

Ce mode garde la plateforme démarrée et exécute seulement :

- `./scripts/reset-scenario.sh`

Estimation raisonnable :

- reset : environ `20 s`
- stabilisation : `5 s`
- injection : `25 s`
- observation : `30 s`

Soit environ `80 s` par cas.

Pour `27` cas :

- `27 x 80 s = 2160 s`
- soit environ `36 min`

Ce mode est généralement le meilleur compromis pour une campagne exhaustive.

## Script

Le script dédié est :

- [scripts/run_semantics_campaign.py](../../scripts/run_semantics_campaign.py)

Il produit :

- un export JSON dans `docs/analyses-techniques/`
- un export Excel `.xlsx` dans `docs/analyses-techniques/`

Les colonnes d'intérêt de l'export sont :

- `published_pix`
- `accepted_pix`
- `rejected_pix`
- `rejected_ttl_pix`
- `acceptance_rate_per_second`
- `rejection_rate_per_second`
- `avg_validation_delay_ms`
- `max_queue_length`

## Usage

### Estimer sans lancer

```sh
python3 scripts/run_semantics_campaign.py --estimate-only
```

### Campagne exhaustive standard

```sh
python3 scripts/run_semantics_campaign.py \
  --total-messages 5000 \
  --rate-per-second 200 \
  --ttl-seconds 10 \
  --prepare-mode reset_only
```

### Campagne stricte avec redémarrage complet

```sh
python3 scripts/run_semantics_campaign.py \
  --total-messages 5000 \
  --rate-per-second 200 \
  --ttl-seconds 10 \
  --prepare-mode full_restart
```

### Sous-ensemble ciblé

```sh
python3 scripts/run_semantics_campaign.py \
  --total-messages 5000 \
  --rate-per-second 200 \
  --traffic-models linear,poisson \
  --producer-semantics at_most_once,at_least_once \
  --consumer-semantics at_most_once,at_least_once \
  --prepare-mode reset_only \
  --output-prefix semantics_campaign_smoke
```

## Paramètres principaux

- `--total-messages` : taille du lot injecté par cas
- `--rate-per-second` : objectif d'injection
- `--ttl-seconds` : TTL métier du Pix
- `--prepare-mode` :
  - `full_restart`
  - `reset_only`
  - `none`
- `--traffic-models` : liste filtrée de trafics
- `--producer-semantics` : liste filtrée de sémantiques PUB
- `--consumer-semantics` : liste filtrée de sémantiques SUB
- `--post-publish-observation-seconds` : temps d'observation après la fin de publication
- `--output-prefix` : nom des fichiers de sortie

## Recommandation pratique

Pour une campagne pédagogique exhaustive :

- utiliser `5000` Pix ;
- garder `200 Pix/s` ;
- fixer `TTL = 10 s` ;
- utiliser `reset_only` au lieu de `full_restart`.

Pour une campagne de recette plus stricte :

- conserver `full_restart` ;
- mais l'exécuter plutôt la nuit ou hors séance de TP.
