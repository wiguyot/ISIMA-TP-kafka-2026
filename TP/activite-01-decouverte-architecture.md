# Activité 01 — Découverte de l'architecture simul-pix

## Objectif

Dans cette première activité, prenez vos repères dans `simul-pix` : identifiez les services, repérez Kafka et comprenez le parcours général avant de produire vos premiers messages.

## Prérequis

- Machine préparée selon la section [Prérequis du README](../README.md#prérequis) : Docker et Docker Compose v2, 4 Go de mémoire pour Docker, ports libres, aucune autre pile Docker en cours.
- Dépôt cloné ; toutes les commandes se lancent depuis la racine du dépôt.

## Étapes

### 1. Démarrer la plateforme

```sh
./start.sh
```

### 2. Vérifier l'état général dans le navigateur

Ouvrir :

- `http://localhost:8082/` pour la météo générale ;
- `http://localhost:8083/` pour le pilotage des TP ;
- `http://localhost:3000` pour Grafana.

La météo HTML et `./scripts/tp-kafka.sh` présentent les informations utiles pour cette activité.

### 3. Identifier les services Docker

```sh
docker compose ps
```

Repérer notamment :

- `kafka-1`, `kafka-2`, `kafka-3` ;
- `generator` ;
- `pix-validator` ;
- `pix-decision-engine` ;
- `pix-outcome-publisher` ;
- `persister-valid` ;
- `persister-rejected` ;
- `service-health` ;
- `metrics-collector`.

### 4. Identifier les topics Kafka et les groupes consommateurs

```sh
./scripts/tp-kafka.sh
```

Cette commande affiche une lecture guidée :

- les topics applicatifs ;
- leur nombre de partitions ;
- leur facteur de réplication ;
- les groupes consommateurs ;
- le lag par groupe.

## Ce que vous devez observer

- La plateforme démarre sans scénario automatique.
- Kafka est un cluster local à 3 brokers.
- Le flux applicatif principal suit cette chaîne :
  `raw -> checked -> decision -> outcome`.
- Les topics complémentaires `validated`, `rejected` et `retry` servent au routage métier et au rejeu.

## Questions

- Quel service publie dans `simulpix.transactions.raw` ?
- Quel service consomme `simulpix.transactions.checked` ?
- Pourquoi la météo `8082` et le pilotage `8083` sont-ils séparés ?

## Critères de réussite

- Vous voyez les conteneurs principaux.
- Vous savez lister les topics Kafka.
- Vous savez désigner au moins un producteur et un consommateur.
