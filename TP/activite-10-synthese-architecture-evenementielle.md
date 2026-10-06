# Activité 10 — Synthèse : expliquer le chemin d'une transaction Pix

## Objectif

Vous allez reconstituer le chemin complet d'une transaction Pix et vous entraîner à expliquer l'architecture de bout en bout avec vos propres mots.

## Prérequis

- Activité 01 à Activité 09 réalisés.

## Étapes

### 1. Rejouer un flux nominal court

```sh
./scripts/run-scenario.sh nominal 20 5
```

### 2. Reconstituer le chemin logique

Compléter oralement ou par écrit le chemin suivant :

```text
generator
  -> simulpix.transactions.raw
  -> pix-validator
  -> simulpix.transactions.checked
  -> pix-decision-engine, qui publie la décision sur trois topics :
       -> simulpix.transactions.validated -> persister-valid    -> PostgreSQL
       -> simulpix.transactions.rejected  -> persister-rejected -> PostgreSQL
       -> simulpix.transactions.decision  -> pix-outcome-publisher -> simulpix.transactions.outcome
```

La persistance et la réponse finale sont deux branches **parallèles** : les persisters lisent `validated` et `rejected`, pas `outcome`.

### 3. Vérifier avec les outils du projet

```sh
./scripts/tp-kafka.sh
./scripts/tp-db.sh
./scripts/tp-status.sh
```

### 4. Modifier un paramètre et prédire l'effet

Choisir une variante :

```sh
./scripts/run-scenario.sh mixed 60 20
```

Le scénario `mixed` produit un mélange de messages valides et de messages intentionnellement invalides, ce qui génère à la fois des transactions dans `validated` et des rejets dans `rejected`.

ou :

```sh
./scripts/run-scenario.sh consumer_lag 120 80
```

Avant d'observer les résultats, formuler une hypothèse sur :

- les topics qui vont grossir ;
- les groupes qui vont prendre du retard ;
- les compteurs métier qui vont changer ;
- les dashboards les plus utiles.

## Ce que vous devez observer

- `tp-kafka.sh` affiche le chemin complet : topics, groupes, lag — tout ce qui a été vu dans les TP précédents est visible dans un seul endroit.
- `tp-status.sh` (section 2) confirme que les compteurs métier (généré / traité / accepté / rejeté / persisté) convergent.
- Kafka découple les producteurs et les consommateurs : un consommateur tombé puis redémarré reprend à son dernier offset **commité**. Les messages lus mais pas encore commités sont relus : c'est le sujet des TP 01 à 03.
- Les topics représentent des étapes du flux ; les consumer groups matérialisent la progression des traitements.
- Les métriques permettent de relier comportement technique (lag, offset) et effet métier (Pix accepté, rejeté, persisté).

## Questions

- Pourquoi l'architecture utilise-t-elle plusieurs topics plutôt qu'un seul ?
- Où le projet préserve-t-il l'ordre relatif d'un même émetteur ?
- Que se passe-t-il si un consommateur tombe puis redémarre ?
- Quel est le rôle de `rejected` dans la robustesse du pipeline ?

## Critères de réussite

- Vous savez expliquer le chemin complet d'une transaction.
- Vous savez relier topic, partition, groupe, offset et observabilité.
- Vous savez distinguer un rejet métier, un retard et un incident Kafka.
