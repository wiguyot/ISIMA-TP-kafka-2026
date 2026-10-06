# Atelier : comparer les sémantiques Kafka

Cette fiche vous propose un mini parcours guidé pour comparer :

- `at most once`
- `at least once`
- `exactly_once_kafka`

Objectif pour vous :

- comprendre ce que vous configurez ;
- voir ce que vous observez ;
- distinguer la théorie Kafka de la réalité de l'application complète.

## Préparation

1. Réinitialiser l'atelier :

```sh
./scripts/reset-scenario.sh
```

2. Ouvrir :

- `http://localhost:8082/`
- `http://localhost:8083/scenario`
- `http://localhost:3000/d/simulpix-pipeline/simul-pix-general-dashboard`
- `http://localhost:3000/d/simulpix-kafka/simul-pix-kafka-dashboard`

3. Utiliser un scénario simple :

- `Total messages = 200`
- `Débit / seconde = 20`
- `Mode trafic = poisson`
- `TTL du Pix = 10`

## Étape 1 : SUB `at most once`

Réglage :

- `Semantique SUB Kafka = At most once`

Lecture attendue :

- risque principal : perte possible ;
- le commit est fait avant traitement ;
- après incident, certains messages peuvent être considérés comme lus sans être réellement traités jusqu'au bout.

À observer :

- dans `8083` : `SUB choisi`
- dans le `General Dashboard` : `SUB Kafka active` et `Risque SUB dominant`
- dans le `Kafka Dashboard` : `Commit SUB actif`

Question pédagogique :

- que se passe-t-il si le service tombe juste après la lecture du message ?

## Étape 2 : SUB `at least once`

Réglage :

- `Semantique SUB Kafka = At least once`

Lecture attendue :

- risque principal : doublon possible ;
- le commit est fait après traitement ;
- après incident, un message peut être rejoué.

À observer :

- `Commit SUB actif = Après traitement`
- le compteur logique de l'étape reste cohérent ;
- en cas d'incident, le rejeu est possible.

Question pédagogique :

- pourquoi cette sémantique protège mieux contre la perte, mais pas contre le doublon ?

## Étape 3 : SUB `exactly_once_kafka`

Réglage :

- `Semantique SUB Kafka = Exactly once Kafka (topics Kafka)`

Lecture attendue :

- transaction Kafka sur `lecture -> traitement -> publication -> commit offset`
- pas de doublon logique sur la chaîne `Kafka -> Kafka`
- pas de garantie `exactly once` jusqu'à PostgreSQL

À observer :

- dans `8083` : le rappel précise `topics Kafka` et `hors PostgreSQL`
- dans `8082` : la chaîne affiche des compteurs métier/logiques
- dans le `General Dashboard` :
  - `SUB Kafka active = Exactly once Kafka`
  - `Risque SUB dominant = Ni doublon logique sur Kafka`
  - `pix-validator`, `pix-decision-engine`, `pix-outcome-publisher` passent en `Transaction Kafka`
- dans le `Kafka Dashboard` :
  - `Étapes en transaction Kafka = 3 / 3`
  - `Résultats finaux logiques`
  - `Topic outcome : offset technique`

Question pédagogique :

- pourquoi `Résultats finaux logiques` peut-il être différent de `Topic outcome : offset technique` ?

## Interprétation finale

- `at most once` : perte possible
- `at least once` : doublon possible
- `exactly_once_kafka` : ni doublon logique ni perte logique sur la chaîne Kafka seulement

Point clé :

- `exactly_once_kafka` ne veut pas dire `exactly once` de bout en bout ;
- PostgreSQL reste hors de cette garantie ;
- les offsets Kafka des topics transactionnels restent des mesures techniques, pas des compteurs métier.
