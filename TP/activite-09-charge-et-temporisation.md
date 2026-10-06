# Activité 09 — Montée en charge et temporisation

## Objectif

Vous allez provoquer un retard consommateur volontaire, mesurer son effet métier (des paiements rejetés pour délai dépassé), puis augmenter la capacité de consommation et en trouver la limite.

## Prérequis

- Activité 08 réalisé.
- Savoir lire le lag consommateur.

## Étapes

Le scénario `consumer_lag` ralentit volontairement les consommateurs : environ 350 ms par Pix dans `pix-decision-engine`, 150 ms dans `persister-valid`, 250 ms dans `persister-rejected`. Le délai est logiciel : le comportement est le même sur toutes les machines.

Chaque Pix doit être décidé dans les **10 secondes** qui suivent son émission (délai de décision, voir [`decision-sla.md`](../docs/architecture/decision-sla.md)). Au-delà, `pix-decision-engine` le rejette avec le motif `processing_timeout`.

### 1. Un seul worker de décision

```sh
./scripts/run-scenario.sh consumer_lag 120 80
```

La commande réinitialise la plateforme puis publie 120 Pix à 80 Pix/s : la production dure moins de 2 secondes, le traitement environ 45 secondes. Attendez 5 à 10 secondes, puis observez le lag, toutes les 15 secondes :

```sh
./scripts/tp-kafka.sh
```

Dans la section 3, le groupe `simulpix-decision-engine-...` affiche `retard à traiter`. Quand le lag est revenu à 0, faites le bilan :

```sh
./scripts/tp-status.sh
./scripts/tp-db.sh
```

### 2. Quatre workers de décision

```sh
SIMULPIX_DECISION_ENGINE_WORKERS=4 ./scripts/run-scenario.sh consumer_lag 120 80
```

Observez de la même façon, puis refaites le bilan.

### 3. Six workers de décision

```sh
SIMULPIX_DECISION_ENGINE_WORKERS=6 ./scripts/run-scenario.sh consumer_lag 120 80
```

Le nombre de workers se règle aussi depuis l'interface `http://localhost:8083/` (champ *decision engine workers*).

## Ce que vous devez observer

Résultats obtenus sur la machine de test (les vôtres doivent être proches) :

| Workers de décision | Lag résorbé en | Pix acceptés | Pix rejetés `processing_timeout` |
|---:|---:|---:|---:|
| 1 | environ 47 s | 22 | 98 (82 %) |
| 4 | environ 24 s | 80 | 40 (33 %) |
| 6 | environ 24 s | 84 | 36 (30 %) |

- **Le lag n'est pas une panne** : tous les services sont `OK`, aucun message n'est perdu. C'est un symptôme de **sous-dimensionnement** : on produit plus vite qu'on ne traite.
- **Le lag devient un problème métier** dès qu'il dépasse le délai de décision : avec un seul worker, la plupart des Pix attendent plus de 10 secondes et sont rejetés. Le client, lui, voit un paiement refusé.
- **Ajouter des workers augmente la capacité**, mais seulement jusqu'au nombre de partitions **qui reçoivent des messages** : ici 4 sur 6 (Activité 05). Au-delà, les workers supplémentaires restent inactifs (Activité 04), d'où l'absence de gain entre 4 et 6.
- Quatre workers ne divisent le temps que par 2, pas par 4 : deux partitions portent chacune **deux émetteurs**. La partition la plus chargée fixe le rythme de l'ensemble.

## Questions

- Pourquoi le lag n'est-il pas forcément une panne ?
- Quand le lag devient-il un problème métier ?
- Comment augmenter la capacité de consommation ?
- Pourquoi passer de 4 à 6 workers n'apporte-t-il presque rien ? Que faudrait-il changer pour en tirer profit ?

## Critères de réussite

- Vous observez un lag non nul et des rejets `processing_timeout`.
- Vous savez expliquer le lien entre charge, retard et délai de décision.
- Vous savez expliquer pourquoi la capacité de consommation est bornée par les partitions.
