# Protocole d'analyse d'un arrêt brutal de validation Pix

## Objectif

Identifier le facteur qui déclenche une interruption brutale de la validation des Pix.

Le protocole cherche a distinguer :

- un probleme de debit pur ;
- un probleme de TTL / SLA de decision ;
- un probleme de retard de consommation cote `pix-decision-engine` ;
- un probleme introduit par la latence reseau ;
- un probleme de persistance PostgreSQL.

## Principe

Le protocole fait varier un seul facteur principal a la fois autour d'un scenario nominal.

Chaque run collecte :

- la configuration exacte du test ;
- les compteurs finaux `generated`, `processed`, `validated`, `rejected` ;
- les depassements SLA ;
- le lag consommateur maximal ;
- l'anciennete maximale du backlog ;
- l'ecart maximal de persistance ;
- la detection d'un eventuel point de rupture de validation.

## Signature d'un arret brutal de validation

On considere qu'il y a rupture de validation si, pendant que le flux continue a etre produit ou traite :

- `validated` cesse de progresser pendant plusieurs echantillons consecutifs ;
- et qu'au meme moment au moins un symptome de degradation apparait :
  - hausse de `rejected` ;
  - hausse de `rejected_sla_breaches` ;
  - hausse du lag consommateur ;
  - hausse de l'anciennete du backlog.

## Matrice d'essais

Runs proposes :

1. `R1_baseline`
   - but : etablir la reference nominale
   - scenario : `nominal`
   - volume : `400`
   - debit : `40/s`
   - TTL Pix : `10 s`

2. `R2_rate_only`
   - but : verifier si le seul debit peut casser la validation
   - scenario : `nominal`
   - volume : `2000`
   - debit : `250/s`
   - TTL Pix : `10 s`

3. `R3_low_ttl`
   - but : verifier si un TTL trop court declenche la rupture
   - scenario : `nominal`
   - volume : `2000`
   - debit : `250/s`
   - TTL Pix : `3 s`

4. `R4_processing_delay`
   - but : verifier si un ralentissement de `pix-decision-engine` suffit a provoquer l'arret
   - scenario : `nominal`
   - volume : `1200`
   - debit : `120/s`
   - TTL Pix : `10 s`
   - delai `pix-decision-engine` : `60 ms`

5. `R5_processing_delay_plus_low_ttl`
   - but : verifier si la combinaison backlog + TTL court est le declencheur principal
   - scenario : `nominal`
   - volume : `1200`
   - debit : `120/s`
   - TTL Pix : `3 s`
   - delai `pix-decision-engine` : `60 ms`

6. `R6_network_latency_mild`
   - but : verifier si une latence reseau moderee commence a degrader la validation
   - scenario : `nominal`
   - volume : `1200`
   - debit : `120/s`
   - TTL Pix : `10 s`
   - perturbation reseau : `kafka_latency` sur `pix-decision-engine`
   - delai reseau : `100 ms`, jitter `20 ms`

7. `R7_network_latency_medium`
   - but : mesurer un palier intermediaire de degradation reseau
   - scenario : `nominal`
   - volume : `1200`
   - debit : `120/s`
   - TTL Pix : `10 s`
   - perturbation reseau : `kafka_latency` sur `pix-decision-engine`
   - delai reseau : `250 ms`, jitter `40 ms`

8. `R8_network_latency_high`
   - but : verifier si une latence reseau forte provoque une rupture nette
   - scenario : `nominal`
   - volume : `1200`
   - debit : `120/s`
   - TTL Pix : `10 s`
   - perturbation reseau : `kafka_latency` sur `pix-decision-engine`
   - delai reseau : `400 ms`, jitter `60 ms`

9. `R9_network_latency_medium_low_ttl`
   - but : verifier si la meme latence reseau devient critique avec un TTL plus court
   - scenario : `nominal`
   - volume : `1200`
   - debit : `120/s`
   - TTL Pix : `3 s`
   - perturbation reseau : `kafka_latency` sur `pix-decision-engine`
   - delai reseau : `250 ms`, jitter `40 ms`

## Lecture attendue

- Si `R2` reste sain mais `R3` casse : le TTL est probablement le facteur declencheur.
- Si `R4` casse alors que `R2` reste sain : la saturation de `pix-decision-engine` est un facteur cle.
- Si `R5` est beaucoup plus brutal que `R4` : la combinaison backlog + TTL est le mecanisme principal.
- Si `R6`, `R7` puis `R8` montrent une degradation croissante, on tient un effet de seuil de la latence reseau.
- Si `R7` reste sain mais `R8` casse : la rupture vient d'un niveau de latence reseau eleve, pas de la simple presence d'une latence.
- Si `R9` casse plus tot que `R7` : le TTL court amplifie l'effet de la latence reseau.
- Si `validated` continue mais que `db_validated_count` decroche, on n'a pas un arret de validation mais un probleme de persistance.

## Livrables

Le protocole produit :

- un [fichier Excel de resultats](../analyses-techniques/pix_validation_stop_experiments.xlsx) ;
- un [fichier JSON brut de collecte](../analyses-techniques/pix_validation_stop_experiments.json) ;
- une conclusion initiale sur le facteur le plus probable.

## Resultats observes

Campagne rejouee apres correction de l'heuristique de fin de run.

| Run | Lecture courte | Conclusion |
| --- | --- | --- |
| `R1_baseline` | `400/400/400/0`, aucun lag, aucune rupture | Reference nominale saine |
| `R2_rate_only` | `2000/2000/2000/0`, aucun lag, aucune rupture | Le debit seul ne casse pas la validation |
| `R3_low_ttl` | `2000/2000/2000/0`, aucun lag, aucune rupture | Un TTL de `3 s` seul ne casse pas la validation |
| `R4_processing_delay` | `1200/1200/1200/0`, aucun lag, aucune rupture | Un ralentissement modere de `pix-decision-engine` seul ne suffit pas |
| `R5_processing_delay_plus_low_ttl` | `1200/1200/1200/0`, aucun lag, aucune rupture | Le couple backlog modere + TTL court ne suffit pas |
| `R6_network_latency_mild` | `1200/572/25/547`, rupture vers `16.53 s`, lag max `1122` | Une latence reseau moderee sur `pix-decision-engine` declenche deja la rupture |
| `R7_network_latency_medium` | `1200/228/9/220`, rupture vers `16.41 s`, lag max `1113` | La latence reseau moyenne aggrave fortement la rupture |
| `R8_network_latency_high` | `1200/141/7/134`, rupture vers `20.47 s`, lag max `1098` | La latence reseau forte degrade encore plus la capacite de validation |
| `R9_network_latency_medium_low_ttl` | `1200/229/9/221`, rupture vers `16.7 s`, lag max `1114` | Avec cette latence, le TTL plus court ne change presque rien : la rupture est deja causee par le reseau |

## Conclusion actuelle

La campagne ne montre pas de rupture lorsque l'on fait varier uniquement :

- le debit ;
- le TTL de decision ;
- un ralentissement modere de `pix-decision-engine`.

La rupture apparait des qu'une latence reseau est appliquee a `pix-decision-engine`, avec une signature stable :

- le generateur termine rapidement ;
- le lag consommateur de `pix-decision-engine` monte brutalement ;
- `validated` se fige tres tot ;
- `rejected` progresse ensuite presque uniquement via des depassements `TTL/SLA`.

Le facteur declencheur le plus probable est donc :

- la latence reseau sur `pix-decision-engine`, qui ralentit sa consommation Kafka ;
- puis la croissance du backlog ;
- puis le depassement du TTL de decision, qui transforme le backlog en rejets.

Autrement dit, le TTL explique le mode de rejet final, mais pas la cause premiere de l'arret brutal de validation. La cause premiere observee est la latence reseau appliquee a `pix-decision-engine`.
