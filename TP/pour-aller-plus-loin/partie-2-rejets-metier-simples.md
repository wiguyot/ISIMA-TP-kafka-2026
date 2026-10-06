# Partie 2 : rejets métier simples

## Objectif

Dans cette fiche avancée, vous allez observer ce qui se passe quand un paiement Pix est refusé par les règles métier du pipeline.

Dans cette partie :

1. `generator` produit volontairement des messages invalides via le scénario `errors_simple`.
2. `pix-validator` contrôle l'entrée et publie dans `simulpix.transactions.checked`.
3. `pix-decision-engine` détecte les erreurs métier, publie dans `simulpix.transactions.rejected` et produit `simulpix.transactions.decision`.
4. `pix-outcome-publisher` transforme `decision` en décision finale dans `simulpix.transactions.outcome`.
5. `persister-rejected` consomme `rejected` et persiste les rejets dans PostgreSQL.
6. `service-health` permet de lire le taux de rejet, les motifs de rejet et la cohérence entre Kafka et PostgreSQL.

Votre objectif n'est pas de montrer un incident technique. Vous devez voir un refus métier explicable, observable et durablement tracé.

## Dashboards Grafana à lire

Dashboards utiles :

- `Simul-Pix - General Dashboard`
- `Simul-Pix - Persistence Dashboard`
- `Simul-Pix Incidents`

La partie 2 se lit surtout à travers le dashboard général pour le flux et le dashboard de persistance pour les rejets persistés.

## Scénario recommandé

```sh
./scripts/run-scenario.sh errors_simple 20 20
```

Signification des arguments :

- `errors_simple` : nom du scénario à exécuter ;
- premier `20` : nombre total de messages Pix à produire ;
- second `20` : débit visé de production, en messages par seconde.

Autrement dit, cette commande demande au `generator` de produire `20` messages au total, selon le scénario `errors_simple`, à un rythme de `20 msg/s`.

Ce scénario injecte des erreurs simples dans les messages produits. La chaîne split doit donc produire à la fois :

- des messages rejetés ;
- une décision finale client associée ;
- une persistance durable des rejets dans PostgreSQL.

## Ce qui est injecté par le scénario

Le scénario `errors_simple` fabrique plusieurs familles d'erreurs, par exemple :

- montant négatif ;
- champ obligatoire manquant ;
- incohérence entre identifiant fiscal et client Pix ;
- statut initial invalide.

Ces erreurs sont détectées par `pix-decision-engine`, puis traduites en :

- `rejection_reason` technique interne ;
- `decision_reason_code` et `decision_reason_label` lisibles côté client ;
- `decision_origin`, en général `BANK` pour un rejet métier simple.

Autrement dit :

- `rejection_reason` explique le détail technique du contrôle qui a échoué ;
- `decision_reason_code` et `decision_reason_label` donnent la lecture métier ou client de ce rejet.

## Lecture du dashboard général

### `messages produits`

Ce panneau indique combien de messages ont été émis par le `generator`.

Lecture attendue :

- la valeur monte jusqu'au volume demandé ;
- dans l'exemple recommandé, la cible est `20`.

### `Étapes visibles de la chaîne pédagogique`

Ce panneau indique comment les messages passent par :

- `checked` ;
- `decision` ;
- `pix rejetes` ;
- `decisions finales`.

Lecture attendue :

- `checked` rejoint `messages produits` ;
- `decision` rejoint `checked` ;
- `pix rejetes` devient majoritaire ;
- `decisions finales` rejoint `decision`.

### `Sortie finale : décisions finales / pix acceptés / pix rejetés`

Ce panneau est central pour la partie 2.

Ce que vous devez comprendre :

- `décisions finales` = nombre total de décisions finales publiées dans le topic `outcome` ;
- `pix acceptés` = messages validés ;
- `pix rejetés` = messages refusés par les règles métier.

Lecture attendue :

- `décisions finales = pix acceptés + pix rejetés` ;
- `pix rejetés` n'est plus nul ;
- avec `./scripts/run-scenario.sh errors_simple 20 20`, l'observation réelle est même `pix acceptés = 0` et `pix rejetés = 20`.

### `Taux de rejet`

Ce panneau permet de lire immédiatement la proportion de rejets.

Lecture attendue :

- le taux de rejet devient significatif ;
- sur le cas de validation recommandé, il monte en pratique à `100%`.

### `Compteurs pipeline`

Ce panneau montre l'évolution cumulée de :

- `messages produits`
- `pix controles`
- `decisions produites`
- `pix acceptes`
- `pix rejetes`
- `decisions finales`

Lecture attendue :

- `messages produits` monte d'abord ;
- `pix controles` suit ;
- `decisions produites` suit ;
- `pix acceptes` et `pix rejetes` se séparent ;
- `decisions finales` rejoint la somme des deux branches finales.

## Lecture du dashboard de persistance

Le dashboard `Simul-Pix - Persistence Dashboard` est le second point de lecture principal de cette partie.

### `Compteurs de persistance`

Ce panneau permet de comparer :

- les messages persistés par les persisters ;
- les lignes réellement présentes en base.

Lecture attendue :

- les compteurs persistés rejetés montent ;
- les lignes PostgreSQL rejetées suivent ;
- il ne doit pas y avoir d'écart durable entre pipeline et base.

### `Ecarts de persistance`

Ce panneau sert à vérifier qu'un rejet annoncé par le pipeline finit bien en base.

Lecture attendue :

- `persistence_gap_rejected` revient à `0` ;
- si un écart s'installe, ce n'est plus un simple sujet métier, mais un problème de sortie durable.

### `État courant de la base`

Ce panneau permet de confirmer rapidement :

- que des rejets ont bien été écrits ;
- que la base reflète le comportement du pipeline.

## Lecture de la météo des services

Sur `http://localhost:8082/`, les indicateurs les plus utiles pour la partie 2 sont :

- `Pix générés`
- `Pix traités`
- `Pix valides`
- `Pix rejetés`
- `Taux de rejet`
- `Alertes`
- `latence validation`
- `services ok` et `services en défaut`

Lecture attendue :

- `Pix rejetés` devient strictement positif ;
- `Pix traités = Pix valides + Pix rejetés` ;
- le taux de rejet devient non nul ;
- les services restent `ok`, mais une alerte de taux de rejet élevé apparaît normalement.

## Ce que vous devez vérifier dans PostgreSQL

Le script utile est :

```sh
./scripts/tp-db.sh
```

Ce que vous devez y lire :

- le nombre de lignes dans `rejected_transactions` ;
- les motifs de rejet les plus fréquents ;
- la cohérence entre la base et les compteurs du pipeline.

Lecture attendue :

- `rejected_count` est supérieur à `0` ;
- les motifs remontent sous forme de `rejection_reason` ;
- les occurrences sont cohérentes avec le scénario injecté.

## Ce que vous devez vérifier côté Kafka

Le script utile est :

```sh
./scripts/tp-kafka.sh
```

Ce que vous devez y lire :

- le topic `simulpix.transactions.rejected` contient bien des messages ;
- le topic `simulpix.transactions.outcome` contient les décisions finales ;
- les consumer groups du run courant finissent par revenir sans lag durable.

Lecture attendue :

- `rejected` et `outcome` progressent ;
- le cluster Kafka reste sain ;
- le sujet observé est bien un rejet métier, pas un incident de transport.

## Interprétation pédagogique

La partie 2 doit faire comprendre la différence entre trois niveaux :

- un message traité ;
- un message rejeté pour raison métier ;
- une décision finale formulée de manière lisible pour le client.

Exemple d'interprétation :

- `rejection_reason=amount_must_be_positive` est une lecture technique interne ;
- `decision_reason_code=BANK_REJECTED` et `decision_reason_label=Paiement Pix rejeté par la banque : montant invalide` sont la traduction métier ou client.

Cette distinction est essentielle :

- le pipeline doit garder un motif technique exploitable ;
- l'application doit aussi être capable d'expliquer le rejet de manière compréhensible.

## Résultat attendu en fin de partie

Pour une exécution de type :

```sh
./scripts/run-scenario.sh errors_simple 20 20
```

on attend au minimum :

- `messages produits = 20`
- `pix controles = 20`
- `decisions produites = 20`
- `pix acceptes = 0`
- `pix rejetes = 20`
- `decisions finales = pix acceptes + pix rejetes`
- `rejected_count > 0` en base
- `persistence_gap_rejected = 0` à la fin
- une alerte de `taux de rejet élevé`

## Question pédagogique centrale

À la fin de cette partie, l'étudiant doit être capable de répondre clairement à cette question :

qu'est-ce qui différencie, dans le pipeline, un rejet métier explicable d'un incident technique, et comment le voit-on dans le flux, dans Kafka et dans PostgreSQL ?
