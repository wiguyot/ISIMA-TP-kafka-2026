# Partie 6 : perturbation réseau Kafka

## Objectif

Dans cette fiche avancée, vous allez voir comment une perturbation réseau contrôlée modifie le comportement du pipeline sans changer le code applicatif.

Dans cette partie :

1. un profil réseau est appliqué à un service cible ;
2. la communication avec Kafka devient plus lente, plus instable ou plus contrainte ;
3. les effets apparaissent dans les débits, les lags, les alertes et parfois dans la persistance ;
4. la perturbation est ensuite retirée pour observer le retour à la normale.

Votre objectif n'est pas d'ajouter un nouveau scénario métier. Vous devez étudier l'impact de la qualité réseau sur un pipeline événementiel.

## Dashboards Grafana à lire

Dashboards utiles :

- `Simul-Pix - General Dashboard`
- `Simul-Pix - Kafka Dashboard`
- `Simul-Pix - Persistence Dashboard`
- `Simul-Pix Incidents`

La partie 6 se lit comme une superposition :

- la vue métier montre les conséquences fonctionnelles ;
- la vue Kafka montre la réaction du transport ;
- la vue persistance montre si la sortie durable prend elle aussi du retard ;
- le dashboard incidents et la météo aident à qualifier la perturbation active.

## Commandes recommandées

```sh
./scripts/run-scenario.sh nominal 400 10
./scripts/network-perturb.sh kafka_latency pix-decision-engine
./scripts/network-reset.sh
```

La première commande lance un flux assez long pour que la perturbation soit appliquée pendant que le pipeline travaille encore.

Signification des arguments de `network-perturb.sh` :

- `kafka_latency` : profil de perturbation réseau à appliquer ;
- `pix-decision-engine` : service cible sur lequel appliquer cette perturbation.

Autrement dit, cette commande ajoute volontairement de la latence réseau au service `pix-decision-engine` dans sa communication avec Kafka. La commande `./scripts/network-reset.sh` retire ensuite les perturbations réseau actives.

Important :

- si le flux s'est déjà terminé quand on applique la perturbation, on verra bien l'alerte réseau active, mais pas forcément d'effet métier significatif ;
- il faut donc injecter la perturbation pendant un scénario encore en cours.

## Profils réseau utiles

Les profils principaux sont :

- `kafka_latency` : ajoute surtout de la latence et du jitter ;
- `kafka_loss` : ajoute de la perte de paquets ;
- `kafka_slow_link` : limite le débit réseau.

Leur effet pédagogique n'est pas le même :

- la latence retarde les échanges ;
- la perte fait discuter retries et acknowledgements ;
- le lien lent crée une contrainte de débit plus structurelle.

## Ce que fait réellement la perturbation

La perturbation n'agit pas sur Kafka en tant que cluster. Elle agit sur l'interface réseau du service cible.

Si on perturbe `pix-decision-engine` :

- `pix-decision-engine` devient plus lent à dialoguer avec Kafka ;
- la lecture ou l'émission de messages prend plus de temps ;
- le lag peut monter ;
- la décision finale peut arriver plus tard ;
- la persistance peut ensuite être décalée à son tour.

Cette partie est donc idéale pour discuter l'effet de l'infrastructure sur le comportement applicatif.

## Lecture du dashboard général

Le dashboard `Simul-Pix - General Dashboard` montre les conséquences visibles sur le flux.

### `messages produits`

Lecture attendue :

- la production peut rester normale si le `generator` n'est pas la cible ;
- cela permet de voir plus clairement le décrochage du reste du pipeline.

### `Étapes visibles de la chaîne pédagogique`

Lecture attendue :

- si `pix-decision-engine` est la cible, `checked` peut continuer à monter alors que `decision` ralentit ;
- un écart apparaît alors entre messages contrôlés et décisions produites.

### `Flux pipeline (msg/s)`

Lecture attendue :

- les débits deviennent plus irréguliers ;
- le flux traité baisse ou se désynchronise ;
- le retour à la normale est visible après `network-reset.sh`.

### `Taux de rejet`

Lecture attendue :

- il peut rester stable si la perturbation est modérée ;
- il peut monter si la perturbation finit par provoquer des décisions hors SLA.

### `Sortie finale : décisions finales / pix acceptés / pix rejetés`

Lecture attendue :

- la production de décisions finales peut ralentir ;
- les décisions peuvent continuer à sortir, mais plus tard ;
- une dégradation plus forte peut entraîner des rejets supplémentaires.

## Lecture du dashboard Kafka

Le dashboard `Simul-Pix - Kafka Dashboard` sert à voir si la perturbation réseau se traduit par :

- du lag consommateur ;
- une dégradation du débit ;
- ou des signes plus structurants sur le cluster.

### `État du cluster Kafka`

Lecture attendue :

- le cluster lui-même peut rester sain ;
- la perturbation agit souvent plus sur les clients que sur les brokers.

### `Partitions sous-répliquées`

Lecture attendue :

- elles doivent idéalement rester à `0` ;
- si elles montent, la perturbation dépasse le simple effet côté client.

### `Retard de réplication par topic/partition`

Lecture attendue :

- il ne doit pas être l'indicateur principal si seule la communication client est perturbée ;
- s'il bouge beaucoup, il faut réévaluer la portée de la perturbation.

## Lecture du dashboard de persistance

Le dashboard `Simul-Pix - Persistence Dashboard` montre si la perturbation réseau finit par décaler la sortie durable.

### `Compteurs de persistance`

Lecture attendue :

- la base peut suivre avec retard ;
- elle doit rattraper quand la perturbation est retirée.

### `Écarts de persistance`

Lecture attendue :

- un écart temporaire peut apparaître ;
- il doit revenir à `0` après normalisation.

### `État courant de la base`

Lecture attendue :

- PostgreSQL permet de vérifier la convergence finale, même si la lecture temps réel a été brouillée pendant la perturbation.

## Lecture de la météo des services

Sur `http://localhost:8082/`, les indicateurs les plus utiles pour la partie 6 sont :

- `alertes actives`
- `lag total`
- `ancienneté backlog`
- `Taux de rejet`
- `latence validation`
- l'état de la perturbation réseau active

Lecture attendue :

- la météo signale qu'une perturbation réseau est active ;
- `lag total` et `ancienneté backlog` montent si la perturbation est appliquée pendant un flux vivant ;
- les services peuvent rester techniquement `ok` tout en étant nettement ralentis.

## Ce que vous devez vérifier côté Kafka

Le script utile est :

```sh
./scripts/tp-kafka.sh
```

Ce que vous devez y lire :

- l'impact de la perturbation sur les groupes consommateurs ;
- l'évolution du lag ;
- la différence entre effet côté client et effet côté cluster.

## Ce que vous devez vérifier dans PostgreSQL

Le script utile est :

```sh
./scripts/tp-db.sh
```

Ce que vous devez y lire :

- si la persistance suit ou non pendant la perturbation ;
- si la convergence finale revient après `network-reset.sh`.

## Interprétation pédagogique

La partie 6 doit faire comprendre que la qualité réseau fait partie du comportement réel du pipeline.

Elle permet de discuter concrètement :

- de la sensibilité aux latences ;
- de l'impact des pertes réseau ;
- du rôle des retries, acknowledgements et timeouts ;
- de la différence entre cluster sain et clients en difficulté ;
- du retour à la normale après suppression d'une contrainte réseau.

## Résultat attendu en fin de partie

Pour une exécution de type :

```sh
./scripts/run-scenario.sh nominal 400 10
./scripts/network-perturb.sh kafka_latency pix-decision-engine
./scripts/network-reset.sh
```

on attend au minimum :

- une dégradation visible pendant la perturbation
- un lag ou un retard fonctionnel observable
- sur la validation réelle, `lag total` est monté au-delà de `200` avec apparition de `processing_timeout`
- une convergence finale après reset réseau
- un retour progressif des indicateurs vers leur niveau nominal

## Question pédagogique centrale

À la fin de cette partie, l'étudiant doit être capable de répondre clairement à cette question :

comment distingue-t-on l'effet d'une perturbation réseau côté clients Kafka d'une panne Kafka elle-même, et comment voit-on cette différence dans le flux, dans la météo des services et dans la persistance ?
