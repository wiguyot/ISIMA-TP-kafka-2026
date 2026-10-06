# Partie 5 : pic de paiements football

## Objectif

Dans cette fiche avancée, vous allez étudier un pic de charge réaliste et comprimé dans le temps, inspiré d'un contexte de match de football.

Dans cette partie :

1. `generator` simule une population de paiements Pix émise autour d'un événement massif.
2. le trafic n'est pas uniforme : il comporte une phase de base puis un pic court et intense.
3. le pipeline doit absorber ce pic sans perdre la lecture de ce qui se passe dans Kafka et dans PostgreSQL.
4. les dashboards permettent de discuter les sémantiques critiques d'écriture, de lecture et de persistance.

Votre objectif n'est pas de provoquer artificiellement une panne. Vous devez voir ce que devient un pipeline quand l'activité métier augmente brutalement.

## Dashboards Grafana à lire

Dashboards utiles :

- `Simul-Pix - General Dashboard`
- `Simul-Pix - Kafka Dashboard`
- `Simul-Pix - Persistence Dashboard`

La partie 5 doit être lue comme une synthèse :

- le dashboard général montre la dynamique métier ;
- le dashboard Kafka montre la pression sur le transport ;
- le dashboard de persistance montre ce qui devient vraiment durable.

## Scénario recommandé

```sh
./scripts/run-scenario.sh football_match_peak
```

Signification des arguments :

- `football_match_peak` : nom du scénario à exécuter.

Ce scénario n'utilise pas les deux arguments `total_messages` et `rate_per_second` de la même manière que les autres. Il calcule en général la charge à produire à partir d'un modèle de match, mais l'interface de pilotage peut aussi laisser ces champs ouverts pour des variantes plus libres :

- nombre de matchs ;
- capacité des stades ;
- part estimée d'utilisateurs Pix ;
- part du trafic concentrée dans la fenêtre de pic ;
- compression temporelle.

Autrement dit, la charge ne vient pas d'un simple volume fixe. Elle vient d'un modèle pédagogique de montée en charge, même si l'enseignant peut ensuite forcer d'autres réglages pour discuter des limites du pipeline.

## Ce que fait réellement le scénario

Le scénario `football_match_peak` crée un trafic en deux temps :

- une longue phase `baseline` ;
- puis une concentration des émissions dans une fenêtre courte.

Ce point est essentiel pédagogiquement :

- on ne regarde plus seulement un total de messages ;
- on regarde la forme temporelle du trafic.

Le même volume total peut être facile à absorber s'il est étalé, et beaucoup plus difficile s'il est concentré sur quelques minutes compressées.

Sur la validation réelle, après une trentaine de secondes, le générateur était encore en phase `baseline` avec `178 / 15400` messages émis sur cette phase. Il ne faut donc pas attendre la fin du scénario pour commencer à lire les dashboards.

## Lecture du dashboard général

Le dashboard `Simul-Pix - General Dashboard` est la vue métier principale de cette partie.

### `messages produits`

Lecture attendue :

- la courbe n'est pas linéaire ;
- elle reste d'abord en montée régulière pendant `baseline` ;
- elle accélère ensuite nettement pendant la phase de pic ;
- elle traduit la concentration du trafic.

### `Étapes visibles de la chaîne pédagogique`

Lecture attendue :

- `pix-validator` suit généralement le flux sans difficulté majeure ;
- `pix-decision-engine` suit le flux tant qu'il absorbe la charge ;
- un décalage peut apparaître pendant le pic entre `checked`, `decision` et `outcome` ;
- ce décalage doit ensuite se résorber si le pipeline rattrape.

### `Flux pipeline (msg/s)`

Ce panneau est central dans cette partie.

Lecture attendue :

- le flux produit montre clairement une montée puis un pic ;
- le flux traité permet de voir si le pipeline suit ou décroche ;
- le flux final permet de voir à quel rythme les décisions arrivent réellement.

Cette lecture est plus importante que les seuls compteurs finaux.

### `Taux de rejet`

Lecture attendue :

- il peut rester faible si le pipeline tient la charge ;
- il peut monter si le pic commence à produire des décisions hors SLA ou des erreurs annexes.

Ce panneau permet de discuter la frontière entre :

- système chargé mais encore maîtrisé ;
- système dont la qualité de service commence à se dégrader.

### `Sortie finale : décisions finales / pix acceptés / pix rejetés`

Lecture attendue :

- `décisions finales` doit suivre l'activité réelle de sortie du pipeline ;
- `pix acceptés` reste le signal majoritaire dans un pic nominal ;
- `pix rejetés` permet de voir si la charge dégrade la décision.

## Lecture du dashboard Kafka

Le dashboard `Simul-Pix - Kafka Dashboard` permet de voir si le pic de charge met sous tension le transport.

### `État du cluster Kafka`

Lecture attendue :

- le cluster reste sain ;
- pas de panne broker ;
- pas de partition offline.

### `Partitions sous-répliquées`

Lecture attendue :

- la valeur doit idéalement rester à `0` ;
- si elle monte, le pic de charge commence à toucher la robustesse du cluster.

### `Retard de réplication par topic/partition`

Lecture attendue :

- le retard peut rester faible dans un cluster bien dimensionné ;
- s'il apparaît, il devient un excellent support pour discuter les sémantiques d'écriture producteur et la résilience des replicas.

## Lecture du dashboard de persistance

Le dashboard `Simul-Pix - Persistence Dashboard` est capital pour cette partie, car il permet de montrer pourquoi PostgreSQL reste nécessaire.

### `Compteurs de persistance`

Lecture attendue :

- la persistance suit avec un léger décalage naturel ;
- elle doit finir par rejoindre les compteurs finaux du pipeline.

### `Écarts de persistance`

Lecture attendue :

- un écart temporaire peut apparaître pendant le pic ;
- cet écart doit revenir à `0` si la sortie durable suit correctement.

Ce panneau illustre très bien l'idée suivante :

- voir un message passer dans Kafka n'est pas encore la même chose que le voir durablement écrit.

### `État courant de la base`

Lecture attendue :

- PostgreSQL continue à refléter le résultat final du pic ;
- la base joue le rôle de référence durable, y compris quand le flux instantané est difficile à lire à l'œil nu.

## Lecture de la météo des services

Sur `http://localhost:8082/`, les indicateurs les plus utiles pour la partie 5 sont :

- `Pix générés`
- `Pix traités`
- `Taux de rejet`
- `Débit raw`
- `lag total`
- `ancienneté backlog`
- `latence validation`
- `alertes actives`

Lecture attendue :

- `Débit raw` montre bien la montée en charge ;
- `lag total` et `ancienneté backlog` disent si le pic est absorbé ou non ;
- `latence validation` montre le coût du pic en temps de décision.

## Ce que vous devez vérifier côté Kafka

Le script utile est :

```sh
./scripts/tp-kafka.sh
```

Ce que vous devez y lire :

- si le cluster reste stable pendant le pic ;
- si des signes de tension apparaissent ;
- si les groupes consommateurs arrivent à revenir à l'équilibre après le pic.

## Ce que vous devez vérifier dans PostgreSQL

Le script utile est :

```sh
./scripts/tp-db.sh
```

Ce que vous devez y lire :

- le nombre final de décisions matérialisées en base ;
- la convergence entre pipeline et persistance ;
- l'absence d'écart durable après la montée en charge.

## Interprétation pédagogique

La partie 5 est la partie la plus utile pour discuter l'architecture dans son ensemble.

Elle doit faire comprendre :

- qu'un volume total ne suffit pas à caractériser une charge ;
- que la forme temporelle du trafic change complètement le comportement du pipeline ;
- que les sémantiques d'écriture et de lecture deviennent visibles quand le débit se concentre ;
- que PostgreSQL sert de référence durable quand le flux temps réel devient plus difficile à interpréter.

Cette partie est donc idéale pour discuter :

- de `acks`, de commits consommateurs et d'idempotence ;
- de la différence entre message vu, message traité, message décidé et message persisté ;
- du dimensionnement global du pipeline.

## Résultat attendu en fin de partie

Pour une exécution de type :

```sh
./scripts/run-scenario.sh football_match_peak
```

on attend au minimum :

- une première lecture utile des dashboards dès la phase `baseline`
- une montée nette des débits pendant la phase de pic
- un éventuel backlog temporaire visible si la charge approche les limites du pipeline
- une convergence finale des compteurs pipeline
- une convergence finale de PostgreSQL
- un cluster Kafka qui reste lisible et interprétable pendant la charge

## Question pédagogique centrale

À la fin de cette partie, l'étudiant doit être capable de répondre clairement à cette question :

qu'est-ce qu'un vrai pic de charge change dans la lecture du pipeline, et pourquoi faut-il regarder ensemble le flux métier, Kafka et PostgreSQL pour comprendre ce qui se passe vraiment ?
