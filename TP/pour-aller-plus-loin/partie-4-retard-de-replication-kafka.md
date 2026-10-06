# Partie 4 : retard de réplication Kafka

## Objectif

Dans cette fiche avancée, vous allez observer ce qui se passe quand le cluster Kafka lui-même est perturbé pendant que des messages Pix continuent à être produits.

Dans cette partie :

1. `generator` émet un volume important de messages.
2. le scénario arrête temporairement un broker Kafka.
3. la réplication prend du retard.
4. le pipeline continue à fonctionner dans un mode dégradé observable.

Votre objectif n'est plus de montrer un consommateur lent. Vous devez voir un incident contrôlé sur la couche transport et réplication.

## Dashboards Grafana à lire

Dashboards utiles :

- `Simul-Pix - General Dashboard`
- `Simul-Pix - Kafka Dashboard`
- `Simul-Pix - Persistence Dashboard`

La partie 4 se lit surtout à travers le dashboard Kafka. Le dashboard général sert à voir si cet incident de transport commence ou non à se propager jusqu'au flux métier.

Point de méthode important :

- il faut observer cette partie pendant l'incident, pas seulement une fois le scénario terminé ;
- en fin de scénario, le cluster peut déjà avoir retrouvé un état nominal.

## Scénario recommandé

```sh
./scripts/run-scenario.sh replication_lag 400 120
```

Signification des arguments :

- `replication_lag` : nom du scénario à exécuter ;
- `400` : nombre total de messages Pix à produire ;
- `120` : débit visé de production, en messages par seconde.

Autrement dit, cette commande demande au `generator` de produire `400` messages au total, à `120 msg/s`, tandis que le scénario provoque en parallèle une interruption temporaire d'un broker pour faire apparaître du retard de réplication.

## Ce que fait réellement le scénario

Le scénario `replication_lag` coupe temporairement un broker Kafka, par défaut `kafka-3`, pendant l'émission des messages, puis le redémarre automatiquement.

Ce que vous devez comprendre :

- les producteurs continuent à émettre ;
- les leaders de partitions continuent à recevoir les écritures ;
- un replica n'arrive plus à suivre pendant l'arrêt du broker ;
- le cluster entre alors dans un état de sous-réplication observable.

La conséquence pédagogique est très différente de la partie 3 :

- le problème n'est pas d'abord un retard consommateur ;
- le problème est un retard de réplication entre brokers.

## Lecture du dashboard Kafka

Le dashboard `Simul-Pix - Kafka Dashboard` est la vue principale de cette partie.

### `Partitions sous-répliquées`

Ce panneau est central.

Lecture attendue :

- la valeur monte au-dessus de `0` pendant l'arrêt du broker ;
- elle redescend ensuite vers `0` après son redémarrage et la resynchronisation ;
- si l'on lit trop tard, on peut déjà être revenu à `0`.

Point pédagogique important :

- une partition sous-répliquée n'est pas une partition offline ;
- le cluster continue souvent à fonctionner, mais avec une marge de sécurité réduite.

### `État du cluster Kafka`

Lecture attendue :

- le nombre de partitions offline doit idéalement rester à `0` ;
- un seul contrôleur reste actif ;
- l'état global des brokers montre qu'un nœud a été arrêté puis redémarré ;
- pendant l'incident, `broker_count` peut temporairement tomber à `2`.

Ce panneau permet de distinguer :

- un incident de réplication ;
- une panne complète du cluster.

### `Retard de réplication par topic/partition`

Ce panneau montre où le retard se concentre.

Lecture attendue :

- certaines partitions affichent un retard de réplication visible ;
- ce retard augmente pendant l'arrêt du broker ;
- puis il diminue à mesure que le broker rattrape son retard.

Ce panneau est la meilleure preuve visuelle que le sujet de cette partie est bien la réplication inter-brokers.

## Lecture du dashboard général

Le dashboard `Simul-Pix - General Dashboard` sert à voir si l'incident Kafka dégrade ensuite le flux métier.

### `messages produits`

Lecture attendue :

- le `generator` continue à produire le volume demandé ;
- la courbe monte vers `400`.

### `Étapes visibles de la chaîne pédagogique`

Lecture attendue :

- les étapes `checked`, `decision`, `validated` et `outcome` peuvent ralentir ou se décaler ;
- mais elles ne doivent pas s'effondrer complètement si le cluster reste disponible.

Ici, on ne cherche pas d'abord à montrer un lag applicatif massif. On cherche à voir si la dégradation Kafka se propage jusque dans le rythme de passage entre les étapes de la chaîne split.

### `Flux pipeline (msg/s)`

Lecture attendue :

- le flux produit reste élevé pendant l'émission ;
- le flux traité peut devenir moins régulier pendant l'incident ;
- le retour à la normale se voit après le redémarrage du broker.

### `Sortie finale : décisions finales / pix acceptés / pix rejetés`

Lecture attendue :

- les décisions finales continuent idéalement à être publiées ;
- les `pix rejetés` ne doivent pas devenir le signal principal de cette partie ;
- si les rejets montent, il faut se demander si l'incident Kafka commence à impacter le SLA de décision.

## Lecture du dashboard de persistance

Le dashboard `Simul-Pix - Persistence Dashboard` permet de voir si l'incident de réplication se propage jusqu'à la sortie durable.

### `Compteurs de persistance`

Lecture attendue :

- la persistance peut suivre avec un léger décalage ;
- elle doit finir par rejoindre le volume traité.

### `Écarts de persistance`

Lecture attendue :

- un écart temporaire peut apparaître ;
- cet écart doit revenir à `0` après stabilisation.

### `État courant de la base`

Lecture attendue :

- PostgreSQL continue à jouer son rôle de point de vérité final ;
- à la fin de l'exercice, la base doit refléter le volume final accepté ou rejeté.

## Lecture de la météo des services

Sur `http://localhost:8082/`, les indicateurs les plus utiles pour la partie 4 sont :

- `alertes actives`
- `lag total`
- `ancienneté backlog`
- `services ok` et `services en défaut`
- les cartes de flux : `Pix générés`, `Pix traités`, `Pix valides`, `Pix rejetés`

Lecture attendue :

- des alertes liées à Kafka peuvent apparaître ;
- le cluster peut être dégradé sans que toute la plateforme soit hors service ;
- le lag consommateur peut rester secondaire par rapport aux indicateurs de réplication.

Le message central est le suivant :

- la santé du transport Kafka se lit différemment d'un simple retard de consommation ;
- la météo permet de voir cette différence en temps réel.

## Ce que vous devez vérifier côté Kafka

Le script utile est :

```sh
./scripts/tp-kafka.sh
```

Ce que vous devez y lire :

- l'état des brokers ;
- les partitions sous-répliquées ;
- l'évolution du lag de réplication ;
- la stabilisation après redémarrage du broker.

Lecture attendue :

- un broker est temporairement absent ;
- des partitions deviennent sous-répliquées ;
- le rattrapage s'observe après son retour ;
- la lecture finale peut redevenir nominale avec `400 / 400 / 400 / 0`.

## Ce que vous devez vérifier dans PostgreSQL

Le script utile est :

```sh
./scripts/tp-db.sh
```

Ce que vous devez y lire :

- si les compteurs en base finissent bien par rejoindre le flux ;
- si l'incident Kafka a provoqué un retard de persistance durable ou non.

Lecture attendue :

- la base peut accuser un retard temporaire ;
- elle doit ensuite converger avec le pipeline.

## Interprétation pédagogique

La partie 4 doit faire comprendre la différence entre :

- un backlog applicatif ;
- un incident de réplication Kafka ;
- une interruption totale du service.

Ici, le point clé est le suivant :

- Kafka peut continuer à accepter et à servir des messages ;
- mais il le fait avec une résilience temporairement réduite ;
- les indicateurs critiques deviennent alors la sous-réplication et le retard de réplication, pas seulement les compteurs fonctionnels.

Cette partie sert donc à discuter :

- de la tolérance aux pannes de broker ;
- du rôle des replicas ;
- de l'intérêt d'observer la couche transport séparément de la logique métier ;
- du lien entre disponibilité apparente et robustesse réelle.

## Résultat attendu en fin de partie

Pour une exécution de type :

```sh
./scripts/run-scenario.sh replication_lag 400 120
```

on attend au minimum :

- `messages produits = 400`
- des `partitions sous-répliquées > 0` pendant l'incident
- un `retard de réplication` visible pendant l'arrêt du broker
- un retour progressif vers `0` après redémarrage
- un pipeline qui finit par converger, en pratique `400 / 400 / 400 / 0` sur la validation réelle
- une base PostgreSQL qui finit elle aussi par converger

## Question pédagogique centrale

À la fin de cette partie, l'étudiant doit être capable de répondre clairement à cette question :

comment reconnaît-on un incident de réplication Kafka, en quoi diffère-t-il d'un simple lag consommateur, et comment voit-on sa propagation éventuelle jusqu'au flux métier et à la persistance ?
