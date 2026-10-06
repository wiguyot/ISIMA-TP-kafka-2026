# Partie 3 : retard consommateur

## Objectif

Dans cette fiche avancée, vous allez observer ce qui se passe quand Kafka continue a recevoir des messages plus vite que le pipeline applicatif ne peut les consommer.

Dans cette partie :

1. `generator` produit rapidement des messages Pix valides.
2. `pix-decision-engine` et les persisters sont volontairement ralentis.
3. Kafka accumule temporairement un backlog.
4. `service-health` et Grafana permettent de voir monter puis redescendre le lag consommateur.

Votre objectif n'est pas de montrer un rejet metier initial. Vous devez voir qu'un retard consommateur peut d'abord creer du backlog, puis degrader la decision jusqu'au rejet de delai.

## Dashboards Grafana a lire

Dashboards utiles :

- `Simul-Pix - General Dashboard`
- `Simul-Pix - Kafka Dashboard`
- `Simul-Pix - Persistence Dashboard`

La partie 3 se lit surtout a travers le dashboard general pour voir le decrochage entre production et traitement, puis a travers le dashboard Kafka pour observer le lag consommateur.

## Scenario recommande

```sh
./scripts/run-scenario.sh consumer_lag 120 80
```

Signification des arguments :

- `consumer_lag` : nom du scenario a executer ;
- `120` : nombre total de messages Pix a produire ;
- `80` : debit vise de production, en messages par seconde.

Autrement dit, cette commande demande au `generator` de produire `120` messages au total, a `80 msg/s`, pendant que le scenario introduit volontairement du retard cote consommation et persistance.

## Ce que fait reellement le scenario

Le scenario `consumer_lag` n'abime pas le cluster Kafka. Il ralentit surtout les consommateurs applicatifs.

Concretement :

- `pix-decision-engine` recoit un delai artificiel de traitement ;
- `persister-valid` recoit lui aussi un delai artificiel ;
- `persister-rejected` recoit egalement un delai artificiel.

La consequence pedagogique est importante :

- les messages continuent a etre produits dans Kafka ;
- Kafka reste globalement sain ;
- mais les consommateurs prennent du retard ;
- ce retard devient visible sous forme de `lag total` et d'`anciennete backlog`.

Autrement dit, la partie 3 montre un probleme de debit applicatif, pas un incident de replication Kafka.

## Lecture du dashboard general

### `messages produits`

Ce panneau indique combien de messages ont ete emis par le `generator`.

Lecture attendue :

- la valeur monte rapidement jusqu'a `120` ;
- la production est plus rapide que le traitement.

### `Étapes visibles de la chaîne pédagogique`

Ce panneau indique combien de messages ont franchi :

- `checked` ;
- `decision` ;
- `pix acceptes` ou `pix rejetes`.

Lecture attendue :

- `checked` monte d'abord vite ;
- `decision` monte moins vite si `pix-decision-engine` n'arrive pas a suivre ;
- un ecart temporaire apparait entre ce qui est controle et ce qui est decide ;
- cet ecart doit finir par disparaitre si on laisse le scenario se drainer jusqu'au bout.

Dans cette partie, ce panneau ne sert pas d'abord a montrer un rejet. Il sert a montrer que `pix-decision-engine` n'arrive plus a suivre instantanement le rythme de production.

### `Flux pipeline (msg/s)`

Ce panneau permet de comparer les flux instantanes du pipeline.

Lecture attendue :

- `messages produits` monte vite au debut ;
- le flux traite reste en dessous pendant un certain temps ;
- puis le pipeline continue a traiter alors meme que la production est deja terminee.

Ce comportement est typique d'un backlog en cours de vidage.

### `Sortie finale : decisions finales / pix acceptes / pix rejetes`

Dans un scenario `consumer_lag`, on attend surtout :

- des `decisions finales` qui progressent plus lentement que la production ;
- une phase initiale ou quelques `pix acceptes` sortent encore ;
- puis une montee des `pix rejetes` si le backlog depasse la fenetre SLA.

Point pedagogique important :

- un lag consommateur ne signifie pas automatiquement rejet ;
- il signifie d'abord que la decision finale arrive plus tard.

Sur la validation reelle de `./scripts/run-scenario.sh consumer_lag 120 80`, ce basculement n'est pas marginal : le scenario finit avec `10` acceptations et `110` rejets `processing_timeout`.

### `Taux de rejet`

Lecture attendue :

- il peut rester faible au tout debut ;
- puis il monte nettement quand le backlog commence a produire des `processing_timeout`.

Ce panneau permet donc de distinguer deux niveaux :

- lag sans rupture fonctionnelle ;
- lag qui degrade ensuite la qualite de service.

### `Compteurs pipeline`

Ce panneau est central pour la partie 3.

Lecture attendue :

- `messages produits` monte d'abord tres vite ;
- `pix controles` suit rapidement ;
- `decisions produites` suivent avec retard ;
- `pix acceptes` monte plus lentement ;
- `decisions finales` rattrape ensuite le total quand le backlog se vide.

Le point important n'est pas seulement la valeur finale. C'est le decalage temporel entre les courbes.

## Lecture du dashboard Kafka

Le dashboard `Simul-Pix - Kafka Dashboard` est la vue technique principale de cette partie.

### `Etat du cluster Kafka`

Lecture attendue :

- le cluster reste sain ;
- pas de panne broker ;
- pas de partition offline.

La partie 3 doit justement faire comprendre qu'on peut avoir un gros lag consommateur alors que Kafka lui-meme fonctionne correctement.

### `Partitions sous-repliquees`

Lecture attendue :

- la valeur reste a `0`.

Si elle monte, on n'est plus seulement dans un probleme de consommateurs lents. On commence a melanger la partie 3 avec une logique de partie 4.

### `Retard de replication par topic/partition`

Lecture attendue :

- pas de derive majeure de replication ;
- la replication ne doit pas etre le signal central de cette partie.

Le vrai indicateur de la partie 3 reste le lag des groupes consommateurs, visible surtout dans la meteo des services et dans `tp-kafka.sh`.

## Lecture du dashboard de persistance

Le dashboard `Simul-Pix - Persistence Dashboard` permet de verifier si le retard consommateur se propage jusqu'a la sortie durable.

### `Compteurs de persistance`

Lecture attendue :

- la persistance suit avec retard ;
- les compteurs en base finissent par rejoindre les compteurs valides ou rejetes du pipeline.

Autrement dit, PostgreSQL confirme la vidange finale du backlog.

### `Ecarts de persistance`

Lecture attendue :

- un ecart temporaire peut apparaitre ;
- cet ecart doit revenir a `0` quand les persisters ont fini de drainer ce qui reste.

Ce panneau montre bien que voir un message dans Kafka ou dans les compteurs applicatifs ne signifie pas encore qu'il est durablement ecrit en base.

### `Etat courant de la base`

Lecture attendue :

- la base continue a recevoir des lignes pendant que la production est deja terminee ;
- l'etat final de PostgreSQL finit par refleter le volume complet traite.

## Lecture de la meteo des services

Sur `http://localhost:8082/`, les indicateurs les plus utiles pour la partie 3 sont :

- `Pix generes`
- `Pix traites`
- `lag total`
- `anciennete backlog`
- `Debit raw`
- `latence validation`
- `alertes actives`
- `services ok` et `services en defaut`

Lecture attendue :

- `Pix generes` monte vite ;
- `Pix traites` reste temporairement en retrait ;
- `lag total` devient positif et visible ;
- `anciennete backlog` devient elle aussi positive ;
- les services peuvent rester `ok` meme si le lag monte.

Point pedagogique essentiel :

- un service peut etre vivant et joignable ;
- mais le systeme peut quand meme etre en retard de traitement.

La meteo doit donc etre lue comme un etat de sante operationnelle, pas seulement comme un indicateur binaire de disponibilite.

## Ce que vous devez verifier dans PostgreSQL

Le script utile est :

```sh
./scripts/tp-db.sh
```

Ce que vous devez y lire :

- les nombres de lignes valides et rejetees ;
- la progression de l'ecriture en base ;
- la coherence finale entre pipeline et persistance.

Lecture attendue :

- `validated_count` monte d'abord ;
- `rejected_count` devient ensuite majoritaire si le backlog depasse le SLA ;
- les compteurs finissent par converger avec le pipeline.

## Ce que vous devez verifier cote Kafka

Le script utile est :

```sh
./scripts/tp-kafka.sh
```

Ce que vous devez y lire :

- les topics continuent a recevoir puis a drainer les messages ;
- les groupes consommateurs du run courant montrent un lag temporaire ;
- ce lag doit finir par redescendre.

Lecture attendue :

- le `lag total` n'est pas nul pendant la montee en charge ;
- il revient ensuite a `0` si on laisse le pipeline finir son travail ;
- Kafka reste stable pendant toute la demonstration.

## Interpretation pedagogique

La partie 3 doit faire comprendre la difference entre quatre idees :

- produire un message dans Kafka ;
- traiter ce message applicativement ;
- le persister durablement ;
- absorber un pic sans se laisser distancer durablement.

Le coeur du scenario est le suivant :

- la production est rapide ;
- les consommateurs sont plus lents ;
- Kafka absorbe temporairement la difference ;
- le backlog devient visible ;
- puis une partie croissante du flux bascule en rejet de delai ;
- enfin le lag revient a `0`, mais avec une qualite de service degradee.

Cette partie sert donc a discuter concretement :

- du dimensionnement des consommateurs ;
- du risque de saturation silencieuse ;
- de l'interet de suivre le lag et son anciennete, pas seulement les compteurs finaux ;
- du fait qu'un pipeline peut etre "fonctionnel" tout en etant deja en train de decrocher.

## Resultat attendu en fin de partie

Pour une execution de type :

```sh
./scripts/run-scenario.sh consumer_lag 120 80
```

on attend au minimum :

- `messages produits = 120`
- `pix controles = 120` a la fin
- `decisions produites = 120` a la fin
- `lag total > 0` pendant la phase de charge
- `anciennete backlog > 0` pendant la phase de charge
- `lag total = 0` a la fin si on attend le drainage complet
- pas d'alerte critique Kafka liee a la replication
- une forte majorite de `processing_timeout` en fin de scenario sur la configuration recommandee
- des compteurs PostgreSQL qui finissent par rejoindre les compteurs pipeline

## Question pedagogique centrale

A la fin de cette partie, l'etudiant doit etre capable de repondre clairement a cette question :

comment distingue-t-on un cluster Kafka sain mais en retard de consommation d'un incident Kafka proprement dit, et comment voit-on ce retard dans le flux, dans la meteo des services et dans la persistance ?
