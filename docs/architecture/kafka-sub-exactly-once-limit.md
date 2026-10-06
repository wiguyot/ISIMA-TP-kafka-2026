# Limite Actuelle Sur `SUB exactly once Kafka`

Cette note explique le périmètre exact de `exactly_once_kafka` côté `SUB` dans Simul-Pix.

Pour une piste d'évolution concrète, voir aussi [kafka-inbox-outbox-design.md](kafka-inbox-outbox-design.md).

## Résumé

Dans l'architecture actuelle, on peut piloter côté consommation Kafka :

- `at_most_once`
- `at_least_once`
- `exactly_once_kafka`

Mais `exactly_once_kafka` signifie uniquement :

- transaction Kafka sur la chaîne `lecture -> traitement -> publication -> commit offset` ;
- pas de perte ou de doublon logique sur les topics Kafka couverts ;
- aucune garantie transactionnelle commune avec PostgreSQL.

La raison est simple : un vrai `exactly once` de bout en bout devrait couvrir Kafka, le code applicatif et PostgreSQL dans une même stratégie transactionnelle ou idempotente. Ce n'est pas ce que promet le bouton `SUB Kafka = Exactly once Kafka`.

## Ce que fait réellement un consumer ici

Dans `Simul-Pix`, un consumer ne se contente pas de lire un message et de valider son offset.

### Cas de la chaîne de décision

La chaîne `pix-validator` -> `pix-decision-engine` -> `pix-outcome-publisher` :

1. lit un message sur Kafka (`raw`, puis `checked`, puis `decision`) ;
2. applique des contrôles métier et la décision client ;
3. publie des événements successifs sur Kafka :
   - `checked`
   - `validated` ou `rejected`
   - `decision`
   - `outcome`
4. commit les offsets consommés à chaque étape.

### Cas des persisters

`persister-valid` et `persister-rejected` :

1. lisent un message sur Kafka ;
2. écrivent dans PostgreSQL ;
3. commit l'offset consommé.

Donc, après la lecture, on a encore des opérations externes importantes :

- publication Kafka ;
- écriture PostgreSQL ;
- commit des offsets.

## Pourquoi le bout-en-bout n'est pas garanti

Pour pouvoir annoncer une vraie sémantique `SUB exactly once` jusqu'à PostgreSQL, il faudrait que l'on puisse rendre atomiques :

1. la lecture du message Kafka ;
2. le traitement ;
3. l'écriture finale ;
4. la validation de l'offset.

Dans l'architecture actuelle, ce n'est pas le cas.

### Problème 1 : Kafka et PostgreSQL ne partagent pas de transaction commune

Les persisters lisent sur Kafka puis écrivent dans PostgreSQL.

Si le service tombe :

- après l'écriture PostgreSQL mais avant le commit Kafka, le message peut être rejoué ;
- après le commit Kafka mais avant l'écriture PostgreSQL, l'événement peut être perdu du point de vue métier.

Ces deux fenêtres empêchent de promettre honnêtement :

- ni perte ;
- ni doublon.

### Ce que couvrent les transactions Kafka

La chaîne de décision lit un message puis publie vers plusieurs topics successifs :

- `checked`
- puis `validated` ou `rejected`
- puis `decision`
- puis `outcome`

ensuite seulement elle commit l'offset consommé.

Quand `consumer_semantics=exactly_once_kafka`, ces étapes Kafka sont coordonnées par transaction :

- si le service tombe avant le commit transactionnel, les publications interrompues sont annulées ;
- l'offset consommé est validé avec les publications Kafka ;
- les topics Kafka restent cohérents du point de vue métier.

### Ce que les transactions Kafka ne couvrent pas

Les persisters PostgreSQL restent en dehors de cette transaction. Si un persister écrit en base puis tombe avant le commit Kafka, Kafka peut rejouer le message. La cohérence finale dépend alors de l'idempotence applicative, notamment de la clé primaire et des `UPSERT`.

## Garanties pilotables aujourd'hui

### `SUB at_most_once`

On commit l'offset avant traitement.

Conséquence :

- faible risque de doublon ;
- risque de perte si le service tombe après lecture.

### `SUB at_least_once`

On commit l'offset après succès.

Conséquence :

- meilleure robustesse face à la perte ;
- risque de rejeu et donc de doublon logique après incident.

### `SUB exactly_once_kafka`

On coordonne la publication Kafka avale et le commit d'offset dans une transaction Kafka.

Conséquence :

- pas de perte ou doublon logique sur la chaîne Kafka couverte ;
- coût supérieur en latence et débit ;
- PostgreSQL reste hors périmètre.

## Pourquoi `PUB exactly once` existe quand même

L'interface expose `exactly_once` côté `PUB` car, à ce niveau, on parle uniquement de publication Kafka.

Avec un producteur idempotent Kafka, on peut limiter fortement les doublons de publication sur Kafka.

Mais cela ne signifie pas :

- `exactly_once` de bout en bout dans toute l'application ;
- ni `exactly_once` entre Kafka et PostgreSQL.

Autrement dit :

- `PUB exactly once` = propriété limitée à la publication Kafka ;
- `SUB exactly_once_kafka` = propriété limitée à la chaîne Kafka transactionnelle ;
- `exactly once` de bout en bout jusqu'à PostgreSQL = propriété non tenue par l'architecture actuelle.

## Ce qu'il faudrait pour s'en approcher

Pour approcher un vrai `SUB exactly once`, il faudrait faire évoluer l'architecture.

Exemples possibles :

- rester dans Kafka avec des transactions Kafka natives sur toute la chaîne utile ;
- introduire un pattern outbox / inbox strictement conçu pour la déduplication ;
- rendre les écritures avales idempotentes partout ;
- coordonner plus fortement la consommation, la persistance et la republication ;
- accepter un coût supplémentaire en complexité, en latence et en observabilité.

## Conclusion

La présence de `SUB Kafka = Exactly once Kafka` dans l'interface est correcte si on lit bien son périmètre :

- Kafka -> Kafka : protégé par transaction Kafka sur la chaîne de décision ;
- Kafka -> PostgreSQL : non couvert par transaction Kafka, protégé seulement par idempotence applicative ;
- `TP 03` doit donc démontrer à la fois la protection Kafka et la limite PostgreSQL.
