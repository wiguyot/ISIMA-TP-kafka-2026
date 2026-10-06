# Design `Inbox/Outbox` Pour Approcher `SUB exactly once`

Cette note complète [kafka-sub-exactly-once-limit.md](kafka-sub-exactly-once-limit.md).

Elle ne prétend pas fournir un `exactly once` magique. Elle décrit une architecture plus robuste pour approcher un comportement `effectively once`, c'est-à-dire :

- pas de perte logique observable ;
- pas de doublon logique observable ;
- malgré des crashes entre lecture Kafka, traitement, persistance et republication.

## Objectif

Dans l'architecture actuelle, les points fragiles sont les suivants :

1. un consumer lit un message Kafka ;
2. il exécute des effets de bord ;
3. il commit ensuite son offset ;
4. un crash entre ces étapes peut provoquer :
   - une perte logique ;
   - un rejeu ;
   - un doublon ;
   - une divergence temporaire.

Le but d'un design `Inbox/Outbox` est de réduire cette zone grise.

## Principe général

On introduit deux journaux applicatifs en PostgreSQL :

- une `inbox`, qui mémorise quels événements entrants ont déjà été traités ;
- une `outbox`, qui mémorise quels événements sortants doivent être publiés.

L'idée est la suivante :

1. un consumer lit un message Kafka ;
2. il ouvre une transaction PostgreSQL ;
3. il vérifie si l'événement entrant a déjà été traité ;
4. si non, il applique la logique métier et écrit l'état durable ;
5. il écrit aussi, dans la même transaction SQL, les événements sortants à republier ;
6. il commit la transaction SQL ;
7. ensuite seulement, il commit l'offset Kafka ;
8. un relay séparé lit l'`outbox` et publie vers Kafka.

Le point clé est que :

- la décision métier durable ;
- la mémoire du traitement entrant ;
- et la préparation de la publication sortante

sont validées ensemble dans une même transaction PostgreSQL.

## Ce que cela change pour Simul-Pix

### Aujourd'hui

`pix-decision-engine` et `pix-outcome-publisher` :

- lisent `checked` puis `decision` ;
- décident `validated` ou `rejected` ;
- publient vers Kafka ;
- matérialisent ensuite `outcome` ;
- commit leurs offsets.

`persister-valid` et `persister-rejected` :

- lisent Kafka ;
- écrivent PostgreSQL ;
- commit l'offset.

### Architecture cible

On déplace le centre de gravité vers PostgreSQL pour les garanties avales.

#### Option minimale

- garder la chaîne `pix-validator` -> `pix-decision-engine` -> `pix-outcome-publisher`, mais rendre ses publications plus déterministes et identifiables ;
- ajouter une `inbox` dans les persisters ;
- ajouter une `outbox` si les persisters doivent ensuite republier des événements ;
- rendre toutes les écritures métier idempotentes.

#### Option plus forte

- faire écrire la décision finale de `pix-decision-engine` en base ;
- faire produire les événements Kafka aval depuis une `outbox` PostgreSQL ;
- faire du relay Kafka un service séparé.

Cette seconde option est plus cohérente si l'objectif devient vraiment :

- une vérité métier durable en base ;
- Kafka comme bus de diffusion cohérent avec cette vérité.

## Tables proposées

## `inbox_processed_events`

Cette table sert à mémoriser qu'un événement Kafka entrant a déjà été consommé logiquement.

Champs typiques :

- `event_id` : identifiant stable de l'événement entrant ;
- `topic_name` : topic source ;
- `partition_id` : partition source ;
- `offset_value` : offset Kafka observé ;
- `consumer_name` : service ayant traité l'événement ;
- `processed_at` : horodatage de traitement ;
- `status` : succès, ignoré, erreur contrôlée ;
- `payload_hash` : empreinte optionnelle pour diagnostic.

Contrainte clé :

- unicité sur `event_id` et `consumer_name`.

But :

- si le message est rejoué, on détecte qu'il a déjà été traité ;
- on évite de refaire l'effet de bord métier.

## `outbox_events`

Cette table sert à mémoriser les événements à republier vers Kafka.

Champs typiques :

- `outbox_id` : identifiant technique ;
- `aggregate_id` : identifiant métier principal ;
- `event_type` : `validated`, `rejected`, `outcome`, etc. ;
- `topic_name` : topic cible ;
- `payload_json` : contenu à publier ;
- `status` : `pending`, `published`, `failed` ;
- `created_at` ;
- `published_at` ;
- `attempt_count` ;
- `last_error`.

Contrainte utile :

- identifiant métier stable dans le payload pour permettre l'idempotence côté consommateurs aval.

## Identifiants à stabiliser

Le design dépend fortement d'identifiants d'événements stables.

Il faut distinguer :

- `transaction_id` : identité métier du Pix ;
- `event_id` : identité de l'événement Kafka ;
- `decision_id` : identité de la décision finale si on veut tracer plusieurs tentatives.

Recommandation :

- chaque message Kafka entrant doit porter un `event_id` unique et stable ;
- chaque événement dérivé doit porter son propre `event_id` et son lien vers le parent ;
- l'idempotence métier doit continuer à s'appuyer sur `transaction_id`.

## Flux cible pour un persister

Voici le flux cible pour `persister-valid` ou `persister-rejected`.

1. lire un message Kafka ;
2. ouvrir une transaction PostgreSQL ;
3. vérifier `inbox_processed_events` ;
4. si l'événement existe déjà :
   - ne pas réécrire la ligne métier ;
   - fermer proprement la transaction ;
   - commit l'offset Kafka ;
5. sinon :
   - écrire la ligne métier ;
   - écrire l'entrée `inbox_processed_events` ;
   - éventuellement écrire un événement dans `outbox_events` ;
   - commit SQL ;
6. commit l'offset Kafka.

### Effet recherché

Si le service tombe après le commit SQL mais avant le commit Kafka :

- Kafka rejouera le message ;
- l'`inbox` empêchera un second effet métier ;
- on conserve donc un comportement `effectively once`.

## Flux cible pour `pix-decision-engine`

`pix-decision-engine` est plus délicat car il transforme et republie.

### Variante A : `pix-decision-engine` reste majoritairement Kafka

1. lire `raw` ;
2. calculer la décision ;
3. écrire la décision dans une table PostgreSQL ;
4. écrire les événements aval dans `outbox_events` ;
5. écrire l'entrée `inbox_processed_events` ;
6. commit SQL ;
7. commit l'offset Kafka ;
8. un relay outbox publie ensuite :
   - `validated` ou `rejected`
   - `outcome`

### Variante B : Kafka transactionnel + outbox minimale

1. lire `raw` ;
2. traiter ;
3. publier dans une transaction Kafka ;
4. enregistrer un état de traitement idempotent en base ;
5. commit transaction Kafka et offset ;
6. laisser la base servir uniquement à la déduplication.

Cette variante peut être utile pour `pix-decision-engine`, mais elle ne résout toujours pas proprement la cohérence forte avec PostgreSQL.

Pour Simul-Pix, la Variante A est plus pédagogique et plus homogène.

## Service de relay `outbox`

Un relay séparé lit périodiquement `outbox_events` :

1. récupérer les événements `pending` ;
2. publier sur Kafka ;
3. marquer `published` si succès ;
4. réessayer sinon.

Avantages :

- la publication Kafka n'est plus dans la fenêtre critique du consumer principal ;
- la panne du broker n'annule pas la décision déjà durable en base ;
- l'état de publication devient observable.

Ce relay doit lui-même être idempotent :

- lecture en lots ;
- verrouillage raisonnable ;
- reprise sur crash ;
- payloads réémissibles sans effet métier multiple.

## Cas de crash à couvrir

Le design doit être étudié contre les cas suivants.

### Crash avant commit SQL

Effet :

- rien n'est durable ;
- Kafka peut rejouer ;
- aucun effet métier final n'a encore été validé.

### Crash après commit SQL, avant commit offset

Effet :

- Kafka peut rejouer ;
- l'`inbox` empêche de refaire l'effet de bord ;
- on ne crée pas de doublon logique.

### Crash après écriture en `outbox`, avant publication Kafka

Effet :

- la décision métier est déjà durable ;
- le relay reprendra plus tard ;
- la diffusion Kafka est retardée mais pas perdue.

### Crash pendant la publication du relay

Effet :

- risque de double émission technique ;
- l'idempotence côté publication et côté consumers aval doit absorber ce cas.

## Ce que cela garantit, et ce que cela ne garantit pas

### Ce que cela améliore fortement

- déduplication logique ;
- cohérence entre décision durable et diffusion ultérieure ;
- résistance aux crashes entre SQL et commit d'offset ;
- observabilité des événements non encore publiés.

### Ce que cela ne transforme pas en magie

- ce n'est pas une transaction distribuée globale Kafka + PostgreSQL ;
- ce n'est pas un `exactly once` absolu dans tous les sens ;
- cela repose toujours sur l'idempotence, la déduplication et des identifiants stables.

Le terme le plus honnête reste :

- `effectively once`

plutôt que :

- `exactly once` absolu.

## Stratégie d'implémentation progressive

Pour Simul-Pix, je recommande l'ordre suivant.

### Étape 1

Ajouter l'`inbox` sur `persister-valid`.

But :

- démontrer rapidement la suppression des doublons logiques sur rejou.

### Étape 2

Ajouter l'`inbox` sur `persister-rejected`.

But :

- obtenir le même comportement côté rejets.

### Étape 3

Ajouter une `outbox` pour les publications finales les plus importantes.

But :

- découpler décision durable et diffusion Kafka.

### Étape 4

Évaluer si `pix-decision-engine` doit devenir le point principal de décision durable en base.

But :

- rapprocher l'architecture d'un modèle piloté par la vérité métier durable.

## Conclusion

La piste `coordination transactionnelle plus forte` est réaliste pour Simul-Pix.

La bonne traduction concrète n'est pas une promesse vague de `SUB exactly once`.

La bonne cible est plutôt :

- transaction locale PostgreSQL ;
- `inbox` pour la déduplication ;
- `outbox` pour la republication ;
- identifiants stables ;
- écritures avales idempotentes ;
- comportement final `effectively once`.
