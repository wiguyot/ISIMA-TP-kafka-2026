# Les sémantiques Kafka en une page

Cette page résume ce que vous devez savoir expliquer à l'issue des TP 01 à 04. Les TP illustrent chaque cas sur la chaîne Pix ; cette page en donne la lecture d'ensemble.

## La question posée

Un message traverse toujours **deux frontières** :

- **l'écriture (PUB)** : un producteur envoie le message à Kafka ;
- **la lecture (SUB)** : un consommateur lit le message, le traite, puis enregistre sa position (l'**offset**) par un **commit**.

Une panne peut survenir à chaque frontière. La sémantique dit ce qui arrive alors au message : **perdu**, **dupliqué**, ou **ni l'un ni l'autre**.

| Sémantique | Promesse | Risque accepté |
|---|---|---|
| **at-most-once** | Jamais de doublon | Perte possible |
| **at-least-once** | Jamais de perte | Doublon possible |
| **exactly-once** | Ni perte ni doublon | Coût (latence, débit) et **périmètre limité** |

## Côté écriture (PUB)

| Sémantique | Réglages producteur | Ce qui peut arriver |
|---|---|---|
| at-most-once | `acks=0`, `retries=0` | Le producteur n'attend aucun accusé de réception. Un message perdu en route, ou encore dans le tampon du producteur quand celui-ci plante, est **perdu sans que personne le sache**. |
| at-least-once | `acks=all`, `retries>0`, `enable.idempotence=false` | Le producteur attend l'accusé de toutes les répliques et réessaie en cas de silence. Si le message avait bien été écrit mais que l'accusé s'est perdu, le nouvel essai l'écrit **une seconde fois** : doublon dans le topic. |
| exactly-once (producteur idempotent) | `enable.idempotence=true` (impose `acks=all` et `retries>0`) | Kafka numérote les envois de chaque producteur (identifiant de producteur + numéro de séquence) et **ignore un nouvel essai déjà écrit**. Ni perte ni doublon **dus aux nouveaux essais**. |

Limites de l'idempotence du producteur :

- elle protège contre les **nouveaux essais internes** du client Kafka, pas contre un programme qui appelle deux fois `produce()` : ce serait deux messages légitimes ;
- elle vaut **pour la durée de vie du producteur** : après un redémarrage, il reçoit un nouvel identifiant.

## Côté lecture (SUB)

| Sémantique | Moment du commit | Ce qui peut arriver |
|---|---|---|
| at-most-once | **Avant** le traitement | Si le consommateur plante entre le commit et la fin du traitement, Kafka considère le message comme lu : il est **perdu**. |
| at-least-once | **Après** le traitement | Si le consommateur plante entre le traitement et le commit, Kafka lui redonne le message au redémarrage : il est **traité deux fois**. |
| exactly-once (Kafka) | **Dans une transaction** avec les messages produits | Voir ci-dessous. |

Attention au commit automatique (`enable.auto.commit=true`) : Kafka commite périodiquement, sans lien avec l'avancement du traitement. Selon le moment de la panne, on peut perdre **ou** dupliquer. Ce n'est aucune des trois sémantiques.

## Exactly-once : ce que Kafka garantit vraiment

Pour un service qui **lit un topic, traite, puis écrit dans un autre topic** (lire-traiter-écrire), Kafka propose des **transactions** :

1. le producteur a un `transactional.id` ;
2. il ouvre une transaction, produit ses messages de sortie, puis ajoute **l'offset lu** à la même transaction (`send_offsets_to_transaction`) ;
3. il valide le tout d'un bloc (`commit_transaction`) ou l'annule (`abort_transaction`) ;
4. les consommateurs suivants lisent en `isolation.level=read_committed` : ils ne voient jamais les messages d'une transaction annulée.

Si le service plante au milieu, la transaction est annulée : ni les sorties ni l'offset ne sont validés, et le message est relu proprement.

**Le périmètre s'arrête à Kafka.** Dès que le traitement écrit ailleurs (PostgreSQL, un e-mail, un appel HTTP), cette écriture n'est pas dans la transaction Kafka. On retombe alors en at-least-once pour cette écriture, et il faut la rendre **idempotente** : une clé unique en base, un `INSERT ... ON CONFLICT`, une table des messages déjà traités (motif *inbox*), ou l'enregistrement de l'offset dans la même transaction que la base.

En pratique, un « exactly-once de bout en bout » se construit presque toujours ainsi : **at-least-once + traitement idempotent**.

Chaque transaction laisse aussi un **marqueur de fin** (`COMMIT` ou `ABORT`) dans chaque partition écrite. Ce marqueur occupe un offset mais n'est jamais livré aux consommateurs : c'est pourquoi l'offset de fin d'un topic transactionnel dépasse le nombre de messages métier.

## Où cela se joue dans simul-pix

| Service | Rôle | Sémantiques disponibles |
|---|---|---|
| `generator` | PUB seul | `at_most_once`, `at_least_once`, `exactly_once` (producteur idempotent) |
| `pix-validator`, `pix-decision-engine`, `pix-outcome-publisher` | Lire-traiter-écrire (Kafka vers Kafka) | `at_most_once`, `at_least_once`, `exactly_once_kafka` (transactions) |
| `persister-valid`, `persister-rejected` | SUB vers PostgreSQL | `at_most_once`, `at_least_once` ; l'idempotence vient de la base (clé unique sur `transaction_id`, `rejection_fingerprint`) |

Les sémantiques se choisissent avec `SIMULPIX_KAFKA_PRODUCER_SEMANTICS` et `SIMULPIX_KAFKA_CONSUMER_SEMANTICS`, ou depuis l'interface `http://localhost:8083/`.

## Ce qu'il faut retenir

1. Toute chaîne a un **PUB** et un **SUB** : la garantie de la chaîne est celle de son maillon le plus faible.
2. **at-most-once** perd, **at-least-once** duplique : choisir, c'est décider lequel des deux risques le métier supporte. Pour un paiement, on refuse la perte, donc on accepte le doublon et on le neutralise.
3. **exactly-once Kafka** couvre la chaîne Kafka vers Kafka, au prix de la latence et du débit. Il ne couvre **pas** la base de données.
4. Un doublon « absorbé » par une clé unique n'est pas un doublon « évité » : le traitement a bien eu lieu deux fois. Un effet de bord non idempotent (notification, e-mail) serait, lui, bien doublé.
5. Un écart entre compteurs juste après une panne n'est pas forcément une perte : il faut **attendre le drainage** avant de conclure.
