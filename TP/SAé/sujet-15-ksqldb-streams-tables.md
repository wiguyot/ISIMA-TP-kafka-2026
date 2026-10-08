# Sujet 15 — Du flux à l'état : suivre les paiements avec ksqlDB

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

**Prérequis :** les activités 01 à 10 et le [TP 04 — ksqlDB](../tp-04-ksqldb.md). Les TP 02 et 03 vous aideront à comprendre les garanties de traitement lors des rejeux et des redémarrages.

## Situation

Un établissement souhaite suivre les paiements Pix en temps réel : nombre de paiements acceptés, montants échangés, activité par émetteur et pics inhabituels.

Les événements arrivent continuellement. Certains arrivent en retard, d'autres sont publiés plusieurs fois, et les informations sur les clients peuvent changer.

**Si Kafka contient 120 messages, cela signifie-t-il que 120 paiements différents ont été réalisés ?** Votre application devra rendre ces situations compréhensibles et permettre de justifier les chiffres affichés.

## Votre mission

Construisez avec ksqlDB une application de suivi des **paiements acceptés**, consultable à la demande et observable en continu.

L'utilisateur doit pouvoir consulter l'activité d'un émetteur, suivre les paiements et leurs montants, puis repérer une activité inhabituelle selon une règle que vous choisissez. Chaque indicateur doit annoncer ce qu'il représente : messages reçus, paiements distincts ou activité sur une période donnée.

## Socle fourni et contribution nouvelle

Le TP 04 fournit le démarrage de ksqlDB, des requêtes de filtrage, des agrégations, une fenêtre temporelle, un référentiel clients et une jointure. Ces exemples constituent votre point de départ.

Votre contribution relie ces notions dans une application : suivi des paiements par identité, référentiel évolutif, interface de consultation et expériences qui expliquent les résultats. Le topic `simulpix.transactions.validated` fournit les paiements acceptés, avec notamment `transaction_id`, `emitter_tax_id`, `amount`, `currency` et `event_time` ; relevez leur contrat dans le code et les messages observés.

Utilisez des noms dédiés pour vos objets SQL et vos topics, par exemple avec le préfixe `sae15`. Les scripts de scénario réinitialisent les topics du pipeline : préparez vos campagnes en conséquence et prévoyez une remise à zéro limitée aux objets de votre application. Des topics d'essai dédiés, reprenant le contrat des paiements acceptés, peuvent servir aux cas contrôlés.

## Réalisation minimale attendue

Construisez les éléments suivants et documentez leurs relations :

| Élément | Rôle dans votre application |
|---|---|
| **STREAM des paiements acceptés** | Représenter les événements d'entrée et les informations utiles au suivi. |
| **TABLE du référentiel clients** | Conserver les informations courantes par client et permettre leur mise à jour. |
| **Jointure STREAM–TABLE** | Enrichir les paiements avec les informations du client. |
| **TABLE des paiements par identité** | Représenter chaque paiement selon une clé métier définie par le groupe. |
| **TABLE d'activité par émetteur** | Calculer le nombre de paiements distincts et leur montant total à partir de l'état des paiements. |
| **Agrégation sur une fenêtre temporelle** | Observer l'activité sur une période et détecter un dépassement de seuil. |

Un paiement publié deux fois avec la même identité et le même contenu ne doit pas augmenter le nombre de paiements distincts ni leur montant total. Expliquez le choix de la clé, notamment la place de `transaction_id`, `source_transaction_id` et `retry_attempt`. Le minimum peut porter sur les paiements sans correction ; annoncez ce périmètre.

L'indicateur fenêtré peut compter les messages ou les paiements distincts, selon votre conception : indiquez ce choix dans l'interface et justifiez la règle de détection. Précisez le timestamp utilisé, la taille de la fenêtre et la période de grâce.

Ajoutez une petite interface avec une fiche d'émetteur et un suivi des changements. Utilisez une requête **pull** sur une vue matérialisée pour consulter l'état et une requête **push** pour suivre ses évolutions. Une interface simple suffit : l'objectif est de rendre les résultats compréhensibles.

## Actions à réaliser

Avant de commencer, formulez votre garantie en une phrase. Par exemple : « Un rejeu identique ne doit pas augmenter le total des paiements distincts affiché pour un émetteur. »

1. Réalisez le TP 04 et repérez les requêtes que vous pouvez utiliser comme référence.
2. Définissez les questions auxquelles répondra votre application et le sens de chaque indicateur.
3. Dessinez les relations entre topics, STREAMS, TABLES, requêtes persistantes et interface. Définissez les clés et le partitionnement nécessaires aux jointures.
4. Construisez un petit jeu de paiements dont vous connaissez les identités et les montants attendus, puis implémentez vos vues.
5. Ajoutez la mise à jour du référentiel et l'interface de consultation.
6. Automatisez les scénarios ci-dessous, conservez les résultats attendus et comparez-les aux résultats observés.
7. Documentez la garantie de traitement retenue pour ksqlDB, puis comment installer les requêtes, lancer l'application, attendre son rattrapage et rejouer les expériences.

## Scénarios à démontrer

- **Flux nominal :** retrouver les identités, le nombre de paiements distincts et les montants d'un jeu connu.
- **Paiement publié deux fois :** observer les deux événements et vérifier que les indicateurs métier restent cohérents.
- **Client absent du référentiel :** montrer le résultat de l'enrichissement et rendre explicite le traitement du paiement concerné.
- **Modification d'un client :** comparer des paiements reçus avant et après la modification, en vérifiant que celle-ci a été traitée avant de poursuivre.
- **Événement tardif :** comparer son traitement à l'intérieur puis au-delà de la période de grâce. Faites progresser les timestamps du flux pour atteindre ces deux situations ; attendre seulement quelques secondes ne suffit pas à établir la fermeture d'une fenêtre.
- **Redémarrage de ksqlDB :** vérifier les résultats après reprise et rattrapage, avec une borne des entrées et un délai maximal d'attente.

La référence des expériences est le jeu d'entrée connu, avec ses identités et ses valeurs. PostgreSQL peut servir de comparaison pour les paiements effectivement persistés ; ses comptages seuls ne prouvent pas que vos indicateurs sont corrects.

## Questions de conception

- Pourquoi une suite d'événements et un état par clé répondent-ils à des questions différentes ?
- Comment passez-vous du nombre de messages au nombre de paiements distincts ?
- Que signifie « dernière valeur » : dernière arrivée dans Kafka ou événement portant le timestamp le plus récent ?
- Que faites-vous si deux messages ont la même identité mais des contenus différents ?
- Un paiement sans client connu doit-il disparaître d'un indicateur ? Comment rendez-vous ce cas visible ?
- Une modification du référentiel doit-elle changer les paiements déjà enrichis ?
- Pourquoi additionner toutes les mises à jour d'une TABLE ne donne-t-il pas nécessairement le montant total des paiements ?

Une jointure STREAM–TABLE produit un résultat à l'arrivée d'un événement du STREAM. Une modification de la TABLE ne recalcule pas automatiquement les résultats déjà émis. Observez ce comportement et expliquez ce qu'il signifie pour votre application ; voir la [documentation des jointures](https://docs.confluent.io/platform/7.9/ksqldb/developer-guide/joins/join-streams-and-tables.html).

## Dimension théorique

L'approfondissement théorique est **encouragé** autour de la relation entre historique et état, des vues matérialisées, du temps des événements et de la mise à jour des résultats. Vous pouvez partir d'une expérience concrète : un doublon qui change un compteur, un client modifié ou un paiement arrivé en retard.

Le [guide des pistes de recherche](analyse-recherche-limos.md) vous aide à choisir une lecture ou à préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- un schéma des flux et des vues, avec les clés et les contrats retenus ;
- les requêtes SQL, l'interface et les configurations nécessaires ;
- un jeu d'entrée connu et une comparaison automatisée des identités, nombres et montants attendus ;
- les résultats des six scénarios, avec une explication des écarts éventuels ;
- une démonstration de la consultation à la demande et du suivi continu ;
- une procédure de lancement et de remise à zéro reproductible ;
- une conclusion sur les garanties et les limites de votre application.

## Extensions facultatives

- Comparez des fenêtres fixes et glissantes, avec une même population d'entrée.
- Corrélez les demandes de paiement et leurs résultats par une jointure STREAM–STREAM.
- Reconstruisez les vues depuis les journaux et expliquez les éventuelles différences, notamment avec un référentiel qui a évolué.
- Intégrez le suivi des corrections et distinguez paiement, tentative et décision.
- Réalisez un traitement équivalent avec Kafka Streams et comparez les démarches.

## Ressources

- [TP 04 — ksqlDB](../tp-04-ksqldb.md) et [requêtes fournies](../../infra/ksqldb/tp14-pix.sql)
- [Activité 05 — Clé métier et partitionnement](../activite-05-partitionnement-cle-emetteur.md)
- [Activité 06 — Rejeu, offsets et rétention](../activite-06-rejeu-offsets-retention.md)
- [Contrats d'événements](../../docs/contrats-evenements/) et [limites du socle](limites-du-socle.md)
- [Streams et tables dans ksqlDB](https://docs.confluent.io/platform/current/ksqldb/reference/sql/data-definition.html)
- [Requêtes persistantes, push et pull](https://docs.confluent.io/platform/current/ksqldb/concepts/queries.html)
- [Temps et fenêtres dans ksqlDB](https://docs.confluent.io/platform/7.9/ksqldb/concepts/time-and-windows-in-ksqldb-queries.html)
