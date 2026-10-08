# Sujet 13 — Basculer la chaîne en exactly-once : coût réel et périmètre réel

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Par défaut, toute la chaîne de `simulpix` fonctionne en `at-least-once` : les producteurs publient avec `acks=all` et les consommateurs valident leur offset après traitement. Un arrêt au mauvais moment peut donc rejouer un message. Le code prévoit pourtant un mode `exactly-once` Kafka (transactions, `transactional.id`, lecture `read_committed`), simplement désactivé.

Beaucoup d'articles présentent `exactly-once` comme le réglage qui « résout le problème des doublons ». Ce sujet vous demande de vérifier cette affirmation sur un pipeline réel : activer le mode, mesurer ce qu'il coûte, et montrer précisément ce qu'il protège — et ce qu'il ne protège pas.

Un rappel utile : `exactly-once` Kafka couvre une chaîne **Kafka vers Kafka** (lecture, traitement, publication, commit d'offset dans une même transaction). Il ne rend pas atomique une écriture dans PostgreSQL : les persisters restent hors de ce périmètre. Consultez [la limite de `exactly-once` Kafka](../../docs/architecture/kafka-sub-exactly-once-limit.md) avant de commencer.

## Votre mission

Basculez l'ensemble de la chaîne Kafka en exactly-once, puis produisez deux démonstrations mesurées :

1. ce que le mode **coûte** : débit, délai de décision, comportement sous charge ;
2. ce que le mode **ne protège pas** : une tentative PostgreSQL peut être répétée ; l'UPSERT actuel absorbe cette répétition sans créer une seconde ligne métier.

La réussite du sujet n'est pas « faire marcher le mode » : c'est de prouver où s'arrête la garantie.

## Socle fourni et contribution nouvelle

Le TP 03 fournit déjà l'activation de la chaîne transactionnelle, une campagne de coût et une démonstration de sa limite PostgreSQL. Les reproduire est une référence, pas une SAé complète.

Votre contribution apporte des assertions automatiques sur les identités et contenus, une matrice de fenêtres de panne déterministes et une comparaison répétée du coût. L'exactly-once lie des **offsets d'entrée** à des sorties Kafka visibles ; deux messages distincts contenant le même identifiant métier ne sont pas automatiquement dédupliqués.

## Réalisation minimale attendue

Sur au moins un service Kafka vers Kafka, automatisez une matrice comprenant un arrêt après production mais avant commit transactionnel, un arrêt après commit avant reprise de boucle, et une transaction avortée. Tracez le point atteint pour chaque essai, puis comparez les identités et contenus visibles en `read_committed` au manifeste attendu, après drainage borné. La chaîne complète sert au bilan nominal et au coût.

Ajoutez un test de fencing avec un second producteur utilisant le même identifiant transactionnel, et distinguez ce défaut de configuration d'un rejeu normal. Répétez la comparaison de coût à charge réellement comparable, avec délais, débit et effectifs. Enfin, vérifiez le rejeu d'un persister par les tentatives auditées et le contenu métier : plusieurs tentatives ne signifient pas plusieurs effets.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ». Ce sujet la découpe en trois garanties, une par segment de la chaîne.

Ensuite, vous devez :

1. Relevez l'état de référence en `at-least-once` : débit, délai de décision, compteurs Kafka et PostgreSQL.
2. Basculez les variables `SIMULPIX_KAFKA_PRODUCER_SEMANTICS` et `SIMULPIX_KAFKA_CONSUMER_SEMANTICS` pour les trois services transactionnels. Vérifiez qu'aucun service n'est resté dans l'ancien mode : la logique Kafka est dupliquée dans chaque service, un réglage oublié invalide la démonstration.
3. Relevez les groupes effectivement utilisés, leur `RUN_ID`, l'isolation des lecteurs et les identifiants transactionnels par worker. Les groupes sont associés à la campagne et au service ; leur nom ne suffit pas à prouver le mode transactionnel.
4. Mesurez le coût : rejouez le même scénario de charge avant et après bascule, et comparez débit, délai de décision et lag.
5. Prouvez le périmètre protégé dans les fenêtres déterministes du minimum : vérifiez la visibilité des sorties, leurs valeurs et les offsets d'entrée associés.
6. Prouvez la limite : arrêtez un persister entre l'écriture PostgreSQL et le commit d'offset, puis montrez que le message rejoué est absorbé par l'`UPSERT` — le mode exactly-once Kafka n'a pas empêché le rejeu.
7. Expliquez l'écart entre les offsets bruts et les messages visibles : les transactions ajoutent des marqueurs techniques, un `end_offsets` ne compte pas les messages métier.
8. Rédigez la conclusion en trois lignes de garantie, une par segment, chacune appuyée sur un essai.

## Extensions facultatives

Étudiez l'amortissement par batching transactionnel ou un mode mixte pour caractériser la frontière de garantie. Les configurations at-least-once, exactly-once et les groupes de campagne doivent être explicitement séparés dans le protocole.

## Questions de conception

- Quelle garantie de chaîne reste-t-il si un segment demeure en `at-least-once` ? Quelles conditions autorisent une migration progressive ?
- Que protège exactement `send_offsets_to_transaction` par rapport à un commit manuel après traitement ?
- Pourquoi les persisters ne peuvent-ils pas entrer dans la transaction Kafka ?
- Le coût mesuré (débit, latence) est-il acceptable pour la garantie obtenue sur les topics internes ?
- Quelle différence entre le `exactly-once` obtenu et le `effectively once` du sujet 1 : laquelle des deux approches privilégieriez-vous sur un pipeline réel, et pourquoi ?

## Dimension théorique

Formalisez l'atomicité des sorties Kafka et des offsets d'entrée, l'isolation read_committed et le fencing des producteurs obsolètes. Séparez cette propriété de la déduplication d'identités métier et de l'atomicité PostgreSQL. Reliez chaque assertion à une fenêtre de panne tracée et discutez le coût mesuré ; une référence au consensus seul n'explique pas la transaction. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- un protocole de mesure identique avant et après bascule, avec les chiffres côte à côte ;
- la matrice des fenêtres atteintes, transactions avortées et résultats visibles, ainsi qu'une preuve du fencing ;
- la démonstration de tentatives PostgreSQL répétées et d'un effet métier unique grâce à l'UPSERT, malgré le mode exactly-once Kafka ;
- une lecture des compteurs qui distingue offsets bruts et messages visibles en `read_committed` ;
- une conclusion en trois garanties, une par segment, chacune avec sa preuve et sa limite.

## Ressources

- [TP 01 à 03 — Sémantiques Kafka](../README.md) (prérequis indispensable)
- [Limite de `exactly-once` Kafka](../../docs/architecture/kafka-sub-exactly-once-limit.md)
- [Exactly-once Kafka et lecture des compteurs](../../docs/architecture/exactly-once-kafka-observability.md)
- [Sujet 1 — Idempotence Kafka/PostgreSQL](sujet-01-idempotence-kafka-postgresql.md) (approche complémentaire)
- [Campagne de sémantique existante](../../docs/experiences/semantics-campaign.md)
