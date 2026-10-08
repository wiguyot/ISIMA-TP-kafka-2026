# Sujet 13 — Basculer la chaîne en exactly-once : coût réel et périmètre réel

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Par défaut, toute la chaîne de `simulpix` fonctionne en `at-least-once` : les producteurs publient avec `acks=all` et les consommateurs valident leur offset après traitement. Un arrêt au mauvais moment peut donc rejouer un message. Le code prévoit pourtant un mode `exactly-once` Kafka (transactions, `transactional.id`, lecture `read_committed`), simplement désactivé.

Beaucoup d'articles présentent `exactly-once` comme le réglage qui « résout le problème des doublons ». Ce sujet vous demande de vérifier cette affirmation sur un pipeline réel : activer le mode, mesurer ce qu'il coûte, et montrer précisément ce qu'il protège — et ce qu'il ne protège pas.

Un rappel utile : `exactly-once` Kafka couvre une chaîne **Kafka vers Kafka** (lecture, traitement, publication, commit d'offset dans une même transaction). Il ne rend pas atomique une écriture dans PostgreSQL : les persisters restent hors de ce périmètre. Consultez [la limite de `exactly-once` Kafka](../../docs/architecture/kafka-sub-exactly-once-limit.md) avant de commencer.

## Votre mission

Basculez l'ensemble de la chaîne Kafka en exactly-once, puis produisez deux démonstrations mesurées :

1. ce que le mode **coûte** : débit, délai de décision, comportement sous charge ;
2. ce que le mode **ne protège pas** : le doublon métier réapparaît dès que l'écriture PostgreSQL entre en jeu.

La réussite du sujet n'est pas « faire marcher le mode » : c'est de prouver où s'arrête la garantie.

## Réalisation minimale attendue

La chaîne `pix-validator`, `pix-decision-engine` et `pix-outcome-publisher` fonctionne en transactions exactly-once, avec un scénario automatisé qui vérifie la cohérence des messages visibles en lecture `read_committed`. Votre rendu inclut une comparaison avant/après sur le débit et le délai de décision, et un essai d'arrêt forcé d'un persister qui démontre que le rejeu crée toujours un doublon absorbé côté PostgreSQL. La garantie annoncée doit distinguer explicitement les trois segments : Kafka vers Kafka, Kafka vers PostgreSQL, effet métier.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ». Ce sujet la découpe en trois garanties, une par segment de la chaîne.

Ensuite, vous devez :

1. Relevez l'état de référence en `at-least-once` : débit, délai de décision, compteurs Kafka et PostgreSQL.
2. Basculez les variables `SIMULPIX_KAFKA_PRODUCER_SEMANTICS` et `SIMULPIX_KAFKA_CONSUMER_SEMANTICS` pour les trois services transactionnels. Vérifiez qu'aucun service n'est resté dans l'ancien mode : la logique Kafka est dupliquée dans chaque service, un réglage oublié invalide la démonstration.
3. Vérifiez que les groupes de consommateurs dédiés au mode transactionnel sont bien utilisés, et que le flux nominal circule.
4. Mesurez le coût : rejouez le même scénario de charge avant et après bascule, et comparez débit, délai de décision et lag.
5. Prouvez le périmètre protégé : arrêtez brutalement un service transactionnel en pleine charge et démontrez qu'aucun doublon logique n'apparaît sur les topics de sortie.
6. Prouvez la limite : arrêtez un persister entre l'écriture PostgreSQL et le commit d'offset, puis montrez que le message rejoué est absorbé par l'`UPSERT` — le mode exactly-once Kafka n'a pas empêché le rejeu.
7. Expliquez l'écart entre les offsets bruts et les messages visibles : les transactions ajoutent des marqueurs techniques, un `end_offsets` ne compte pas les messages métier.
8. Rédigez la conclusion en trois lignes de garantie, une par segment, chacune appuyée sur un essai.

## Questions de conception

- Pourquoi la bascule doit-elle être faite sur tous les services en même temps ? Que se passe-t-il si un seul reste en `at-least-once` ?
- Que protège exactement `send_offsets_to_transaction` par rapport à un commit manuel après traitement ?
- Pourquoi les persisters ne peuvent-ils pas entrer dans la transaction Kafka ?
- Le coût mesuré (débit, latence) est-il acceptable pour la garantie obtenue sur les topics internes ?
- Quelle différence entre le `exactly-once` obtenu et le `effectively once` du sujet 1 : laquelle des deux approches privilégieriez-vous sur un pipeline réel, et pourquoi ?

## Dimension théorique

Votre sujet porte un aspect théorique formalisable : les transactions distribuées, le niveau d'isolation `read_committed` et le fencing des producteurs obsolètes. Approfondissez-les : formalisez la garantie exacte que `send_offsets_to_transaction` apporte à chaque segment, et situez-la par rapport au concept de consensus. Consultez [l'analyse recherche](analyse-recherche-limos.md) pour la référence détaillée : la dimension algorithmique et complexité de l'axe [MAAD](https://www.limos.fr/axes/1) du LIMOS est le point d'entrée le plus proche, mais ce thème relève surtout de références littéraires sur les systèmes distribués. Cet approfondissement fait partie de l'évaluation. L'aspect identifié ici n'est pas exhaustif : votre réalisation peut révéler d'autres aspects théoriques, à approfondir et à signaler également.

## Preuves attendues

- un protocole de mesure identique avant et après bascule, avec les chiffres côte à côte ;
- la démonstration d'absence de doublon logique sur les topics lors d'un arrêt forcé d'un service transactionnel ;
- la démonstration que le doublon métier persiste côté PostgreSQL malgré le mode exactly-once ;
- une lecture des compteurs qui distingue offsets bruts et messages visibles en `read_committed` ;
- une conclusion en trois garanties, une par segment, chacune avec sa preuve et sa limite.

## Ressources

- [TP 01 à 03 — Sémantiques Kafka](../README.md) (prérequis indispensable)
- [Limite de `exactly-once` Kafka](../../docs/architecture/kafka-sub-exactly-once-limit.md)
- [Exactly-once Kafka et lecture des compteurs](../../docs/architecture/exactly-once-kafka-observability.md)
- [Sujet 1 — Idempotence Kafka/PostgreSQL](sujet-01-idempotence-kafka-postgresql.md) (approche complémentaire)
- [Campagne de sémantique existante](../../docs/experiences/semantics-campaign.md)
