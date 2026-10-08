# Sujet 12 — Partager le cluster entre plusieurs émetteurs

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Le générateur de `simulpix` émet pour une population de clients unique. Dans la réalité, plusieurs établissements émettent des paiements Pix vers la même plateforme : la situation du [pic de match de football](../pour-aller-plus-loin/partie-5-pic-de-paiements-football.md) montre qu'un seul émetteur peut saturer le pipeline.

Question du sujet : si l'émetteur A subit un pic, l'émetteur B subit-il les conséquences ? Le cluster Kafka est une ressource partagée, et le partage non régulé peut conduire à une situation où un flux affame l'autre.

## Votre mission

Introduisez deux émetteurs dans la simulation, choisissez et justifiez une organisation du partage (clé métier enrichie, topic dédié par émetteur, quotas), puis démontrez par la mesure ce que votre organisation garantit — et ce qu'elle ne garantit pas.

## Réalisation minimale attendue

Distinguez deux émetteurs identifiables dans le flux (champ de traçabilité ou clé de partitionnement), chargez l'un avec un pic `football_match_peak` pendant que l'autre reste nominal, et mesurez le délai de décision de chacun. Votre solution doit s'appuyer sur un choix documenté, et votre preuve doit comparer les deux flux simultanément.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Choisissez le mécanisme d'identification de l'émetteur : champ du message, clé de partitionnement, ou topic dédié. Justifiez.
2. Modifiez le générateur pour émettre deux flux distincts, avec des profils de charge indépendants.
3. Vérifiez l'impact sur les services en aval : un champ nouveau dans le message passe-t-il sans rupture ? (inspiration possible : sujet 3)
4. Écrivez l'hypothèse que vous testez : « avec notre organisation, un pic de A ne dégrade pas le délai de décision de B de plus de X % ».
5. Exécutez l'essai : pic sur A, flux nominal sur B, mesures simultanées des deux délais.
6. Comparez au moins deux organisations (par exemple clé enrichie contre topic dédié) et leurs effets sur l'isolation et le débit global.
7. Explorez les quotas Kafka comme mécanisme de régulation, si votre organisation ne suffit pas.

## Questions de conception

- Pourquoi la clé métier, telle qu'elle est utilisée aujourd'hui, ne protège-t-elle pas contre l'affamement entre émetteurs ?
- Un topic dédié par émetteur garantit-il l'isolation ? À quelle condition côté consommateur ?
- Que devient le parallélisme du pipeline si chaque émetteur a son topic ? Combien de partitions faut-il ?
- Un quota Kafka limite-t-il le débit d'un émetteur ou d'un consommateur ?
- Quel est le coût de l'isolation : complexité d'exploitation, sous-utilisation du cluster, équilibre des partitions ?

## Dimension théorique

Votre sujet porte un aspect théorique formalisable : l'équité d'allocation de ressources (max-min fairness, quotas) et l'isolation de performances. Approfondissez-le : définissez formellement ce que « un pic de A ne dégrade pas B » signifie, et situez votre mécanisme par rapport aux politiques d'équité connues. Consultez [l'analyse recherche](analyse-recherche-limos.md) pour la référence détaillée : l'optimisation multi-objectif et les problèmes d'équité relèvent de l'axe [MAAD](https://www.limos.fr/axes/1) du LIMOS. Cet approfondissement fait partie de l'évaluation. L'aspect identifié ici n'est pas exhaustif : votre réalisation peut révéler d'autres aspects théoriques, à approfondir et à signaler également.

## Preuves attendues

- des mesures simultanées des deux émetteurs, sur un même protocole rejouable ;
- un verdict documenté pour chaque organisation testée : isolation obtenue, débit global, équilibre des partitions ;
- la comparaison entre au moins deux organisations ;
- une chronologie montrant le pic de A et la stabilité (ou dégradation) de B ;
- une conclusion qui précise le compromis retenu et ses limites sous charge extrême des deux émetteurs à la fois.

## Ressources

- [Activité 05 — Clé métier et partitionnement](../activite-05-partitionnement-cle-emetteur.md)
- [Pic de paiements football](../pour-aller-plus-loin/partie-5-pic-de-paiements-football.md)
- [Sujet 6 — Qualification de capacité](sujet-06-capacite-charge.md) (méthodologie de campagne)
- [Modèles de trafic](../../docs/architecture/traffic-models.md)
