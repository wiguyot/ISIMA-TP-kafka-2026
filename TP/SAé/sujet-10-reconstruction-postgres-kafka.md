# Sujet 10 — Reconstruire PostgreSQL depuis Kafka

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

`./stop.sh` supprime les volumes : les données PostgreSQL sont perdues. Mais Kafka conserve les messages pendant sa rétention, et le pipeline publie des événements qui décrivent chaque étape du paiement : message reçu, contrôlé, décision, rejet éventuel et résultat final.

La question du sujet : **la base de données est-elle un état reconstruisible, ou une source de vérité irremplaçable ?** Une architecture événementielle prétend souvent que Kafka permet de rejouer l'historique ; il faut le démontrer. Et la démonstration dépend de ce qu'il reste à rejouer : disposez-vous de tous les topics, de certains seulement, ou d'un topic déjà purgé par sa rétention ? Elle dépend aussi de l'arbitre auquel on se fie : l'empreinte avant destruction suffit-elle, ou faut-il un témoin de persistance extérieur au couple Kafka/PostgreSQL ?

## Votre mission

Construisez une procédure de reconstruction de PostgreSQL à partir des seuls topics Kafka, puis démontrez qu'elle produit un état cohérent et idempotent. Poussez l'analyse plus loin : reconstruisez sous contrainte d'information (tous les topics, certains, un seul), proposez une politique de rétention fondée sur la criticité de chaque topic, et vérifiez le résultat grâce à un témoin de persistance extérieur à PostgreSQL. Vous devez aussi identifier honnêtement ce qui n'est pas reconstruisible.

## Réalisation minimale attendue

Implémentez un script de reconstruction qui rejoue les topics vers des persisters propres, avec un protocole de comparaison de l'état avant destruction et après reconstruction : comptages par table, cohérence entre `validated`, `rejected` et `outcome`. Le protocole doit échouer explicitement si l'état reconstruit diverge de l'état initial.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Établissez une empreinte de référence : comptages par table, sommes contrôlées, invariants métier (`validated` = `valid` + `rejected`, etc.).
2. Détruisez le volume PostgreSQL, recréez les conteneurs, puis rejouez les topics depuis le début.
3. Vérifiez la cohérence de l'état reconstruit contre l'empreinte de référence.
4. Identifiez les événements qui ne produisent pas d'écriture PostgreSQL et analysez leur impact sur la reconstruction.
5. Déterminez l'impact de la fenêtre de rétention : que se passe-t-il si un topic a déjà purgé ses anciens messages ?
6. Mesurez le temps de reconstruction selon le volume de messages et déduisez-en une limite opérationnelle.
7. Documentez la procédure complète : empreinte, destruction, rejeu, vérification, durée attendue.
8. Construisez une matrice de reconstruction par sous-ensemble de topics : rejouez avec tous les topics, puis avec des sous-ensembles choisis — `raw` seul (le journal complet), `outcome` seul (l'état final sans l'histoire), `validated` + `rejected` (ce que voient les persisters) — et dites pour chaque cas ce qui est reconstruit fidèlement, ce qui se dégrade, ce qui devient impossible.
9. Classez les topics du pipeline selon leur criticité pour la reconstruction : lesquels portent une information irremplaçable, lesquels ne portent que des étapes dérivables d'un autre ? Déduisez-en une proposition de rétention par topic — rétention longue sur les critiques, plus courte sur les dérivables — et justifiez l'arbitrage coût de stockage contre risque de perte de reconstruisibilité.
10. Construisez un témoin de persistance extérieur au couple Kafka/PostgreSQL (par exemple un journal `append-only` exporté hors du cluster, ou une seconde projection légère dans un autre stockage), puis utilisez-le pour vérifier de façon indépendante que le rejeu des topics conduit PostgreSQL au même état. Démontrez son apport sur le cas limite : un topic déjà purgé, où le témoin établit ce qui a été perdu et ce qui reste reconstruisible.

## Questions de conception

- Quelle donnée fait foi pour reconstruire une ligne : l'événement `validated`, l'événement `outcome`, ou une combinaison ?
- Un événement rejoué deux fois crée-t-il un doublon ? Quel mécanisme l'empêche ?
- Quels éléments de la base ne proviennent d'aucun topic (séquences, index, agrégats) ?
- À partir de quel volume ou de quel âge la reconstruction devient-elle plus coûteuse qu'une sauvegarde classique ?
- Quelle garantie de livraison faut-il pour que le rejeu soit fidèle, et que se passe-t-il sans elle ?
- Quel topic unique suffit à tout reconstruire, et à quel coût de temps et de traitement ? Ce serait-il pour toutes les tables ?
- Comment arbitrer la rétention d'un topic : ce que coûte une rétention longue contre ce que risquerait une rétention courte ? Ce calcul est-il le même pour `raw` et pour `checked` ?
- Que prouve un témoin de persistance extérieur que l'empreinte avant/après ne prouve pas ? Qui veille sur le veilleur ?

## Dimension théorique

Votre sujet porte des aspects théoriques formalisables : l'event sourcing (l'état comme fonction dérivable du journal d'événements), la projection, et — par extension — la distinction entre journal complet et sous-journaux dérivables, ainsi que le rôle d'une archive immuable comme témoin extérieur. Approfondissez-les : formalisez votre base comme une projection des topics, caractérisez précisément ce qui n'est pas dérivable et pourquoi, et formalisez ce qu'un témoin extérieur ajoute à la vérification (indépendance de l'arbitre, non-répudiation de l'état). Consultez [l'analyse recherche](analyse-recherche-limos.md) pour la référence détaillée : les bases de données distribuées sont un thème « Données, services, intelligence » de l'axe [SIC](https://www.limos.fr/axes/2) du LIMOS. Cet approfondissement fait partie de l'évaluation. L'aspect identifié ici n'est pas exhaustif : votre réalisation peut révéler d'autres aspects théoriques, à approfondir et à signaler également.

## Preuves attendues

- une empreinte avant/après montrant l'équivalence des états, ou l'explication précise des écarts ;
- la preuve qu'un rejeu complet ne crée pas de doublon ;
- une chronologie de reconstruction avec les durées par étape ;
- un test automatisé rejouable qui échoue si l'état reconstruit est incohérent ;
- la matrice des sous-ensembles de topics testés et de l'état qu'ils permettent de reconstruire ;
- une classification de criticité des topics, la proposition de rétention qui en découle et sa justification coût/risque ;
- la démonstration du témoin de persistance, y compris sur un cas de topic purgé où le témoin établit ce qui a été perdu ;
- une conclusion précisant les limites : rétention dépassée, données hors topics, temps de reconstruction.

## Ressources

- [Sujet 1 — Idempotence Kafka/PostgreSQL](sujet-01-idempotence-kafka-postgresql.md) (prérequis recommandé)
- [Limite de `exactly-once` Kafka](../../docs/architecture/kafka-sub-exactly-once-limit.md)
- [Reset des topics et tables](../../scripts/reset-scenario.sh)
- [Activité 06 — Rejeu, offsets et rétention](../activite-06-rejeu-offsets-retention.md)
