# Sujet 3 — Faire évoluer un contrat d'événement sans casser le pipeline

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Dans `simul-pix`, un paiement devient plusieurs messages successifs : message reçu, message contrôlé, décision, rejet éventuel et résultat final. Un **contrat d'événement** est la description de ces messages : leurs champs, leurs types et leur sens.

Lorsqu'un producteur ajoute ou modifie un champ, les services qui lisent ce message peuvent ne plus comprendre ce qu'ils reçoivent. Le pipeline peut alors s'arrêter, ignorer une information importante ou enregistrer une donnée incohérente.

## Votre mission

Faites évoluer un contrat d'événement Pix sans interrompre les services qui l'utilisent. Votre évolution doit répondre à un besoin concret, par exemple :

- ajouter un identifiant d'événement stable ;
- ajouter une version de contrat ;
- améliorer la traçabilité d'une décision ;
- introduire un nouveau motif de rejet.

Vous devez montrer comment un message ancien et un message nouveau peuvent circuler pendant une période de transition.

## Réalisation minimale attendue

Versionnez ou faites évoluer un contrat du répertoire `docs/contrats-evenements/`, puis adaptez le producteur et tous les consommateurs de votre périmètre. Ajoutez une validation automatisée qui exécute le même traitement avec un message ancien et un message nouveau, et vérifie le résultat persistant lorsque le contrat atteint PostgreSQL.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Choisissez l'événement à faire évoluer : `raw`, `validated`, `rejected` ou `outcome`.
2. Décrivez le contrat actuel avec les champs obligatoires, facultatifs et les identifiants utilisés.
3. Écrivez le contrat cible et la règle de compatibilité : que doit faire un consommateur si le nouveau champ est absent ou inconnu ?
4. Adaptez le producteur, les consommateurs concernés et la persistance lorsque cela est nécessaire.
5. Ajoutez une validation explicite des champs obligatoires et des valeurs par défaut pour les champs facultatifs.
6. Testez un message conforme à l'ancien contrat et un message conforme au nouveau contrat.
7. Documentez l'ordre de déploiement des services et expliquez pourquoi cet ordre évite une rupture.

## Questions de conception

- Quel champ identifie le paiement et quel champ identifie l'événement ?
- Le nouveau champ est-il obligatoire immédiatement, ou peut-il être ajouté progressivement ?
- Comment un consommateur doit-il réagir à un champ inconnu ou absent ?
- Quelles données doivent aussi évoluer dans PostgreSQL ?
- Comment prouver qu'un ancien message reste lisible après l'évolution ?

## Dimension théorique

Votre sujet porte un aspect théorique formalisable : la compatibilité de schéma (backward, forward, full) et le versionnement des contrats. Approfondissez-le : classez votre évolution selon la compatibilité qu'elle exige, et justifiez l'ordre de déploiement qui en découle. Consultez [l'analyse recherche](analyse-recherche-limos.md) pour la référence détaillée : l'interopérabilité et la qualité logicielle sont des thèmes « Données, services, intelligence » de l'axe [SIC](https://www.limos.fr/axes/2) du LIMOS. Cet approfondissement fait partie de l'évaluation. L'aspect identifié ici n'est pas exhaustif : votre réalisation peut révéler d'autres aspects théoriques, à approfondir et à signaler également.

## Preuves attendues

- les contrats avant et après évolution ;
- un message ancien et un message nouveau traités avec le comportement attendu ;
- la preuve que les consommateurs du périmètre continuent à fonctionner ;
- des données PostgreSQL cohérentes avec le nouveau contrat ;
- une stratégie de déploiement et de retour arrière documentée.

## Ressources

- [Exemples de contrats](../../docs/contrats-evenements/)
- [Activité 02 — Publication de transactions](../activite-02-premiers-messages-kafka.md)
- [Activité 05 — Clé métier et partitionnement](../activite-05-partitionnement-cle-emetteur.md)
- [Activité 10 — Architecture événementielle](../activite-10-synthese-architecture-evenementielle.md)
