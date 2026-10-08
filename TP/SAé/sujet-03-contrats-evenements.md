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

## Socle fourni et contribution nouvelle

Le répertoire `docs/contrats-evenements/` fournit des **exemples JSON**, pas des schémas exécutables. Les étapes `checked` et `decision` font partie du pipeline au même titre que `raw`, `validated`, `rejected` et `outcome`. Relevez les champs réellement produits et consommés dans le code, y compris les champs de rejeu.

Votre contribution apporte une validation exécutable et une preuve de compatibilité de l'évolution choisie. Un champ nouveau peut être conservé par une copie du message puis supprimé par un constructeur à liste fixe, notamment pour `decision` et `outcome` : vérifiez chaque étape utile.

## Réalisation minimale attendue

Formalisez un contrat exécutable, faites-le évoluer et adaptez le producteur ainsi que tous les consommateurs de votre périmètre. Automatisez une matrice ancien producteur / nouveau consommateur, nouveau producteur / ancien consommateur, et versions identiques. Déclarez pour chaque case succès attendu, valeur par défaut ou erreur contrôlée, puis vérifiez le contenu persistant lorsque le contrat atteint PostgreSQL.

`BACKWARD` signifie que le nouveau consommateur lit les anciens messages ; `FORWARD`, que l'ancien consommateur lit les nouveaux ; `FULL` exige les deux. La compatibilité du format ne suffit pas si le sens d'un champ change. Voir les [définitions de compatibilité](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html).

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Choisissez l'événement à faire évoluer : `raw`, `checked`, `validated`, `rejected`, `decision` ou `outcome`.
2. Décrivez le contrat actuel avec les champs obligatoires, facultatifs et les identifiants utilisés.
3. Écrivez le contrat cible et la règle de compatibilité : que doit faire un consommateur si le nouveau champ est absent ou inconnu ?
4. Adaptez le producteur, les consommateurs concernés et la persistance lorsque cela est nécessaire.
5. Ajoutez une validation explicite des champs obligatoires et des valeurs par défaut pour les champs facultatifs.
6. Testez un message conforme à l'ancien contrat et un message conforme au nouveau contrat.
7. Documentez l'ordre de déploiement des services et expliquez pourquoi cet ordre évite une rupture.

## Extensions facultatives

Vous pouvez intégrer un registre de schémas. Son installation seule ne valide pas les JSON arbitraires envoyés au broker : reliez explicitement les clients à la sérialisation et à la validation retenues, puis testez un message incompatible.

## Questions de conception

- Quel champ identifie le paiement et quel champ identifie l'événement ?
- Le nouveau champ est-il obligatoire immédiatement, ou peut-il être ajouté progressivement ?
- Comment un consommateur doit-il réagir à un champ inconnu ou absent ?
- Quelles données doivent aussi évoluer dans PostgreSQL ?
- Comment prouver qu'un ancien message reste lisible après l'évolution ?

## Dimension théorique

Définissez les relations BACKWARD, FORWARD et FULL sur une matrice de producteurs et consommateurs. Séparez validité syntaxique, sens des champs et conservation des informations dans le pipeline. Reliez chaque relation exigée à l'ordre de déploiement et à un essai. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- les contrats avant et après évolution ;
- la matrice ancien/nouveau producteur et consommateur, avec messages et résultats attendus pour chaque case ;
- la preuve que les consommateurs du périmètre continuent à fonctionner ;
- des données PostgreSQL cohérentes avec le nouveau contrat ;
- une stratégie de déploiement et de retour arrière documentée.

## Ressources

- [Exemples de contrats](../../docs/contrats-evenements/)
- [Activité 02 — Publication de transactions](../activite-02-premiers-messages-kafka.md)
- [Activité 05 — Clé métier et partitionnement](../activite-05-partitionnement-cle-emetteur.md)
- [Activité 10 — Architecture événementielle](../activite-10-synthese-architecture-evenementielle.md)
