# Sujet 8 — Ajouter un contrôle métier asynchrone au pipeline Pix

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Le pipeline actuel contrôle la forme d'un paiement, prend une décision puis publie un résultat final. Dans une application réelle, certaines décisions demandent une étape supplémentaire : repérer un montant inhabituel, contrôler une règle de conformité ou comparer le paiement à une information de référence.

Un contrôle **asynchrone** est réalisé par un service séparé qui lit un événement Kafka, effectue sa règle, puis publie un nouveau résultat. Il n'est pas exécuté directement par le producteur du paiement. Cette séparation permet d'ajouter une règle sans bloquer l'ensemble du pipeline, mais elle impose de préserver la traçabilité et l'ordre métier.

## Votre mission

Ajoutez une étape de contrôle métier asynchrone au pipeline. Cette étape doit consommer un événement existant, publier un résultat explicite, préserver la trace de la transaction et rester observable de bout en bout.

Vous choisissez la règle métier, mais elle doit être justifiée et ne pas se réduire à une simple vérification de format déjà faite par le validateur.

## Réalisation minimale attendue

Créez un service consommateur autonome, son câblage Docker et ses contrats d'entrée et de sortie. Il doit publier une décision traçable, être intégré au flux final ou à sa persistance, exposer au moins une métrique et disposer de tests pour les décisions acceptée, rejetée et rejouée.

## Actions à réaliser

1. Définissez la règle métier et les données nécessaires à son évaluation.
2. Décrivez un exemple de paiement accepté et un exemple de paiement rejeté par cette nouvelle règle.
3. Choisissez le topic d'entrée, le ou les topics de sortie et la clé de partitionnement.
4. Décrivez les contrats d'événements et les champs de traçabilité qui doivent être conservés.
5. Implémentez le consumer, le traitement et la publication de la décision.
6. Intégrez le résultat à la persistance ou au flux `outcome` sans casser les comportements existants.
7. Ajoutez les métriques et les tests couvrant le cas accepté, le cas rejeté et le rejeu d'un message.

## Questions de conception

- Pourquoi ce contrôle doit-il être un nouveau service plutôt qu'une règle ajoutée au validateur existant ?
- Quel identifiant permet de relier la décision du nouveau service au Pix initial ?
- Comment évitez-vous que deux consommateurs donnent deux décisions contradictoires ?
- Quel topic transporte le résultat final et quel composant en est responsable ?
- Comment vérifiez-vous que le nouveau service ne casse ni l'ordre relatif ni la persistance ?

## Preuves attendues

- un schéma du nouveau flux Kafka ;
- des contrats d'événements documentés ;
- des messages traçables de l'entrée jusqu'au résultat final ;
- une démonstration des cas acceptés, rejetés et rejoués ;
- une explication du partitionnement et des garanties retenues ;
- une vérification que les comportements existants restent fonctionnels.

## Ressources

- [Activité 10 — Architecture événementielle](../activite-10-synthese-architecture-evenementielle.md)
- [Contrats d'événements](../../docs/contrats-evenements/)
- [Architecture de la chaîne de traitement](../../docs/architecture/container-interactions.md)
- [Atelier avancé](../pour-aller-plus-loin/realisation-des-tp.md)
