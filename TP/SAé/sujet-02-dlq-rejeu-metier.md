# Sujet 2 — Gérer un Pix invalide, de la détection à la correction

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Le scénario `errors_simple` produit actuellement plusieurs Pix invalides : montant négatif, champ manquant, identité Pix incohérente ou statut initial invalide. Le pipeline les contrôle, les transforme en événements `rejected`, puis les conserve dans PostgreSQL.

Ce mécanisme montre qu'un rejet ne bloque pas le flux principal. Il ne formalise toutefois pas complètement le parcours d'un Pix invalide : pourquoi est-il rejeté, peut-il être corrigé, qui autorise son rejeu, combien de fois peut-il être rejoué et comment retrouve-t-on sa décision finale ?

## Votre mission

Concevez et implémentez une chaîne de gestion des Pix invalides qui couvre l'ensemble du parcours :

```text
Pix invalide
-> détection et classification
-> rejet explicite et persistance
-> correction, clôture ou investigation
-> rejeu contrôlé si une correction est possible
-> décision finale traçable
```

Votre solution doit permettre de savoir, pour chaque Pix invalide, s'il est corrigeable, définitivement rejeté, en attente d'investigation ou déjà rejoué.

## Réalisation minimale attendue

Implémentez un statut de suivi de rejet, les champs de traçabilité associés et un mécanisme de rejeu contrôlé. Cette évolution doit modifier au moins un contrat, un service du pipeline et la persistance PostgreSQL. Elle doit inclure des essais automatisés pour un rejet définitif et deux corrections distinctes.

## Actions à réaliser

1. Recensez les variantes invalides existantes et identifiez où elles sont détectées dans la chaîne.
2. Définissez une taxonomie de rejets adaptée au projet. Elle doit au minimum distinguer erreur de format ou de données, erreur métier, donnée de référence incohérente, dépassement de délai et incident technique.
3. Pour chaque catégorie, documentez la décision attendue : rejet définitif, correction possible, investigation nécessaire ou rejeu autorisé.
4. Faites évoluer les contrats d'événements nécessaires pour transporter le diagnostic et le statut de traitement du rejet.
5. Adaptez le générateur, le validateur, le moteur de décision, la persistance et le rejeu lorsque cela est nécessaire à votre solution.
6. Implémentez au moins deux cas de correction réalistes, par exemple une identité Pix incohérente et une donnée de référence manquante.
7. Assurez la traçabilité entre le Pix initial, son rejet, sa correction éventuelle, chaque tentative de rejeu et sa décision finale.
8. Empêchez une même correction de provoquer un rejeu infini ou plusieurs résultats métier identiques.

## Questions de conception

- À quelle étape un message structurellement incomplet doit-il être classé ?
- Quelles données faut-il conserver pour diagnostiquer un rejet sans perdre le message initial ?
- Quelle identité relie un rejet à son Pix d'origine et à une tentative de correction ?
- Quelle différence faites-vous entre corriger un Pix et rejouer le même Pix inchangé ?
- Comment limitez-vous le nombre de tentatives tout en gardant une trace des essais ?
- Quel événement ou quelle donnée permet de déclarer le dossier définitivement clos ?

## Preuves attendues

- une taxonomie documentée et des contrats d'événements mis à jour ;
- un schéma du parcours d'un Pix invalide, de son entrée à sa décision finale ;
- des essais couvrant au moins un rejet définitif et deux corrections différentes ;
- la preuve qu'un rejeu contrôlé conduit à un résultat final traçable ;
- la preuve qu'une même correction ne crée pas de doublon métier ni de boucle de rejeu ;
- des compteurs cohérents entre Kafka, PostgreSQL et les métriques de l'application.

## Ressources

- [Activité 07 — Rejets, erreurs et DLQ (file de rejets)](../activite-07-rejets-dlq.md)
- [Contrat d'un rejet](../../docs/contrats-evenements/transaction.rejected.example.json)
- [Contrat d'un résultat final](../../docs/contrats-evenements/transaction.outcome.example.json)
- [Scripts de rejeu](../pour-aller-plus-loin/realisation-des-tp.md)
- [Architecture de la chaîne de traitement](../../docs/architecture/container-interactions.md)
