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

## Socle fourni et contribution nouvelle

Les rejets métier, leur persistance, `source_transaction_id`, `retry_attempt` et les scripts de rejeu existent déjà. `replay-corrected.sh` numérote les tentatives localement à son invocation et renouvelle `event_time` et le délai ; il ne constitue pas un workflow durable de correction.

Les messages des scénarios sont des objets JSON décodables. Un JSON malformé ou un type inattendu peut interrompre le validateur et rester à relire : un tel message ne dispose pas nécessairement d'une identité métier ni d'un événement `rejected`.

## Réalisation minimale attendue

Implémentez un suivi durable des rejets et corrections : identité de correction, état, autorisation, nombre maximal de tentatives et résultat de chaque tentative. Cette évolution doit modifier au moins un contrat, un service et la persistance PostgreSQL. Deux exécutions du même rejeu doivent retrouver le même état et éviter deux effets identiques.

Distinguez rejet métier et **quarantaine technique**. Rendez explicite le sort d'un message indécodable ou sans identité, identifié au minimum par topic, partition et offset : conservation du diagnostic, progression contrôlée du consommateur et absence de boucle de crash. Automatisez un rejet définitif, deux corrections différentes, une correction soumise deux fois et un message techniquement illisible.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ». Les sujets 1 et 11 en donnent des exemples.

Ensuite, vous devez :

1. Recensez les variantes invalides existantes et identifiez où elles sont détectées dans la chaîne.
2. Définissez une taxonomie de rejets adaptée au projet. Elle doit au minimum distinguer erreur de format ou de données, erreur métier, donnée de référence incohérente, dépassement de délai et incident technique.
3. Pour chaque catégorie, documentez la décision attendue : rejet définitif, correction possible, investigation nécessaire ou rejeu autorisé.
4. Faites évoluer les contrats d'événements nécessaires pour transporter le diagnostic et le statut de traitement du rejet.
5. Adaptez le générateur, le validateur, le moteur de décision, la persistance et le rejeu lorsque cela est nécessaire à votre solution.
6. Implémentez au moins deux cas de correction réalistes, par exemple une identité Pix incohérente et une donnée de référence manquante.
7. Assurez la traçabilité entre le Pix initial, son rejet, sa correction éventuelle, chaque tentative de rejeu et sa décision finale.
8. Empêchez une même correction de provoquer un rejeu infini ou plusieurs résultats métier identiques.
9. Conservez le temps initial et distinguez-le du temps de chaque tentative. Si une correction reçoit un nouveau délai, documentez-le : ce nouveau délai ne prouve pas le respect du SLA du paiement initial.

## Extensions facultatives

Étudiez un backoff adaptatif, l'autorisation par rôle ou un outil de correction. Le minimum exige une politique bornée et traçable ; il n'exige pas un moteur d'optimisation des retries.

## Questions de conception

- À quelle étape un message structurellement incomplet doit-il être classé ?
- Quelles données faut-il conserver pour diagnostiquer un rejet sans perdre le message initial ?
- Quelle identité relie un rejet à son Pix d'origine et à une tentative de correction ?
- Quelle différence faites-vous entre corriger un Pix et rejouer le même Pix inchangé ?
- Comment limitez-vous le nombre de tentatives tout en gardant une trace des essais ?
- Quel événement ou quelle donnée permet de déclarer le dossier définitivement clos ?

## Dimension théorique

Formalisez le parcours comme un automate de rejets, corrections et tentatives, avec transitions autorisées et condition de terminaison. Justifiez la limite et le backoff par des coûts et risques explicites. Un processus de décision markovien est une extension possible seulement si états, actions, transitions et coût sont définis. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- une taxonomie documentée et des contrats d'événements mis à jour ;
- un schéma du parcours d'un Pix invalide, de son entrée à sa décision finale ;
- des essais couvrant un rejet définitif, deux corrections différentes, une correction répétée et un message techniquement illisible ;
- la preuve qu'un rejeu contrôlé conduit à un résultat final traçable ;
- la preuve qu'une même correction ne crée pas de doublon métier ni de boucle de rejeu ;
- des compteurs cohérents entre Kafka, PostgreSQL et les métriques de l'application.

## Ressources

- [Activité 07 — Rejets, erreurs et DLQ (file de rejets)](../activite-07-rejets-dlq.md)
- [Contrat d'un rejet](../../docs/contrats-evenements/transaction.rejected.example.json)
- [Contrat d'un résultat final](../../docs/contrats-evenements/transaction.outcome.example.json)
- [Scripts de rejeu](../pour-aller-plus-loin/realisation-des-tp.md)
- [Architecture de la chaîne de traitement](../../docs/architecture/container-interactions.md)
