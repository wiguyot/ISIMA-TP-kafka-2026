# Sujet 8 — Ajouter un contrôle métier asynchrone au pipeline Pix

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Le pipeline actuel contrôle la forme d'un paiement, prend une décision puis publie un résultat final. Dans une application réelle, certaines décisions demandent une étape supplémentaire : repérer un montant inhabituel, contrôler une règle de conformité ou comparer le paiement à une information de référence.

Un contrôle **asynchrone** est réalisé par un service séparé qui lit un événement Kafka, effectue sa règle, puis publie un nouveau résultat. Il n'est pas exécuté directement par le producteur du paiement. S'il conditionne l'acceptation finale, son attente et sa capacité font partie du chemin de décision et du SLA.

Dans une application réelle, la décision dépend aussi d'un état externe évolutif — le solde d'un compte bancaire, par exemple — et pas seulement de la forme du paiement. Ce sujet propose, en extension, de simuler de tels comptes.

## Votre mission

Ajoutez une étape de contrôle métier asynchrone au pipeline. Cette étape doit consommer un événement existant, publier un résultat explicite, préserver la trace de la transaction et rester observable de bout en bout.

Vous choisissez la règle métier, mais elle doit être justifiée et ne pas se réduire à une simple vérification de format déjà faite par le validateur. L'[extension](#extension--simuler-des-comptes-bancaires) en fin de fiche propose une instance particulièrement réaliste de cette règle : le contrôle de solde.

## Socle fourni et contribution nouvelle

Validation, décision bancaire et publication de `outcome` existent déjà. Votre contribution est un contrôle métier autonome avec une autorité et un effet explicitement définis.

Choisissez entre un **contrôle préalable**, dont le refus empêche l'acceptation finale, et un **audit après décision**, qui ajoute un diagnostic sans modifier une décision déjà finale. Si vous retenez l'audit, annoncez cette garantie. Pour réviser une décision finale, il faut un protocole explicite de révision et de consommation de ces versions ; deux publications contradictoires ne suffisent pas.

## Réalisation minimale attendue

Créez un service consommateur autonome, son câblage Docker et ses contrats. Il doit publier un résultat traçable, être intégré au flux final ou à une persistance d'audit selon l'autorité choisie, exposer une métrique et disposer de tests pour acceptation, refus et rejeu. Vérifiez que l'autorité de décision finale est unique et mesurez l'impact du retard du contrôle sur le SLA.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Définissez la règle métier et les données nécessaires à son évaluation.
2. Décrivez un exemple de paiement accepté et un exemple de paiement rejeté par cette nouvelle règle.
3. Choisissez le topic d'entrée, le ou les topics de sortie et la clé de partitionnement.
4. Décrivez les contrats d'événements et les champs de traçabilité qui doivent être conservés.
5. Implémentez le consumer, le traitement et la publication de la décision.
6. Intégrez le résultat à la persistance ou au flux `outcome` sans casser les comportements existants.
7. Ajoutez les métriques et les tests couvrant le cas accepté, le cas rejeté et le rejeu d'un message.
8. Décrivez le comportement si le contrôle est indisponible ou répond après l'échéance : attente bornée, refus, diagnostic incomplet ou politique justifiée.

## Extension : simuler des comptes bancaires

Le générateur pioche déjà émetteur et bénéficiaire dans un référentiel de clients ([`infra/reference/reference-clients.json`](../../infra/reference/reference-clients.json)), et les messages Pix portent les identifiants de comptes (`emitter_account_id`, `beneficiary_account_id`). L'extension consiste à donner à ces comptes une vie propre, indépendante des Pix :

- **Étendre le référentiel** : ajouter à chaque compte un solde initial, en s'assurant que toute référence bancaire d'un Pix correspond à un compte existant du référentiel ;
- **Simuler des mouvements de compte** (dépôts, retraits) publiés comme événements dans Kafka, indépendants du flux Pix ;
- **Maintenir un état de solde** par compte, alimenté par ces mouvements ;
- **Rendre la règle de contrôle dépendante de cet état** : un Pix valide en forme peut être rejeté pour `insufficient_balance` — un rejet métier légitime, décalé dans le temps, sur un paiement dont la forme est correcte.

Cette extension transforme la nature du problème, et c'est l'objet de l'étude demandée :

1. **Ordre et partitionnement** : un Pix engage deux comptes (émetteur et bénéficiaire). Quelle clé garantit l'ordre par compte, alors qu'un ordre global n'est pas atteignable ? Deux Pix simultanés sur le même compte créent une course entre lecture du solde, vérification et débit.
2. **Idempotence du solde** : en `at-least-once`, un message rejoué débite deux fois. Le solde est un agrégat mutable où l'idempotence (voir le sujet 1) devient indispensable, pas optionnelle.
3. **Causalité temporelle** : un Pix peut arriver avant le mouvement qui l'aurait rendu payable. Faut-il rejeter définitivement, ré-évaluer plus tard, ou mettre en attente ? Distinguez temps de l'événement et temps de traitement.
4. **Deux natures de rejet** : le rejet de forme (synchrone, par le validateur) et le rejet métier (asynchrone, différé) n'ont ni le même moment ni le même sens. Que devient la chaîne `outcome` ? Le persister doit-il les distinguer ?
5. **Invariant auditable** : chaque solde doit rester explicable par l'historique des événements le composant. Définissez comment vérifier cet invariant — c'est le même esprit que la reconstruction du sujet 10.

Opérationnellement, cette extension ajoute un nouveau flux d'événements (donc un topic et un producteur), une table de soldes à intégrer au démarrage et au reset de la plateforme, et un segment supplémentaire sur le chemin critique, à mesurer contre le SLA de décision.

Le débit et le crédit d'un paiement doivent respecter un invariant commun, y compris face aux rejeux et aux paiements concurrents. Fixez avec l'enseignant un périmètre d'extension réalisable avant d'ajouter une simulation bancaire complète.

## Questions de conception

- Pourquoi ce contrôle doit-il être un nouveau service plutôt qu'une règle ajoutée au validateur existant ?
- Quel identifiant permet de relier la décision du nouveau service au Pix initial ?
- Comment évitez-vous que deux consommateurs donnent deux décisions contradictoires ?
- Quel topic transporte le résultat final et quel composant en est responsable ?
- Comment vérifiez-vous que le nouveau service ne casse ni l'ordre relatif ni la persistance ?
- Pour un contrôle dépendant d'un solde : quelle clé de partitionnement garantit l'ordre des opérations sur un même compte, alors qu'un Pix engage deux comptes ?
- Que devient un Pix accepté par le validateur puis rejeté plus tard pour solde insuffisant : quel statut, quel topic, quelle trace pour la transaction ?
- Où doit vivre l'état du solde — table PostgreSQL, topic compacté, les deux — et quelles sont les conséquences de chaque choix sur la reconstruction et l'audit ?

## Dimension théorique

Formalisez l'ordre partiel et l'autorité de décision : quelles conditions précèdent une acceptation finale, et quelles observations n'ont qu'une valeur d'audit ? Si vous réalisez l'extension bancaire, ajoutez les invariants de débit/crédit, d'idempotence et de concurrence des soldes. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- un schéma du nouveau flux Kafka ;
- des contrats d'événements documentés ;
- des messages traçables de l'entrée jusqu'au résultat final ;
- une démonstration des cas acceptés, rejetés et rejoués ;
- une explication du partitionnement et des garanties retenues ;
- une vérification que les comportements existants restent fonctionnels.

Si vous réalisez l'extension bancaire :

- une démonstration d'un Pix valide en forme et rejeté pour solde insuffisant, avec sa trace complète ;
- un état des soldes cohérent, chaque solde étant vérifiable par l'historique des mouvements et des Pix qui le composent ;
- une description des impacts sémantiques observés : ordre, idempotence du solde, causalité temporelle, coexistence des deux natures de rejet.

## Ressources

- [Activité 10 — Architecture événementielle](../activite-10-synthese-architecture-evenementielle.md)
- [Contrats d'événements](../../docs/contrats-evenements/)
- [Référentiel clients et comptes](../../infra/reference/reference-clients.json)
- [Architecture de la chaîne de traitement](../../docs/architecture/container-interactions.md)
- [Atelier avancé](../pour-aller-plus-loin/realisation-des-tp.md)
