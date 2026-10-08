# Sujet 1 — Idempotence entre Kafka et PostgreSQL

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Vous intervenez sur une simulation de paiements Pix. Le pipeline Kafka fonctionne en situation nominale : un message est produit, traité, puis enregistré dans PostgreSQL.

Votre enjeu est de vérifier ce qui se passe lorsqu'un service s'arrête au mauvais moment, puis de proposer et d'implémenter une réponse technique adaptée.

## Votre point de départ

Dans ce projet, un **persister** est un service consommateur Kafka chargé d'enregistrer durablement un résultat métier dans PostgreSQL. Par exemple, `persister-valid` enregistre les paiements acceptés et `persister-rejected` enregistre les rejets.

Un persister réalise les opérations suivantes :

```text
1. lire un message Kafka
2. écrire le résultat dans PostgreSQL
3. valider l'offset Kafka
```

Kafka et PostgreSQL ne partagent pas de transaction commune. Un arrêt du service entre ces opérations crée une zone d'incertitude.

| Moment de l'arrêt | Ce qui peut arriver |
|---|---|
| L'offset Kafka est validé, mais l'écriture PostgreSQL n'est pas faite. | Kafka considère le message traité, mais la donnée n'est jamais écrite : c'est une perte métier. |
| L'écriture PostgreSQL est faite, mais l'offset Kafka n'est pas validé. | Kafka rejoue le message après redémarrage : c'est un risque de doublon de traitement. |

## Ce que vous devez comprendre

Les sémantiques Kafka proposent des compromis :

- `at-most-once` réduit le risque de doublon, mais peut perdre un message ;
- `at-least-once` réduit le risque de perte, mais peut rejouer un message ;
- `exactly-once` Kafka protège une chaîne Kafka vers Kafka, mais ne rend pas atomique une écriture dans PostgreSQL.

L'architecture actuelle absorbe déjà certains doublons grâce aux clés primaires et aux `UPSERT`.

Une **clé primaire** est une contrainte PostgreSQL qui impose qu'une identité métier ne soit présente qu'une seule fois dans une table. Pour un paiement accepté, `transaction_id` joue ce rôle : PostgreSQL refuse la création d'une seconde ligne portant le même identifiant.

Un **UPSERT** est une instruction qui tente d'insérer une ligne puis gère le cas où cette identité existe déjà : `INSERT ... ON CONFLICT DO UPDATE`. Lors d'un rejeu Kafka du même message, le persister tente une nouvelle écriture, mais PostgreSQL met à jour la ligne existante au lieu d'en créer une deuxième. Le doublon est alors **absorbé** : la seconde tentative existe, mais elle ne crée pas un second paiement métier.

Cette protection suppose qu'un même `transaction_id` désigne toujours le même paiement. L'`UPSERT` actuel recopie les valeurs du message reçu dans la ligne existante. Il empêche donc une seconde ligne, mais ne protège pas contre un message différent envoyé avec le même identifiant : ce message mettrait à jour la ligne existante. L'identifiant stable et l'immuabilité du message font donc partie du contrat à vérifier.

Ce mécanisme évite deux lignes pour une même transaction, mais ne suffit pas à garantir un traitement « exactement une fois » :

- sans audit des tentatives, le nombre de lignes de la table ne permet pas de savoir qu'un rejeu a eu lieu : le TP 01 ajoute précisément un trigger pour rendre ces doublons absorbés observables ;
- les persisters actuels n'envoient ni email ni appel bancaire. Toutefois, si un tel effet de bord était ajouté sans son propre mécanisme d'idempotence, un rejeu Kafka pourrait l'exécuter une seconde fois ;
- un offset Kafka validé prouve seulement que le groupe consommateur a validé sa progression. Il ne prouve pas, à lui seul, que l'écriture PostgreSQL existe et contient la donnée attendue.

Pour approfondir cette limite, consultez [la note sur `exactly-once` Kafka](../../docs/architecture/kafka-sub-exactly-once-limit.md).

## Votre mission

Vous devez rendre le traitement Kafka vers PostgreSQL **idempotent et observable** pour le périmètre que vous choisissez.

## Socle fourni et contribution nouvelle

Les persisters disposent déjà de clés primaires et d'UPSERT. Le [TP 01](../README.md) fournit l'audit des conflits et un arrêt déterministe après commit PostgreSQL, avant commit Kafka. Les reproduire constitue votre référence.

Votre contribution doit protéger aussi le **contenu** de l'opération. Le trigger fourni trace INSERT et UPDATE, mais le script additionne actuellement ces traces : un UPSERT en conflit peut donc gonfler le compteur de tentatives. Pour vos preuves SAé, distinguez les traces INSERT des traces UPDATE et vérifiez leur sens ; voir les [limites du socle](limites-du-socle.md). L'audit ne conserve que les opérations de transactions PostgreSQL validées et ne prouve pas que deux messages de même identité sont identiques.

## Réalisation minimale attendue

Faites évoluer au moins un persister et son schéma pour enregistrer l'identité et le contenu immuable, ou son empreinte canonique, **dans la même transaction PostgreSQL que l'effet métier**. Distinguez un premier traitement, un rejeu identique absorbé et un message de contenu différent portant la même identité. Définissez et exposez le traitement de ce conflit sans écraser silencieusement la donnée initiale.

Automatisez les essais nominaux, le rejeu identique, le conflit de contenu, une erreur PostgreSQL et l'arrêt après commit PostgreSQL avant commit Kafka. La preuve doit vérifier les valeurs métier, l'absence de seconde application de l'effet et les offsets, avec une trace du point d'arrêt atteint.

## Actions à réaliser

Avant de coder, formulez votre garantie cible en une phrase. Par exemple :

> Un rejeu Kafka ne doit pas créer deux effets métier pour la même transaction Pix.

Ensuite, vous devez :

1. choisir un identifiant stable permettant de reconnaître un événement ou une opération déjà traitée ;
2. définir le comportement attendu lors d'un premier traitement, d'un rejeu, d'une erreur PostgreSQL et d'un arrêt forcé ;
3. implémenter le mécanisme retenu ;
4. provoquer un arrêt dans une fenêtre de risque ;
5. démontrer, par les données et les métriques, ce que votre solution garantit réellement.

## Piste minimale : l'idempotence

La première réponse possible consiste à mémoriser qu'une opération a déjà été traitée et à empêcher la répétition de son effet métier.

Après un arrêt survenant après le commit PostgreSQL mais avant le commit Kafka :

1. Kafka rejoue le message ;
2. votre service reconnaît qu'il a déjà été traité ;
3. la seconde tentative ne crée pas de nouvel effet métier ;
4. l'offset Kafka peut alors être validé.

Vous obtenez ainsi un comportement proche de `effectively once` sur le périmètre métier traité. Vous ne devez pas l'appeler `exactly-once` de bout en bout sans en démontrer le périmètre.

## Extension : Inbox/Outbox

Si votre solution doit aussi publier un événement après l'écriture PostgreSQL, l'idempotence seule ne suffit pas toujours. Vous pouvez alors étudier le pattern Inbox/Outbox :

```text
Kafka -> consumer
          -> transaction PostgreSQL :
             - écriture métier
             - mémoire de l'événement entrant (inbox)
             - événement sortant à diffuser (outbox)
          -> commit offset Kafka
outbox relay -> Kafka
```

L'`inbox` protège le traitement entrant contre les rejeux. L'`outbox` garantit qu'un événement enregistré dans la même transaction que la donnée métier pourra être publié ultérieurement, même après un arrêt.

Cette architecture et ses compromis sont détaillés dans le [design Inbox/Outbox](../../docs/architecture/kafka-inbox-outbox-design.md). Il s'agit d'une piste de conception : vous devez justifier les adaptations nécessaires à votre périmètre.

## Questions de conception

- Quelle donnée fait foi : le message Kafka, l'offset, ou la ligne PostgreSQL ?
- Quel identifiant rend votre traitement idempotent ?
- Que se passe-t-il si le service tombe après l'écriture PostgreSQL et avant le commit Kafka ?
- Comment distinguez-vous un premier traitement d'un rejeu absorbé ?
- Quelle garantie apportez-vous réellement, et quelle limite reste présente ?

Les scripts, les dashboards et les perturbations réseau ne servent pas seulement à faire une démonstration. Ils doivent vous permettre de produire des preuves reproductibles de vos réponses.

## Dimension théorique

Formalisez la zone entre commit PostgreSQL et commit d'offset, puis l'invariant identité/contenu/effet métier. Montrez pourquoi la mémoire de traitement et l'effet doivent partager une transaction. Reliez les rejeux identiques et les conflits de contenu à des transitions différentes ; l'Inbox/Outbox est une extension de ce modèle. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- un schéma de la séquence lecture Kafka, écriture PostgreSQL et commit d'offset ;
- des essais nominaux, de rejeu identique, de conflit de contenu et d'erreur PostgreSQL, ainsi qu'un arrêt forcé dans la fenêtre annoncée ;
- la preuve qu'un rejeu n'ajoute pas un second effet métier dans le périmètre choisi ;
- une lecture qui distingue les lignes métier, les tentatives et les offsets Kafka ;
- une conclusion qui précise les limites de la solution.

## Ressources

- [TP 01 à 03 — Sémantiques Kafka](../README.md)
- [Limite de `exactly-once` Kafka](../../docs/architecture/kafka-sub-exactly-once-limit.md)
- [Design Inbox/Outbox](../../docs/architecture/kafka-inbox-outbox-design.md)
- [Audit des doublons absorbés](../../scripts/toggle-unique.sh)
