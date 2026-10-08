# Portefeuille de SAÉ Kafka

Ce répertoire propose des sujets de SAÉ construits à partir de `simul-pix`. Chaque sujet est autonome : un groupe peut en réaliser un seul, ou en associer deux lorsque le temps disponible et le niveau technique le permettent.

Avant de choisir un sujet, réalisez les [activités 01 à 10](../README.md) pour les fondamentaux Kafka. Les sujets qui traitent des garanties de livraison supposent aussi les TP 01 à 03.

Le sujet 15 s'appuie aussi sur le [TP 04 — ksqlDB](../tp-04-ksqldb.md), consacré au SQL sur les flux Kafka.

Pour modifier le code de la plateforme, lisez le [guide développeur](../../docs/developpement/guide-developpeur.md) : anatomie d'un service, reconstruction, ajout d'un topic ou d'une table, tests, mesures et limites connues.

## Choisir un sujet

Chaque sujet est prévu pour un groupe d'élèves, quatre au minimum.

| Sujet | Temps prévu | Difficulté |
|---|---|:---:|
| [1. Idempotence Kafka/PostgreSQL](sujet-01-idempotence-kafka-postgresql.md) | une SAé complète | **** |
| [2. DLQ (file de rejets) et rejeu métier](sujet-02-dlq-rejeu-metier.md) | une SAé complète | **** |
| [3. Évolution des contrats d'événements](sujet-03-contrats-evenements.md) | une SAé complète | *** |
| [4. Respect du SLA de décision](sujet-04-sla-decision.md) | une SAé complète | *** |
| [5. Observabilité et diagnostic d'incidents](sujet-05-observabilite-incidents.md) | une SAé complète | *** |
| [6. Qualification de déploiement et capacité](sujet-06-capacite-charge.md) | une SAé complète | **** |
| [7. Résilience réseau et reprise](sujet-07-resilience-reseau.md) | une SAé complète | **** |
| [8. Contrôle métier asynchrone](sujet-08-controle-metier-asynchrone.md) | une SAé complète | **** |
| [9. Sécuriser l'accès au cluster](sujet-09-securite-acces-cluster.md) | une SAé complète | *** |
| [10. Reconstruction de PostgreSQL depuis Kafka](sujet-10-reconstruction-postgres-kafka.md) | une SAé complète | **** |
| [11. Mise à jour et continuité métier](sujet-11-mise-a-jour-sans-interruption.md) | une SAé complète | *** |
| [12. Partage du cluster entre émetteurs](sujet-12-partage-cluster-emetteurs.md) | une SAé complète | **** |
| [13. Exactly-once : coût réel et périmètre réel](sujet-13-exactly-once-cout-et-limites.md) | une SAé complète | **** |
| [14. Au-delà de Poisson : simuler la variabilité réelle](sujet-14-variabilite-trafic.md) | une SAé complète | **** |
| [15. Du flux à l'état : suivre les paiements avec ksqlDB](sujet-15-ksqldb-streams-tables.md) | une SAé complète | **** |

Chaque sujet occupe la durée entière de la SAé : le temps reste libre, seul le périmètre livré fait foi. La difficulté (★) indique l'exigence du périmètre à périmètre de preuve équivalent : *** pour les sujets les plus directs, **** pour les plus ambitieux. Les essais et campagnes de mesure demandent du temps machine : lancez les batteries d'essais tôt et régulièrement, pas seulement en fin de parcours.

Pour vous aider à choisir selon vos centres d'intérêt, consultez le [guide des pistes de recherche](analyse-recherche-limos.md). Chaque fiche distingue le socle déjà fourni, la contribution minimale et les extensions facultatives ; reproduire un TP existant constitue une référence, pas la contribution de la SAé.

Le sujet 14 comporte une rencontre scientifique à organiser avec l'enseignant, avec une solution de remplacement prévue dans la fiche. Les coopérations entre groupes sont utiles ; chaque rendu doit cependant rester exécutable sans dépendre de l'achèvement d'un autre sujet.

Un groupe de quatre permet de répartir l'analyse, le développement, les essais et la documentation. Ce gain disparaît si l'intégration est traitée trop tard. Dès la première séance, répartissez des responsabilités, désignez un responsable de l'intégration et prévoyez un point de synchronisation à chaque séance. Au-delà de quatre étudiants, le périmètre doit augmenter ou le groupe doit être scindé : ajouter des personnes sans modifier le périmètre augmente surtout le coût de coordination.

## Règle d'affectation

Un groupe doit choisir un **sujet principal** et produire les livrables demandés par celui-ci. L'association de deux sujets est pertinente seulement si le premier est stabilisé : un mécanisme démontré par des tests et une conclusion sur ses limites.

Les combinaisons les plus cohérentes sont :

- sujet 2 + sujet 5 : traitement des rejets et preuves d'observabilité ;
- sujet 3 + sujet 8 : évolution de contrat et nouveau traitement métier ;
- sujet 4 + sujet 6 : délai métier et capacité du pipeline ;
- sujet 1 + sujet 7 : idempotence et perturbations qui provoquent les rejeux ;
- sujet 1 + sujet 10 : idempotence et reconstruction après perte de la base ;
- sujet 1 + sujet 13 : idempotence applicative et exactly-once natif, deux réponses au même problème ;
- sujet 3 + sujet 11 : évolution de contrat et déploiement sans interruption ;
- sujet 4 + sujet 12 : délai métier et isolation entre émetteurs ;
- sujet 6 + sujet 12 : méthodologie de campagne et partage du cluster ;
- sujet 7 + sujet 13 : perturbations réseau et garanties transactionnelles ;
- sujet 4 + sujet 14 : SLA et variabilité du trafic à moyenne égale ;
- sujet 6 + sujet 14 : qualification de capacité et modèles de charge avancés ;
- sujet 3 + sujet 15 : évolution des messages et adaptation des vues ksqlDB ;
- sujet 10 + sujet 15 : reconstruction des données et des vues matérialisées.

## Attendus communs

Quel que soit le sujet, votre rendu doit contenir :

1. une formulation précise du problème et de la garantie recherchée ;
2. une courte note de conception expliquant les choix effectués ;
3. le code, la configuration et les migrations nécessaires ;
4. un protocole d'essai exécutable par un autre groupe ;
5. des preuves fondées sur les tests, Kafka, PostgreSQL et les métriques ;
6. une conclusion qui distingue ce que votre solution garantit de ce qu'elle ne garantit pas ;
7. un dépôt GitHub dérivé, privé pendant la SAé puis public la veille de la soutenance (voir « Rendu sur GitHub » ci-dessous).

Un approfondissement théorique est **encouragé** pour éclairer vos choix et mieux comprendre vos résultats. Le [guide des pistes de recherche](analyse-recherche-limos.md) vous aide à choisir une question, une lecture ou un échange en lien avec vos centres d'intérêt.

Ne concluez jamais à partir d'un offset Kafka seul : vérifiez le résultat métier et la donnée durable.

## Évaluation commune

L'évaluation porte sur la formulation du problème, la conception et la justification des choix, la qualité de l'implémentation, l'intégration, les preuves reproductibles et la discussion des limites. Chaque membre doit pouvoir expliquer les décisions et les résultats du groupe.

Rendez visibles les contributions individuelles : code, conception, revues, tests, analyse et documentation. L'historique Git aide à suivre la démarche ; le nombre de commits n'est pas une mesure de contribution ni de qualité. Les extensions s'évaluent après un minimum démontré.

## Rendu sur GitHub

Faites une copie locale du dépôt public [`ISIMA-TP-kafka-2026`](https://github.com/wiguyot/ISIMA-TP-kafka-2026). Supprimez les informations Git de cette copie, puis créez votre propre dépôt GitHub **privé** à partir de celle-ci.

Le calendrier de publication :

1. **Dès la première séance** : créez votre dépôt privé, puis invitez le compte **williamguyotlenat@icloud.com** comme collaborateur. L'enseignant doit pouvoir suivre votre travail pendant la SAé.
2. **Pendant toute la SAé** : alimentez le dépôt au fil du travail — code, configuration, migrations, note de conception, protocole d'essai et preuves.
3. **La veille de la soutenance** : basculez la visibilité du dépôt en **public** et transmettez son URL à l'enseignant.

Le dépôt contient :

- une version exécutable de la plateforme incluant vos évolutions : code, configuration, migrations et ajouts aux scripts de la plateforme ;
- la note de conception, le protocole d'essai et les preuves recueillies ;
- un `README` à la racine : sujet traité, garantie visée, démarche pour lancer la plateforme et rejouer les essais, mention que le projet est dérivé de `simul-pix`.

Trois règles comptent pour l'évaluation :

1. **Commitez régulièrement** : des messages explicites et les traces de contribution permettent de suivre la démarche. Documentez aussi les contributions qui ne sont pas des commits de code.
2. **Publiez des preuves exploitables** : excluez les secrets, les volumes, caches et sorties volumineuses de `runtime/`. Versionnez dans un répertoire `preuves/` de petits jeux synthétiques, résultats et commandes nécessaires au rejeu, après vérification de leur contenu. Les exclusions de `infra/kafka/secrets/` et `runtime/` doivent être vérifiées avant publication.
3. **Respectez le calendrier** : dépôt privé accessible à l'enseignant dès le début, passage en public et URL transmise **la veille de la soutenance**. Un dépôt vide ou inaccessible à l'échéance équivaut à un rendu manquant.

## Dimension recherche

Chaque sujet peut donner envie d'aller plus loin : comprendre une file d'attente, expliquer un rejeu ou comparer des façons de partager les ressources. L'approfondissement théorique est encouragé à partir d'une question rencontrée dans votre projet.

Le guide [Choisir sa SAé et explorer les pistes du LIMOS](analyse-recherche-limos.md) présente les sujets par centres d'intérêt, des questions concrètes et des lectures pour démarrer. Il propose aussi des pistes pour préparer un échange avec le [LIMOS](https://www.limos.fr), avec l'aide de l'enseignant.

La section « Dimension théorique » de chaque fiche propose des idées à explorer. Vous pouvez aussi partir d'une question découverte pendant vos essais et en discuter avec l'enseignant.
