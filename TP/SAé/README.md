# Portefeuille de SAÉ Kafka

Ce répertoire propose des sujets de SAÉ construits à partir de `simul-pix`. Chaque sujet est autonome : un groupe peut en réaliser un seul, ou en associer deux lorsque le temps disponible et le niveau technique le permettent.

Avant de choisir un sujet, réalisez les [activités 01 à 10](../README.md) pour les fondamentaux Kafka. Les sujets qui traitent des garanties de livraison supposent aussi les TP 01 à 03.

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
| [7. Résilience réseau et reprise](sujet-07-resilience-reseau.md) | une SAé complète | *** |
| [8. Contrôle métier asynchrone](sujet-08-controle-metier-asynchrone.md) | une SAé complète | **** |
| [9. Sécuriser l'accès au cluster](sujet-09-securite-acces-cluster.md) | une SAé complète | *** |
| [10. Reconstruction de PostgreSQL depuis Kafka](sujet-10-reconstruction-postgres-kafka.md) | une SAé complète | **** |
| [11. Mise à jour sans interruption](sujet-11-mise-a-jour-sans-interruption.md) | une SAé complète | *** |
| [12. Partage du cluster entre émetteurs](sujet-12-partage-cluster-emetteurs.md) | une SAé complète | **** |
| [13. Exactly-once : coût réel et périmètre réel](sujet-13-exactly-once-cout-et-limites.md) | une SAé complète | **** |
| [14. Au-delà de Poisson : simuler la variabilité réelle](sujet-14-variabilite-trafic.md) | une SAé complète | **** |

Chaque sujet occupe la durée entière de la SAé : le temps reste libre, seul le périmètre livré fait foi. La difficulté (★) indique l'exigence du périmètre à périmètre de preuve équivalent : *** pour les sujets les plus directs, **** pour les plus ambitieux. Les essais et campagnes de mesure demandent du temps machine : lancez les batteries d'essais tôt et régulièrement, pas seulement en fin de parcours.

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
- sujet 6 + sujet 14 : qualification de capacité et modèles de charge avancés.

## Attendus communs

Quel que soit le sujet, votre rendu doit contenir :

1. une formulation précise du problème et de la garantie recherchée ;
2. une courte note de conception expliquant les choix effectués ;
3. le code, la configuration et les migrations nécessaires ;
4. un protocole d'essai exécutable par un autre groupe ;
5. des preuves fondées sur les tests, Kafka, PostgreSQL et les métriques ;
6. une conclusion qui distingue ce que votre solution garantit de ce qu'elle ne garantit pas ;
7. un approfondissement théorique : consultez [l'analyse recherche](analyse-recherche-limos.md) pour identifier l'aspect théorique de votre sujet (section « Dimension théorique » de sa fiche), l'axe LIMOS concerné, et le travail d'approfondissement attendu ;
8. la publication du projet sous forme de fork GitHub, privé pendant la SAé puis public la veille de la soutenance (voir « Rendu sur GitHub » ci-dessous).

Ne concluez jamais à partir d'un offset Kafka seul : vérifiez le résultat métier et la donnée durable.

## Rendu sur GitHub

Chaque groupe publie son projet final sous la forme d'un **fork du dépôt de la plateforme** [`ISIMA-TP-kafka-2026`](https://github.com/wiguyot/ISIMA-TP-kafka-2026). Ce fork sert de rendu final et de support à la soutenance : il doit permettre à une autre personne de lancer la plateforme et de rejouer vos essais.

Le calendrier de publication :

1. **Dès la première séance** : créez votre fork en visibilité **privée** (l'option de visibilité se choisit au moment du fork ; si votre compte ne la propose pas, dupliquez le dépôt en privé par clonage miroir), puis invitez le compte **williamguyotlenat@icloud.com** comme collaborateur. L'enseignant doit pouvoir suivre votre travail pendant la SAé, pas seulement le découvrir à la fin.
2. **Pendant toute la SAé** : alimentez le fork au fil du travail — code, configuration, migrations, note de conception, protocole d'essai et preuves.
3. **La veille de la soutenance** : basculez la visibilité du fork en **public** et transmettez son URL à l'enseignant.

Le dépôt contient :

- une version exécutable de la plateforme incluant vos évolutions : code, configuration, migrations et ajouts aux scripts de la plateforme ;
- la note de conception, le protocole d'essai et les preuves recueillies ;
- un `README` à la racine : sujet traité, garantie visée, démarche pour lancer la plateforme et rejouer les essais, mention que le projet est dérivé de `simul-pix`.

Trois règles comptent pour l'évaluation :

1. **Commitez régulièrement** : l'historique Git (messages explicites, travail étalé dans le temps, contribution visible de chaque membre) fait partie de l'évaluation. Un fork créé et rempli la veille de la soutenance ne prouve rien sur la démarche.
2. **Ne publiez aucun secret ni donnée générée** : les chemins `infra/kafka/secrets/` et `runtime/` sont déjà exclus par le `.gitignore` du projet ; vérifiez-le avant votre première publication. Un fork public ne doit jamais contenir de secret, même poussé puis supprimé.
3. **Respectez le calendrier** : fork privé avec l'enseignant en collaborateur dès le début, passage en public et URL transmises **la veille de la soutenance**. Un fork privé sans l'enseignant en collaborateur, vide, ou inaccessible à ce moment équivaut à un rendu manquant.

## Dimension recherche

Chaque sujet recouvre un ou plusieurs aspects théoriques formalisables (files d'attente, transactions distribuées, équité d'allocation, processus stochastiques, contrôle d'accès...). Certains de ces aspects correspondent aux thèmes de recherche du [laboratoire LIMOS](https://www.limos.fr).

Le document [analyse recherche et ancrage LIMOS](analyse-recherche-limos.md) recense, sujet par sujet : l'aspect théorique, les domaines de recherche associés et l'axe LIMOS concerné. **Consultez-le dès le choix de votre sujet** : l'approfondissement de cet aspect théorique fait partie de l'évaluation de la SAé, et peut inclure une rencontre avec les chercheurs dont les travaux se rapprochent de votre thème.

Cette liste n'est **pas exhaustive** : la section « Dimension théorique » de votre fiche indique un aspect identifié, mais votre sujet peut en receler d'autres. Si votre réalisation en révèle un nouveau, approfondissez-le aussi — et signalez-le à l'enseignant pour enrichir l'analyse.
