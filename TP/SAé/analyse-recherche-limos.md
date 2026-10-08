# Choisir sa SAé et explorer les pistes du LIMOS

Une SAé peut partir d'une question très concrète : éviter qu'un paiement soit traité deux fois, comprendre pourquoi le système ralentit, ou remettre une base en état après une panne. Ces questions ouvrent aussi sur des idées étudiées en recherche.

Ce guide vous aide à **repérer les sujets qui vous intéressent**, puis à trouver des pistes pour mieux comprendre ce que vous allez construire. Vous pouvez commencer par les thèmes ci-dessous et lire les fiches qui vous attirent.

**L'approfondissement théorique est encouragé.** Une lecture, un petit modèle ou un échange avec un chercheur peut éclairer un choix technique et vous aider à expliquer vos résultats. Choisissez une piste en lien avec votre projet, à un niveau que vous pourrez vous approprier.

## Par quoi avez-vous envie de commencer ?

| Ce qui vous intéresse | Sujets à regarder |
|---|---|
| Comprendre les pannes et rendre les paiements fiables | [1. Idempotence](sujet-01-idempotence-kafka-postgresql.md), [10. Reconstruction](sujet-10-reconstruction-postgres-kafka.md), [13. Exactly-once](sujet-13-exactly-once-cout-et-limites.md) |
| Enquêter sur les erreurs et aider à les résoudre | [2. Correction et rejeu](sujet-02-dlq-rejeu-metier.md), [5. Diagnostic](sujet-05-observabilite-incidents.md) |
| Faire évoluer une application utilisée en continu | [3. Contrats d'événements](sujet-03-contrats-evenements.md), [11. Mise à jour](sujet-11-mise-a-jour-sans-interruption.md) |
| Mesurer les performances et comprendre les ralentissements | [4. Délai de décision](sujet-04-sla-decision.md), [6. Capacité](sujet-06-capacite-charge.md), [12. Partage du cluster](sujet-12-partage-cluster-emetteurs.md) |
| Explorer le réseau ou la sécurité | [7. Résilience réseau](sujet-07-resilience-reseau.md), [9. Accès au cluster](sujet-09-securite-acces-cluster.md) |
| Ajouter une fonctionnalité métier à la plateforme | [8. Contrôle asynchrone](sujet-08-controle-metier-asynchrone.md) |
| Programmer des simulations et explorer les probabilités | [14. Variabilité du trafic](sujet-14-variabilite-trafic.md) |
| Travailler les données et construire une application avec SQL | [15. Streams et tables avec ksqlDB](sujet-15-ksqldb-streams-tables.md) |

Plusieurs sujets peuvent vous plaire pour des raisons différentes. Le [portefeuille de SAé](README.md#choisir-un-sujet) complète cette première sélection avec les prérequis, la difficulté et le travail de réalisation.

## Les questions derrière les sujets

### Des paiements fiables, même quand un service s'arrête

- **[Sujet 1 — Idempotence](sujet-01-idempotence-kafka-postgresql.md)** : comment reconnaître un paiement déjà traité et éviter un second effet ? Vous pouvez explorer les identifiants, la mémoire du traitement et ce qui se passe entre l'écriture en base et la validation de l'offset Kafka.
- **[Sujet 2 — Correction et rejeu](sujet-02-dlq-rejeu-metier.md)** : comment donner une seconde chance à un paiement invalide sans le rejouer indéfiniment ? Une représentation du parcours — rejet, correction, nouvelle tentative, clôture — aide à concevoir le suivi.
- **[Sujet 10 — Reconstruction](sujet-10-reconstruction-postgres-kafka.md)** : peut-on retrouver l'état de la base à partir des messages conservés ? Le sujet invite à comprendre comment un historique produit un état, et quelles informations doivent rester disponibles.
- **[Sujet 13 — Exactly-once](sujet-13-exactly-once-cout-et-limites.md)** : que protège réellement une transaction Kafka, et quel est son coût ? Vous pouvez étudier où commence et où s'arrête sa garantie, notamment lorsque PostgreSQL intervient.

### Une application qui évolue

- **[Sujet 3 — Contrats d'événements](sujet-03-contrats-evenements.md)** : comment ajouter un champ sans empêcher les anciennes versions de lire les messages ? C'est une occasion d'explorer la compatibilité entre versions à partir d'exemples simples.
- **[Sujet 8 — Contrôle asynchrone](sujet-08-controle-metier-asynchrone.md)** : comment faire travailler plusieurs services sur un même paiement ? Vous pouvez réfléchir à l'ordre des étapes et au service qui prend la décision finale.
- **[Sujet 11 — Mise à jour](sujet-11-mise-a-jour-sans-interruption.md)** : que deviennent les paiements pendant le remplacement d'un service ? Le sujet relie déploiement, répartition du travail et durée de reprise.

### Des performances qui tiennent sous charge

- **[Sujet 4 — Délai de décision](sujet-04-sla-decision.md)** : pourquoi certains paiements attendent-ils trop longtemps alors que le délai moyen semble bon ? Les files d'attente et la répartition des délais permettent de comprendre ce décalage.
- **[Sujet 6 — Capacité](sujet-06-capacite-charge.md)** : jusqu'où votre machine peut-elle suivre le rythme ? Vous pouvez explorer les goulots d'étranglement et comprendre pourquoi ajouter des workers n'accélère pas toujours le système.
- **[Sujet 12 — Partage du cluster](sujet-12-partage-cluster-emetteurs.md)** : comment éviter que le pic de trafic d'un établissement pénalise les autres ? Ce sujet ouvre sur le partage des ressources, les quotas et l'équité.
- **[Sujet 14 — Variabilité du trafic](sujet-14-variabilite-trafic.md)** : deux flux de même débit moyen sollicitent-ils le système de la même façon ? Vous pouvez programmer des modèles d'arrivées et étudier l'effet des pics et des dépendances entre événements.

### Des incidents compréhensibles et des accès maîtrisés

- **[Sujet 5 — Diagnostic](sujet-05-observabilite-incidents.md)** : comment passer d'une courbe inquiétante à une cause vérifiable ? Vous pouvez étudier les signaux qui distinguent les incidents et les situations où plusieurs explications restent possibles.
- **[Sujet 7 — Résilience réseau](sujet-07-resilience-reseau.md)** : comment le pipeline réagit-il aux coupures et aux pertes de paquets ? Le sujet permet de relier les mécanismes du réseau à ce que l'application observe.
- **[Sujet 9 — Accès au cluster](sujet-09-securite-acces-cluster.md)** : comment donner à chaque service les droits nécessaires à son travail ? Vous pouvez explorer le contrôle d'accès en reliant chaque permission à un usage concret.

### Des données qu'on peut interroger en continu

- **[Sujet 15 — Streams et tables avec ksqlDB](sujet-15-ksqldb-streams-tables.md)** : comment transformer les événements de paiement en informations que l'on peut consulter et suivre en direct ? Vous pouvez explorer le passage d'un historique à un état, puis expliquer ce que changent un doublon, un paiement tardif ou une mise à jour du référentiel clients.

## Une façon simple d'aller plus loin

Pour explorer une piste théorique, vous pouvez suivre ce chemin :

1. **Partir d'une observation.** Un paiement a été rejoué, un délai a augmenté, un nouveau champ a disparu.
2. **Poser une question précise.** Dans quelles conditions ce comportement apparaît-il ? Quelle décision technique pourrait le changer ?
3. **Chercher une idée utile.** Une documentation, un article conseillé par l'enseignant ou un schéma peut vous aider à expliquer le mécanisme.
4. **Revenir à votre expérience.** Comparez ce que vous aviez prévu à ce que vous observez, puis expliquez les écarts.

Par exemple, pour le sujet 4, vous pouvez envoyer le même volume de paiements à rythme régulier puis par pics. Observer la file d'attente et les délais vous donnera une raison concrète de vous intéresser à la théorie des files.

Une expérience qui contredit votre idée de départ peut aussi enrichir le projet : elle aide à identifier les conditions dans lesquelles votre explication fonctionne. Les [attendus communs](README.md#attendus-communs) et les [limites du socle](limites-du-socle.md) vous aideront à interpréter ces résultats.

## Ce qu'un échange avec le LIMOS peut vous apporter

Le [LIMOS](https://www.limos.fr) est le laboratoire de recherche associé à l'ISIMA. Échanger avec un chercheur peut vous aider à choisir un modèle, trouver une lecture accessible ou imaginer une expérience qui départage deux explications.

Présentez votre projet simplement : **ce que vous construisez, ce que vous observez et la question que vous vous posez**. L'enseignant pourra vous aider à préparer l'échange et à chercher un interlocuteur.

Voici quelques points d'entrée à explorer avec lui :

| Point d'entrée | Pistes à rapprocher de votre projet |
|---|---|
| [SIC](https://www.limos.fr/axes/2) | données, échanges entre services, réseaux, sécurité ; notamment les sujets 1, 3, 5, 7, 8, 9, 10 et 15 |
| [ODPS](https://www.limos.fr/axes/3) | simulation, performances et utilisation des ressources ; notamment les sujets 4, 6, 12 et 14 |
| [MAAD](https://www.limos.fr/axes/1) | modèles, algorithmes et allocation des ressources ; notamment les sujets 12 et 14 |

Ces rapprochements servent à orienter la recherche d'un contact ; l'enseignant vous aidera à vérifier quels travaux se rapprochent de votre question. Pour le sujet 13, le bon interlocuteur dépendra de l'aspect des transactions distribuées que vous souhaitez explorer.

Le **sujet 14 prévoit un échange scientifique**. Son organisation et la possibilité d'un entretien de remplacement sont expliquées dans la [fiche du sujet](sujet-14-variabilite-trafic.md).

## Quelques lectures pour démarrer

Choisissez une lecture qui répond à une question rencontrée pendant votre projet. Vous pouvez commencer par une documentation et demander à l'enseignant de vous accompagner dans la lecture d'un article.

| Pour explorer… | Première lecture |
|---|---|
| Les rejeux et la frontière entre Kafka et PostgreSQL — sujets 1 et 13 | [La limite de l'exactly-once dans simul-pix](../../docs/architecture/kafka-sub-exactly-once-limit.md), puis le [principe Inbox/Outbox](../../docs/architecture/kafka-inbox-outbox-design.md) |
| L'évolution des messages — sujets 3 et 11 | [La compatibilité des schémas, expliquée par Confluent](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html) |
| Ce que compte l'audit PostgreSQL — sujets 1 et 13 | [Le comportement des triggers PostgreSQL](https://www.postgresql.org/docs/current/trigger-definition.html) |
| Le lien entre nombre de paiements en attente, débit et délai — sujets 4, 6 et 14 | John D. C. Little (1961), [A Proof for the Queuing Formula: L = λW](https://pubsonline.informs.org/doi/abs/10.1287/opre.9.3.383) |
| Les arrivées qui favorisent d'autres arrivées — sujet 14 | Alan G. Hawkes (1971), [Spectra of some self-exciting and mutually exciting point processes](https://academic.oup.com/biomet/article-abstract/58/1/83/224809) |
| Le passage des événements à un état consultable — sujet 15 | [Streams et tables dans ksqlDB](https://docs.confluent.io/platform/current/ksqldb/reference/sql/data-definition.html), puis [requêtes persistantes, push et pull](https://docs.confluent.io/platform/current/ksqldb/concepts/queries.html) |

Gardez une trace de ce qu'une lecture ou un échange vous a apporté : une hypothèse modifiée, une expérience ajoutée, un choix mieux expliqué. C'est ainsi que l'approfondissement peut prendre une place utile dans votre SAé.
