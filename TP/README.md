# Parcours étudiant Kafka

Ce parcours utilise `simul-pix`, une simulation de paiements qui permet d'observer des producteurs, des topics, des consommateurs et la persistance des résultats.

## Commencer

1. Vérifiez les [prérequis](../README.md#prérequis), clonez le dépôt et lancez `./start.sh` depuis sa racine.
2. Suivez les [activités 01 à 10](#activités-01-à-10) dans l'ordre pour apprendre les bases de Kafka.
3. Lisez [Les sémantiques Kafka en une page](semantiques-kafka-en-une-page.md), puis les [TP 01 à 04](#tp-01-à-04) pour comprendre les garanties de traitement et découvrir ksqlDB.

Réalisez les expériences de reproduction proposées dans les TP. Pour interpréter les résultats, distinguez toujours une perte, un doublon et un retard de traitement (backlog).

Pour vous orienter si vous avez peu de temps, consultez le [parcours de lecture en 30 minutes](../docs/README.md#parcours-de-lecture-en-30-minutes). La documentation d'architecture et de développement est facultative pour les activités.

Si vous travaillez sur une machine aux ressources limitées, consultez le [protocole de test sur une machine moins puissante](protocole-test-machine-modeste.md) : il explique comment relever les ressources, qualifier une lenteur et essayer une variante sans confondre ses résultats avec ceux du parcours de référence.

Si un conteneur ou un port est déjà utilisé, repérez le service concerné avec `docker ps` et arrêtez l'autre plateforme avant de relancer `./start.sh`. Si les compteurs reflètent un essai précédent, relancez le scénario ou utilisez `./scripts/reset-scenario.sh`. Un message `TimeoutException` après une lecture Kafka avec `--timeout-ms` signifie simplement que la console a terminé son attente sans nouveau message.

## Activités 01 à 10

Réalisez ces activités dans l'ordre. Elles vous conduisent de la découverte de la plateforme à la synthèse du parcours d'une transaction.

1. [Découvrir l'architecture simul-pix](activite-01-decouverte-architecture.md)
2. [Publier et lire des transactions](activite-02-premiers-messages-kafka.md)
3. [Comprendre PUB/SUB](activite-03-semantique-pub-sub.md)
4. [Répartir le travail avec les consumer groups](activite-04-groupes-consommateurs.md)
5. [Relier clé métier et partition](activite-05-partitionnement-cle-emetteur.md)
6. [Comprendre offsets, reprise et rejeu](activite-06-rejeu-offsets-retention.md)
7. [Observer les rejets métier](activite-07-rejets-dlq.md)
8. [Lire les métriques et l'état des services](activite-08-observabilite-metrologie.md)
9. [Observer charge, lag et temporisation](activite-09-charge-et-temporisation.md)
10. [Reconstituer le parcours d'une transaction](activite-10-synthese-architecture-evenementielle.md)

À la fin, vous devez pouvoir expliquer comment une transaction traverse les services et Kafka, et interpréter les principaux indicateurs observés.

## TP 01 à 04

Ces fiches sont des lectures guidées fondées sur des résultats mesurés. Les expériences sont proposées en complément si vous souhaitez les reproduire.

1. [TP 01 — Comprendre at-most-once](tp-01-at-most-once.md) : où une perte peut-elle se produire ?
2. [TP 02 — Comprendre at-least-once](tp-02-at-least-once.md) : pourquoi des doublons apparaissent-ils ?
3. [TP 03 — Comprendre exactly-once](tp-03-exactly-once.md) : que protège Kafka, et où cette protection s'arrête-t-elle ?
4. [TP 04 — Découvrir ksqlDB](tp-04-ksqldb.md) : comment interroger et transformer des flux Kafka en SQL ?

À la fin des TP 01 à 03, vous devez savoir distinguer les risques de perte et de doublon et préciser le périmètre d'une garantie. Le TP 04 présente ksqlDB et ses usages sur le flux simulé.

## Pour continuer

- [Ateliers complémentaires](pour-aller-plus-loin/README.md) : incidents, réplication, charge et perturbations réseau.
- [Parcours SAÉ](README-SAé.md) : choisir et réaliser un projet qui prolonge les activités.
- [Documentation complémentaire](../docs/README.md) : ressources facultatives classées par usage.
