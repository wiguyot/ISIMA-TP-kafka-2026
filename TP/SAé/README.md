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

Chaque sujet occupe la durée entière de la SAé : le temps reste libre, seul le périmètre livré fait foi. La difficulté (★) indique l'exigence du périmètre à périmètre de preuve équivalent : *** pour les sujets les plus directs, **** pour les plus ambitieux. Les essais et campagnes de mesure demandent du temps machine : lancez les batteries d'essais tôt et régulièrement, pas seulement en fin de parcours.

Un groupe de quatre permet de répartir l'analyse, le développement, les essais et la documentation. Ce gain disparaît si l'intégration est traitée trop tard. Dès la première séance, répartissez des responsabilités, désignez un responsable de l'intégration et prévoyez un point de synchronisation à chaque séance. Au-delà de quatre étudiants, le périmètre doit augmenter ou le groupe doit être scindé : ajouter des personnes sans modifier le périmètre augmente surtout le coût de coordination.

## Règle d'affectation

Un groupe doit choisir un **sujet principal** et produire les livrables demandés par celui-ci. L'association de deux sujets est pertinente seulement si le premier est stabilisé : un mécanisme démontré par des tests et une conclusion sur ses limites.

Les combinaisons les plus cohérentes sont :

- sujet 2 + sujet 5 : traitement des rejets et preuves d'observabilité ;
- sujet 3 + sujet 8 : évolution de contrat et nouveau traitement métier ;
- sujet 4 + sujet 6 : délai métier et capacité du pipeline ;
- sujet 1 + sujet 7 : idempotence et perturbations qui provoquent les rejeux.

## Attendus communs

Quel que soit le sujet, votre rendu doit contenir :

1. une formulation précise du problème et de la garantie recherchée ;
2. une courte note de conception expliquant les choix effectués ;
3. le code, la configuration et les migrations nécessaires ;
4. un protocole d'essai exécutable par un autre groupe ;
5. des preuves fondées sur les tests, Kafka, PostgreSQL et les métriques ;
6. une conclusion qui distingue ce que votre solution garantit de ce qu'elle ne garantit pas.

Ne concluez jamais à partir d'un offset Kafka seul : vérifiez le résultat métier et la donnée durable.
