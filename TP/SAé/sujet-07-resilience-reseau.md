# Sujet 7 — Tester la résilience réseau et organiser la reprise

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Kafka fonctionne avec plusieurs brokers, c'est-à-dire plusieurs serveurs qui se répartissent et répliquent les messages. Une latence réseau, une perte de paquets ou l'arrêt d'un broker ne produisent pas les mêmes effets. Le pipeline peut continuer à recevoir des messages tout en dégradant la réplication, le lag, les délais de décision ou la persistance.

Une perturbation ne prouve donc pas automatiquement une perte de données. Votre travail est de distinguer ce qui est temporairement en attente, ce qui est rejeté pour raison métier, et ce qui serait réellement perdu.

## Votre mission

Définissez le comportement attendu du pipeline lors d'une perturbation réseau, puis construisez et exécutez un protocole qui vérifie ce comportement et le retour à la normale.

## Réalisation minimale attendue

Ajoutez un scénario ou un script automatisé qui applique une perturbation réseau, observe les critères de reprise puis échoue explicitement si le pipeline ne revient pas à l'état attendu. Le script doit conserver les données permettant d'interpréter l'essai : profil de perturbation, durée, métriques Kafka, compteurs métier et état de la persistance.

## Actions à réaliser

1. Choisissez une perturbation : latence, perte de paquets, lien ralenti ou arrêt temporaire d'un broker.
2. Écrivez une hypothèse sur les métriques qui doivent évoluer et celles qui doivent rester cohérentes.
3. Lancez un flux suffisamment long, appliquez la perturbation pendant le flux, puis retirez-la.
4. Observez la réplication Kafka, le lag consommateur, les compteurs métier et PostgreSQL.
5. Décrivez la chronologie : état nominal, perturbation, dégradation, rattrapage et retour à la normale.
6. Définissez les conditions qui permettent de déclarer le système rétabli.
7. Comparez le résultat avec la garantie de livraison retenue par le scénario.

## Questions de conception

- Quels symptômes indiquent un problème de réplication plutôt qu'un consommateur lent ?
- Quand un lag est-il un état temporaire acceptable et quand devient-il un risque métier ?
- Comment vérifiez-vous qu'un message en attente sera traité après le rétablissement du réseau ?
- Quelles données faut-il contrôler dans PostgreSQL avant de conclure qu'il n'y a pas de perte ?
- Quelle alerte ou quelle procédure aiderait un opérateur à décider quoi faire pendant l'incident ?

## Preuves attendues

- une hypothèse écrite avant chaque essai ;
- une chronologie de la perturbation et du rattrapage ;
- des mesures couvrant Kafka, le métier et PostgreSQL ;
- une conclusion qui distingue indisponibilité, retard, rejet métier et perte réelle ;
- une procédure de retour à la normale reproductible.

## Ressources

- [Perturbation réseau Kafka](../pour-aller-plus-loin/partie-6-perturbation-reseau-kafka.md)
- [Retard de réplication Kafka](../pour-aller-plus-loin/partie-4-retard-de-replication-kafka.md)
- [Protocole d'arrêt de validation](../../docs/architecture/pix-validation-stop-protocol.md)
