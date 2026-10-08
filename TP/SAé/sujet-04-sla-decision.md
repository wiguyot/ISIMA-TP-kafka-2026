# Sujet 4 — Préserver le délai de décision d'un paiement Pix

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Un paiement Pix doit recevoir une décision dans un délai donné. Ce délai est le **SLA** : l'engagement de service attendu par le métier. Kafka peut conserver les messages en attente, mais un paiement traité trop tard peut rester techniquement correct tout en étant inutile pour l'utilisateur.

Le **lag** mesure le nombre de messages qu'un consommateur n'a pas encore traités. Il devient un problème métier lorsque son délai de traitement fait dépasser le SLA.

## Votre mission

Concevez une stratégie qui permette de détecter, expliquer et améliorer le respect du délai de décision. Votre solution peut agir sur la capacité de traitement, la priorité des messages, les alertes ou la règle appliquée lorsqu'un délai est dépassé. Vous devez justifier le choix retenu.

## Réalisation minimale attendue

Implémentez un mécanisme automatisé qui rend le risque de dépassement SLA exploitable : par exemple une métrique dérivée, une alerte dans `service-health`, une vue Grafana ou une règle explicite de traitement tardif. Il doit utiliser les données réelles du pipeline, être configurable et être couvert par un scénario qui provoque puis résorbe un retard.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Exécutez un flux nominal et relevez le débit, le lag, le délai de décision et le nombre de dépassements SLA.
2. Provoquez un retard avec `consumer_lag` ou un pic de trafic.
3. Expliquez pourquoi le pipeline prend du retard et à quel moment ce retard devient un dépassement de délai métier.
4. Choisissez une amélioration mesurable : capacité supplémentaire, priorisation, seuil d'alerte, adaptation du flux ou règle de rejet explicite.
5. Implémentez l'amélioration et rejouez exactement le même protocole d'essai.
6. Comparez les mesures avant et après, puis expliquez le compromis entre débit, latence, taux de rejet et coût de la solution.

## Questions de conception

- Quel délai doit être mesuré : entre émission et décision, ou entre réception et persistance ?
- Quel indicateur permet d'anticiper un dépassement plutôt que de le constater après coup ?
- Faut-il traiter tous les Pix dans l'ordre, ou certains peuvent-ils être priorisés ?
- Que doit-il se passer lorsqu'un paiement dépasse son délai : rejet, alerte, traitement tardif ou autre décision ?
- Comment vérifiez-vous que l'amélioration ne déplace pas simplement le problème vers PostgreSQL ?

## Dimension théorique

Votre sujet porte des aspects théoriques formalisables : la loi de Little (relation entre lag, débit et délai), les percentiles contre la moyenne pour mesurer un SLA, et le comportement d'un système quand le taux d'occupation approche 1. Approfondissez-les : formalisez la relation lag/délai de votre pipeline et vérifiez-la expérimentalement. Consultez [l'analyse recherche](analyse-recherche-limos.md) pour la référence détaillée : la théorie des files d'attente et l'évaluation de performances sont le cœur de l'axe [ODPS](https://www.limos.fr/axes/3) du LIMOS. Cet approfondissement fait partie de l'évaluation. L'aspect identifié ici n'est pas exhaustif : votre réalisation peut révéler d'autres aspects théoriques, à approfondir et à signaler également.

## Preuves attendues

- un protocole de mesure reproductible ;
- une comparaison avant/après sur le délai, le lag et les dépassements SLA ;
- une lecture cohérente des compteurs Kafka, métier et PostgreSQL ;
- une décision argumentée sur la politique retenue ;
- une explication des limites de cette politique sous charge forte.

## Ressources

- [Activité 08 — Observabilité](../activite-08-observabilite-metrologie.md)
- [Activité 09 — Charge et temporisation](../activite-09-charge-et-temporisation.md)
- [Scénario de pic de paiements](../pour-aller-plus-loin/partie-5-pic-de-paiements-football.md)
- [Décision et SLA](../../docs/architecture/decision-sla.md)
- [Modèles de trafic](../../docs/architecture/traffic-models.md)
