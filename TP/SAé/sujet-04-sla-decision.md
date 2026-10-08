# Sujet 4 — Préserver le délai de décision d'un paiement Pix

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Un paiement Pix doit recevoir une décision dans un délai donné. Ce délai est le **SLA** : l'engagement de service attendu par le métier. Kafka peut conserver les messages en attente, mais un paiement traité trop tard peut rester techniquement correct tout en étant inutile pour l'utilisateur.

Le **lag** mesure le nombre de messages qu'un consommateur n'a pas encore traités. Il devient un problème métier lorsque son délai de traitement fait dépasser le SLA.

## Votre mission

Concevez une stratégie qui permette de détecter, expliquer et améliorer le respect du délai de décision. Votre solution peut agir sur la capacité de traitement, la priorité des messages, les alertes ou la règle appliquée lorsqu'un délai est dépassé. Vous devez justifier le choix retenu.

## Socle fourni et contribution nouvelle

Le pipeline calcule déjà `decision_latency_ms` entre `event_time` et `decision_at`, un délai et un statut `decision_within_sla`. Il possède des alertes, des moyennes et des scénarios de retard. Ce délai ne mesure ni la visibilité de `outcome` ni le commit PostgreSQL.

Le ratio historique `estimated_oldest_lag_seconds` additionne des lags puis les divise par un débit récent : il n'est pas l'âge du plus ancien paiement. Les moyennes et les seules décisions déjà persistées ne suffisent pas à qualifier la population en attente.

## Réalisation minimale attendue

Choisissez et annoncez l'une des deux garanties :

- **Détection** : ajoutez une mesure et une alerte configurable ; mesurez le délai de détection, les faux positifs et les dépassements non détectés. Une alerte seule ne garantit pas le maintien du SLA.
- **Amélioration du respect du SLA** : implémentez une action sur le traitement ou la charge, puis comparez les dépassements avant/après à charge réellement comparable.

Dans les deux cas, automatisez un scénario nominal et un retard suivi d'un drainage borné. Produisez les p95/p99, leur effectif et le bilan de toutes les entrées : décisions à temps, décisions tardives et entrées sans décision à l'échéance. Utilisez un manifeste ou un suivi indépendant pour compter ces dernières. Les rejets font partie du bilan.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Exécutez un flux nominal et relevez le débit, le lag, le délai de décision et le nombre de dépassements SLA.
2. Provoquez un retard avec `consumer_lag` ou un pic de trafic.
3. Expliquez pourquoi le pipeline prend du retard et à quel moment ce retard devient un dépassement de délai métier.
4. Choisissez une amélioration mesurable : capacité supplémentaire, priorisation, seuil d'alerte, adaptation du flux ou règle de rejet explicite.
5. Implémentez le mécanisme choisi et rejouez le même protocole avec plusieurs répétitions ; contrôlez la charge effectivement publiée.
6. Comparez les mesures avant et après, puis expliquez le compromis entre débit, latence, taux de rejet et coût de la solution.

## Extensions facultatives

Comparez plusieurs politiques de régulation ou mesurez séparément décision, publication du résultat et persistance. Définissez un engagement distinct pour chacune de ces frontières.

## Questions de conception

- Quel délai doit être mesuré : entre émission et décision, ou entre réception et persistance ?
- Quel indicateur permet d'anticiper un dépassement plutôt que de le constater après coup ?
- Faut-il traiter tous les Pix dans l'ordre, ou certains peuvent-ils être priorisés ?
- Que doit-il se passer lorsqu'un paiement dépasse son délai : rejet, alerte, traitement tardif ou autre décision ?
- Comment vérifiez-vous que l'amélioration ne déplace pas simplement le problème vers PostgreSQL ?

## Dimension théorique

Définissez la file, sa population et les moyennes auxquelles vous appliquez la loi de Little. Cette loi ne donne ni le plus vieux délai ni un p99 instantané. Formalisez aussi le critère choisi : détection et faux positifs, ou réduction des dépassements par une action. Incluez les entrées sans décision dans le bilan. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- un protocole de mesure reproductible ;
- pour la détection, délai d'alerte, faux positifs et dépassements non détectés ; pour la régulation, comparaison avant/après à charge comparable ;
- p95/p99 avec effectifs et bilan incluant rejets, décisions tardives et entrées sans décision ;
- une lecture cohérente des compteurs Kafka, métier et PostgreSQL ;
- une décision argumentée sur la politique retenue ;
- une explication des limites de cette politique sous charge forte.

## Ressources

- [Activité 08 — Observabilité](../activite-08-observabilite-metrologie.md)
- [Activité 09 — Charge et temporisation](../activite-09-charge-et-temporisation.md)
- [Scénario de pic de paiements](../pour-aller-plus-loin/partie-5-pic-de-paiements-football.md)
- [Décision et SLA](../../docs/architecture/decision-sla.md)
- [Modèles de trafic](../../docs/architecture/traffic-models.md)
