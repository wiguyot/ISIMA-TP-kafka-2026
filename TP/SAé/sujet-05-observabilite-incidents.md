# Sujet 5 — Diagnostiquer un incident à partir des signaux du pipeline

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Un pipeline peut sembler disponible parce que ses conteneurs sont démarrés, tout en accumulant du retard, en perdant sa capacité de réplication ou en n'écrivant plus correctement dans PostgreSQL. Une courbe isolée ne suffit donc pas à comprendre ce qui se passe.

L'**observabilité** consiste à recueillir des signaux, puis à les interpréter pour répondre à une question concrète : quel incident est en cours, quel est son impact métier, et quelle vérification faut-il faire ensuite ?

## Votre mission

Construisez une lecture d'incident qui distingue au moins trois situations :

- un consommateur prend du retard ;
- la réplication entre les brokers Kafka est dégradée ;
- la persistance PostgreSQL ne suit plus le flux.

Votre objectif n'est pas seulement d'afficher des métriques. Vous devez aider une personne qui découvre l'incident à passer d'un symptôme à un diagnostic vérifiable.

## Réalisation minimale attendue

Ajoutez un élément d'observabilité qui n'existe pas encore ou qui relie des données aujourd'hui séparées : métrique dérivée, alerte `service-health`, panneau Grafana ou commande de diagnostic. Il doit permettre de distinguer automatiquement au moins deux causes plausibles d'un même symptôme et être vérifié par un scénario d'incident rejouable.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Recensez les informations déjà visibles dans la météo, Grafana, `tp-kafka.sh` et `tp-db.sh`.
2. Définissez, pour chaque famille d'incident, les symptômes attendus, les hypothèses possibles et la vérification suivante.
3. Provoquez un retard consommateur et un retard de réplication.
4. Construisez ou améliorez une vue, un indicateur ou une règle de diagnostic qui supprime une ambiguïté réelle.
5. Vérifiez chaque conclusion contre Kafka et PostgreSQL, pas seulement contre un dashboard.
6. Rédigez un guide de diagnostic qu'un autre groupe peut suivre pendant un incident.

## Questions de conception

- Quelle différence voyez-vous entre un backlog temporaire et une perte de données ?
- Qu'est-ce qui permet de distinguer un problème applicatif d'un problème de cluster Kafka ?
- Quel indicateur relie le mieux un signal technique à son impact sur un paiement Pix ?
- Quelles métriques doivent être lues ensemble pour conclure sur la persistance ?
- Quel signal est assez fiable pour déclencher une alerte ?

## Dimension théorique

Votre sujet porte un aspect théorique formalisable : la détection d'anomalies dans des séries temporelles (seuils statiques contre méthodes adaptatives). Approfondissez-le : justifiez la méthode de détection qui déclenche vos diagnostics et sa robustesse aux faux positifs. Consultez [l'analyse recherche](analyse-recherche-limos.md) pour la référence détaillée : la détection d'anomalies en flux est un thème « Données, services, intelligence » de l'axe [SIC](https://www.limos.fr/axes/2) du LIMOS. Cet approfondissement fait partie de l'évaluation. L'aspect identifié ici n'est pas exhaustif : votre réalisation peut révéler d'autres aspects théoriques, à approfondir et à signaler également.

## Preuves attendues

- une matrice « symptôme, cause probable, vérification » ;
- des relevés pour au moins trois situations distinctes ;
- une amélioration d'observabilité réellement exploitable ;
- la démonstration que le diagnostic ne confond pas backlog, rejet métier et perte ;
- un guide de diagnostic reproductible.

## Ressources

- [Activité 08 — Observabilité et métrologie](../activite-08-observabilite-metrologie.md)
- [Atelier avancé](../pour-aller-plus-loin/realisation-des-tp.md)
- [Dashboards Grafana](../../docs/architecture/grafana-dashboards.md)
- [Service-health HTTP](../../docs/architecture/service-health-http.md)
