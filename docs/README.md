# Ressources complémentaires pour les étudiants

Commencez par le [parcours Kafka](../TP/README.md). Les ressources ci-dessous servent à approfondir une activité, un TP ou un sujet de SAÉ ; elles ne sont pas des prérequis à lire en bloc.

## Parcours de lecture en 30 minutes

1. **5 min** : vérifiez les [prérequis](../README.md#prérequis).
2. **5 min** : repérez l'[ordre des activités et des TP](../TP/README.md#commencer).
3. **10 min** : parcourez l'[activité 01](../TP/activite-01-decouverte-architecture.md) pour suivre un Pix dans la plateforme.
4. **10 min** : lisez [les sémantiques Kafka en une page](../TP/semantiques-kafka-en-une-page.md) pour distinguer perte, doublon et retard.

## Pour les activités et les TP

- [Interactions entre conteneurs](architecture/container-interactions.md) : suivre le trajet d'un Pix.
- [Délai de décision](architecture/decision-sla.md) : comprendre les rejets pour dépassement du délai métier.
- [Modèles de trafic](architecture/traffic-models.md) et [dashboards Grafana](architecture/grafana-dashboards.md) : interpréter les essais de charge.
- [Exactly-once et compteurs](architecture/exactly-once-kafka-observability.md) et [limite côté PostgreSQL](architecture/kafka-sub-exactly-once-limit.md) : lire les résultats du TP 03.
- [Résultats de débit](analyses-techniques/exactly_once_rate_sweep_analysis.md) et [comparaison des sémantiques](analyses-techniques/semantics_campaign_analysis-campaign_5000-v4.md) : données citées dans le TP 03.

## Pour les SAÉ

- [Guide de développement](developpement/guide-developpeur.md) et [exemples de messages](contrats-evenements/) : modifier les services et leurs contrats.
- [API de service-health](architecture/service-health-http.md) : exploiter l'état de la plateforme dans un sujet d'observabilité.
- [Design Inbox/Outbox](architecture/kafka-inbox-outbox-design.md) : étudier la frontière Kafka/PostgreSQL.
- [Protocole d'arrêt de validation](architecture/pix-validation-stop-protocol.md) : construire une expérience de résilience.
- [Campagne de sémantiques](experiences/semantics-campaign.md) et [balayage du délai métier](analyses-techniques/exactly_once_ttl_sweep_analysis.md) : préparer des mesures reproductibles.
