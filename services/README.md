# Services

Chaque sous-dossier contient un service applicatif Python réellement utilisé par la plateforme locale.

## Services présents

- `generator` : produit les transactions simulées et les variantes d'erreur, expose un endpoint HTTP de suivi sur le port interne `8081` ;
- `pix-validator` : consomme `raw` et `retry`, applique les contrôles d'entrée et publie dans `checked`, expose un endpoint HTTP sur `8086` ;
- `pix-decision-engine` : consomme `checked`, prend la décision métier puis publie dans `validated`, `rejected` et `decision`, expose un endpoint HTTP sur `8087` ;
- `pix-outcome-publisher` : consomme `decision` et publie la décision finale dans `outcome`, expose un endpoint HTTP sur `8088` ;
- `persister-valid` : consomme `validated` et persiste dans PostgreSQL, expose un endpoint HTTP sur `8084` ;
- `persister-rejected` : consomme `rejected` et persiste les rejets de façon idempotente dans PostgreSQL, expose un endpoint HTTP sur `8085` ;
- `service-health` : agrège l'état de la plateforme, expose les vues HTML/JSON et pilote les scénarios, expose un endpoint HTTP sur `8082`.

## Conventions techniques communes

- communication Kafka via `confluent-kafka` ;
- communication PostgreSQL via `psycopg` pour les persisters et `service-health` ;
- endpoint minimal `GET /` et `GET /health` ;
- état applicatif gardé en mémoire dans une structure `STATE` ;
- configuration principalement portée par des variables d'environnement `SIMULPIX_*`.

## Sémantique de traitement

- les producteurs applicatifs publient avec `acks=all` ;
- les consommateurs Kafka font un commit explicite après succès du traitement ;
- `persister-valid` est idempotent sur `transaction_id` ;
- `persister-rejected` est idempotent sur `rejection_fingerprint`.

## Données de rejeu

Les messages retraités peuvent transporter :

- `source_transaction_id` ;
- `retry_attempt`.

Ces champs sont conservés jusqu'à la persistance pour permettre un rejeu pédagogique traçable.

## Contrat de décision

Les messages transportent aussi désormais une décision client structurée :

- `decision_sla_seconds` ;
- `decision_deadline` ;
- `decision_status` ;
- `decision_reason_code` ;
- `decision_reason_label` ;
- `decision_origin` ;
- `decision_latency_ms` ;
- `decision_within_sla` ;
- `client_message`.

L'objectif est de distinguer la décision finale lisible côté client du détail technique interne du pipeline.
