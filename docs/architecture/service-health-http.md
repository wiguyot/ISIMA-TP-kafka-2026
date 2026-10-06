# Service-Health HTTP

Cette note décrit l'API HTTP réellement exposée par `service-health`.

Base locale par défaut :

- `http://localhost:8082` : météo générale
- `http://localhost:8083` : vue atelier orientée pilotage

Les deux ports sont servis par le même composant. La différence principale est la vue HTML par défaut.

## Endpoints `GET`

### `GET /`

- sur le port `8082` : rend la météo HTML générale ;
- sur le port `8083` : rend la vue atelier HTML.

### `GET /ui`

- rend explicitement la météo HTML générale.

### `GET /scenario`

- rend la vue atelier HTML ;
- accepte des paramètres de préremplissage d'interface, par exemple :
  - `scenario`
  - `total_messages`
  - `rate_per_second`
  - `traffic_model`
  - `decision_sla_seconds`
  - `producer_semantics`
  - `producer_acks`
  - `producer_retries`
  - `consumer_semantics`
  - `consumer_commit_mode`
  - `consumer_auto_commit`
  - `consumer_max_poll_interval_ms`
  - `consumer_commit_batch_size`
  - `match_count`
  - `stadium_capacity`
  - `pix_usage_rate`
  - `peak_share`
  - `peak_window_minutes`
  - `total_window_minutes`
  - `time_compression_factor`
  - `network_profile`
  - `network_target_service`
  - `network_delay_ms`
  - `network_jitter_ms`
  - `network_loss_percent`
  - `network_rate_kbit`
  - `control_action`
  - `result`

Le paramètre `scenario` reste un paramètre technique de préremplissage et de lancement. Il ne structure plus l'interface comme une liste de parties visibles.

### `GET /health`

- retourne la synthèse JSON de la plateforme ;
- expose notamment `status`, `services`, `metrics`, `alerts` et `control`.

### `GET /health/services`

- retourne le même document JSON détaillé que l'API de santé complète ;
- utile pour des intégrations ou outils qui veulent cibler une route plus explicite.

### `GET /health/details`

- retourne la vue JSON détaillée ;
- ajoute les détails Kafka, PostgreSQL, historique, état réseau courant et groupes consommateurs.

## Endpoint `POST`

### `POST /control`

- exécute une action de pilotage de manière asynchrone ;
- répond par une redirection `303` vers `/?control_action=...&result=...` ou `/scenario?...` ;
- le résultat effectif se lit ensuite dans `control.status`, `control.last_action`, `control.last_result` et `control.last_output` via `GET /health` ou `GET /health/details`.

Format attendu :

- `application/x-www-form-urlencoded`

Champ commun :

- `action`

Champ optionnel d'UI :

- `return_view=scenario` pour revenir vers `/scenario` au lieu de la météo générale.

## Actions supportées

### `action=reset`

Lance `./scripts/reset-scenario.sh`.

Exemple :

```sh
curl -X POST http://127.0.0.1:8082/control \
  --data "action=reset"
```

### `action=stop_scenario`

Lance `./scripts/stop-scenario.sh`.

Exemple :

```sh
curl -X POST http://127.0.0.1:8082/control \
  --data "action=stop_scenario"
```

### `action=run_scenario`

Lance `./scripts/run-scenario.sh`.

Paramètres :

- `scenario`
- `total_messages`
- `rate_per_second`
- `traffic_model`
- `decision_sla_seconds`
- `producer_semantics`
- `producer_acks`
- `producer_retries`
- `consumer_semantics`
- `consumer_commit_mode`
- `consumer_auto_commit`
- `consumer_max_poll_interval_ms`
- `consumer_commit_batch_size`

Paramètres complémentaires pour `football_match_peak` :

- `match_count`
- `stadium_capacity`
- `pix_usage_rate`
- `peak_share`
- `peak_window_minutes`
- `total_window_minutes`
- `time_compression_factor`

Exemple :

```sh
curl -X POST http://127.0.0.1:8082/control \
  --data "action=run_scenario" \
  --data "scenario=nominal" \
  --data "total_messages=100" \
  --data "rate_per_second=10" \
  --data "traffic_model=poisson" \
  --data "decision_sla_seconds=10" \
  --data "producer_acks=all" \
  --data "producer_retries=0"
```

Comportement des champs :

- `total_messages` vide : pas de borne de volume imposée ;
- `rate_per_second` vide ou `0` : émission aussi vite que possible ;
- `traffic_model` : forme du trafic émis par le générateur :
  - `linear` pour un débit fixe ;
  - `poisson` pour une émission stochastique autour d'une moyenne ;
  - `bursty` pour une émission avec pics et creux ;
- `decision_sla_seconds` : TTL fonctionnel d'une transaction Pix, avec minimum imposé ;
- `producer_semantics` : sémantique Kafka d'écriture côté génération ;
- `producer_acks` et `producer_retries` : paramètres fins d'écriture Kafka, surtout utiles quand `producer_semantics=custom` ;
- `consumer_semantics` : sémantique Kafka de lecture appliquée à la chaîne `pix-validator -> pix-decision-engine -> pix-outcome-publisher` ;
- `consumer_commit_mode`, `consumer_auto_commit`, `consumer_max_poll_interval_ms`, `consumer_commit_batch_size` : réglages fins de lecture, surtout utiles quand `consumer_semantics=custom`.

Valeurs utiles de `consumer_semantics` :

- `at_most_once`
- `at_least_once`
- `exactly_once_kafka`
- `custom`

### `action=replay_rejected`

Lance `./scripts/replay-rejected.sh`.

Paramètre :

- `replay_limit`

### `action=replay_corrected`

Lance `./scripts/replay-corrected.sh`.

Paramètre :

- `corrected_limit`

### `action=apply_network_profile`

Lance `./scripts/network-perturb.sh`.

Paramètres :

- `network_profile`
- `network_target_service`
- `network_delay_ms`
- `network_jitter_ms`
- `network_loss_percent`
- `network_rate_kbit`

Profils supportés :

- `kafka_latency`
- `kafka_loss`
- `kafka_slow_link`

### `action=reset_network_profile`

Lance `./scripts/network-reset.sh`.

## Lecture de l'état de pilotage

Les champs suivants sont exposés dans `control` :

- `status` : `idle`, `running`, `busy`, `ok` ou `error`
- `last_action`
- `last_started_at`
- `last_finished_at`
- `last_result`
- `busy_message`
- `last_output`

## Exemple de boucle de supervision

```sh
curl -fsS http://127.0.0.1:8082/health/details | python3 -m json.tool
```

Pour des vérifications complètes sur la stack Docker, voir aussi [verify-e2e.sh](../../scripts/verify-e2e.sh).

Pour la limite actuelle sur `SUB exactly once`, voir aussi [kafka-sub-exactly-once-limit.md](kafka-sub-exactly-once-limit.md).
Pour une architecture cible `Inbox/Outbox`, voir aussi [kafka-inbox-outbox-design.md](kafka-inbox-outbox-design.md).
Pour la lecture d'observabilité de `exactly_once_kafka`, voir aussi [exactly-once-kafka-observability.md](exactly-once-kafka-observability.md).
