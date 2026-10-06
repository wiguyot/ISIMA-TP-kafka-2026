# Guide développeur — modifier et étendre simul-pix

Ce guide s'adresse aux groupes de SAÉ qui modifient le code de la plateforme. Il suppose les activités 01 à 10 réalisées.

## 1. Vue d'ensemble

| Élément | Emplacement | Rôle |
|---|---|---|
| Services Python | `services/<service>/` | Un dossier = une image Docker = un conteneur. |
| Orchestration | `docker-compose.yml` | Définit tous les conteneurs, leurs variables d'environnement et leurs dépendances. Nom de projet Compose : `simulpix`. |
| Topics Kafka | `infra/kafka/scripts/create-topics.sh` | Création des topics au démarrage. |
| Schéma PostgreSQL | `infra/postgres/init/*.sql` | Exécuté **une seule fois**, à la création du volume de la base. |
| Référentiel clients | `infra/reference/reference-clients.json` | Les 6 clients Pix (numéro fiscal, identifiant Pix, comptes). |
| Exemples de messages | `docs/contrats-evenements/*.json` | Forme des messages de chaque topic (documentation, non vérifiée à l'exécution). |
| Scripts | `scripts/` | Scénarios, réinitialisation, mesures, pannes. Lançables depuis n'importe quel dossier. |
| Tests | `tests/` | Tests unitaires sans Docker. |

Chaîne des services : voir [container-interactions.md](../architecture/container-interactions.md).

## 2. Anatomie d'un service

Exemple : `services/pix-validator/`.

| Fichier | Contenu |
|---|---|
| `main.py` | Point d'entrée : appelle `runtime.run()`. |
| `runtime.py` | Boucle principale : lire un message, le traiter, publier, commiter l'offset. Gère les workers (un thread = un membre du consumer group). |
| `kafka_io.py` | Configuration Kafka : sémantiques PUB et SUB (`acks`, idempotence, moment du commit, transactions), consumer group. |
| `state.py` | Dictionnaire `STATE` : compteurs et état exposés par la santé du service. |
| `health.py` | Petit serveur HTTP `/health` qui expose `STATE` (lu par `service-health`). |
| `validation.py` | Logique métier propre au service (ici, les contrôles d'un Pix). |
| `Dockerfile`, `requirements.txt` | Construction de l'image (Python 3.12, `confluent-kafka`). |

**Attention : la logique Kafka est dupliquée dans chaque service.** `get_consumer_settings()`, `build_kafka_client_config()` et les fonctions voisines existent en plusieurs exemplaires (validator, decision-engine, outcome-publisher, persisters), avec de petites variantes. Une modification de sémantique doit être reportée dans chaque service concerné.

## 3. Cycle de modification

### Modifier un service et le reconstruire

```sh
# après avoir modifié services/pix-validator/...
docker compose up -d --build --no-deps pix-validator
docker compose logs -f pix-validator
```

`--no-deps` évite de redémarrer les autres services. Les scénarios lancés ensuite (`./scripts/run-scenario.sh`) utilisent la nouvelle image.

### Lire les journaux

Chaque service écrit une ligne JSON par événement :

```sh
docker compose logs --tail 50 pix-decision-engine
docker compose logs -f generator | grep -v message_delivered
```

### Remettre la plateforme dans un état propre

| Besoin | Commande |
|---|---|
| Vider les topics et les tables, garder les conteneurs | `./scripts/reset-scenario.sh` |
| Lancer un scénario (fait déjà un reset) | `./scripts/run-scenario.sh nominal 100 10` |
| Tout supprimer (conteneurs, volumes, état), puis redémarrer | `./stop.sh` puis `./start.sh` |

`stop.sh` supprime les conteneurs et volumes du projet simul-pix ; les données persistées sont perdues.

## 4. Ajouter un topic

La liste des topics existe à **deux** endroits :

1. `infra/kafka/scripts/create-topics.sh` : création au démarrage (`create_topic <nom> <partitions>`) ;
2. `scripts/reset-scenario.sh`, fonction `reset_kafka_topics` : suppression puis recréation à chaque reset (`delete_topic` et `create_topic`).

Ajoutez le topic aux deux, sinon il disparaîtra au premier reset. Pensez aussi à la variable d'environnement du topic dans `docker-compose.yml` (modèle : `SIMULPIX_TOPIC_VALIDATED`) et, si vous voulez le voir dans `tp-kafka.sh`, à la liste de ce script.

## 5. Modifier le schéma PostgreSQL

Les fichiers `infra/postgres/init/*.sql` ne s'exécutent qu'**à la création du volume** de la base. Pour une nouvelle table ou une nouvelle colonne :

1. ajoutez un fichier numéroté (`005-....sql`) dans `infra/postgres/init/` ;
2. pour l'appliquer sur une plateforme existante : soit `./stop.sh` puis `./start.sh` (la base est recréée), soit l'exécuter à la main :

```sh
docker compose exec -T postgres psql -U simulpix -d simulpix < infra/postgres/init/005-....sql
```

Les persisters ajoutent aussi certaines colonnes au démarrage (`ensure_validated_schema` dans `services/persister-valid/main.py`). Si vous ajoutez une table, pensez à la vider dans `truncate_postgres_tables` (`scripts/reset-scenario.sh`).

## 6. Ajouter une variable de configuration

1. Lisez-la dans le service avec `os.getenv("SIMULPIX_...", "valeur par défaut")`.
2. Déclarez-la dans `docker-compose.yml`, avec une valeur par défaut : `SIMULPIX_MA_VARIABLE: ${SIMULPIX_MA_VARIABLE:-valeur}`.
3. Vous pouvez alors la fixer au lancement : `SIMULPIX_MA_VARIABLE=... ./scripts/run-scenario.sh nominal 100 10`.

## 7. Tester

Tests unitaires, sans Docker (les bibliothèques Kafka et PostgreSQL sont simulées) :

```sh
python3 -m unittest discover -s tests
```

Pour tester une fonction d'un service, inspirez-vous de `load_module()` dans `tests/test_pipeline_contracts.py`, qui charge un module d'un service par son chemin.

Vérification de bout en bout, stack lancée (environ 2 minutes) :

```sh
./scripts/verify-e2e.sh
./scripts/validate-observability.sh
```

## 8. Mesurer

Pour prouver un comportement (exigence des SAÉ), appuyez-vous sur :

- `./scripts/tp-status.sh`, `./scripts/tp-kafka.sh`, `./scripts/tp-db.sh` : lectures ponctuelles ;
- `http://localhost:8082/health/details` : tous les compteurs en JSON ;
- `./scripts/measure-tp.sh` et `./scripts/inject-fault.sh` : mesure avec panne injectée (voir TP 01 à 03) ;
- Grafana (`http://localhost:3000`) : évolution dans le temps.

Ne concluez jamais à partir d'un offset Kafka seul : vérifiez le résultat métier et la donnée en base, après drainage.

## 9. Limites connues

Ces limites sont connues et documentées ; plusieurs recoupent des sujets de SAÉ.

| Limite | Où | Sujet de SAÉ lié |
|---|---|---|
| Un message illisible (JSON invalide) fait échouer le traitement en boucle : le consommateur se reconnecte, relit le même message, échoue à nouveau. Aucune file de rejet technique n'existe. | boucles `runtime.py` / `main.py` des consommateurs | 2 (DLQ et rejeu) |
| Les persisters recomptent toute la table (`select count(*)`) après **chaque** message. | `services/persister-*/main.py` | 1 (idempotence), 6 (capacité) |
| `pix-decision-engine` relit des topics entiers pour recalculer ses compteurs au démarrage et après chaque erreur. | `count_visible_messages()` | 6 (capacité) |
| `service-health` ouvre une connexion PostgreSQL et lance plusieurs `count(*)` à chaque appel de `/health`. | `services/service-health/probes.py` | 5 (observabilité) |
| La logique Kafka est dupliquée dans chaque service. | `kafka_io.py` des services | tous |
| Les contrats de messages ne sont pas vérifiés à l'exécution (pas de registre de schémas). | `docs/contrats-evenements/` | 3 (contrats d'événements) |
