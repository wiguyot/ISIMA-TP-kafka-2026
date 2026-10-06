# TP 04 — ksqlDB : interroger les flux Kafka en SQL

## L'essentiel

### Ce qu'est ksqlDB

ksqlDB est un moteur qui exécute du **SQL en continu sur des topics Kafka**. Une requête ksqlDB ne s'exécute pas une fois pour rendre un résultat : elle tourne en permanence et traite chaque nouveau message dès qu'il arrive.

- Il est construit sur **Kafka Streams**, la bibliothèque Java de traitement de flux de Kafka.
- Il **lit** des topics et **écrit** ses résultats dans de nouveaux topics.
- Son état (compteurs, agrégats) est lui-même **stocké dans Kafka** : en cas de panne, il se reconstruit à partir des topics.

Sans ksqlDB, chacun des traitements ci-dessous demanderait d'écrire un service comme `pix-validator` : un consommateur, un producteur, la gestion des offsets et de l'état. Avec ksqlDB, c'est une requête SQL.

### Deux objets : STREAM et TABLE

| Objet | Ce qu'il représente | Exemple Pix |
|---|---|---|
| `STREAM` | Une suite d'événements, chacun indépendant. Rien n'est remplacé. | Chaque Pix validé. |
| `TABLE` | L'état **courant** par clé. Une nouvelle valeur remplace l'ancienne. | Le nombre de Pix et le montant total par émetteur. |

Un topic peut être lu comme l'un ou l'autre : c'est la même donnée, vue comme un historique ou comme un état.

### Trois sortes de requêtes

| Requête | Forme | Comportement |
|---|---|---|
| **Persistante** | `CREATE STREAM ... AS SELECT` / `CREATE TABLE ... AS SELECT` | Tourne en permanence sur le serveur et alimente un topic de sortie. |
| **Push** | `SELECT ... EMIT CHANGES;` | Affiche en continu chaque nouveau résultat, jusqu'à ce qu'on l'interrompe. |
| **Pull** | `SELECT ... FROM table WHERE cle = ...;` | Lit l'état courant d'une table et rend la main, comme une requête SQL classique. |

### Cas d'usage, sur le flux Pix

Le fichier [`infra/ksqldb/tp14-pix.sql`](../infra/ksqldb/tp14-pix.sql) contient une requête pour chaque cas.

| Cas d'usage | Requête (résumé) | Résultat |
|---|---|---|
| **Filtrer et router** | `SELECT ... FROM pix_valides WHERE amount >= 150` | Topic `simulpix.ksql.pix_montant_eleve` : les Pix de montant élevé, pour un contrôle renforcé. |
| **Agréger en temps réel** | `COUNT(*)`, `SUM(amount)` ... `GROUP BY emitter_tax_id` | Table `pix_par_emetteur` : activité de chaque émetteur, mise à jour à chaque Pix. |
| **Détecter une anomalie sur une fenêtre de temps** | `WINDOW TUMBLING (SIZE 1 MINUTE)` ... `HAVING COUNT(*) > 50` | Table `pix_rafales_emetteur` : émetteurs anormalement actifs sur une minute (suspicion de fraude). |
| **Enrichir par jointure** | `pix_valides JOIN clients ON emitter_tax_id = tax_id` | Topic `simulpix.ksql.pix_valides_enrichis` : chaque Pix complété du nom du client. |
| **Servir un état à la demande** | `SELECT * FROM pix_par_emetteur WHERE emitter_tax_id = 'TAX-0003';` | Réponse immédiate, sans relire tout l'historique. |

Autres usages courants : nettoyer ou reformater des événements, convertir un format (JSON vers Avro), alimenter un tableau de bord.

### Garanties de traitement : le lien avec les TP 01 à 03

ksqlDB est un service lire-traiter-écrire, comme `pix-decision-engine`. Il pose donc la même question qu'au TP 03, réglée par un seul paramètre, `processing.guarantee` :

| Réglage | Après un crash | Conséquence sur un agrégat |
|---|---|---|
| `at_least_once` (défaut) | Les messages lus depuis le dernier commit sont **retraités**. | Un compteur peut être **surcompté**. |
| `exactly_once_v2` | Les sorties, l'état et l'offset sont validés dans **une même transaction Kafka**. | Le compteur reste **exact**. |

Résultat réel : 1200 Pix, ksqlDB tué brutalement en plein flux pendant 10 secondes, puis redémarré. On compare le total de ses `COUNT(*)` au nombre de Pix réellement en base :

```
                               at_least_once   exactly_once_v2
Pix validés en base          :      1200             1200
Pix comptés par ksqlDB       :      1317             1200
Écart                        :      +117                0
```

En `at_least_once`, les messages traités depuis le dernier commit ont été **recomptés** au redémarrage : l'agrégat est faux, et rien ne le signale. En `exactly_once_v2`, le compteur est exact.

Deux limites, déjà vues aux TP 02 et 03 :

- `exactly_once_v2` ne **déduplique pas** un doublon déjà présent dans le topic d'entrée : si le producteur a écrit deux fois un Pix (TP 02), ksqlDB le compte deux fois ;
- la garantie s'arrête à Kafka : si un connecteur recopie ensuite le résultat dans une base, cette écriture est hors transaction.

### Limites et positionnement

- ksqlDB ne sait pas écrire de façon transactionnelle dans PostgreSQL : il écrit dans des topics.
- Confluent, son éditeur, le maintient mais ne le fait plus évoluer. Pour un nouveau projet, les alternatives sont **Kafka Streams** (le même moteur, en Java) et **Flink SQL** (SQL sur flux, sur un moteur distribué indépendant de Kafka).
- Le SQL ne remplace pas un service quand la logique métier est riche (appels externes, règles complexes) : c'est pourquoi la décision Pix reste dans `pix-decision-engine`.

### Ce qu'il faut retenir

1. ksqlDB = SQL **continu** sur des topics Kafka, construit sur Kafka Streams.
2. `STREAM` = historique d'événements ; `TABLE` = état courant par clé.
3. Il remplace un petit service de traitement par une requête : filtrer, agréger, fenêtrer, joindre, servir un état.
4. Ses garanties sont celles des TP 02 et 03 : `at_least_once` peut surcompter après un crash, `exactly_once_v2` reste exact dans Kafka, et aucune des deux ne corrige un doublon d'entrée.

### Questions

- Quelle différence faites-vous entre le `STREAM` `pix_valides` et la `TABLE` `pix_par_emetteur` ?
- Pourquoi une requête `GROUP BY` produit-elle une `TABLE` et non un `STREAM` ?
- Le producteur du TP 02 a écrit 300 doublons dans `raw`. Que vaut le `COUNT(*)` de `pix_par_emetteur` en `exactly_once_v2` ? Pourquoi ?
- Quel cas d'usage de ce TP confieriez-vous à ksqlDB, et lequel laisseriez-vous dans un service écrit en Python ?

---

## Reproduire l'expérience

Réalisez l'expérience : elle fait passer les notions de la première partie de la lecture à l'observation mesurée.

<details>
<summary>Afficher les consignes de reproduction</summary>

### Prérequis

- Plateforme démarrée (`./start.sh`).
- Environ **1,5 Go de RAM** en plus, et un premier téléchargement d'environ 1 Go (image `cp-ksqldb-server`).
- ksqlDB est dans un **profil Compose séparé** : il ne démarre pas avec `./start.sh`.

### 1. Produire un flux de Pix

```sh
./scripts/run-scenario.sh nominal 600 10
```

### 2. Ouvrir ksqlDB et créer les requêtes

```sh
./scripts/ksql.sh
```

Le script démarre `ksqldb-server` si besoin, puis ouvre la CLI. Dans la CLI :

```sql
RUN SCRIPT '/sql/tp14-pix.sql';
SHOW STREAMS;
SHOW TABLES;
SHOW QUERIES;
```

`SHOW QUERIES;` doit lister quatre requêtes persistantes à l'état `RUNNING` : `PIX_MONTANT_ELEVE`, `PIX_PAR_EMETTEUR`, `PIX_RAFALES_EMETTEUR` et `PIX_VALIDES_ENRICHIS`.

Le wrapper prépare également le topic source `simulpix.ksql.clients` (6 partitions, réplication 3), requis par le référentiel de la jointure.

### 3. Observer

```sql
-- Requête push : chaque Pix de montant élevé, en continu (Ctrl+C pour arrêter)
SELECT * FROM pix_montant_eleve EMIT CHANGES;

-- Requête push sur une table : l'agrégat évolue à chaque Pix
SELECT * FROM pix_par_emetteur EMIT CHANGES;

-- Requête pull : l'état courant d'un émetteur
-- (après 600 Pix : TAX-0003 | 100 | 10000.0)
SELECT * FROM pix_par_emetteur WHERE emitter_tax_id = 'TAX-0003';

-- Jointure
SELECT * FROM pix_valides_enrichis EMIT CHANGES LIMIT 5;
```

Les topics créés par ksqlDB sont visibles dans Kafka comme les autres :

```sh
./scripts/tp-kafka.sh
```

### 4. Recommencer

`./scripts/run-scenario.sh` et `./scripts/reset-scenario.sh` suppriment et recréent les topics `simulpix.transactions.*` que lisent les requêtes. Avant de relancer un flux, supprimez les requêtes et leurs topics, puis recréez-les :

```sh
./scripts/ksql.sh --file /sql/tp14-reset.sql
./scripts/run-scenario.sh nominal 600 10
./scripts/ksql.sh --file /sql/tp14-pix.sql
```

### 5. Démonstration de la garantie de traitement

```sh
./scripts/ksql-semantique.sh at_least_once
./scripts/ksql-semantique.sh exactly_once_v2
```

Le script recrée `ksqldb-server` avec la garantie demandée, lance 1200 Pix à 20 Pix/s, crée la table `pix_par_emetteur` (fichier `infra/ksqldb/tp14-semantique.sql`), tue ksqlDB au bout de 25 secondes, le redémarre 10 secondes plus tard, attend la fin du flux, puis compare le total calculé par ksqlDB au nombre de Pix en base.

Pour rendre le surcomptage visible, le script espace les commits de ksqlDB (`commit.interval.ms` = 10 s) : plus l'intervalle est long, plus il y a de messages à retraiter après un crash. L'écart `at_least_once` dépend du moment du crash et peut être nul si aucun travail non commité n'est rejoué. En `exactly_once_v2`, l'écart doit être nul; le script signale une erreur si les comptes ne convergent pas ou si l'écart final est non nul.

### 6. Arrêter ksqlDB

```sh
./scripts/ksql.sh stop
```

## Critères de réussite

- Vous savez expliquer ce qu'est ksqlDB et ce qui le distingue d'une base de données SQL classique.
- Vous savez distinguer `STREAM` et `TABLE`, et requête persistante, push et pull.
- Vous savez citer au moins trois cas d'usage et les illustrer sur le flux Pix.
- Vous savez relier `processing.guarantee` aux sémantiques des TP 02 et 03.


</details>
