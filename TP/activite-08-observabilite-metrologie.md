# Activité 08 — Métrologie et météo des services

## Objectif

Vous allez apprendre à lire l'état d'un système événementiel avec les bons indicateurs : compteurs métier, lag Kafka, état des services, métriques et dashboards.

## Prérequis

- Plateforme démarrée.
- Au moins un scénario exécuté.

## Étapes

### 1. Observer le flux sain

```sh
./scripts/run-scenario.sh nominal 100 10
```

Lancez ensuite `tp-status.sh` (étape 3) et ouvrez les vues locales (étape 4) pendant que le flux tourne. Sur une machine rapide, les consommateurs rattrapent les producteurs quasi instantanément : le lag peut rester à 0 en permanence. C'est normal — c'est la preuve que le pipeline est bien dimensionné pour cette charge. L'état sain est aussi une information.

### 2. Forcer un lag durablement visible

Le scénario `consumer_lag` injecte des délais logiciels dans les consommateurs : ≈350 ms par message dans `pix-decision-engine`, ≈150 ms dans `persister-valid`. Avec 1 worker, `pix-decision-engine` traite au maximum ~3 msg/s. Si on produit à 50 msg/s, le lag grossit à ~47 msg/s — **quelle que soit la puissance de la machine**, car le délai est dans le code, pas dans le CPU.

```sh
./scripts/run-scenario.sh consumer_lag 200 50
```

200 messages à 50 msg/s : la production dure ~4 secondes. Dès les premières secondes, le lag dépasse la centaine de messages et met plusieurs dizaines de secondes à se résorber après la fin de la production. C'est une fenêtre confortable pour lire tous les indicateurs.

Attendez **5 à 10 secondes** après le retour de la commande (le générateur démarre juste après), puis lancez `tp-kafka.sh` (section 3) : le groupe `simulpix-decision-engine-...` affiche un lag de l'ordre de la centaine de messages. Relancez-le toutes les 15 secondes pour le voir diminuer. `tp-status.sh` lit des compteurs que `service-health` rafraîchit périodiquement : il peut afficher quelques secondes de retard sur `tp-kafka.sh`, qui interroge Kafka directement.

### 3. Lire la météo résumée

```sh
./scripts/tp-status.sh
```

Cette commande produit une lecture guidée en 6 sections : état global, compteurs métier (Pix générés / traités / acceptés / rejetés), état des services, lag Kafka par groupe, flux courant et liens de navigation. Pour obtenir le JSON brut complet : `SIMULPIX_TP_STATUS_VERBOSE=1 ./scripts/tp-status.sh`.

### 4. Ouvrir les vues locales

- météo générale : `http://localhost:8082/`
- pilotage TP : `http://localhost:8083/`
- Grafana : `http://localhost:3000`

Identifiants Grafana :

- utilisateur : `admin`
- mot de passe : `adminpass`

### 5. Consulter les dashboards

Dashboards utiles :

- `Simul-Pix - General Dashboard`
- `Simul-Pix - Kafka Dashboard`
- `Simul-Pix - Persistence Dashboard`
- `Simul-Pix Incidents`

## Ce que vous devez observer

**Où est l'indicateur de retard d'un consommateur ?**

Il existe à quatre endroits, chacun avec un niveau de détail différent :

| Outil | Où regarder | Ce qu'on voit |
|-------|-------------|---------------|
| `tp-status.sh` | Section 4 — "Lag Kafka par groupe" | Lag agrégé par consumer group ; libellé `retard à traiter` si > 0 |
| `tp-kafka.sh` | Section 3 — colonne `lag` | Lag par topic et par groupe, avec libellé `retard à traiter` ou `à jour` |
| Dashboard `http://localhost:8082/` | Carte **lag total** | Somme de tous les lags ; 0 = tout le monde est à jour |
| Grafana `Simul-Pix - Kafka Dashboard` | Panel "Consumer Lag" | Courbe du lag dans le temps, par groupe |

Sur le flux nominal (étape 1), le lag peut rester à 0 en permanence : les consommateurs absorbent les messages aussi vite qu'ils arrivent. C'est un état sain.

Sur le flux `consumer_lag` (étape 2), le lag est visible pendant environ une minute (200 Pix × 350 ms de traitement). Si `tp-status.sh` affiche encore `Pix générés : 0`, attendez quelques secondes : ses compteurs ne sont pas encore rafraîchis.

**Pourquoi 90 % de rejets ?** À la fin du scénario, `tp-status.sh` et `tp-db.sh` montrent environ 20 Pix acceptés et 180 rejetés, tous pour `processing_timeout`. Ce n'est pas une panne : chaque Pix doit être décidé dans les **10 secondes** qui suivent son émission (délai de décision, voir [`decision-sla.md`](../docs/architecture/decision-sla.md)). Avec 350 ms par Pix, seuls les 25 à 30 premiers sont décidés à temps ; les autres attendent trop longtemps dans le topic et sont rejetés par `pix-decision-engine`. Le lag n'est donc pas qu'un chiffre technique : ici, il a un **effet métier** direct. L'activité 09 revient sur ce lien.

> Le délai est injecté par logiciel, pas par le matériel : la puissance de la machine n'aide pas à éviter ce lag. C'est voulu — cela garantit le même comportement observable sur tous les postes.

**Autres indicateurs à lire pendant le flux :**
- Les compteurs métier (généré, traité, accepté, rejeté) montent et convergent : la section 2 de `tp-status.sh` et les cartes du dashboard `8082` sont les deux lectures en parallèle.
- `tp-status.sh` et `tp-kafka.sh` sont complémentaires : l'un résume le flux métier, l'autre détaille la topologie Kafka (partitions, replication, offsets bruts).
- Le dashboard `Simul-Pix - Persistence Dashboard` montre les insertions PostgreSQL en temps réel.

## Questions

- Quel indicateur montre qu'un consommateur est en retard ?
- Pourquoi faut-il distinguer offset Kafka et compteur métier ?
- Quel dashboard utiliser pour observer la persistance PostgreSQL ?

## Critères de réussite

- Vous savez ouvrir la météo et Grafana.
- Vous savez citer au moins trois indicateurs d'observabilité.
