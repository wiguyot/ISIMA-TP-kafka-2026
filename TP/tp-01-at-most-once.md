# TP 01 — Sémantique at-most-once (producteur et consommateur)

## L'essentiel

**at-most-once** promet qu'**aucun message n'est dupliqué**. Le prix : un message peut être **perdu**, sans que personne ne le sache.

| Côté | Réglage | Comment naît la perte |
|---|---|---|
| PUB | `acks=0`, `retries=0` | Le producteur n'attend aucun accusé de réception : il compte un message comme envoyé dès qu'il est parti sur le réseau. Si le broker ne le reçoit jamais, personne ne réessaie. |
| SUB | commit de l'offset **avant** le traitement | Le consommateur déclare le message lu, puis plante avant de l'avoir écrit en base. Au redémarrage, Kafka reprend après ce message : il est **perdu définitivement**. |

```
PUB :  generator ──(acks=0)──► broker isolé du réseau : le Pix part dans le vide, compté comme émis

SUB :  lire → commit offset → [crash] → écriture en base jamais faite → Pix perdu
```

### Résultat réel côté PUB

Pendant un flux de 3000 Pix à 100 Pix/s, le broker `kafka-3` est **isolé du réseau** pendant 20 secondes : les paquets qui lui sont destinés partent dans le vide, sans que la connexion soit coupée.

```
                                 acks=0 (at-most-once)   acks=all (at-least-once)
Pix ÉMIS par le generator      :        3000                     3000
Pix ARRIVÉS dans Kafka (raw)   :        2892                     3306
PERTE PUB (émis mais absents)  :         108                        0
DOUBLONS PUB                   :           0                      306
```

- En `acks=0`, **108 Pix** ont été comptés comme émis mais ne sont jamais arrivés dans Kafka. Aucune erreur n'a été signalée : c'est une **perte silencieuse**. Le nombre varie d'une exécution à l'autre (de 108 à 492 lors de nos essais), selon ce qui était en vol au moment de la panne.
- La **même panne** en `acks=all` ne perd rien : le producteur attend un accusé, ne le reçoit pas, et réessaie. En contrepartie, il crée **306 doublons** : c'est le sujet du TP 02.

### Résultat réel côté SUB

`persister-valid` est tué brutalement pendant un flux de 600 Pix ; il commite chaque offset **avant** d'écrire en base (avec 80 ms d'écriture pour élargir la fenêtre) :

```
Pix ARRIVÉS dans Kafka (raw)       :  600
Pix PERSISTÉS en base              :  599
PERTE SUB                          :    1
```

Un seul Pix est perdu : celui qui était entre le commit et l'écriture au moment du crash. La perte côté SUB n'est pas massive, mais elle est **définitive** et **invisible pour Kafka**, qui considère ce message comme traité.

### Ne pas confondre perte et non-émission

Si l'on tue le **generator** en plein flux, les Pix restants ne sont simplement jamais produits (1476 sur 3000 lors de nos essais). Ce n'est pas une perte at-most-once : cela arriverait avec n'importe quelle sémantique, même `acks=all`. Une perte at-most-once, c'est un message que le producteur **croit envoyé** et qui n'existe pas dans Kafka.

### Ce qu'il faut retenir

1. at-most-once : **pas de doublon, perte possible**, à l'écriture comme à la lecture.
2. La perte est **silencieuse** : ni le producteur ni Kafka ne la signalent.
3. Côté PUB, la perte apparaît quand le réseau ou un broker défaille ; côté SUB, quand le consommateur plante entre le commit et le traitement.
4. C'est acceptable pour des données dont on peut perdre une partie (métriques, journaux), **inacceptable pour des paiements**.

### Questions

- Pourquoi `acks=0` est-il acceptable pour des métriques de supervision, mais pas pour des paiements ?
- Pourquoi la perte côté SUB ne concerne-t-elle qu'un message par crash ?
- Comment détecter une perte at-most-once en production si le producteur ne signale rien ?

---

## Reproduire l'expérience

Réalisez l'expérience : elle fait passer les notions de la première partie de la lecture à l'observation mesurée.

<details>
<summary>Afficher les consignes de reproduction</summary>

## Prérequis

- Plateforme démarrée (`./start.sh`).
- Activités 01 à 10 réalisées (lecture des dashboards et de la météo).

## Outils utilisés

- `scripts/measure-tp.sh` : orchestre une mesure (sémantiques, flux, injection de la panne, attente du drainage, bilan).
- `scripts/inject-fault.sh` : primitives de panne (isolement réseau d'un broker, arrêt brutal d'un service).
- Dashboards `General Dashboard` et `Kafka Dashboard`, météo `http://localhost:8082/`.

Forme générale :

```sh
./scripts/measure-tp.sh <action> [TOTAL] [RATE] [INJECTION_DELAY] [DOWNTIME] [OBSERVATION] [PERSIST_DELAY]
```

`TOTAL` et `RATE` fixent le flux ; `INJECTION_DELAY` est la durée de régime stable avant la panne, `DOWNTIME` la durée de la panne, `OBSERVATION` la durée d'observation après la panne ; `PERSIST_DELAY` ralentit l'écriture en base (en ms).

## Que signifie le drainage ?

Après une panne, Kafka peut encore contenir des Pix arrivés dans les topics mais pas encore traités jusqu'au bout : c'est le **backlog**. Le **drainage** est la phase où l'on attend que les consommateurs le rattrapent avant de tirer le bilan : `measure-tp.sh` attend que les Pix arrivés dans Kafka soient couverts par les Pix persistés en base (`validated` + `rejected`), à quelques messages près.

Sans drainage, on confondrait une vraie perte avec un simple retard. Si le script annonce `drainage non terminé`, l'écart restant peut encore être du retard.

## Étapes

### 1. Perte côté PUB

```sh
./scripts/measure-tp.sh 11_perte_pub 3000 100 10 20 20
```

Le script configure le generator en `at_most_once` (`acks=0`, `retries=0`), lance 3000 Pix à 100 Pix/s, attend 10 secondes, **isole `kafka-3` du réseau** pendant 20 secondes, puis attend la fin de l'émission et le drainage. Lisez la ligne `PERTE PUB (émis mais absents, AMO PUB)`.

Pendant la mesure, observez :

- **Kafka Dashboard** : les partitions sous-répliquées montent pendant l'isolement de `kafka-3`.
- **Météo `8082`** : des rejets `processing_timeout` apparaissent. Les Pix des partitions de `kafka-3` sont bloqués pendant 20 secondes et dépassent leur délai de décision. Ce ne sont pas des pertes.

Rejouez la même panne en `at_least_once` pour comparer :

```sh
SIMULPIX_TP_PUB_SEMANTICS=at_least_once ./scripts/measure-tp.sh 11_perte_pub 3000 100 10 20 20
```

La perte disparaît et des doublons apparaissent. Le drainage peut alors se terminer sur `drainage non terminé` : les doublons gonflent le topic `raw` alors que la base, protégée par sa clé unique, n'en garde qu'un exemplaire.

### 2. Perte côté SUB

```sh
./scripts/measure-tp.sh 11_perte_sub 600 10 20 10 20 80
```

Le script garde le generator en `at_least_once` (pour ne rien perdre côté PUB), configure les consommateurs en `at_most_once` (commit avant traitement), ralentit l'écriture en base de 80 ms pour élargir la fenêtre, puis tue `persister-valid` pendant 10 secondes. Lisez la ligne `PERTE SUB / backlog résiduel` après drainage.

Pendant la mesure, la courbe `db_validated_count` du **General Dashboard** reste plate pendant l'arrêt, puis reprend ; le lag du groupe `persister-valid` monte puis se résorbe.

### 3. Comparaison : arrêter le generator

```sh
./scripts/measure-tp.sh 11_arret_generator 3000 100 10 20 20
```

Le generator est tué en plein flux. La ligne `Pix attendus mais NON ÉMIS` est grande, mais `PERTE PUB` reste à 0 : ces Pix n'ont jamais été produits.

## Lecture des indicateurs de configuration

Sur `http://localhost:8082/` :

- carte **PUB Kafka** → `at_most_once` ;
- carte **réglage PUB** → `acks=0 retries=0` ;
- carte **SUB Kafka** → `at_most_once` (mesure SUB).

Dans Grafana `Simul-Pix - Kafka Dashboard` :

- panneau **Commit SUB actif** → `Avant traitement` ;
- panneau **Risque SUB dominant** → `Perte possible`.

## Si l'écart mesuré est nul

- Côté PUB : augmentez le débit (`RATE`) ou la durée d'isolement (`DOWNTIME`) : plus il y a de Pix en vol vers le broker isolé, plus la perte est visible.
- Côté SUB : augmentez `PERSIST_DELAY` (par exemple 300) pour élargir la fenêtre entre le commit et l'écriture ; baissez alors `RATE` pour que le persister puisse suivre (capacité ≈ 1000 / `PERSIST_DELAY` Pix/s).

## Critères de réussite

- Vous savez expliquer d'où vient la perte côté PUB et côté SUB.
- Vous savez distinguer une perte at-most-once d'une non-émission et d'un retard (backlog).
- Vous savez expliquer pourquoi la même panne produit une perte en `acks=0` et un doublon en `acks=all`.


</details>
