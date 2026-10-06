# TP 02 — Sémantique at-least-once (producteur et consommateur)

## L'essentiel

**at-least-once** promet qu'**aucun message n'est perdu**. Le prix : un message peut être **écrit ou traité deux fois**. C'est la configuration par défaut de simul-pix.

| Côté | Réglage | Comment naît le doublon |
|---|---|---|
| PUB | `acks=all`, `retries > 0`, sans idempotence | Le broker a écrit le message, mais l'accusé de réception se perd. Le producteur réessaie : le message est écrit **deux fois** dans le topic. |
| SUB | commit de l'offset **après** le traitement | Le consommateur écrit en base, puis plante avant de commiter. Au redémarrage, Kafka lui redonne le message : il est **traité deux fois**. |

### Résultat réel côté PUB

5 % des Pix sont publiés deux fois, comme si leur accusé de réception s'était perdu (injection contrôlée, dans la section de reproduction) :

```
Pix ÉMIS par le generator               :  6000
Pix ARRIVÉS dans Kafka (raw)            :  6300   (+300)
Outcome count observé                   :  6300
Pix persistés TOTAL (valid + rejected)  :  6000
DOUBLONS PUB                            :   300
PERTE PUB                               :     0
```

- **Aucune perte** : les 6000 Pix sont tous arrivés.
- **300 doublons dans Kafka**. La base n'en contient aucun : la clé primaire sur `transaction_id` refuse la seconde ligne.
- Mais **6300 réponses finales** sont parties dans `outcome` : 300 clients auraient reçu leur confirmation deux fois. La clé unique protège la base, pas les autres effets.

### Et avec une vraie panne réseau ?

Au TP 01, le broker `kafka-3` a été isolé du réseau pendant 20 secondes, producteur en `acks=all`. Sans aucune injection, les nouveaux essais du client Kafka ont créé **306 doublons** dans `raw` (453 lors d'un autre essai). Et **17 Pix ont reçu deux décisions contradictoires** : leur première copie a été acceptée à temps, leur seconde copie, réécrite après la panne, est arrivée hors délai et a été rejetée pour `processing_timeout`. Un même paiement se retrouve à la fois dans `validated_transactions` et dans `rejected_transactions`.

### Résultat réel côté SUB

`persister-valid` s'arrête juste après avoir écrit un Pix en base et avant d'avoir commité l'offset. Un audit compte chaque tentative d'écriture :

```
 lignes_en_base | tentatives_totales | doublons_absorbes
            120 |                122 |                 2
```

Kafka a bien rejoué les messages non commités (aucune perte), et deux Pix ont été écrits deux fois. La base reste juste parce que l'écriture est **idempotente** (`INSERT ... ON CONFLICT`).

### Ce qu'il faut retenir

1. at-least-once : **pas de perte, doublons possibles**, à la fois à l'écriture et à la lecture.
2. La protection contre le doublon ne vient **pas de Kafka** : elle vient de l'application (clé unique, écriture idempotente).
3. Un doublon « absorbé » n'est pas un doublon « évité » : le traitement a eu lieu deux fois. Tout effet de bord non idempotent (notification, e-mail, virement) serait doublé.

### Questions

- Pourquoi `acks=all` seul ne suffit-il pas à éviter les doublons ?
- La base contient 6000 Pix, `outcome` en contient 6300 : que faudrait-il faire pour que le client ne reçoive qu'une réponse ?
- Pourquoi dit-on « doublon absorbé » et non « doublon évité » ?
- Comment un même Pix peut-il être à la fois accepté et rejeté ? Comment l'empêcher ?

---

## Reproduire l'expérience

Réalisez l'expérience : elle fait passer les notions de la première partie de la lecture à l'observation mesurée.

<details>
<summary>Afficher les consignes de reproduction</summary>

## Définition rapide

**at-least-once côté PUB** — `acks=all`, `retries > 0`, **sans** idempotence producteur. Le producteur attend la confirmation de toutes les répliques ISR. Si l'ack ne revient pas, il **réessaie**. Si le premier envoi avait effectivement abouti, le retry crée un **doublon dans le topic**. **Garantie : aucune perte. Risque : doublon.**

**at-least-once côté SUB** — commit offset **après** traitement. Si le service crashe entre le traitement (par ex. INSERT en base) et le commit, Kafka rejouera le message au redémarrage. La même opération est tentée deux fois. **Garantie : aucune perte. Risque : doublon de traitement.**

```
PUB :  generator ── envoie ──► broker
              ack perdu (timeout)
       generator ── retry ──► broker reçoit 2 fois le même message

SUB :  consumer lit → INSERT en base → [crash] → pas de commit
              redémarrage → Kafka rejoue → INSERT tenté à nouveau
```

C'est la **configuration par défaut de simul-pix** des deux côtés.

## Pourquoi un seul TP pour les deux côtés ?

Vous allez observer le doublon côté production et côté consommation dans la même session, parce qu'une chaîne Kafka comporte toujours un producteur et un consommateur. Les deux doublons ne naissent pas au même endroit et ne se corrigent pas de la même façon.

## Objectif

Vous allez provoquer empiriquement deux types de doublon, mesurer **où** ils apparaissent, puis identifier **ce qui les absorbe** quand ils sont absorbés.

## Référentiel de volume et adaptation à la machine

Mêmes paramètres par défaut que le TP 01 :

- **6000 Pix** à **50 Pix/s** → ~2 min de production.
- **90 s de régime stable** + **60 s de panne** + **60 s d'observation** + drainage = environ **4 min par mesure**.

Tous les paramètres passent en argument CLI :

```sh
./scripts/measure-tp.sh <action> [TOTAL] [RATE] [INJECTION_DELAY] [DOWNTIME] [OBSERVATION] [PERSIST_DELAY]
```

Pour une démonstration PUB reproductible en salle, utilisez l'injection contrôlée d'ack ambigu. Elle matérialise le cas où le broker a écrit le message mais où le producteur n'a pas reçu l'ack à temps, puis réessaie :

```sh
SIMULPIX_TP_LOSS_PERCENT=0 \
SIMULPIX_TP_NET_DELAY_MS=0 \
SIMULPIX_TP_BROKER_OUTAGE=0 \
SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT=5 \
  ./scripts/measure-tp.sh 12_doublon_pub 6000 100 5 0 15
```

Côté SUB, on cherche un phénomène différent : un traitement déjà fait en base, puis un arrêt avant commit d'offset. Le script déclenche ce cas une seule fois de manière contrôlée pour éviter de dépendre d'un timing de `SIGKILL`.

## Que signifie le drainage ?

Après une perturbation, les messages déjà produits peuvent rester quelques secondes ou minutes en attente dans Kafka. Ce stock de messages non encore traités jusqu'au bout est le **backlog**.

Le **drainage** est la phase où l'on laisse les consommateurs rattraper ce backlog avant de mesurer. `measure-tp.sh` attend que les Pix arrivés dans Kafka soient couverts par les Pix persistés en base (`validated` + `rejected`), à quelques messages près.

C'est indispensable dans ce TP : `at-least-once` garantit surtout qu'un message sera rejoué plutôt que perdu. Juste après une panne, l'écart entre Kafka et PostgreSQL peut donc simplement signifier "pas encore traité". Après drainage, si l'écart disparaît, on parle de retard absorbé. Si un doublon apparaît, il faut regarder où il apparaît : dans Kafka côté PUB, ou dans les tentatives PostgreSQL côté SUB.

Dans le bilan, la ligne `PERTE SUB / backlog résiduel` doit donc se lire prudemment : elle mesure l'écart restant après attente, mais cet écart peut encore être du backlog si le script annonce `drainage non terminé`.

## Comment lire le bilan

Le bilan sépare explicitement les écarts de production, de Kafka et de base. Pour at-least-once, on s'attend à **0 perte** et à des **doublons non nuls côté Kafka** quand le PUB est mis sous stress.

```
==========================================================================
                       BILAN DU TP (12_doublon_pub)
==========================================================================
  Pix ATTENDUS (paramètre TOTAL)              :       6000
  Pix ÉMIS par le generator                   :       6000   (delta vs attendu : +0)
  Pix ARRIVÉS dans Kafka (raw)                :       6300   (delta vs émis    : +300)
  Pix avec RÉSULTAT FINAL logique             :       6300   (validated + rejected)
  Outcome count observé                       :       6300   (réponses finales publiées)
  Topic outcome technique                     :       6300   (offset brut, marqueurs transactions inclus)
  Pix PERSISTÉS en base (validated)           :       6000   (delta vs Kafka raw : -300)
  Pix REJETÉS métier (compteur)              :          0
  Pix rejetés en base (db_rejected)           :          0
  Pix persistés TOTAL (valid + rejected)      :       6000   (delta vs Kafka raw : -300)
  ----------------------------------------------------------------------
  ÉCART entre ÉMIS et ARRIVÉS DANS KAFKA :
    - PERTE PUB (buffer producteur, AMO PUB)     : 0
    - DOUBLONS PUB (retries acceptés, ALO PUB)  : 300   <-- TP 02 PUB vise cette ligne
  ÉCART entre ARRIVÉS DANS KAFKA et BASE PG :
    - PERTE SUB / backlog résiduel              : 0
  ----------------------------------------------------------------------
  Pix attendus mais NON ÉMIS par le generator :          0
==========================================================================
```

C'est l'inverse complémentaire du TP 01 : **at-most-once = pertes / pas de doublon**, **at-least-once = doublons / pas de perte**. Ici, les 300 Pix excédentaires sont bien des doublons PUB : ils sont présents dans Kafka, mais ils ne créent pas 300 lignes métier supplémentaires parce que PostgreSQL (`PG`) protège `transaction_id` par unicité. Le TP 03 démontrera l'absence des deux côté Kafka (au prix d'un coût visible).

**Regardez aussi `Outcome count observé : 6300`.** Les doublons ont traversé toute la chaîne Kafka : 300 Pix ont reçu **deux réponses finales**. La clé unique protège la base, pas les messages envoyés au client. Si `outcome` déclenchait une notification au payeur, il la recevrait deux fois : c'est pourquoi un doublon doit être neutralisé **à chaque effet de bord**, pas seulement en base.

**Note sur `outcome`** : `outcome` est le topic Kafka des réponses finales publiées par `pix-outcome-publisher`. `outcome_count` compte les vrais résultats métier finaux : paiement accepté, paiement rejeté métier, paiement rejeté pour délai dépassé, ou autre rejet technique/fonctionnel. `Topic outcome technique` est l'offset brut Kafka ; en `at_least_once`, il ne contient généralement pas de marqueurs transactionnels, contrairement au TP 03. Le libellé du script est volontairement commun aux TP 01, 12 et 13.

## Prérequis

- TP 01 réalisé.
- Plateforme démarrée.

## Outils utilisés

- `scripts/measure-tp.sh` — orchestration ;
- `scripts/inject-fault.sh` — primitives de perturbation ;
- `scripts/toggle-unique.sh` — installation/retrait du trigger d'audit ;
- `scripts/tp-db.sh` — consultation manuelle de la base.

## Étapes

### 1. Réinitialiser

```sh
./scripts/reset-scenario.sh
```

### 2. Provoquer le doublon côté PUB

```sh
SIMULPIX_TP_LOSS_PERCENT=0 \
SIMULPIX_TP_NET_DELAY_MS=0 \
SIMULPIX_TP_BROKER_OUTAGE=0 \
SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT=5 \
  ./scripts/measure-tp.sh 12_doublon_pub 6000 100 5 0 15
```

**Pourquoi c'est difficile à provoquer**. Sous TCP, les paquets perdus sont retransmis automatiquement. Sous `acks=all`, librdkafka attend l'ack ; tant qu'il finit par arriver, pas de retry, pas de doublon. Pour générer **vraiment** des doublons, il faut une combinaison de stress qui force le **timeout d'une requête entière**, puis le retry. Le script dispose de trois leviers :

1. **Perturbation réseau agressive** : par défaut `kafka_loss 35%` + `latence 1500 ms` + `jitter 500 ms` sur le generator. La latence élevée rapproche le RTT du timeout de requête ; la perte multiplie les retransmissions TCP qui finissent par échouer.
2. **Outage du broker kafka-3** pendant `DOWNTIME` (60 s par défaut). Quand un broker leader chute, librdkafka voit ses requêtes en vol échouer et **retry vers le nouveau leader** — c'est là que naissent les doublons.
3. **Injection contrôlée d'ack ambigu** : pour rendre le TP reproductible sur Docker local, le generator republie 5 % des Pix, comme si l'accusé de réception du premier envoi avait été perdu après l'écriture sur le broker. Ce n'est **pas** un vrai nouvel essai du client Kafka : c'est l'application qui appelle `produce()` une seconde fois avec le même contenu. Le résultat dans le topic est le même qu'un nouvel essai réussi (deux copies du même Pix) et `generated` n'est pas incrémenté, ce qui donne `raw_topic_end_offsets > generated`.

La commande recommandée ci-dessus **désactive les leviers 1 et 2** et n'utilise que le levier 3 : le doublon est alors certain et chiffré à l'avance, mais simulé.

Avec la commande reproductible ci-dessus, le script :

1. configure PUB et SUB en `at_least_once` ;
2. lance un flux de **6000 Pix à 100 Pix/s** ;
3. injecte 5% de retries à ack ambigu côté generator ;
4. vérifie que le generator a bien émis les 6000 Pix logiques ;
5. observe le drainage ;
6. imprime le bilan.

Le bilan attendu est exactement : `generated=6000`, `raw=6300`, `DOUBLONS PUB=300`, `PERTE PUB=0`, `PERTE SUB=0`, puis `Pix persistés TOTAL=6000`. Autrement dit, Kafka contient 300 entrées excédentaires, mais la base métier ne contient que 6000 Pix uniques.

Variable utile pour isoler le phénomène réel sans injection contrôlée :

```sh
SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT=0 ./scripts/measure-tp.sh 12_doublon_pub
```

Dans ce mode, le doublon dépend entièrement du réseau Docker, du timing broker et de librdkafka ; il peut donc rester à zéro sur certaines machines.

**Ce que vous devez regarder dans Grafana** :

- **Kafka Dashboard, `raw_topic_end_offsets`** : monte plus vite que `generated`.
- Seulement avec les leviers 1 et 2 (mode « réseau réel ») : **Kafka Dashboard, `under-replicated partitions`** monte pendant l'arrêt de kafka-3, et le **General Dashboard** montre un pipeline ralenti pendant la perturbation. Avec la commande recommandée, ces deux effets n'apparaissent pas.

**Ce que vous devez lire dans le bilan** : la ligne `DOUBLONS PUB (retries acceptés, ALO PUB)` doit être **non nulle**.

**Adapter pour votre machine** :

```sh
# Cas court et propre recommandé pour le TP
SIMULPIX_TP_LOSS_PERCENT=0 \
SIMULPIX_TP_NET_DELAY_MS=0 \
SIMULPIX_TP_BROKER_OUTAGE=0 \
SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT=5 \
  ./scripts/measure-tp.sh 12_doublon_pub 6000 100 5 0 15

# Cas "réseau réel" : peut rester à zéro selon Docker / machine
SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT=0 \
./scripts/measure-tp.sh 12_doublon_pub

# Pousser plus fort le mode réseau réel
SIMULPIX_TP_LOSS_PERCENT=50 SIMULPIX_TP_NET_DELAY_MS=3000 \
  SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT=0 \
  ./scripts/measure-tp.sh 12_doublon_pub
```

**Honnêteté pédagogique** : sur une machine très performante avec un réseau Docker local très fiable, même en combinant ces leviers, le nombre de doublons peut rester faible (quelques unités). C'est en soi un message pédagogique : librdkafka est **très robuste**, et le doublon at-least-once PUB est en pratique un événement rare qui suppose des conditions réseau dégradées **et** un crash partiel d'infrastructure. C'est précisément pourquoi l'idempotence producteur (TP 03) est une garantie de **dernier rempart** et non une nécessité de tous les jours.

### 3. Provoquer le doublon côté SUB

C'est l'étape la plus riche. Le doublon SUB n'est pas visible en base parce que la contrainte `PRIMARY KEY (transaction_id)` absorbe la deuxième tentative. Pour le rendre visible, on installe un **trigger d'audit** qui logue chaque tentative d'INSERT ou d'UPSERT.

```sh
./scripts/reset-scenario.sh
./scripts/measure-tp.sh 12_doublon_sub 120 40 3 5 20 0
```

Ce que le script fait :

1. installe le trigger d'audit (`toggle-unique.sh off`) ;
2. lance un flux at_least_once / at_least_once de **120 Pix à 40 Pix/s** ;
3. force une sortie du persister-valid **après UPSERT PostgreSQL mais avant commit Kafka**, une seule fois ;
4. laisse 5 s d'arrêt contrôlé ;
5. redémarre le persister, observe 20 s de rattrapage puis draine ;
6. mesure les compteurs ;
7. exécute `toggle-unique.sh count` pour afficher les tentatives ;
8. nettoie (`reset-scenario.sh` puis `toggle-unique.sh on`).

À observer dans Grafana pendant la mesure :

- **General Dashboard, `db_validated_count`** : ralentit puis reste plate pendant l'arrêt du persister.
- **Kafka Dashboard, panneau `LAG` du groupe persister-valid** : grimpe pendant l'arrêt, puis se résorbe après le restart. Pendant ce rattrapage, le doublon rejoué est absorbé par la PK et tracé par le trigger d'audit.

**Ce que vous devez lire** dans la sortie de `count` :

```
lignes_en_base | tentatives_totales | doublons_absorbes 
----------------+--------------------+-------------------
            120 |                122 |                 2
```

- **lignes_en_base** = `N` : le nombre de Pix réellement persistés (uniques).
- **tentatives_totales** = `N + K` : chaque INSERT ou UPSERT a été comptabilisé.
- **doublons_absorbes** = `K` : les `K` tentatives en doublon que la PK a empêchées d'écrire deux fois.

Le détail par `transaction_id` montre quels messages ont été tentés plus d'une fois.

### 4. Ce que vous devez retenir de cette démonstration

La protection contre le doublon **ne vient pas de Kafka**. Kafka **rejoue effectivement** le message après le crash — c'est sa promesse `at_least_once`. Ce qui empêche le doublon en base, c'est la **contrainte d'unicité applicative** sur `transaction_id`.

Sans cette contrainte (par exemple si on persistait dans un système qui n'a pas d'index unique), le doublon serait visible dans la table.

**Note pédagogique** : nous n'avons pas désactivé la PK parce que cela aurait cassé le `INSERT ... ON CONFLICT DO UPDATE` du persister. À la place, l'audit nous a permis d'observer la protection à l'œuvre — c'est strictement équivalent en terme de compréhension du risque.

## Lecture des indicateurs de configuration

Sur `http://localhost:8082/` :

- carte **PUB Kafka** → `at_least_once`
- carte **réglage PUB** → `acks=all retries=N`
- carte **SUB Kafka** → `at_least_once`

Dans Grafana `Simul-Pix - Kafka Dashboard` :

- panneau **Commit SUB actif** → `Après traitement`
- panneau **Risque SUB dominant** → `Doublon possible`

## Comparaison synthétique

| Côté | Faille | Levier de provocation | Mesure |
|---|---|---|---|
| PUB | doublon dans le topic | injection d'ack ambigu ou réseau dégradé | `DOUBLONS PUB > 0`, avec `raw_topic_end_offsets > generated` en mode contrôlé |
| SUB | doublon de traitement absorbé | arrêt après écriture PostgreSQL mais avant commit Kafka | `tentatives_totales - lignes_en_base` via audit |

## Reproductibilité

Le doublon PUB est reproductible avec `SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT=5`. Sans cette injection contrôlée, il est **fortement** affecté par le taux de perte réseau, le débit, le timing de broker et les timeouts librdkafka.

Le doublon SUB est provoqué par une faute contrôlée : le persister écrit un Pix en PostgreSQL puis s'arrête avant de committer l'offset Kafka. Au redémarrage, Kafka rejoue ce Pix ; la PK absorbe la deuxième tentative.

La commande recommandée est :

```sh
./scripts/measure-tp.sh 12_doublon_sub 120 40 3 5 20 0
```

Le résultat attendu est `doublons_absorbes > 0`, généralement `1` à `2`. C'est peu, mais c'est le phénomène exact qu'on veut isoler avec un commit après chaque message.

## Questions

- Pourquoi `acks=all` seul ne suffit-il pas à éviter les doublons ?
- En quoi `enable.idempotence=true` (étudié au TP 03) change-t-il ce résultat ?
- Si la contrainte PK n'existait pas, quel serait le contrat métier exposé au client final ?
- Pourquoi la note du TP indique-t-elle "doublon absorbé" et pas "doublon évité" ?

## Critères de réussite

- Vous avez **mesuré** un `raw_topic_end_offsets > generated` (doublon PUB).
- Vous avez **mesuré** `doublons_absorbes > 0` via l'audit (doublon SUB).
- Vous savez expliquer pourquoi la protection vient de l'applicatif et non de Kafka.


</details>
