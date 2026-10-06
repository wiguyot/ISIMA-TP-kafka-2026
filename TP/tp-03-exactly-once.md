# TP 03 — Sémantique exactly-once (producteur et consommateur)

## L'essentiel

**exactly-once** promet **ni perte ni doublon**. Kafka l'obtient par deux mécanismes distincts, et seulement **à l'intérieur de Kafka**.

| Mécanisme | Où | Ce qu'il fait |
|---|---|---|
| **Producteur idempotent** (`enable.idempotence=true`) | PUB | Kafka numérote les envois de chaque producteur et ignore un nouvel essai déjà écrit : les retries ne créent plus de doublon. |
| **Transactions** (`transactional.id`, `send_offsets_to_transaction`, lecture en `read_committed`) | SUB lire-traiter-écrire | Les messages produits **et** l'offset lu sont validés ensemble, ou annulés ensemble. Un crash au milieu ne laisse ni sortie partielle ni offset avancé. |

### Résultat réel : crash d'un service transactionnel

Les trois services `pix-validator`, `pix-decision-engine` et `pix-outcome-publisher` fonctionnent en transactions. On tue `pix-decision-engine` pendant 10 secondes, en plein flux de 600 Pix :

```
Pix ARRIVÉS dans Kafka (raw)       :   600
Outcome count observé              :   600   (réponses finales)
Topic outcome technique            :  1200   (offset brut)
Pix PERSISTÉS en base (validated)  :   394
Pix rejetés en base                :   206
DOUBLONS PUB / PERTE SUB           :     0
```

- **600 réponses pour 600 Pix** : ni perte ni doublon malgré le crash. Les transactions interrompues ont été annulées puis rejouées.
- **1200 positions** dans le topic `outcome` : chaque transaction ajoute un **marqueur de fin** (`COMMIT`) qui occupe un offset mais n'est jamais livré aux consommateurs.
- **206 Pix rejetés** : ce ne sont pas des pertes. Pendant les 10 secondes d'arrêt, ces Pix ont dépassé leur délai de décision de 10 secondes, et le moteur les a correctement rejetés à la reprise. Une garantie technique n'empêche pas un effet métier.

### Résultat réel : la limite PostgreSQL

Même configuration, mais c'est `persister-valid` qui s'arrête entre l'écriture en base et le commit :

```
 lignes_en_base | tentatives_totales | doublons_absorbes
            120 |                122 |                 2
```

Exactement comme au TP 02 : l'écriture dans PostgreSQL est **hors** de la transaction Kafka. Le persister retombe en at-least-once, et c'est encore la clé unique qui absorbe le doublon.

### Le prix : latence et débit

Sur la machine de test, la chaîne transactionnelle tient **jusqu'à 50 Pix/s** et s'effondre **dès 60 Pix/s**. À 200 Pix/s, la décision prend en moyenne **6 s** au lieu de **0,55 s** en at-least-once, soit environ **11 fois plus**. Détail en section 5.

### Ce qu'il faut retenir

1. Producteur idempotent : plus de doublon **dû aux retries**. Transactions : sorties et offset validés **ensemble**.
2. La garantie s'arrête **à la frontière de Kafka** : une base de données, un e-mail ou un appel HTTP restent à rendre idempotents par l'application (voir [`kafka-inbox-outbox-design.md`](../docs/architecture/kafka-inbox-outbox-design.md)).
3. exactly-once a un **coût** : on le réserve aux traitements où un doublon Kafka serait inacceptable.

### Questions

- Pourquoi l'offset technique du topic `outcome` vaut-il le double du nombre de réponses ?
- Les 206 rejets de la démonstration sont-ils des pertes ? Justifiez.
- Comment obtenir un « exactly-once » qui inclut PostgreSQL ?

---

## Reproduire l'expérience

Réalisez l'expérience : elle fait passer les notions de la première partie de la lecture à l'observation mesurée.

<details>
<summary>Afficher les consignes de reproduction</summary>

Dans ce TP, vous allez reprendre les pertes du TP 01 et les doublons du TP 02, puis vérifier ce que Kafka sait réellement protéger en `exactly-once`. Point important dès le départ : cette sémantique ne couvre pas les aspects `PG`, c'est-à-dire PostgreSQL.

## Définition rapide

**exactly-once côté PUB** — producteur idempotent (`enable.idempotence=true`, `acks=all`, `retries > 0`). Chaque message reçoit un identifiant `(producer ID, sequence number)`. Kafka **déduplique** les retries dans la même session. **Garantie : ni perte ni doublon sur la chaîne producteur → topic, dans la session.**

**exactly-once-kafka côté SUB** — transaction Kafka regroupant `lecture → traitement → publication → commit offset` de manière atomique. Si l'une des étapes échoue, l'ensemble est annulé puis repris. **Garantie : ni perte ni doublon logique sur la chaîne Kafka → Kafka.**

## Deux mécanismes à ne pas confondre

Le terme `exactly-once` peut donner l'impression qu'il existe un seul bouton magique. Dans Kafka, il faut distinguer deux mécanismes complémentaires.

### 1. Côté PUB : l'idempotence producteur

Pour une publication simple `producteur → topic`, la protection vient d'abord du **producteur idempotent**.

Le producteur est configuré avec :

- `enable.idempotence=true` ;
- `acks=all` ;
- `retries > 0`.

Kafka associe alors les envois à un `producer ID` et à des numéros de séquence. Si le producteur réessaie le même envoi parce qu'un accusé de réception a été perdu, Kafka reconnaît le retry et évite d'écrire deux fois le même message.

Dans ce cas, il n'est pas nécessaire d'imaginer une transaction autour de chaque message. La garantie vient surtout de la déduplication des retries par Kafka.

### 2. Côté SUB : la transaction Kafka

Pour une étape qui lit un topic, traite le message, publie un résultat puis valide son offset, l'idempotence producteur ne suffit pas. Ce que vous devez retenir : le service doit rendre atomiques les écritures Kafka de sortie et le commit d'offset.

Le service fait conceptuellement :

```text
begin_transaction
  lire un message
  traiter
  produire le ou les messages de sortie
  envoyer l'offset consommé dans la transaction
commit_transaction
```

Si le service tombe avant `commit_transaction`, Kafka annule les publications interrompues et l'offset consommé n'est pas validé. Au redémarrage, le message est relu proprement.

Côté **SUB Kafka → Kafka**, le service ouvre donc une transaction au début de l'opération et la valide à la fin. Le producteur transactionnel reste idempotent : les deux mécanismes se complètent.

**Limite essentielle** : la transaction Kafka **ne s'étend pas à PostgreSQL**. `persister-valid` et `persister-rejected` écrivent en base **hors transaction Kafka**. La garantie s'arrête à la frontière du dernier topic Kafka couvert.

```
generator ──► raw ──► pix-validator ──► checked ──► pix-decision-engine
                  └──────────────────────────────────────────────────────┘
                       Transaction Kafka exactly-once
                                          │
                                          ▼
                       validated ──► persister-valid ──► PostgreSQL
                                                            │
                                                  (at-least-once applicatif,
                                                   protégé par PRIMARY KEY)
```

## Objectif

Vous allez démontrer que les failles observées au TP 01 et TP 02 **disparaissent** dans la zone couverte par exactly-once Kafka, puis constater que la **limite PostgreSQL** subsiste. Vous mesurerez aussi le **coût** en latence et en débit.

## Référentiel de volume et adaptation à la machine

Les paramètres restent les mêmes que les TP 01 et 02, mais les commandes ci-dessous utilisent des volumes plus courts pour garder le TP lisible :

- PUB : **6000 Pix** à **100 Pix/s**, pour comparer avec le TP 02 PUB reproductible.
- SUB Kafka : **600 Pix** à **50 Pix/s**, pour observer le restart transactionnel sans attendre plusieurs minutes.
- Limite PG : **120 Pix** à **40 Pix/s**, pour montrer l'audit PostgreSQL comme au TP 02 SUB.

Tous les paramètres passent en argument CLI :

```sh
./scripts/measure-tp.sh <action> [TOTAL] [RATE] [INJECTION_DELAY] [DOWNTIME] [OBSERVATION] [PERSIST_DELAY]
```

Attention au débit : sur la machine de test, la chaîne transactionnelle (3 services en `exactly_once_kafka`) tient **jusqu'à 50 Pix/s** et s'effondre **dès 60 Pix/s** (balayage archivé, voir la section 5). La démonstration SUB à 50 Pix/s est donc déjà à la limite : c'est volontaire, pour que le coût reste visible. Sur une machine plus lente, baissez `RATE`.

**Pour comparer avec la zone hors confort**, augmentez RATE :

```sh
./scripts/measure-tp.sh 13_protection_sub 18000 200 90 60 90
```

Au-delà de 60 Pix/s, le coût de coordination transactionnelle explose : la file d'attente grossit et les Pix dépassent leur délai de décision. Le balayage dédié `run_exactly_once_rate_sweep.py` est l'outil prévu pour cette mesure.

## Que signifie le drainage ?

Dans ce TP, le mot **drainage** désigne le rattrapage du backlog Kafka après une perturbation ou un redémarrage.

Pendant la panne, les producteurs ou les services amont peuvent continuer à alimenter Kafka, alors que certains consommateurs ne progressent plus. Les messages s'accumulent donc dans les topics. Quand le service redémarre, il doit relire ces messages en attente, les traiter, publier les sorties et valider les offsets : c'est le drainage.

`measure-tp.sh` attend cette phase avant d'imprimer le bilan. Il compare principalement :

- les Pix arrivés dans Kafka (`raw_topic_end_offsets`) ;
- les Pix finalement persistés en base (`db_validated_count + db_rejected_count`).

L'objectif est d'éviter une mauvaise interprétation : un écart observé juste après le crash peut être un **retard temporaire**, pas une perte. Après drainage, le TP peut distinguer plus clairement :

- la protection Kafka : pas de perte ou doublon logique sur la chaîne transactionnelle ;
- le coût de coordination : le rattrapage peut être plus lent en `exactly_once_kafka` ;
- la limite PostgreSQL : une tentative doublée en base peut exister même si la chaîne Kafka est cohérente.

Attention : en `exactly_once_kafka`, les offsets techniques des topics transactionnels incluent des marqueurs de fin de transaction (`COMMIT` ou `ABORT`). Le drainage se lit donc avec les compteurs métier et la base, pas uniquement avec les offsets bruts.

## Prérequis

- TP 01 et 02 réalisés.
- Plateforme démarrée.

## Outils utilisés

- `scripts/measure-tp.sh` — orchestrateur des 3 démonstrations ;
- `scripts/inject-fault.sh` — injections (kill, perturbation) ;
- `../docs/analyses-techniques/exactly_once_rate_sweep_*` — résultats archivés pour le coût.

## Étapes

### 1. Réinitialiser

```sh
./scripts/reset-scenario.sh
```

### 2. Démonstration 1 — vérifier la configuration du producteur idempotent

**Portée de cette démonstration** : elle vérifie que le producteur est bien configuré en mode idempotent et qu'il n'écrit aucun doublon dans un fonctionnement normal. Elle ne **prouve pas** la déduplication des retries : les doublons du TP 02 venaient d'une injection applicative (un second `produce()`), que l'idempotence ne peut pas dédupliquer et que le script désactive donc ici. Provoquer de vrais retries internes demande un réseau très dégradé (variante en fin de section), et le résultat dépend alors de la machine.

Cette première commande est volontairement **déterministe** : elle désactive la perte réseau, la latence et l'arrêt broker. Elle ne cherche pas à provoquer un incident réseau réel ; elle sert à comparer proprement avec la démonstration contrôlée du TP 02.

```sh
SIMULPIX_TP_LOSS_PERCENT=0 \
SIMULPIX_TP_NET_DELAY_MS=0 \
SIMULPIX_TP_BROKER_OUTAGE=0 \
  ./scripts/measure-tp.sh 13_protection_pub 6000 100 5 0 15
```

Ce que le script fait :

1. configure PUB en `exactly_once`, SUB en `at_least_once` ;
2. lance un flux de **6000 Pix à 100 Pix/s** ;
3. désactive l'injection contrôlée de doublon applicatif utilisée au TP 02 ;
4. vérifie que le generator a bien émis les 6000 Pix logiques ;
5. observe le drainage ;
6. imprime le bilan.

Ce que cette démonstration prouve :

- le producteur est configuré en mode idempotent (`enable.idempotence=true`, `acks=all`, `retries > 0`) ;
- aucun doublon PUB n'apparaît dans le topic `raw` ;
- l'écart artificiel du TP 02 (`raw_topic_end_offsets > generated`) disparaît.

Ce qu'elle ne prétend pas prouver seule :

- elle ne force pas nécessairement un retry interne réel de librdkafka ;
- elle ne rejoue pas l'injection contrôlée `SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT=5`, car cette injection simule un **second appel applicatif à `produce()`**, qui serait un nouveau message légitime et non un retry interne dédupliqué par Kafka.

Pendant la mesure, comparer dans Grafana avec la démo équivalente du TP 02 (`12_doublon_pub`) :

- TP 02 : avec `SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT=5`, `raw_topic_end_offsets` montait **plus vite** que `generated` (`6300` vs `6000`).
- TP 03 : `raw_topic_end_offsets` reste **aligné** avec `generated`. Le producteur idempotent est le mode qui supprime ce risque côté Kafka.

**Ce que vous devez lire** :

- `generated == raw_topic_end_offsets`.
- `DOUBLONS PUB = 0`.
- Le réglage de producteur est `enable.idempotence=true`, `acks=all`, `retries > 0`.

Le script affiche désormais cette lecture explicitement dans son bilan :

```text
Lecture attendue TP 03 — protection PUB exactly-once :
  [OK] generated == raw_topic_end_offsets : 6000 == 6000
  [OK] DOUBLONS PUB = 0
  [OK] Réglage producteur attendu : enable.idempotence=true, acks=all, retries > 0
  Comparaison TP 02 : dans 12_doublon_pub, raw_topic_end_offsets > generated ; ici l'écart doit disparaître.
```

**Comparer avec le TP 02** : on avait `raw_topic_end_offsets > generated`. Ici l'écart disparaît.

**Note de méthode** : l'injection contrôlée du TP 02 modélise un ack ambigu côté producteur. Elle n'est pas rejouée comme un second `produce()` en exactly-once, car un second appel applicatif serait un nouveau message légitime, pas un retry interne dédupliqué par Kafka.

Pour tester le cas réseau réel, lancer ensuite une variante plus agressive :

```sh
SIMULPIX_TP_AMBIGUOUS_ACK_DUPLICATE_PERCENT=0 \
SIMULPIX_TP_LOSS_PERCENT=50 \
SIMULPIX_TP_NET_DELAY_MS=3000 \
  ./scripts/measure-tp.sh 13_protection_pub
```

Dans cette variante, le résultat peut dépendre davantage de Docker, de la machine et du timing Kafka. Le but est de pousser librdkafka à exercer ses retries réels. Si `generated == raw_topic_end_offsets`, la lecture est simple. Si le réseau est très dégradé, le compteur `generated` peut devenir moins fiable que les compteurs Kafka et base.

Sur une perturbation très forte, il peut arriver que `raw_topic_end_offsets > generated` alors que `DOUBLONS PUB = 0`. Ce n'est pas nécessairement un échec exactly-once : le compteur `generated` du generator repose sur les callbacks de livraison observés par l'application. Si le réseau est très dégradé, certains messages peuvent être bien écrits dans Kafka mais ne pas être comptés côté callback avant la mesure.

Dans ce cas, lire plutôt :

- `raw_topic_end_offsets == Pix persistés TOTAL` ;
- `DOUBLONS PUB = 0` ;
- pas d'écart durable entre Kafka et la base après drainage.

Exemple interprétable :

```text
Pix ÉMIS par le generator              : 291
Pix ARRIVÉS dans Kafka (raw)           : 325
Pix persistés TOTAL (valid + rejected) : 325
DOUBLONS PUB                           : 0
```

Ici, les 34 Pix d'écart ne sont pas des doublons : ils sont arrivés dans Kafka, puis ont été traités et persistés. Le compteur generator a simplement sous-estimé les livraisons observées pendant la perturbation.

### 3. Démonstration 2 — la PROTECTION côté SUB sur la chaîne Kafka

```sh
./scripts/reset-scenario.sh
./scripts/measure-tp.sh 13_protection_sub 600 50 5 10 25 0
```

Ce que le script fait :

1. configure PUB en `exactly_once`, SUB en `exactly_once_kafka` ;
2. lance un flux de **600 Pix à 50 Pix/s**, avec un TTL métier de **10 s** par défaut (les 3 services validator → decision → outcome passent en mode transactionnel) ;
3. laisse 5 s de régime stable ;
4. envoie `SIGKILL` au pix-decision-engine, le laisse arrêté **10 s**, soit le même ordre de grandeur que le TTL ;
5. redémarre le service (Kafka annule les transactions interrompues) ;
6. observe 25 s de rattrapage ;
7. mesure.

Pourquoi aligner l'arrêt sur le TTL ? Pour bien séparer deux notions :

- **cohérence Kafka** : `exactly_once_kafka` évite la perte et le doublon logique sur la chaîne transactionnelle ;
- **décision métier** : un Pix peut tout de même être rejeté si le délai de décision est dépassé.

Autrement dit, voir quelques rejets `processing_timeout` dans cette démonstration n'invalide pas exactly-once. Cela montre au contraire qu'une garantie technique Kafka ne signifie pas "aucun impact métier". Kafka peut rejouer proprement un message, mais si le message est désormais trop ancien, le moteur de décision le rejette correctement.

Pendant la mesure, dans Grafana, regarder simultanément :

- **Kafka Dashboard, `Étapes en transaction Kafka`** → reste à `3/3` pendant le flux.
- Au moment du kill : la chaîne arrête de progresser, le LAG du groupe `pix-decision-engine` explose.
- Au redémarrage : la chaîne rattrape sans créer de doublon logique sur `outcome_count`.

**Ce que vous devez lire** :

- Pas de doublon **logique** sur `outcome_count` malgré le crash de l'engine en plein traitement.
- Pas de perte logique : la chaîne Kafka finit drainée.
- Des rejets métier peuvent apparaître si le TTL est dépassé pendant l'arrêt ou le rattrapage : ils sont attendus et distincts d'une perte Kafka.
- **Mais** `outcome_topic_end_offsets > outcome_count` : les marqueurs techniques de fin de transaction (`COMMIT`, `ABORT`) gonflent l'offset brut du topic.

Dans Grafana `Simul-Pix - Kafka Dashboard` :

- panneau **Étapes en transaction Kafka** → `3 / 3` ;
- panneau **Résultats finaux logiques** → cohérent avec `outcome_count` ;
- panneau **Topic outcome : offset technique** → plus élevé que les résultats logiques.

### 4. Démonstration 3 — la LIMITE côté PostgreSQL

Même configuration que la démo 2, mais cette fois on tue le `persister-valid`, qui écrit en PG **hors** transaction Kafka. Les persisters ne savent d'ailleurs pas faire de transaction Kafka : demandé en `exactly_once_kafka`, un persister fonctionne en `at_least_once` (commit après l'écriture en base).

```sh
./scripts/reset-scenario.sh
./scripts/measure-tp.sh 13_limite_pg 120 40 3 5 20 0
```

Ce que le script fait :

1. configure PUB en `exactly_once`, SUB en `exactly_once_kafka` ;
2. installe le trigger d'audit PostgreSQL ;
3. lance un flux de **120 Pix à 40 Pix/s** ;
4. force une sortie du `persister-valid` **après UPSERT PostgreSQL mais avant commit Kafka**, une seule fois ;
5. redémarre, observe 20 s de rattrapage ;
6. mesure puis affiche `toggle-unique.sh count`.

**Ce que vous devez lire** :

- Sur la chaîne Kafka : aucun écart logique.
- En PostgreSQL : `db_valid_count` reste cohérent grâce à la PK/UPSERT.
- L'audit montre pourtant `doublons_absorbes > 0` : le message a été tenté plusieurs fois côté PG.

**Lecture pédagogique** : la frontière n'est pas un détail. PostgreSQL **n'est pas** couvert par exactly-once Kafka. Ici, on ne mesure pas une perte durable en base ; on mesure une tentative doublée absorbée par l'idempotence applicative.

### 5. Le coût de coordination

L'atelier dispose de deux campagnes archivées dans `../docs/analyses-techniques/` (5000 Pix par cas, TTL de 60 s).

**Balayage de débit** en `exactly_once / exactly_once_kafka` (`../docs/analyses-techniques/exactly_once_rate_sweep_analysis.md`) :

| Débit | Pix acceptés sur 5000 | Délai moyen de décision | File d'attente max |
|---:|---:|---:|---:|
| 20 Pix/s | 5000 | 225 ms | 37 |
| 40 Pix/s | 5000 | 739 ms | 181 |
| 50 Pix/s | 5000 | 3995 ms | 607 |
| 60 Pix/s | 268 | 6761 ms | 3337 |
| 200 Pix/s | 482 | 5710 ms | 4441 |

Entre 50 et 60 Pix/s, la chaîne bascule : la file d'attente explose et presque tous les Pix dépassent leur délai.

**Comparaison des sémantiques SUB à 200 Pix/s** (`../docs/analyses-techniques/semantics_campaign_analysis-campaign_5000-v4.md`, moyenne sur les 3 sémantiques PUB et les 3 modèles de trafic) :

| SUB | Délai moyen de décision | File d'attente max moyenne | Pix acceptés |
|---|---:|---:|---:|
| `at_least_once` | 551 ms | 51 à 290 | 100 % |
| `exactly_once_kafka` | 6082 ms | 4277 | environ 8 % |

À débit égal, la consommation transactionnelle est environ **11 fois plus lente** : c'est le prix de la garantie.

Pour relancer une mesure ciblée :

```sh
python3 scripts/run_exactly_once_rate_sweep.py --rates 200,150,100,50 --total-messages 1000
```

## Lecture des indicateurs de configuration

Sur `http://localhost:8082/` :

- carte **PUB Kafka** → `exactly_once`
- carte **réglage PUB** → `acks=all` (l'idempotence requiert acks=all)
- carte **SUB Kafka** → `exactly_once_kafka`

Dans Grafana `Simul-Pix - Kafka Dashboard` :

- panneau **Étapes en transaction Kafka** → `3 / 3`
- panneau **Commit SUB actif** → `Transaction Kafka`
- panneau **Risque SUB dominant** → `Ni doublon logique sur Kafka`

## Comparaison synthétique des 3 sémantiques

| Sémantique | Perte PUB | Doublon PUB | Perte SUB | Doublon SUB | PG couvert | Latence |
|---|---|---|---|---|---|---|
| at-most-once (TP 01) | **Oui** | Non | **Oui** | Non | non applicable | minimale |
| at-least-once (TP 02) | Non | **Oui** | Non | **Oui** (absorbé) | non applicable | faible |
| exactly-once (TP 03) | Non | Non | Non | Non sur Kafka | **Non** (doublon PG possible, absorbé par PK) | supérieure |

## Pourquoi `outcome_count` peut différer de `outcome_topic_end_offsets`

`outcome` est le topic Kafka des réponses finales publiées par `pix-outcome-publisher`.

`outcome_count` compte les vrais résultats métier finaux :

- paiement accepté ;
- paiement rejeté métier ;
- paiement rejeté pour délai dépassé ;
- autre rejet technique ou fonctionnel.

Quand un topic est écrit en mode transactionnel, Kafka insère :

- à la fin de chaque transaction, un **marqueur de contrôle** `COMMIT` ou `ABORT` dans chaque partition écrite. Il n'existe pas de marqueur de début.

Ces marqueurs **occupent un offset** du topic mais ne sont **pas** des messages métier : aucun consommateur ne les reçoit. Un consommateur en `read_committed` ne voit que les messages des transactions validées ; en `read_uncommitted`, il verrait aussi ceux des transactions annulées, mais jamais les marqueurs eux-mêmes.

### Et les tentatives réessayées ?

Quand `pix-outcome-publisher` tombe ou qu'une transaction échoue avant `commit_transaction`, il peut avoir tenté de publier une réponse finale dans `outcome`. Mais tant que la transaction Kafka n'est pas commitée, cette réponse n'est pas visible comme message métier.

Au redémarrage :

1. Kafka considère que l'offset consommé n'a pas été validé ;
2. le message d'entrée est relu ;
3. une nouvelle transaction est ouverte ;
4. la réponse finale est republiée ;
5. seule la transaction commitée devient visible pour les consommateurs en `read_committed`.

Donc les tentatives annulées ou abortées **ne doivent pas être comptées comme des outcomes métier**. Elles peuvent en revanche laisser des traces dans le log Kafka technique, via les offsets et les marqueurs `ABORT`.

La bonne lecture est donc :

- `validated + rejected` = résultats métier finaux ;
- `outcome_count` logique = résultats finaux visibles si le compteur est bien lu en mode logique ;
- `outcome_topic_end_offsets` = volume technique du log Kafka, incluant les marqueurs transactionnels et les tentatives abortées.

**Exemple typique observé dans l'atelier** :

- `outcome_count = 112` (compteur métier logique)
- `outcome_topic_end_offsets = 224` (offset technique brut)

Ici, l'offset technique vaut exactement **2 ×** le compteur métier. La lecture pédagogique est :

- 112 réponses finales métier visibles ;
- 112 positions techniques supplémentaires liées aux marqueurs de transaction ;
- donc 224 positions dans le log Kafka.

Dans une exécution du TP 03, on peut par exemple voir :

- `Pix avec RÉSULTAT FINAL logique = 600`
- `Outcome count observé = 600`
- `Topic outcome technique = 1200`

Cela signifie bien que les 600 Pix ont une réponse finale métier, et que le log Kafka `outcome` contient aussi des marqueurs transactionnels. C'est **normal et attendu** en `exactly_once_kafka`.

## Questions

- Que se passe-t-il si le processus du producteur redémarre (nouveau `producer ID`) ? L'idempotence couvre-t-elle ce cas ?
- Pourquoi le coût d'exactly-once-kafka explose-t-il en haut débit ?
- Comment construirait-on un vrai exactly-once de bout en bout incluant PostgreSQL ? (cf. [`kafka-inbox-outbox-design.md`](../docs/architecture/kafka-inbox-outbox-design.md)).
- Dans quel cas métier accepteriez-vous le coût d'exactly-once par rapport à un at-least-once + idempotence applicative ?

## Critères de réussite

- Vous savez expliquer ce que déduplique le producteur idempotent (les retries internes) et ce qu'il ne déduplique pas (un second envoi décidé par l'application).
- Vous avez vu que le crash d'un service transactionnel Kafka ne crée pas de doublon logique sur `outcome_count`.
- Vous avez vu **persister** la limite PG malgré exactly-once-kafka : `doublons_absorbes > 0` dans l'audit PostgreSQL.
- Vous savez expliquer la divergence entre `outcome_count` et `outcome_topic_end_offsets`.
- Vous savez situer la frontière exacte de la garantie sur le schéma de la chaîne.


</details>
