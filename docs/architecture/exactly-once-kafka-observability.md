# Exactly Once Kafka et lecture des compteurs

Cette note précise ce que l'atelier montre réellement quand `SUB Kafka` est réglé sur `Exactly once Kafka`.

## Périmètre réel

Dans Simul-Pix, `Exactly once Kafka` signifie :

- transaction Kafka sur la chaîne `lecture -> traitement -> publication -> commit offset` ;
- pas de doublon logique sur les topics Kafka concernés ;
- pas de garantie `exactly once` de bout en bout jusqu'à PostgreSQL.

Les services actuellement concernés sont :

- `pix-validator`
- `pix-decision-engine`
- `pix-outcome-publisher`

Les persisters PostgreSQL restent hors de ce périmètre.

## Pourquoi les offsets Kafka peuvent surprendre

Quand un topic Kafka est écrit en mode transactionnel, Kafka ajoute des marqueurs techniques de transaction.

Conséquence :

- les offsets bruts du topic peuvent monter plus vite que le nombre logique de messages métier ;
- un topic transactionnel peut donc afficher un `end_offsets_total` supérieur au nombre réel de Pix visibles.

Exemple typique observé dans l'atelier :

- `outcome_count = 112`
- `outcome_topic_end_offsets = 224`

Ici :

- `112` correspond au nombre logique de `Résultats finaux` publiés ;
- `224` correspond au volume technique du log Kafka, marqueurs inclus.

Quand l'offset technique vaut exactement deux fois le compteur logique, la lecture la plus simple est :

- une position pour la réponse finale métier ;
- une position supplémentaire liée au marqueur de transaction ;
- donc `outcome_topic_end_offsets = 2 × outcome_count`.

Exemple TP :

- `Outcome count observé = 600`
- `Topic outcome technique = 1200`

Cela ne signifie pas 1200 réponses métier. Cela signifie 600 réponses finales et 600 positions techniques supplémentaires dans le log Kafka transactionnel.

## Tentatives réessayées et transactions abortées

Une transaction Kafka peut produire une sortie puis échouer avant `commit_transaction`.

Dans ce cas :

1. les messages produits dans cette transaction sont annulés ;
2. l'offset consommé n'est pas validé ;
3. le service relit le message au redémarrage ;
4. une nouvelle transaction republie la sortie ;
5. seule la transaction commitée est visible comme message métier pour un consommateur en `read_committed`.

Les tentatives abortées ne doivent donc pas être comptées comme des résultats métier. Elles peuvent toutefois faire monter les offsets techniques, car Kafka garde les marqueurs de transaction dans le log.

Règle de lecture :

- `validated + rejected` : résultat métier final du pipeline ;
- compteur logique d'outcome : réponses finales visibles ;
- `outcome_topic_end_offsets` : volume technique du log Kafka, marqueurs transactionnels inclus.

## Ce qu'il faut lire dans l'atelier

Pour l'interprétation pédagogique :

- `Pix bruts`
- `Pix contrôlés`
- `Décisions Pix`
- `Résultats finaux`

doivent être lus comme des compteurs métier/logiques.

En revanche :

- `raw_topic_end_offsets`
- `checked_topic_end_offsets`
- `decision_topic_end_offsets`
- `validated_topic_end_offsets`
- `rejected_topic_end_offsets`
- `outcome_topic_end_offsets`

restent des mesures de log Kafka.

Dans `service-health`, les compteurs affichés pour les étapes transactionnelles sont corrigés pour rester logiques.

## Ce qui est visible dans l'observabilité

`8082` et Grafana exposent désormais :

- `validator_transactional_code`
- `decision_transactional_code`
- `outcome_transactional_code`
- `kafka_exactly_once_chain_count`

Lecture :

- `0` = étape standard ;
- `1` = étape en transaction Kafka.

Quand les trois étapes amont sont transactionnelles :

- `validator_transactional_code = 1`
- `decision_transactional_code = 1`
- `outcome_transactional_code = 1`
- `kafka_exactly_once_chain_count = 3`

## Règle pédagogique simple

Pour expliquer l'atelier sans ambiguïté :

- les compteurs affichés dans la chaîne sont des compteurs métier ;
- les offsets Kafka sont des compteurs techniques ;
- sur les topics transactionnels, ces deux lectures ne sont pas nécessairement égales ;
- c'est normal et attendu.
