# Comparaison des semantiques Kafka sous charge

Source analysee :
- [semantics_campaign_5000_reset_only_ttl60_obs60.json](semantics_campaign_5000_reset_only_ttl60_obs60.json)
- [semantics_campaign_5000_reset_only_ttl60_obs60.xlsx](semantics_campaign_5000_reset_only_ttl60_obs60.xlsx)

## Perimetre

Cette campagne mesure 27 combinaisons avec :

- `TTL = 60 s`
- observation finale = `60 s`
- `5000` Pix par cas
- `200` Pix par seconde cibles
- `prepare_mode = reset_only`

Combinaisons testees :
- trafics : `linear`, `poisson`, `bursty`
- semantiques PUB : `at_most_once`, `at_least_once`, `exactly_once`
- semantiques SUB : `at_most_once`, `at_least_once`, `exactly_once_kafka`

## Resultat global

- `27` cas executes
- `18` cas en `ok`
- `9` cas en `warning`
- `0` cas en erreur dure

Volumes globaux :
- `135000` Pix publies
- `93715` Pix acceptes
- `32239` Pix rejetes
- `32245` Pix rejetes sur TTL

Indicateurs moyens :
- taux moyen d'acceptation observe : `37.815 Pix/s`
- taux moyen de rejet observe : `12.913 Pix/s`
- delai moyen de validation : `2358.88 ms`

## Constat principal

Le diagnostic principal est le suivant :

- tous les cas `SUB = at_most_once` passent
- tous les cas `SUB = at_least_once` passent
- tous les cas `SUB = exactly_once_kafka` sont en `warning`

Donc, meme avec `TTL = 60 s`, la consommation `exactly_once_kafka` reste structurellement en dehors d'une zone de confort a `200 Pix/s`.

## Lecture des resultats

Avec `SUB = exactly_once_kafka`, la file d'attente atteint plusieurs milliers de messages et le delai moyen de validation approche `6 s`. Le TTL de `60 s` n'empeche pas les rejets lorsque la consommation ne suit plus le debit demande. Ces chiffres decrivent cette machine et cette charge ; reproduisez les mesures avant d'en tirer un seuil de capacite general.

## Analyse par semantique SUB

### SUB = at_most_once

- `9` cas
- `0` warning
- `45000` publies
- `45000` acceptes
- `0` rejet
- delai moyen : `443.64 ms`
- file max moyenne : `298.11`

### SUB = at_least_once

- `9` cas
- `0` warning
- `45000` publies
- `45000` acceptes
- `0` rejet
- delai moyen : `550.60 ms`
- file max moyenne : `138.89`

### SUB = exactly_once_kafka

- `9` cas
- `9` warnings
- `45000` publies
- `3715` acceptes
- `32239` rejetes
- `32245` rejetes TTL
- delai moyen : `6082.41 ms`
- file max moyenne : `4277.33`

Lecture :
- la consommation transactionnelle reste le point de rupture
- le pipeline traite, mais trop lentement pour rester confortable
- le TTL plus grand limite un peu les expirations, sans les faire disparaitre

## Analyse par trafic et semantique SUB

### Traffic linear

| SUB | Warnings | Publies | Acceptes | Rejetes | TTL | Delai moyen | File max moyenne |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| at_most_once | 0 | 15000 | 15000 | 0 | 0 | 437.11 ms | 175.67 |
| at_least_once | 0 | 15000 | 15000 | 0 | 0 | 507.48 ms | 75.67 |
| exactly_once_kafka | 3 | 15000 | 1284 | 10776 | 10779 | 6128.06 ms | 4249.67 |

### Traffic poisson

| SUB | Warnings | Publies | Acceptes | Rejetes | TTL | Delai moyen | File max moyenne |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| at_most_once | 0 | 15000 | 15000 | 0 | 0 | 382.32 ms | 392.00 |
| at_least_once | 0 | 15000 | 15000 | 0 | 0 | 539.16 ms | 51.00 |
| exactly_once_kafka | 3 | 15000 | 1258 | 10790 | 10792 | 6179.57 ms | 4234.00 |

### Traffic bursty

| SUB | Warnings | Publies | Acceptes | Rejetes | TTL | Delai moyen | File max moyenne |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| at_most_once | 0 | 15000 | 15000 | 0 | 0 | 511.50 ms | 326.67 |
| at_least_once | 0 | 15000 | 15000 | 0 | 0 | 605.16 ms | 290.00 |
| exactly_once_kafka | 3 | 15000 | 1173 | 10673 | 10674 | 5939.59 ms | 4348.33 |

Lecture :
- les trois trafics restent proches
- aucun trafic ne “sauve” `exactly_once_kafka`
- `bursty` est encore legerement plus dur sur la taille de file

## Analyse par semantique PUB

La semantique de production reste secondaire.

| PUB | Warnings | Publies | Acceptes | Rejetes | TTL | Delai moyen | File max moyenne |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| at_most_once | 3 | 45000 | 31158 | 10761 | 10763 | 2248.18 ms | 1574.56 |
| at_least_once | 3 | 45000 | 31367 | 11009 | 11011 | 2381.55 ms | 1526.00 |
| exactly_once | 3 | 45000 | 31190 | 10469 | 10471 | 2446.92 ms | 1613.78 |

Lecture :
- les trois modes PUB restent tres proches
- le probleme ne vient pas d'abord de la semantique de production

## Cas les plus tendus

Les pires files d'attente restent toutes en `SUB exactly_once_kafka` :

1. `bursty / PUB exactly_once / SUB exactly_once_kafka`
   - file max : `4489`
   - acceptes : `387`
   - rejetes : `3455`
   - TTL : `3455`
2. `linear / PUB exactly_once / SUB exactly_once_kafka`
   - file max : `4421`
   - acceptes : `401`
   - rejetes : `3214`
   - TTL : `3215`
3. `linear / PUB at_most_once / SUB exactly_once_kafka`
   - file max : `4329`
   - acceptes : `386`
   - rejetes : `3652`
   - TTL : `3653`

## Interpretation

Cette campagne permet de conclure plus fermement :

1. `TTL = 60 s` n'est toujours pas suffisant pour rendre `SUB exactly_once_kafka` comparable aux autres modes a `200 Pix/s`.
2. Le surcout observe est structurel.
3. Le prochain levier utile n'est plus vraiment le TTL.

## Conclusion

La conclusion pratique est la suivante :

- si l'objectif est une comparaison equitable des modes,
- alors il faut maintenant agir sur le debit plutot que continuer a augmenter le TTL.

Le prochain chantier logique est donc bien une campagne de seuil de debit sur un cas fixe, par exemple :

- `linear / PUB exactly_once / SUB exactly_once_kafka`
- en partant de `200 Pix/s`
- puis `190`, `180`, ..., jusqu'a identifier le point de bascule.
