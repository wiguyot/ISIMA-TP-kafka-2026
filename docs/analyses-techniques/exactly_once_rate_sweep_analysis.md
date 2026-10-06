# Analyse du balayage de debit

Source analysee :
- [exactly_once_rate_sweep_ttl60_obs60.json](exactly_once_rate_sweep_ttl60_obs60.json)
- [exactly_once_rate_sweep_ttl60_obs60.xlsx](exactly_once_rate_sweep_ttl60_obs60.xlsx)

## Perimetre

Campagne a cas fixe :

- trafic : `linear`
- semantique PUB : `exactly_once`
- semantique SUB : `exactly_once_kafka`
- `5000` Pix par test
- `TTL = 60 s`
- observation finale = `60 s`

La seule variable est le debit :

- `200`
- `190`
- `180`
- ...
- `10`

## Resultat principal

Le point de bascule utile est tres net :

- a partir de `50 Pix/s`, les rejets TTL tombent a `0`
- a `60 Pix/s`, le pipeline est encore tres nettement en echec

Autrement dit, sur cette architecture et avec ces regles :

- `60 Pix/s` est trop haut
- `50 Pix/s` devient soutenable

## Lecture rapide par debit

| Debit | Statut | Publies | Acceptes | Rejetes | TTL | Delai moyen | File max |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 200 | warning | 5000 | 482 | 3620 | 3622 | 5709.77 ms | 4441 |
| 190 | warning | 5130 | 476 | 3679 | 3679 | 6221.22 ms | 4358 |
| 180 | warning | 5040 | 470 | 3551 | 3551 | 6093.76 ms | 4178 |
| 170 | warning | 5100 | 453 | 3899 | 3899 | 6174.09 ms | 4067 |
| 160 | warning | 5120 | 537 | 2617 | 2618 | 6088.21 ms | 4448 |
| 150 | warning | 5100 | 183 | 1806 | 1806 | 6469.42 ms | 4581 |
| 140 | warning | 5040 | 182 | 1819 | 1820 | 6351.98 ms | 4393 |
| 130 | warning | 5070 | 180 | 1826 | 1826 | 6266.52 ms | 4334 |
| 120 | warning | 5040 | 221 | 2075 | 2075 | 5980.35 ms | 4235 |
| 110 | warning | 5060 | 176 | 2231 | 2232 | 6686.72 ms | 4220 |
| 100 | warning | 5000 | 224 | 2156 | 2157 | 6070.62 ms | 4014 |
| 90 | warning | 5040 | 171 | 2406 | 2407 | 6122.32 ms | 4032 |
| 80 | warning | 5040 | 211 | 2509 | 2510 | 6281.82 ms | 3905 |
| 70 | warning | 5040 | 217 | 2861 | 2862 | 6090.23 ms | 3654 |
| 60 | warning | 5040 | 268 | 2984 | 2985 | 6761.16 ms | 3337 |
| 50 | warning | 5000 | 5000 | 0 | 0 | 3994.98 ms | 607 |
| 40 | warning | 5000 | 5000 | 0 | 0 | 739.41 ms | 181 |
| 30 | warning | 5010 | 5010 | 0 | 0 | 363.60 ms | 62 |
| 20 | warning | 5000 | 5000 | 0 | 0 | 225.35 ms | 37 |
| 10 | warning | 5000 | 5000 | 0 | 0 | 133.19 ms | 21 |

## Conclusion technique

Le resultat important n'est pas le champ `status`, qui reste `warning` partout, mais :

- le nombre de rejets TTL
- la taille de file
- le nombre de Pix finalement acceptes

Pourquoi le `status` reste en `warning` meme quand `50/s` et en dessous passent bien :

- la meteo garde encore des alertes de lag residuel Kafka
- il reste donc un petit signal de retard au moment de la collecte
- mais ce retard n'entraine plus de rejet TTL

Donc, pour cette campagne, le bon critere de succes est :

- `rejected_ttl_pix = 0`
- et `accepted_pix ~= 5000`

Avec ce critere, le debit soutenable observe est :

- `50 Pix/s`

## Interpretation pedagogique

Cette campagne montre quelque chose de tres utile :

- la semantique `exactly_once_kafka` ne rend pas le pipeline inutilisable
- mais elle reduit fortement le debit soutenable

Ici, avec `TTL = 60 s` :

- au-dessus de `50 Pix/s`, le backlog grossit trop
- les messages attendent trop longtemps
- puis ils expirent et sont rejetes

En dessous ou a `50 Pix/s` :

- le pipeline finit par absorber le flux
- il n'y a plus de rejet TTL
- la file d'attente reste presente mais devient gerable

## Point de bascule observe

On peut donc formuler le point de bascule ainsi :

- `60 Pix/s` : encore non tenable
- `50 Pix/s` : tenable

Le seuil utile se situe donc dans la zone :

- `50 < seuil critique <= 60`

Si l'on veut l'affiner, la prochaine campagne naturelle serait :

- `60`
- `58`
- `56`
- `54`
- `52`
- `50`

## Reserve methodologique

Quelques lignes montrent :

- `published_pix > 5000`
- ou `accepted_pix > 5000`

Exemples :
- `190/s -> 5130 publies`
- `30/s -> 5010 acceptes`

Ce n'est pas coherent avec un lot strict de `5000` Pix. Cela signale un bruit de mesure residuel, probablement lie a :

- la maniere dont certains compteurs Kafka sont releves
- ou un leger recouvrement entre deux runs lors des resets

Ce biais ne change cependant pas la conclusion principale, car :

- la zone `200 -> 60` montre massivement des rejets TTL
- la zone `50 -> 10` montre `0` rejet TTL

Le signal de bascule reste donc tres clair.

## Conclusion finale

Pour le cas :

- `linear / PUB exactly_once / SUB exactly_once_kafka`
- `TTL = 60 s`
- observation finale `60 s`

le debit soutenable observe est de l'ordre de :

- `50 Pix/s`

Le prochain raffinage utile, si necessaire, est un zoom entre `50` et `60 Pix/s`.
