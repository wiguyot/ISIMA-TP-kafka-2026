# Analyse du balayage de TTL a 50 Pix/s

Source analysee :
- [exactly_once_ttl_sweep_rate50_obs60.json](exactly_once_ttl_sweep_rate50_obs60.json)
- [exactly_once_ttl_sweep_rate50_obs60.xlsx](exactly_once_ttl_sweep_rate50_obs60.xlsx)

## Perimetre

Cas fixe :

- trafic : `linear`
- semantique PUB : `exactly_once`
- semantique SUB : `exactly_once_kafka`
- debit fixe : `50 Pix/s`
- `5000` Pix par test
- observation finale = `60 s`

Seule variable :
- `TTL = 60, 59, 58, ..., 50`

## Resultat principal

Le resultat n'est **pas monotone**.

Autrement dit :
- certains TTL plus bas passent tres bien
- alors que certains TTL plus hauts echouent partiellement

Cela veut dire que, dans cette zone de fonctionnement, on ne mesure pas seulement l'effet theorique du TTL. On mesure aussi une **variabilite d'execution** du pipeline autour d'un point de charge limite.

## Tableau de lecture

| TTL | Publies | Acceptes | Rejetes TTL | Delai moyen | File max |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 60 | 5000 | 5000 | 0 | 784.14 ms | 171 |
| 59 | 5000 | 5000 | 0 | 797.43 ms | 167 |
| 58 | 5000 | 4929 | 71 | 5829.95 ms | 812 |
| 57 | 5000 | 5000 | 0 | 1250.11 ms | 251 |
| 56 | 5000 | 5000 | 0 | 4181.75 ms | 473 |
| 55 | 5000 | 5000 | 0 | 3396.60 ms | 506 |
| 54 | 5000 | 5000 | 0 | 5567.58 ms | 780 |
| 53 | 5000 | 3248 | 1752 | 6286.51 ms | 1249 |
| 52 | 5000 | 5000 | 0 | 1863.42 ms | 287 |
| 51 | 5000 | 3409 | 1591 | 6931.36 ms | 1087 |
| 50 | 5000 | 5000 | 0 | 1025.31 ms | 245 |

## TTL qui passent proprement

TTLs avec :
- `5000` acceptes
- `0` rejet TTL

Liste observee :
- `60`
- `59`
- `57`
- `56`
- `55`
- `54`
- `52`
- `50`

## TTL qui echouent partiellement

TTLs avec rejets TTL observes :
- `58`
- `53`
- `51`

Volumes observes :

### TTL = 58
- `4929` acceptes
- `71` rejets TTL
- file max `812`

### TTL = 53
- `3248` acceptes
- `1752` rejets TTL
- file max `1249`

### TTL = 51
- `3409` acceptes
- `1591` rejets TTL
- file max `1087`

## Lecture technique

On voit deux regimes assez distincts.

### Regime stable

Quand le run passe :
- delai moyen de validation autour de `2358 ms` en moyenne
- file max autour de `360`
- aucun rejet TTL

### Regime degrade

Quand le run derape :
- delai moyen de validation autour de `6349 ms`
- file max autour de `1049`
- apparition immediate de rejets TTL

Donc, a `50 Pix/s`, on est dans une zone ou :
- la plupart des runs passent
- mais certains runs derapent encore

## Interpretation

Le point important est le suivant :

- le debit `50 Pix/s` est globalement tenable
- mais il reste **a la limite**
- le pipeline n'est pas encore dans une zone de confort completement robuste

Le fait que `TTL = 50` passe alors que `TTL = 51` echoue, ou que `TTL = 57` passe alors que `TTL = 58` echoue, montre bien que :
- l'effet n'est pas purement determine par le TTL
- on observe aussi du bruit d'exploitation, des variations de scheduling, de backlog, ou de latence transactionnelle

## Conclusion pedagogique

Cette campagne ne permet pas de dire :

- "le seuil TTL exact est 54" ou "le seuil TTL exact est 57"

Elle permet plutot de dire :

- a `50 Pix/s`, le pipeline est **presque** soutenable pour `exactly_once_kafka`
- mais il reste une fragilite residuelle
- le TTL seul n'explique pas tout

## Conclusion pratique

Si l'objectif est d'avoir un scenario vraiment propre et reproductible pour le TP :

- `50 Pix/s` est une bonne zone
- mais pas encore une zone totalement confortable

La suite logique serait plutot :

1. soit descendre legerement le debit, par exemple `45 Pix/s` ou `40 Pix/s`
2. soit refaire plusieurs repetitions au meme couple `50 Pix/s / TTL 60 s`

Mon avis :
- pour un TP robuste, `40 Pix/s` serait probablement plus sain
- pour un TP qui montre une zone limite interessante, `50 Pix/s` est tres pedagogique
