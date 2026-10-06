# Modeles de trafic Pix

Cette note explique la logique pedagogique du generateur de trafic Pix.

## Pourquoi ne pas emettre a rythme fixe

Un trafic reel de paiements Pix n'arrive pas comme un metronome.

Si l'on demande `1000` messages par seconde, un systeme reel ne produit pas exactement :

- `1000` messages a chaque seconde ;
- `1` message toutes les `1 ms` ;
- avec un espacement parfaitement regulier.

En pratique, on observe plutot :

- des secondes plus calmes ;
- des secondes plus chargees ;
- des petits paquets de messages emis presque en meme temps ;
- des creux temporaires suivis de reprises ;
- des variations lentes a l'echelle de la minute, de la dizaine de minutes ou de l'heure.

Le generateur de `Simul-Pix` cherche donc a conserver une **moyenne cible**, sans imposer une emission parfaitement lineaire.

## Les trois modes disponibles

### `linear`

Le mode `linear` emet un volume fixe a chaque bucket temporel.

Exemple :

- cible : `40 msg/s`
- bucket : `1 s`
- resultat : `40`, `40`, `40`, `40`, ...

Ce mode est utile pour :

- les tests simples ;
- les demonstrations tres controlees ;
- les comparaisons avec les modes non lineaires.

### `poisson`

Le mode `poisson` utilise un tirage stochastique autour d'une moyenne attendue.

Exemple avec une cible a `40 msg/s` :

- une seconde peut produire `34` messages ;
- la suivante `46` ;
- puis `41`, `50`, `37`, etc.

L'idee pedagogique est la suivante :

- la moyenne attendue reste voisine de `40 msg/s` ;
- mais le trafic n'est plus artificiellement regulier ;
- les fluctuations courtes deviennent visibles dans les dashboards.

Ce mode est le plus naturel pour expliquer qu'un flux d'arrivees peut etre aleatoire tout en respectant une moyenne globale.

### `bursty`

Le mode `bursty` part de la meme logique de base, mais ajoute :

- des **bursts** : pics d'emission temporaires ;
- des **creux** : ralentissements temporaires ;
- un bruit lisse ;
- des variations lentes sur plusieurs echelles de temps.

On ne voit donc plus seulement une fluctuation aleatoire autour d'une moyenne, mais de vraies phases :

- acceleration ;
- paquet de messages ;
- retour progressif ;
- eventuel creux.

Ce mode est utile pour approcher des cas plus realistes :

- sortie de stade ;
- paiement en caisse lors d'une pause ;
- notification envoyee a beaucoup d'utilisateurs en meme temps ;
- reprise d'activite apres une courte accalmie.

## Comment la moyenne reste respectee

Le generateur ne choisit pas uniquement un nombre au hasard a chaque seconde.

Il applique plusieurs mecanismes :

1. Il calcule un taux instantane cible.
2. Il tire un volume a produire sur un bucket temporel.
3. Il repartit ce volume en microbursts dans le bucket.
4. Il applique un correctif de budget pour converger vers le volume total attendu.

Cela signifie :

- a court terme, le trafic peut varier ;
- a moyen terme, on reste proche de la moyenne demandee ;
- a la fin d'un scenario borne, on converge vers le total demande.

Exemple :

- scenario demande : `320` messages a une moyenne cible de `40 msg/s`
- mode `poisson`
- buckets observes : `46`, `41`, `44`, `34`, `43`, `37`, `50`, `25`

On voit bien la variabilite, mais la somme finale reste `320`.

## Echelles de temps

Le generateur peut faire varier le trafic a plusieurs niveaux :

- seconde ;
- minute ;
- dizaine de minutes ;
- heure.

L'objectif n'est pas de simuler exactement une journee reelle, mais de rendre visibles :

- les fluctuations courtes ;
- les pics ponctuels ;
- les tendances plus lentes.

Dans l'atelier, cela permet de discuter :

- la capacite d'absorption du pipeline ;
- le lag consommateur ;
- l'impact d'un TTL Pix trop court ;
- la difference entre debit moyen et debit instantane.

## Cas particulier : `rate_per_second = 0`

Quand `rate_per_second` vaut `0` ou est laisse vide depuis l'interface de pilotage, le generateur passe en mode **open-loop** :

- il n'essaie plus de tenir une cadence par seconde ;
- il emet aussi vite que possible ;
- il continue en revanche a respecter un volume total si un `total_messages` est fourni.

Ce mode est utile pour :

- charger rapidement le pipeline ;
- provoquer une montee de backlog ;
- tester la tenue de `pix-decision-engine`, de Kafka et de PostgreSQL.

## Comment lire les dashboards

Avec un trafic non lineaire, il ne faut pas s'attendre a voir des courbes parfaitement plates.

Ce qu'il faut lire :

- dans le `General Dashboard` :
  - les debits recents ;
  - les variations de messages produits ;
  - la difference entre produits, traites et decisions finales ;
- dans le `Kafka Dashboard` :
  - la montee ou la stabilisation du lag ;
  - l'anciennete estimee du backlog ;
- dans le `Persistence Dashboard` :
  - l'ecart entre le pipeline et la base ;
  - le debit d'ecriture PostgreSQL ;
  - la fraicheur des dernieres ecritures.

Une courbe irrégulière n'est donc pas un probleme en soi.
Ce qui compte est de savoir si :

- la moyenne reste conforme a l'objectif ;
- le pipeline absorbe les fluctuations ;
- ou au contraire une fluctuation fait apparaitre un retard ou une rupture.

## Recommandations pedagogiques

Pour introduire l'atelier :

1. Commencer par `linear` pour montrer un flux simple et lisible.
2. Passer a `poisson` pour introduire l'alea tout en gardant une moyenne stable.
3. Finir avec `bursty` pour montrer des pics et des creux plus credibles.

Pour discuter des limites du systeme :

1. Augmenter le debit moyen.
2. Garder `poisson` ou `bursty`.
3. Observer a partir de quel seuil le lag monte ou les validations s'arretent.

## Parametres utiles

Les principaux parametres sont :

- `SIMULPIX_TRAFFIC_MODEL`
- `SIMULPIX_TRAFFIC_BUCKET_SECONDS`
- `SIMULPIX_TRAFFIC_SECOND_AMPLITUDE`
- `SIMULPIX_TRAFFIC_MINUTE_AMPLITUDE`
- `SIMULPIX_TRAFFIC_TEN_MINUTE_AMPLITUDE`
- `SIMULPIX_TRAFFIC_HOUR_AMPLITUDE`
- `SIMULPIX_TRAFFIC_NOISE_AMPLITUDE`
- `SIMULPIX_TRAFFIC_BURST_PROBABILITY`
- `SIMULPIX_TRAFFIC_BURST_AMPLITUDE`
- `SIMULPIX_TRAFFIC_BURST_DURATION_SECONDS`
- `SIMULPIX_TRAFFIC_LULL_PROBABILITY`
- `SIMULPIX_TRAFFIC_LULL_AMPLITUDE`
- `SIMULPIX_TRAFFIC_LULL_DURATION_SECONDS`

Le mode `poisson` suffit en general pour un flux plausible.
Le mode `bursty` est plus demonstratif pour les séquences de charge ou de stabilité.
