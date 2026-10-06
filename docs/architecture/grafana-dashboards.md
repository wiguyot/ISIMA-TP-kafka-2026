# Dashboards Grafana

Cette note explicite les dashboards les moins triviaux de la plateforme.

Le dashboard le plus important pour les séquences techniques de l'atelier est :

- `Simul-Pix - Kafka Dashboard`

## Dashboard Kafka

Ce dashboard sert a distinguer trois familles de problemes qui se ressemblent parfois visuellement, mais qui ne parlent pas de la meme chose :

- la sante interne du cluster Kafka ;
- le retard des consommateurs applicatifs ;
- la traduction temporelle de ce retard.

### Partitions sous-repliquees

`Partitions sous-repliquees` concerne le cluster Kafka lui-meme.

Cette metrique compte les partitions pour lesquelles tous les replicas ne sont pas a jour derriere le leader. En pratique :

- c'est un sujet broker / stockage ;
- cela parle de la replication interne entre brokers ;
- cela mesure une fragilite du cluster, pas directement un retard metier ;
- plus cette valeur monte, plus Kafka est dans un etat degrade du point de vue tolerance aux pannes.

Lecture pedagogique :

- si cette valeur monte pendant un incident broker, on discute d'abord replication, ISR et resilience ;
- si elle reste a `0`, cela signifie que le cluster replique correctement a cet instant.

### Lag consommateur total

`Lag consommateur total` concerne les applications qui lisent Kafka.

Cette metrique mesure le nombre de messages deja presents dans Kafka mais pas encore lus et commits par les consumer groups suivis. En pratique :

- c'est un sujet consommation applicative ;
- l'unite est un nombre de messages ou d'offsets ;
- cela parle du retard des services comme `pix-validator`, `pix-decision-engine`, `pix-outcome-publisher`, `persister-valid` ou `persister-rejected` ;
- plus cette valeur monte, plus les applications prennent du retard sur le flux disponible.

Lecture pedagogique :

- si cette valeur monte alors que `Partitions sous-repliquees` reste a `0`, le probleme est plutot cote consommateurs que cote replication Kafka ;
- si elle redescend, cela signifie que les consommateurs rattrapent le backlog.

### Anciennete backlog

`Anciennete backlog` est une lecture temporelle du lag.

Le lag consommateur dit : "il reste X messages a traiter".  
L'anciennete backlog essaie de dire : "le plus vieux retard visible correspond a environ combien de temps".

Autrement dit :

- `Lag consommateur total` repond a une question de volume ;
- `Anciennete backlog` repond a une question de temps.

C'est souvent plus pedagogique, parce qu'un etudiant comprend plus vite :

- "nous avons 3 000 messages de retard" si le systeme est gros, ce n'est pas toujours parlant ;
- "nous avons environ 45 secondes de retard" est beaucoup plus concret.

Point important : cette metrique reste une estimation.

Elle depend du debit observe :

- si le debit est stable, l'estimation est souvent parlante ;
- si le debit varie fortement, l'equivalence messages -> secondes devient plus approximative ;
- si le flux ralentit ou s'arrete, un meme lag en offsets peut representer des situations tres differentes.

Lecture pedagogique :

- une anciennete backlog qui monte alors que le lag monte aussi signifie que le retard n'est pas seulement volumique, il commence a se traduire en delai visible ;
- si le lag reste non nul mais que l'anciennete reste faible, on est plutot dans un systeme qui absorbe encore la charge ;
- si les deux montent ensemble, le systeme accumule un retard qui devient perceptible dans le temps.

## Lire ensemble les trois metriques

Les trois metriques doivent etre comparees ensemble :

- `Partitions sous-repliquees` monte : probleme de replication Kafka entre brokers ;
- `Lag consommateur total` monte : probleme de lecture ou de traitement cote applications ;
- `Anciennete backlog` monte : le retard applicatif commence a se transformer en delai concret.

Exemple typique :

- `Partitions sous-repliquees > 0`, mais `Lag consommateur total` faible :
  le cluster Kafka est perturbe, mais les applications ne sont pas encore en vrai retard.

- `Partitions sous-repliquees = 0`, mais `Lag consommateur total` et `Anciennete backlog` montent :
  Kafka replique correctement, mais les consommateurs n'absorbent plus le flux assez vite.

- les trois montent :
  l'incident commence a toucher a la fois le transport, la replication et le traitement applicatif.

## Actuel, max 5 min, max 30 min

Le haut du dashboard Kafka presente chaque metrique sous trois angles :

- `actuel` : l'etat courant ;
- `max 5 min` : le pire niveau observe sur les 5 dernieres minutes ;
- `max 30 min` : le pire niveau observe sur les 30 dernieres minutes.

Cela permet de distinguer :

- un incident termine mais encore visible dans l'historique ;
- un incident encore en cours ;
- une pointe breve mais importante.

Exemple :

- `actuel = 0`
- `max 30 min = 74`

signifie qu'il n'y a plus de partitions sous-repliquees maintenant, mais qu'un pic a bien atteint `74` dans les 30 dernieres minutes.
