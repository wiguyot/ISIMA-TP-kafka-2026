# Sujet 14 — Au-delà de Poisson : simuler la variabilité réelle du trafic

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Le générateur de `simulpix` émet déjà des flux variés : le mode `linear` émet à rythme fixe, le mode `poisson` tire un nombre d'arrivées par seconde autour d'une moyenne, et le mode `bursty` ajoute des pics, des creux et des oscillations multi-échelles. Cette organisation est décrite dans [les modèles de trafic](../../docs/architecture/traffic-models.md).

Ces modèles reposent pourtant sur une hypothèse forte : les arrivées sont **indépendantes** les unes des autres. Or un flux de paiements réel est corrélé — un événement déclenche d'autres événements (une notification pousse des centaines d'utilisateurs à payer), l'activité suit un profil horaire, et ce n'est pas la moyenne qui fait tomber le système, c'est la **variabilité**.

La plateforme est développée dans un contexte académique : le laboratoire [LIMOS](https://www.limos.fr) (CNRS / Université Clermont Auvergne / Mines Saint-Étienne) mène des travaux sur les modèles stochastiques, l'évaluation de performances et les systèmes distribués. Ces thématiques recouvrent directement le sujet. Vous n'avez pas à inventer vos modèles en partant de zéro : vous devez **aller à la rencontre des chercheurs** du laboratoire, recueillir leurs suggestions, et les adapter aux contraintes d'un générateur réel.

Ce sujet croise quatre compétences : **démarche scientifique** (rencontre, choix et validation de modèles), **mathématiques** (formalisation des processus d'arrivées), **programmation** (implémentation dans le shaper), **interface** (les rendre composables et lisibles). Il impose une exigence finale : prouver que ce que vous avez conçu **apporte** quelque chose que Poisson ne montre pas.

## Votre mission

Rencontrez des chercheurs du LIMOS dont les thèmes se rapprochent de la modélisation du trafic. À partir de cet échange, concevez **au moins deux modèles de trafic nouveaux et complémentaires**, implémentez-les dans le générateur, rendez leurs paramètres accessibles par une interface, puis démontrez expérimentalement leur apport : à moyenne identique, vos modèles doivent révéler sur la chaîne Pix des dégradations mesurables que le mode `poisson` actuel ne produit pas — et pas nécessairement les mêmes pour chaque modèle.

## Réalisation minimale attendue

Deux modèles de trafic au minimum, de natures différentes (par exemple un processus à intensité variable et un processus à dépendance temporelle), implémentés dans `pix-traffic-shaper`, activables comme les modèles existants et reproductibles (graine fixée). Une validation statistique prouve que les séries émises suivent bien les modèles annoncés (test d'ajustement : Khi-deux ou Kolmogorov-Smirnov). Une interface permet de paramétrer ou de visualiser les profils sans éditer de code. Une campagne d'essais compare `poisson` et vos modèles à moyenne égale et mesure les conséquences de bout en bout : lag Kafka, percentiles du délai de décision, temps de drainage, cohérence de la persistance. Le compte rendu de la rencontre avec les chercheurs est un livrable à part entière.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ». La preuve visée est ici une dégradation invisible en `poisson` à moyenne identique.

Ensuite, vous devez :

1. Identifiez au LIMOS les équipes ou chercheurs dont les travaux touchent la modélisation stochastique, les files d'attente ou l'évaluation de performances. Préparez la rencontre : lisez [les modèles actuels](../../docs/architecture/traffic-models.md) et formulez à l'avance ce que vous cherchez (des modèles réalistes de trafic de paiements, pilotables et testables).
2. Tenez la rencontre et produisez-en un compte rendu : modèles proposés, hypothèses à vérifier, références données, pistes écartées et pourquoi. Ce compte rendu justifie vos choix suivants.
3. Retenez **au moins deux modèles** de natures différentes et justifiez-les mathématiquement. Pistes à discuter avec les chercheurs : processus de Poisson **non homogène** (intensité λ(t) pilotée par un profil), processus **auto-excitant de Hawkes** (chaque arrivée augmente l'intensité des suivantes), superposition de processus, arrivées à corrélations longues, modèles de files à saturation. Un modèle simple bien validé vaut mieux qu'un modèle sophistiqué non prouvé.
4. Implémentez les modèles dans le shaper en respectant l'architecture existante : un taux instantané, un volume par bucket, la convergence vers le total demandé. Conservez la reproductibilité par graine.
5. Validez les lois émises : collectez les séries produites, testez l'ajustement à chaque modèle annoncé, publiez moyenne, variance et autocorrélation. Un modèle qui n'émet pas ce qu'il prétend invalide tout le reste.
6. Construisez l'interface : formulaire de profil dans `service-health`, panneau Grafana comparant débit cible et débit réel avec bandes de confiance, ou éditeur de profil visuel permettant de changer de modèle et de régler ses paramètres. Le choix doit être justifié.
7. Définissez le protocole d'apport : des campagnes à **moyenne identique**, une en `poisson`, une par modèle nouveau. Mesurez ce qui change, modèle par modèle.
8. Poussez la charge jusqu'à la rupture pour chaque mode : à quelle variabilité le pipeline cesse-t-il de respecter le SLA alors que la moyenne le laisse croire au contraire ? Chaque modèle provoque-t-il la même rupture ?
9. Analysez les conséquences sur toute la chaîne : lag consommateur, délai de décision (moyenne **et** percentiles p95/p99), temps de drainage, taux d'écriture PostgreSQL.
10. Documentez les modèles, leurs paramètres, leurs limites, la procédure pour rejouer vos campagnes, et renvoyez vos conclusions aux chercheurs rencontrés : c'est ainsi qu'une SAé alimente un vrai dialogue avec la recherche.

## Questions de conception

- Quels chercheurs du laboratoire travailleront sur des thèmes proches (files d'attente, processus ponctuels, performance des systèmes distribués) et que peuvent-ils vous apporter qu'un cours ne donne pas ?
- Pourquoi une moyenne stable ne garantit-elle pas l'absence de saturation ? (la théorie des files a une réponse : que se passe-t-il quand le taux d'occupation s'approche de 1 ?)
- Que mesure un percentile p99 qu'une moyenne cache ? Pourquoi est-ce la bonne métrique pour un SLA de paiement ?
- Comment prouver que vos séries émises suivent vos modèles, et pas seulement qu'elles « ressemblent à quelque chose de varié » ?
- Pourquoi avoir retenu deux modèles de natures différentes plutôt qu'un seul ? Quel phénomène réel chacun représente-t-il, et lequel est pertinent pour Pix ?
- Quel est le coût de la reproductibilité stochastique : la graine suffit-elle quand le shaper interagit avec un pipeline asynchrone ?
- Votre interface doit-elle montrer le profil avant l'essai, pendant, ou après ? Pourquoi ?

## Dimension théorique

Votre sujet porte des aspects théoriques au cœur de la fiche : les processus ponctuels (Poisson non homogène, Hawkes), la validation statistique des lois émises et la théorie des files sous variabilité. C'est le sujet le plus directement connecté à la recherche : la rencontre avec les chercheurs est déjà un livrable de la fiche. Consultez [l'analyse recherche](analyse-recherche-limos.md) pour la référence détaillée : la simulation à événements discrets et l'optimisation stochastique sont les méthodologies de l'axe [ODPS](https://www.limos.fr/axes/3) du LIMOS. Cet approfondissement fait partie de l'évaluation. L'aspect identifié ici n'est pas exhaustif : votre réalisation peut révéler d'autres aspects théoriques, à approfondir et à signaler également.

## Preuves attendues

- le compte rendu de la rencontre avec les chercheurs : modèles suggérés, références, pistes écartées ;
- une note mathématique : modèles retenus, hypothèses, formules d'intensité, tests d'ajustement passés sur les séries émises ;
- une campagne à moyenne égale comparant `poisson` et chacun de vos modèles, avec lag, délais (moyenne et p95/p99), drainage et persistance ;
- la démonstration qu'au moins un de vos modèles révèle un mode de défaillance invisible en `poisson` à moyenne identique, et que les modèles ne sont pas interchangeables ;
- l'interface en fonctionnement, permettant de changer de modèle et de régler ses paramètres sans modification du code ;
- des essais rejouables à l'identique (graines documentées) donnant les mêmes conclusions ;
- une conclusion distinguant ce que vos simulations apportent de ce qu'elles restent : des modèles, pas la réalité — et ce que le dialogue avec le laboratoire a changé dans vos choix.

## Ressources

- [Modèles de trafic](../../docs/architecture/traffic-models.md)
- [Activité 09 — Charge et temporisation](../activite-09-charge-et-temporisation.md)
- [Sujet 4 — Respect du SLA](sujet-04-sla-decision.md) et [Sujet 6 — Qualification de capacité](sujet-06-capacite-charge.md) (méthodologie de campagne)
- [Pic de paiements football](../pour-aller-plus-loin/partie-5-pic-de-paiements-football.md)
- [Laboratoire LIMOS](https://www.limos.fr) — l'axe [ODPS](https://www.limos.fr/axes/3) (simulation à événements discrets, optimisation stochastique) et le thème [MOCA](https://www.limos.fr/themes/7) de l'axe MAAD (simulation, méta-modélisation) recouvrent les processus ponctuels et la modélisation du trafic ; voir aussi [l'analyse recherche](analyse-recherche-limos.md)
- Shaper : `services/pix-traffic-shaper/` (`traffic_model.py`, `runtime.py`)
