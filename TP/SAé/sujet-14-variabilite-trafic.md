# Sujet 14 — Au-delà de Poisson : simuler la variabilité réelle du trafic

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Le shaper de `simulpix` fournit `linear`, `poisson`, `bursty` et `profiled`. Le nom `poisson` ne désigne pas ici un processus de Poisson homogène pur : le code module l'intensité par des oscillations et du bruit lissé, corrige le budget d'une phase finie, puis limite le volume restant. Le tirage utilise en outre une approximation normale lorsque la moyenne par bucket atteint 30. Cette organisation est décrite dans [les modèles de trafic](../../docs/architecture/traffic-models.md).

Il serait donc incorrect de supposer indépendantes toutes les arrivées des modèles actuels. Le sujet consiste à caractériser leurs dépendances, puis à ajouter des modèles définis mathématiquement pour étudier l'effet de la variabilité sur les files et les délais. Leur pertinence pour des paiements réels reste une hypothèse à justifier.

La démarche comprend un échange scientifique organisé avec l'enseignant, en recherchant un interlocuteur du [LIMOS](https://www.limos.fr) dont les travaux se rapprochent des modèles étudiés. Les pages de l'[analyse recherche](analyse-recherche-limos.md) sont des orientations de contact ; elles ne constituent pas une collaboration déjà convenue.

Ce sujet croise démarche scientifique, mathématiques, programmation et interface. La conclusion doit établir ce que les modèles changent dans les conditions étudiées ; elle peut constater une absence de différence significative ou une limite de puissance expérimentale.

## Votre mission

Concevez au moins deux modèles nouveaux et complémentaires, implémentez-les dans le shaper, rendez leurs paramètres accessibles et comparez-les à la référence actuelle à moyenne effectivement comparable. Décrivez les changements de distribution et leurs conséquences mesurées sur la chaîne Pix.

## Socle fourni et contribution nouvelle

Les modèles, les seeds et le pilotage existent. Votre contribution est une formalisation des deux nouveaux modèles, leur calibration, une validation hors pipeline puis en émission réelle, et une campagne répétée.

Organisez l'échange scientifique dès le début avec l'enseignant. Si aucun chercheur n'est disponible dans le calendrier, l'enseignant organise un entretien de remplacement à partir des articles étudiés. Conservez le compte rendu et les démarches effectuées ; la disponibilité d'un tiers ne conditionne pas la réussite du groupe.

## Réalisation minimale attendue

Deux modèles de natures différentes, activables dans `pix-traffic-shaper`, avec paramètres, seeds et séries cibles archivés. Une interface permet de choisir le modèle et ses paramètres, puis de sauvegarder une configuration rejouable.

Validez statistiquement les séries cibles hors pipeline, puis caractérisez les écarts des séries réellement confirmées par le générateur. Comparez la référence et les deux modèles sur une fenêtre commune, à moyenne réalisée comparable et avec plusieurs seeds indépendantes. Mesurez lag, p95/p99 et effectifs de décision, entrées sans décision, drainage borné et contenu persistant. Le compte rendu de l'échange scientifique, ou de son remplacement, est demandé.

## Actions à réaliser

Avant toute modification, formulez l'hypothèse scientifique testée, le critère de comparaison et les conditions susceptibles de la réfuter. Une dégradation particulière est une hypothèse, pas un résultat imposé.

Ensuite, vous devez :

1. Identifiez au LIMOS les équipes ou chercheurs dont les travaux touchent la modélisation stochastique, les files d'attente ou l'évaluation de performances. Préparez la rencontre : lisez [les modèles actuels](../../docs/architecture/traffic-models.md) et formulez à l'avance ce que vous cherchez (des modèles réalistes de trafic de paiements, pilotables et testables).
2. Tenez la rencontre ou l'entretien de remplacement organisé avec l'enseignant, et produisez-en un compte rendu : modèles, hypothèses, références, pistes écartées et raisons. Reliez-le à vos choix.
3. Retenez **au moins deux modèles** de natures différentes et justifiez-les mathématiquement. Pistes : Poisson non homogène avec intensité λ(t) définie, processus auto-excitant de Hawkes, superposition ou arrivées corrélées. Justifiez ce qui distingue vos modèles du socle et l'un de l'autre ; des noms différents ne suffisent pas.
4. Implémentez les modèles en déclarant l'effet des buckets, de la compression temporelle et d'un éventuel conditionnement sur le total. Conservez les seeds et les profils : une correction forcée du total peut modifier la loi et sa dépendance temporelle.
5. Validez les modèles par des tests adaptés aux variables étudiées : comptes discrets, temps inter-arrivées ou intensité. Justifiez hypothèses et calibration, notamment si les paramètres sont estimés sur les données ; utilisez une simulation de référence si nécessaire. Publiez moyenne, variance et autocorrélation. Pour un modèle dépendant tel que Hawkes, un histogramme seul est insuffisant. Une p-value supérieure au seuil ne prouve pas la loi.
6. Construisez l'interface : formulaire de profil dans `service-health`, panneau Grafana comparant débit cible et débit réel avec bandes de confiance, ou éditeur de profil visuel permettant de changer de modèle et de régler ses paramètres. Le choix doit être justifié.
7. Définissez une campagne à moyenne **réalisée** comparable sur une fenêtre commune et vérifiez que le générateur suit la charge. Une même valeur `rate_per_second` ou un même total sur des durées différentes ne suffit pas.
8. Comparez plusieurs niveaux de variabilité dans une enveloppe de charge bornée et répétée. Relevez le respect du SLA et les limites rencontrées ; si aucune rupture n'est atteinte, dites-le.
9. Analysez les conséquences sur toute la chaîne : lag consommateur, délai de décision (moyenne **et** percentiles p95/p99), temps de drainage, taux d'écriture PostgreSQL.
10. Documentez les modèles, leurs paramètres, leurs limites, la procédure pour rejouer vos campagnes, et renvoyez vos conclusions aux chercheurs rencontrés : c'est ainsi qu'une SAé alimente un vrai dialogue avec la recherche.

## Extensions facultatives

Ajoutez une référence de Poisson homogène explicitement distincte du mode historique, calibrez un modèle sur un jeu de traces autorisé ou explorez une dépendance à longue portée. Pour une référence homogène, désactiver les oscillations seul ne suffit pas : le rattrapage du total, le tirage approché et le placement des arrivées doivent aussi être définis.

## Questions de conception

- Quels chercheurs du laboratoire travailleront sur des thèmes proches (files d'attente, processus ponctuels, performance des systèmes distribués) et que peuvent-ils vous apporter qu'un cours ne donne pas ?
- Pourquoi une moyenne stable ne garantit-elle pas l'absence de saturation ? (la théorie des files a une réponse : que se passe-t-il quand le taux d'occupation s'approche de 1 ?)
- Que mesure un percentile p99 qu'une moyenne cache ? Pourquoi est-ce la bonne métrique pour un SLA de paiement ?
- Comment prouver que vos séries émises suivent vos modèles, et pas seulement qu'elles « ressemblent à quelque chose de varié » ?
- Pourquoi avoir retenu deux modèles de natures différentes plutôt qu'un seul ? Quel phénomène réel chacun représente-t-il, et lequel est pertinent pour Pix ?
- Quel est le coût de la reproductibilité stochastique : la graine suffit-elle quand le shaper interagit avec un pipeline asynchrone ?
- Votre interface doit-elle montrer le profil avant l'essai, pendant, ou après ? Pourquoi ?

## Dimension théorique

Définissez l'intensité, la dépendance temporelle et les conditions de validité de chaque modèle. Justifiez le test statistique, sa calibration et sa puissance, puis reliez variabilité et comportement des files. Séparez série cible, lots demandés et publications effectives. Le compte rendu de l'échange scientifique explique les choix de modèle et leurs limites. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- le compte rendu de l'échange scientifique ou de son remplacement : modèles suggérés, références, pistes écartées ;
- une note mathématique : modèles, hypothèses, formules, protocole et résultats de validation des séries cibles et réellement publiées ;
- une campagne à moyenne égale comparant `poisson` et chacun de vos modèles, avec lag, délais (moyenne et p95/p99), drainage et persistance ;
- une conclusion étayée sur les différences observées, leur incertitude et les conditions dans lesquelles elles apparaissent ou restent non concluantes ;
- l'interface en fonctionnement, permettant de changer de modèle et de régler ses paramètres sans modification du code ;
- des profils cibles reproductibles avec seeds documentées et des essais répétés ; la seed ne reproduit pas l'ordonnancement du réseau et des services ;
- une conclusion distinguant ce que vos simulations apportent de ce qu'elles restent : des modèles, pas la réalité — et ce que le dialogue avec le laboratoire a changé dans vos choix.

## Ressources

- [Modèles de trafic](../../docs/architecture/traffic-models.md)
- [Activité 09 — Charge et temporisation](../activite-09-charge-et-temporisation.md)
- [Sujet 4 — Respect du SLA](sujet-04-sla-decision.md) et [Sujet 6 — Qualification de capacité](sujet-06-capacite-charge.md) (méthodologie de campagne)
- [Pic de paiements football](../pour-aller-plus-loin/partie-5-pic-de-paiements-football.md)
- [Laboratoire LIMOS](https://www.limos.fr) — orientations possibles [ODPS](https://www.limos.fr/axes/3) et [MAAD](https://www.limos.fr/axes/1), à confirmer avec l'enseignant et les travaux identifiés ; voir l'[analyse recherche](analyse-recherche-limos.md)
- Shaper : `services/pix-traffic-shaper/` (`traffic_model.py`, `runtime.py`)
