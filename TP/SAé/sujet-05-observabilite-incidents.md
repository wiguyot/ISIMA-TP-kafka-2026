# Sujet 5 — Diagnostiquer un incident à partir des signaux du pipeline

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Un pipeline peut sembler disponible parce que ses conteneurs sont démarrés, tout en accumulant du retard, en perdant sa capacité de réplication ou en n'écrivant plus correctement dans PostgreSQL. Une courbe isolée ne suffit donc pas à comprendre ce qui se passe.

L'**observabilité** consiste à recueillir des signaux, puis à les interpréter pour répondre à une question concrète : quel incident est en cours, quel est son impact métier, et quelle vérification faut-il faire ensuite ?

## Votre mission

Construisez une lecture d'incident qui distingue au moins trois situations :

- un consommateur prend du retard ;
- la réplication entre les brokers Kafka est dégradée ;
- la persistance PostgreSQL ne suit plus le flux.

Votre objectif n'est pas seulement d'afficher des métriques. Vous devez aider une personne qui découvre l'incident à passer d'un symptôme à un diagnostic vérifiable.

## Socle fourni et contribution nouvelle

La météo, Grafana, les sondes Kafka/PostgreSQL et plusieurs alertes existent déjà. Votre contribution est une règle ou un outil qui relie ces signaux pour lever une ambiguïté mesurée, avec une vérification indépendante de la cause.

## Réalisation minimale attendue

Ajoutez une métrique dérivée, une alerte, une vue ou une commande de diagnostic. Couvrez les trois situations de la mission, dont une dégradation ou une indisponibilité PostgreSQL. Distinguez automatiquement au moins deux causes plausibles d'un même symptôme ; si les données ne permettent pas de trancher, rendez le diagnostic « indéterminé » et indiquez la vérification suivante.

Automatisez les incidents et un témoin nominal, puis mesurez justesse du diagnostic, délai de détection et faux positifs. Vérifiez aussi la fraîcheur des sondes : le système d'observation peut être affecté par l'incident.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Recensez les informations déjà visibles dans la météo, Grafana, `tp-kafka.sh` et `tp-db.sh`.
2. Définissez, pour chaque famille d'incident, les symptômes attendus, les hypothèses possibles et la vérification suivante.
3. Provoquez un retard consommateur, une dégradation de réplication et un incident PostgreSQL ; ajoutez un essai nominal pour contrôler les faux positifs.
4. Construisez ou améliorez une vue, un indicateur ou une règle de diagnostic qui supprime une ambiguïté réelle.
5. Vérifiez chaque conclusion contre Kafka et PostgreSQL, pas seulement contre un dashboard.
6. Rédigez un guide de diagnostic qu'un autre groupe peut suivre pendant un incident.

## Extensions facultatives

Comparez des seuils fixes à une détection adaptative. Une méthode d'apprentissage n'est pas exigée pour le minimum ; une règle explicite, justifiée et évaluée suffit.

## Questions de conception

- Quelle différence voyez-vous entre un backlog temporaire et une perte de données ?
- Qu'est-ce qui permet de distinguer un problème applicatif d'un problème de cluster Kafka ?
- Quel indicateur relie le mieux un signal technique à son impact sur un paiement Pix ?
- Quelles métriques doivent être lues ensemble pour conclure sur la persistance ?
- Quel signal est assez fiable pour déclencher une alerte ?

## Dimension théorique

Formalisez les hypothèses de cause, les observations qui les discriminent et le cas indéterminé. Justifiez vos seuils et mesurez délai de détection, faux positifs et erreurs de diagnostic sur des incidents connus. La détection adaptative est une extension, pas une exigence du minimum. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- une matrice « symptôme, cause probable, vérification » ;
- des relevés pour au moins trois situations distinctes ;
- une amélioration d'observabilité réellement exploitable ;
- la démonstration que le diagnostic ne confond pas backlog, rejet métier et perte ;
- un guide de diagnostic reproductible.

## Ressources

- [Activité 08 — Observabilité et métrologie](../activite-08-observabilite-metrologie.md)
- [Atelier avancé](../pour-aller-plus-loin/realisation-des-tp.md)
- [Dashboards Grafana](../../docs/architecture/grafana-dashboards.md)
- [Service-health HTTP](../../docs/architecture/service-health-http.md)
