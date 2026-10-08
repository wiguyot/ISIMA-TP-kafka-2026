# Sujet 10 — Reconstruire PostgreSQL depuis Kafka

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Une base PostgreSQL perdue peut être reconstruite si les événements nécessaires sont encore disponibles et si la projection est définie. Il faut démontrer cette propriété sur le contenu métier, avec une borne précise du journal.

**`./stop.sh` supprime aussi les volumes Kafka**, et `reset-scenario.sh` recrée les topics. Ils ne conviennent donc pas à l'expérience de perte de PostgreSQL avec conservation du journal. Préférez une base cible isolée et vide ; si vous détruisez un volume, ciblez exclusivement PostgreSQL dans un environnement dédié.

## Socle fourni et contribution nouvelle

Les persisters consomment déjà `validated` et `rejected` et écrivent par UPSERT. Votre contribution est une procédure de reconstruction isolée, avec un oracle d'équivalence automatisé et une vérification de complétude des journaux.

Les topics ont des rôles différents :

| Source | Ce qu'elle permet dans le socle actuel | Limite |
|---|---|---|
| `validated` + `rejected` | rejouer les décisions enregistrées vers leurs projections PostgreSQL | dépend de leur rétention et de leur complétude |
| `raw` | refaire un traitement à partir des entrées présentes | le délai, les règles ou le référentiel peuvent avoir changé ; les corrections passent par `retry` |
| `outcome` | retrouver les résultats clients publiés | ne contient pas tous les comptes et montants nécessaires à la projection complète |

Un retraitement de `raw` longtemps après l'émission peut transformer une ancienne acceptation en rejet pour délai dépassé. Ce n'est pas une reconstruction fidèle de la décision historique.

## Votre mission

Reconstruisez la projection PostgreSQL des paiements acceptés et des rejets depuis les événements finaux conservés. Démontrez l'équivalence de contenu et l'idempotence, puis expliquez les limites de rétention et les données non reconstructibles.

## Réalisation minimale attendue

Implémentez un script qui lit `validated` et `rejected` vers des persisters configurés pour une base cible vide. Utilisez de nouveaux groupes de consommation, une lecture depuis les premiers offsets disponibles et une borne de fin enregistrée par partition. Le script doit échouer si la projection diverge ou si le journal nécessaire est incomplet.

La comparaison porte sur les identités et les valeurs métier normalisées. Excluez uniquement les champs techniques explicitement justifiés, tels que séquences locales ou horodatages d'audit de reconstruction ; ne masquez pas les décisions et leurs horodatages historiques. Les volumes seuls ne prouvent pas l'équivalence.

## Actions à réaliser

1. Définissez la projection : clés des paiements, clés des tentatives/rejets, champs conservés, règle en présence de plusieurs événements pour une même clé.
2. Arrêtez les écritures de la campagne ou isolez une campagne et fixez ses bornes par partition. Conservez le manifeste attendu, les offsets de début/fin et les empreintes du contenu métier.
3. Vérifiez que la rétention contient encore les événements nécessaires. Atteindre les premiers offsets disponibles ne prouve pas que l'historique est complet.
4. Préparez une base cible PostgreSQL vide et séparée, avec le schéma et le référentiel nécessaires, en conservant Kafka. Documentez les connexions pour éviter une écriture dans la base source.
5. Lancez la reconstruction avec des groupes neufs et attendez les bornes de fin avec un timeout.
6. Comparez automatiquement les ensembles de clés et les valeurs de toutes les projections du périmètre ; échouez sur toute divergence.
7. Rejouez une seconde fois et vérifiez que le contenu métier reste identique.
8. Mesurez les durées et documentez les données hors journal : référentiel, configuration, éventuels agrégats et données administratives.

Pour une campagne complète drainée, sans contrôle supplémentaire ni révision, l'ensemble des **tentatives** acceptées et celui des tentatives rejetées sont disjoints ; leur union correspond aux tentatives terminales attendues dans `decision` et `outcome`. Cette égalité doit porter sur les identités, après déduplication selon le contrat, et non sur les offsets ou les publications brutes. `validated` ne désigne que les acceptations : l'égalité « validated = valid + rejected » est incorrecte.

## Extensions facultatives

- Comparez des sous-ensembles de topics, dont `raw` et `outcome`, pour établir précisément les informations manquantes et les conditions d'un retraitement déterministe.
- Testez une rétention insuffisante et proposez une politique par topic, en justifiant coût et perte de reconstructibilité.
- Ajoutez un témoin extérieur au couple Kafka/PostgreSQL pour détecter les omissions. Un journal append-only ne fournit pas à lui seul une preuve de non-répudiation : explicitez son intégrité, son indépendance et son modèle de confiance.
- Étudiez la reconstruction en présence d'écritures concurrentes avec une coupure cohérente et un protocole de rattrapage.

Les preuves associées à ces extensions ne sont exigées que si l'extension est retenue.

## Questions de conception

- Quel événement fait foi pour conserver une décision historique ?
- Quelles tables dépendent de données absentes des topics ?
- Quelle clé différencie paiement initial, correction et tentative de rejet ?
- Comment détectez-vous une portion du journal déjà purgée ?
- Existe-t-il réellement un topic unique suffisant à votre projection ?
- Que mesurez-vous lorsque vous comparez l'état avant et après reconstruction ?
- À quelles conditions un témoin extérieur révèle-t-il une omission ?

## Dimension théorique

Formalisez la projection comme une fonction du journal borné et de ses dépendances. Énoncez les conditions de déterminisme, complétude et idempotence, puis reliez chaque condition à un test ou à un contre-exemple. Comparez projection des décisions historiques et recalcul depuis les entrées. L'approfondissement théorique est encouragé ; utilisez le [guide des pistes de recherche](analyse-recherche-limos.md) pour les références et les orientations de contact.

## Preuves attendues

- le périmètre de projection et les dépendances ;
- les bornes du journal conservé et le manifeste attendu ;
- une comparaison automatisée des identités et du contenu métier ;
- la preuve qu'un second rejeu conserve le même état ;
- une chronologie avec les durées et les timeouts ;
- une conclusion explicite sur la rétention, les données hors topics et la complétude.

## Ressources

- [Sujet 1 — Idempotence Kafka/PostgreSQL](sujet-01-idempotence-kafka-postgresql.md)
- [Limite de `exactly-once` Kafka](../../docs/architecture/kafka-sub-exactly-once-limit.md)
- [Script de reset — destructif pour le journal, à ne pas utiliser dans cette expérience](../../scripts/reset-scenario.sh)
- [Activité 06 — Rejeu, offsets et rétention](../activite-06-rejeu-offsets-retention.md)
