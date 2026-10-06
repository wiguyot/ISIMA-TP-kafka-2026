# Sujet 6 — Qualifier un déploiement et identifier ses limites de capacité

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Un pipeline Kafka répartit les messages dans des **partitions**. Un groupe de consommateurs peut traiter plusieurs partitions en parallèle, mais il ne peut pas créer plus de parallélisme que le nombre de partitions disponibles. La forme du trafic compte aussi : un même volume est plus difficile à absorber s'il arrive en quelques secondes plutôt que sur plusieurs minutes.

Il n'existe toutefois pas un débit maximal valable pour toutes les machines. Le résultat dépend notamment des ressources allouées aux conteneurs, du processeur, de la mémoire, du stockage, du réseau, de la configuration Kafka et PostgreSQL, ainsi que de la version du projet. Une valeur mesurée sur un poste ne peut donc pas être directement transposée à un nouveau déploiement.

Le projet fournit des scénarios réguliers, aléatoires et concentrés. Votre travail consiste à construire une méthode, idéalement accompagnée d'un outil exécutable, qui qualifie rapidement un déploiement donné et indique par des mesures où se situe sa première limite.

## Votre mission

Réalisez un outil ou un script de qualification qui, face à un nouveau déploiement de `simul-pix`, produit un rapport répondant aux questions suivantes :

- quelle configuration matérielle et logicielle a été testée ;
- quelles charges ont été absorbées sans dégrader la garantie métier choisie ;
- à quel palier le système commence à accumuler durablement du retard ou à dégrader son délai ;
- quel composant est probablement le premier goulot d'étranglement ;
- quelles vérifications ou modifications permettraient de confirmer ce diagnostic.

L'objectif n'est pas d'annoncer une capacité universelle. Le résultat attendu est une enveloppe de fonctionnement documentée pour un déploiement précis et une méthode réutilisable sur le suivant.

## Réalisation minimale attendue

Développez un script de qualification qui lance une campagne par paliers, collecte les métriques utiles et produit un rapport structuré. Il doit accepter des paramètres de charge et des critères de conformité, enregistrer le contexte du déploiement testé, puis signaler le premier palier non conforme. Réutilisez les scripts de scénario et les métriques existants lorsque cela est pertinent ; ne dupliquez pas le pipeline de mesure.

## Actions à réaliser

1. Définissez les critères de bon fonctionnement : délai maximal accepté, lag toléré, temps de drainage, intégrité de la persistance et absence de perte ou de duplication non maîtrisée. Ces critères doivent être configurables ; ne fixez pas une valeur universelle sans l'associer à une attente métier.
2. Relevez automatiquement, ou documentez dans le rapport, le contexte du test : version du projet, configuration des services, nombre de partitions et de workers, ressources allouées aux conteneurs, machine et date de l'essai.
3. Construisez une campagne par paliers : charge initiale faible, augmentation progressive du débit, puis arrêt lorsque les critères de bon fonctionnement ne sont plus respectés. Rejouez au moins un palier pour écarter une conclusion fondée sur une variation ponctuelle.
4. Comparez au moins un trafic régulier et un trafic concentré. À chaque palier, laissez le pipeline se vider avant de conclure qu'il a absorbé la charge.
5. Mesurez le débit réellement émis et traité, le lag maximal et son évolution, le temps de drainage, le délai de décision, la persistance finale et les ressources des principaux conteneurs.
6. Déterminez le premier goulot observé. Distinguez au minimum : générateur incapable d'émettre la charge demandée, consommateurs applicatifs saturés, Kafka saturé ou PostgreSQL limitant la persistance. Appuyez chaque diagnostic sur des indicateurs observables, pas sur une seule métrique.
7. Modifiez un paramètre de capacité réellement disponible dans le projet, rejouez le même cas et indiquez si le goulot s'est déplacé, a disparu ou est resté identique.
8. Faites produire à votre outil un rapport exploitable par un autre groupe : contexte, protocole, données mesurées, critères retenus, premier palier non conforme, diagnostic et limites de confiance.

## Questions de conception

- Pourquoi un nombre de messages par seconde mesuré sur une machine ne constitue-t-il pas une recommandation de capacité générale ?
- Comment choisissez-vous les paliers et la durée des essais pour distinguer une saturation durable d'un pic temporaire ?
- Quel indicateur montre que le pipeline rattrape le flux, plutôt qu'il ne fait que l'accumuler ?
- Pourquoi ajouter des workers ne garantit-il pas toujours une accélération ?
- Quel est le lien entre la clé métier, l'ordre des messages et la répartition de charge ?
- Comment distinguez-vous une limite du générateur, de Kafka, d'un consommateur applicatif et de PostgreSQL ?
- Quelles données doivent figurer dans le rapport pour pouvoir comparer deux déploiements sans confondre leurs contextes ?

## Preuves attendues

- un outil ou script de qualification rejouable, avec ses paramètres documentés ;
- un rapport décrivant le déploiement réellement testé et les critères de bon fonctionnement retenus ;
- une campagne par paliers, ses données recueillies et une comparaison entre trafic régulier et concentré ;
- l'identification argumentée du premier goulot observé, étayée par plusieurs indicateurs ;
- une comparaison avant/après sur un paramètre de capacité ;
- une enveloppe de fonctionnement propre au déploiement testé, avec les limites de cette conclusion ;
- une vérification de la cohérence de la persistance après chaque essai.

## Ressources

- [Activité 04 — Consumer groups](../activite-04-groupes-consommateurs.md)
- [Activité 05 — Partitionnement](../activite-05-partitionnement-cle-emetteur.md)
- [Activité 09 — Charge et temporisation](../activite-09-charge-et-temporisation.md)
- [Modèles de trafic](../../docs/architecture/traffic-models.md)
- [Campagne de sémantique existante](../../docs/experiences/semantics-campaign.md)
