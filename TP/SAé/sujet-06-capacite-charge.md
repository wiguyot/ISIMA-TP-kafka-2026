# Sujet 6 — Qualifier un déploiement et identifier ses limites de capacité

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Un pipeline Kafka répartit les messages dans des **partitions**. Un groupe de consommateurs peut traiter plusieurs partitions en parallèle, mais il ne peut pas créer plus de parallélisme que le nombre de partitions disponibles. La forme du trafic compte aussi : un même volume est plus difficile à absorber s'il arrive en quelques secondes plutôt que sur plusieurs minutes.

Il n'existe toutefois pas un débit maximal valable pour toutes les machines. Le résultat dépend notamment des ressources allouées aux conteneurs, du processeur, de la mémoire, du stockage, du réseau, de la configuration Kafka et PostgreSQL, ainsi que de la version du projet. Une valeur mesurée sur un poste ne peut donc pas être directement transposée à un nouveau déploiement.

Le projet fournit des scénarios réguliers, aléatoires et concentrés. Votre travail consiste à construire une méthode et un outil exécutable qui qualifient rapidement un déploiement donné et indiquent par des mesures où se situe sa première limite. L'usage visé est opérationnel : on lance l'outil, quelques minutes plus tard on obtient des réglages prêts à copier dans l'interface de pilotage du simulateur, chacun justifié par une mesure.

## Votre mission

Réalisez un outil ou un script de qualification qui, face à un nouveau déploiement de `simul-pix`, produit un rapport répondant aux questions suivantes :

- quelle configuration matérielle et logicielle a été testée ;
- quelles charges ont été absorbées sans dégrader la garantie métier choisie ;
- à quel palier le système commence à accumuler durablement du retard ou à dégrader son délai ;
- quel composant est probablement le premier goulot d'étranglement ;
- quelles vérifications ou modifications permettraient de confirmer ce diagnostic.

L'objectif n'est pas d'annoncer une capacité universelle. Le résultat attendu est une enveloppe de fonctionnement documentée pour un déploiement précis et une méthode réutilisable sur le suivant.

Votre qualification a aussi une finalité collective : elle produit les **valeurs de base** dont les autres SAé ont besoin pour calibrer leurs campagnes sans écraser la machine — respect d'un SLA (sujet 4), partage entre plusieurs émetteurs (sujet 12), coût du exactly-once (sujet 13), comparaison de modèles de trafic (sujet 14). Vos recommandations doivent donc être exprimées dans les réglages réels du simulateur, pas dans une unité que personne ne consomme.

## Réalisation minimale attendue

Développez un script de qualification qui lance une campagne par paliers, collecte les métriques utiles et produit un rapport structuré. Il doit accepter des paramètres de charge et des critères de conformité, enregistrer le contexte du déploiement testé, puis signaler le premier palier non conforme. La campagne est **bornée dans le temps** (borne indicative : 10 minutes ; la durée de chaque palier et le nombre de paliers sont des paramètres de l'outil). À l'issue, l'outil produit automatiquement un tableau de réglages proposés pour l'interface de pilotage — scénario, `total_messages`, `rate_per_second`, `traffic_model`, sémantiques producteur et consommateur — chaque valeur accompagnée d'une justification courte et de sa nature (mesurée ou déduite avec marge). Réutilisez les scripts de scénario et les métriques existants lorsque cela est pertinent ; ne dupliquez pas le pipeline de mesure.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Définissez les critères de bon fonctionnement : délai maximal accepté, lag toléré, temps de drainage, intégrité de la persistance et absence de perte ou de duplication non maîtrisée. Ces critères doivent être configurables ; ne fixez pas une valeur universelle sans l'associer à une attente métier.
2. Relevez automatiquement, ou documentez dans le rapport, le contexte du test : version du projet, configuration des services, nombre de partitions et de workers, ressources allouées aux conteneurs, machine et date de l'essai.
3. Construisez une campagne par paliers : charge initiale faible, augmentation progressive du débit, puis arrêt lorsque les critères de bon fonctionnement ne sont plus respectés. Rejouez au moins un palier pour écarter une conclusion fondée sur une variation ponctuelle.
4. Comparez au moins un trafic régulier et un trafic concentré. À chaque palier, laissez le pipeline se vider avant de conclure qu'il a absorbé la charge.
5. Mesurez le débit réellement émis et traité, le lag maximal et son évolution, le temps de drainage, le délai de décision, la persistance finale et les ressources des principaux conteneurs.
6. Déterminez le premier goulot observé. Distinguez au minimum : générateur incapable d'émettre la charge demandée, consommateurs applicatifs saturés, Kafka saturé ou PostgreSQL limitant la persistance. Appuyez chaque diagnostic sur des indicateurs observables, pas sur une seule métrique.
7. Modifiez un paramètre de capacité réellement disponible dans le projet, rejouez le même cas et indiquez si le goulot s'est déplacé, a disparu ou est resté identique.
8. Définissez la sortie opérationnelle de l'outil : le format du tableau de réglages proposés (champs du formulaire de l'interface, unités, plage de valeurs), la règle de marge de sécurité entre la limite mesurée et la valeur proposée, et la mention explicite de ce qui est mesuré contre ce qui est extrapolé. Justifiez votre marge : elle n'est ni nulle (risque de saturer dès l'usage nominal) ni arbitraire.
9. Générez ce tableau automatiquement en fin de campagne, puis **démontrez la boucle complète** : copier les réglages dans l'interface de pilotage, lancer le scénario correspondant, vérifier que le critère de bon fonctionnement est respecté sans saturation. C'est cette boucle, pas le rapport, qui prouve l'usage opérationnel.
10. Faites produire à votre outil un rapport exploitable par un autre groupe : contexte, protocole, données mesurées, critères retenus, premier palier non conforme, diagnostic, limites de confiance et valeurs de base recommandées.

## Questions de conception

- Pourquoi un nombre de messages par seconde mesuré sur une machine ne constitue-t-il pas une recommandation de capacité générale ?
- Comment choisissez-vous les paliers et la durée des essais pour distinguer une saturation durable d'un pic temporaire ?
- Quel indicateur montre que le pipeline rattrape le flux, plutôt qu'il ne fait que l'accumuler ?
- Pourquoi ajouter des workers ne garantit-il pas toujours une accélération ?
- Quel est le lien entre la clé métier, l'ordre des messages et la répartition de charge ?
- Comment distinguez-vous une limite du générateur, de Kafka, d'un consommateur applicatif et de PostgreSQL ?
- Quelles données doivent figurer dans le rapport pour pouvoir comparer deux déploiements sans confondre leurs contextes ?
- Vos valeurs de base restent-elles valables pour un trafic différent de celui mesuré (par exemple un modèle `bursty` quand vous avez qualifié en `poisson`) ? Sinon, que faut-il requalifier ?
- Qui fixe la marge de sécurité entre la limite mesurée et la valeur proposée, et sur quel argument ? Que se passe-t-il si cette marge est trop faible, ou trop grande ?

## Dimension théorique

Votre sujet porte des aspects théoriques formalisables : les lois de saturation (pourquoi le débit ne linéarise pas avec le parallélisme, loi d'Amdahl), la distinction pic temporaire contre saturation durable, et la loi de Little appliquée au drainage. Approfondissez-les : formalisez la limite de capacité que votre campagne révèle. Consultez [l'analyse recherche](analyse-recherche-limos.md) pour la référence détaillée : l'évaluation de performances par simulation est une méthodologie centrale de l'axe [ODPS](https://www.limos.fr/axes/3) du LIMOS. Cet approfondissement fait partie de l'évaluation. L'aspect identifié ici n'est pas exhaustif : votre réalisation peut révéler d'autres aspects théoriques, à approfondir et à signaler également.

## Preuves attendues

- un outil ou script de qualification rejouable, avec ses paramètres documentés ;
- un rapport décrivant le déploiement réellement testé et les critères de bon fonctionnement retenus ;
- une campagne par paliers, ses données recueillies et une comparaison entre trafic régulier et concentré ;
- l'identification argumentée du premier goulot observé, étayée par plusieurs indicateurs ;
- une comparaison avant/après sur un paramètre de capacité ;
- une enveloppe de fonctionnement propre au déploiement testé, avec les limites de cette conclusion ;
- le tableau des réglages proposés pour l'interface de pilotage, chaque valeur justifiée et distinguée comme mesurée ou extrapolée avec marge ;
- la démonstration de la boucle opérationnelle complète : outil lancé, tableau obtenu en quelques minutes, réglages appliqués dans l'interface, scénario tenu sans saturation ;
- une vérification de la cohérence de la persistance après chaque essai.

## Ressources

- [Activité 04 — Consumer groups](../activite-04-groupes-consommateurs.md)
- [Activité 05 — Partitionnement](../activite-05-partitionnement-cle-emetteur.md)
- [Activité 09 — Charge et temporisation](../activite-09-charge-et-temporisation.md)
- [Modèles de trafic](../../docs/architecture/traffic-models.md)
- [Campagne de sémantique existante](../../docs/experiences/semantics-campaign.md)
