# Sujet 11 — Mettre à jour un service avec une continuité métier mesurée

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Reconstruire un service (`docker compose up -d --build <service>`) remplace son conteneur. Un changement des membres du groupe consommateur entraîne une réaffectation des partitions. Si le groupe n'a plus aucun membre actif, les messages attendent la reprise : aucune autre instance ne les traite pendant cette pause.

Un rebalance est une opération normale, avec des fenêtres de risque : message en cours, offset non validé, rejeu possible. L'absence de perte après reprise ne prouve pas l'absence de pause pendant l'opération ; mesurez ces deux propriétés séparément.

Une mise à jour peut aussi modifier le contrat. Les anciens messages restent dans les topics ; la cohabitation de deux versions actives nécessite un déploiement prévu à cet effet et n'est pas produite automatiquement par la commande de reconstruction.

## Socle fourni et contribution nouvelle

Le Compose fournit par défaut une instance par service ; plusieurs workers dans le même conteneur s'arrêtent ensemble. Votre contribution est un remplacement sous trafic assorti d'un oracle de continuité et d'une mesure de pause. Un déploiement roulant sans pause demande plusieurs instances, une orchestration et une capacité disponible qui ne sont pas fournies par défaut.

## Votre mission

Construisez un protocole automatisé qui met à jour un service consommateur **en pleine charge**, observe le rebalance, et vérifie la continuité métier : aucun paiement perdu, aucun doublon d'effet, retard résorbé après reprise.

## Réalisation minimale attendue

Un script qui remplace un service sous flux, mesure la pause de traitement et le retard maximal, puis vérifie l'état après drainage borné : toutes les identités et valeurs attendues, absence de second effet métier et respect de la borne de continuité choisie. Cette borne peut être une durée de pause, un délai de reprise ou un SLA. Le script doit échouer sur dépassement ou divergence.

Le minimum démontre une **reprise avec continuité métier bornée**. Il ne peut être présenté comme « sans interruption » si une pause est observée.

## Actions à réaliser

1. Définissez la garantie de continuité que vous voulez démontrer, en une phrase.
2. Observez un rebalance en situation contrôlée : quelles partitions changent de main, pendant combien de temps, quel message était en cours.
3. Provoquez la mise à jour sous flux nominal, puis sous scénario de charge.
4. Comparez le comportement avec les différentes sémantiques de livraison : que change `at-most-once` par rapport à `at-least-once` pendant un rebalance ?
5. Vérifiez la continuité dans PostgreSQL : trous de `transaction_id`, doublons, lignes incohérentes.
6. Répétez l'opération plusieurs fois : le résultat est-il stable ?
7. Documentez la procédure de mise à jour sûre d'un service : conditions préalables, opération, vérifications après.

## Extensions facultatives

### Déploiement roulant

Déployez au moins deux instances actives dans le même groupe, avec des noms de conteneur et ports compatibles, puis remplacez-les successivement. Vérifiez la capacité pendant le remplacement et la continuité pendant toute l'opération. Chaque producteur transactionnel actif doit avoir un `transactional.id` distinct, stable à son propre redémarrage ; deux instances partageant cet identifiant peuvent se neutraliser par fencing.

### Changer le contrat pendant la mise à jour

Étudiez une évolution de contrat pendant le remplacement. Si vous annoncez deux versions simultanément actives, construisez cette cohabitation explicitement. Dans tous les cas, les messages anciens conservés doivent rester lisibles selon la politique retenue.

Faites évoluer un champ d'un événement du pipeline **et** déployez le service à chaud, en suivant ces étapes d'analyse :

1. **Choisissez la compatibilité** : `BACKWARD` = nouveau consommateur lisant l'ancien format ; `FORWARD` = ancien consommateur lisant le nouveau ; `FULL` = les deux. Construisez la matrice de versions du sujet 3.
2. **Justifiez l'ordre de déploiement**, en tenant compte des messages déjà stockés. Ne cherchez un contre-exemple de l'ordre inverse que si la compatibilité est asymétrique : une évolution FULL peut autoriser les deux ordres.
3. **Prouvez la cohabitation** : pendant la mise à jour, capturez des messages aux deux formats dans le même topic et montrez comment chaque version de service les traite.
4. **Recherchez la vérification à l'exécution** : dans la plateforme, aucun registre de schéma ne contrôle les messages. Démontrez-le par une expérience — poussez un message au format invalide pendant la mise à jour et observez le comportement du consommateur (rejet silencieux, crash, boucle ?). Ce constat doit nourrir votre procédure de mise à jour sûre : qui vérifie le contrat, et quand ?

## Questions de conception

- Que se passe-t-il pour le message en cours de traitement au moment de l'arrêt du conteneur ?
- Pourquoi le rebalance prend-il du temps, et que devient le flux pendant ce temps ?
- Un trou de `transaction_id` dans PostgreSQL est-il une perte ? Comment le distinguer d'un rejet métier ?
- La garantie de continuité dépend-elle de la sémantique de livraison configurée ?
- Quelle différence entre redémarrer un producteur et redémarrer un consommateur ?
- Pendant la mise à jour, que devient un message au format ancien traité par la nouvelle version du service — et l'inverse, pendant la fenêtre du rebalance ?
- L'ordre de déploiement (consommateur d'abord ou producteur d'abord) se déduit-il de la compatibilité de schéma choisie ? Que se passe-t-il si on l'inverse ?

## Dimension théorique

Formalisez la continuité comme des invariants métier accompagnés d'une borne temporelle. Décrivez l'appartenance au groupe, les réaffectations et le cas où aucun consommateur n'est actif. Pour les extensions, ajoutez capacité pendant remplacement, unicité des identifiants transactionnels et matrice de compatibilité. Une analogie avec la maintenance industrielle ne décrit pas le protocole Kafka. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- une chronologie du rebalance : détection, redistribution des partitions, reprise ;
- la pause de traitement et le retard maximal mesurés, comparés à la borne de continuité annoncée ;
- des compteurs métier avant, pendant et après l'opération ;
- la preuve d'absence de perte et de doublon, fondée sur PostgreSQL et pas seulement sur les offsets ;
- une comparaison du comportement selon au moins deux sémantiques de livraison ;
- une procédure de mise à jour rejouable par un autre groupe.

Si vous réalisez l'extension :

- la matrice de compatibilité, l'ordre justifié et, si la compatibilité est asymétrique, un contre-exemple de l'ordre inverse ;
- la preuve de cohabitation si plusieurs versions actives sont annoncées ;
- le résultat de l'expérience de message invalide pendant la mise à jour, et sa conséquence sur votre procédure.

## Ressources

- [Activité 04 — Groupes de consommateurs](../activite-04-groupes-consommateurs.md)
- [TP 01 à 03 — Sémantiques Kafka](../README.md)
- [Sujet 3 — Évolution des contrats d'événements](sujet-03-contrats-evenements.md)
- [Contrats d'événements](../../docs/contrats-evenements/)
- [Protocole d'arrêt de validation](../../docs/architecture/pix-validation-stop-protocol.md)
- [Guide développeur — reconstruction d'un service](../../docs/developpement/guide-developpeur.md)
