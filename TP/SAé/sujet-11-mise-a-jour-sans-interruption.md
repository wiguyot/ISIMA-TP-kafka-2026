# Sujet 11 — Mettre à jour un service sans interruption de service

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Reconstruire un service (`docker compose up -d --build <service>`) arrête le conteneur, puis le redémarre. Pour un consommateur Kafka, cet arrêt déclenche un **rebalance** : les partitions qu'il traitait sont redistribuées aux autres membres du groupe, puis rendues à son retour.

Un rebalance n'est pas un incident : c'est une opération normale d'exploitation. Mais c'est une fenêtre de risque — message en cours de traitement au moment de l'arrêt, offset non encore validé, doublon possible au rejeu. Une mise à jour doit être invisible pour le métier, et cette invisibilité doit être prouvée pendant l'opération, pas seulement constatée après.

Une mise à jour n'est pas toujours neutre pour le format des messages. Pendant l'opération, la version ancienne et la version nouvelle du service cohabitent, et les topics transportent temporairement deux formats : un contrat d'événement modifié en même temps que le déploiement crée des fenêtres d'incompatibilité qu'il faut anticiper.

## Votre mission

Construisez un protocole automatisé qui met à jour un service consommateur **en pleine charge**, observe le rebalance, et vérifie la continuité métier : aucun paiement perdu, aucun doublon d'effet, retard résorbé après reprise.

## Réalisation minimale attendue

Un script qui recharge ou redémarre un service sous flux (par exemple `pix-validator` ou un persister), collecte les métriques pendant l'opération et vérifie l'état final : continuité des `transaction_id` traités, absence de doublon d'effet métier, lag résorbé. Le script doit échouer explicitement si la continuité n'est pas respectée.

## Actions à réaliser

1. Définissez la garantie de continuité que vous voulez démontrer, en une phrase.
2. Observez un rebalance en situation contrôlée : quelles partitions changent de main, pendant combien de temps, quel message était en cours.
3. Provoquez la mise à jour sous flux nominal, puis sous scénario de charge.
4. Comparez le comportement avec les différentes sémantiques de livraison : que change `at-most-once` par rapport à `at-least-once` pendant un rebalance ?
5. Vérifiez la continuité dans PostgreSQL : trous de `transaction_id`, doublons, lignes incohérentes.
6. Répétez l'opération plusieurs fois : le résultat est-il stable ?
7. Documentez la procédure de mise à jour sûre d'un service : conditions préalables, opération, vérifications après.

## Extension : changer le contrat pendant la mise à jour

La mise à jour d'un service est le moment idéal pour faire évoluer un contrat d'événement — et le moment le plus dangereux. Pendant l'opération, deux versions du service coexistent : le nouveau consommateur peut lire des messages écrits par l'ancien producteur, et l'ancien consommateur (encore en vie pendant le rebalance) peut lire des messages du nouveau producteur.

Faites évoluer un champ d'un événement du pipeline **et** déployez le service à chaud, en suivant ces étapes d'analyse :

1. **Choisissez la stratégie de compatibilité** de votre évolution : rétrocompatible (l'ancien consommateur lit le nouveau format), avant-compatible (le nouveau lit l'ancien), ou incompatible. Consultez le sujet 3 pour la théorie de la compatibilité : ici, l'angle est différent — *quand* changer le format par rapport à *quand* redémarrer les services.
2. **Déduisez l'ordre de déploiement** de la compatibilité choisie : consumer d'abord ou producteur d'abord ? Justifiez, puis démontrez ce que casse l'ordre inverse.
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

Votre sujet porte des aspects théoriques formalisables : le rebalance de groupe de consommateurs (déclenchement, redistribution des partitions, message en cours), les fenêtres de risque d'une mise à jour roulante, et — si vous réalisez l'extension — la compatibilité de schéma et les fenêtres d'incompatibilité pendant un déploiement. Approfondissez-les : formalisez le protocole de rebalance observé, les invariants que votre mise à jour doit préserver, et la condition de compatibilité qui rend votre ordre de déploiement sûr. Consultez [l'analyse recherche](analyse-recherche-limos.md) pour la référence détaillée : les politiques de maintenance et de continuité d'activité relèvent de l'axe [ODPS](https://www.limos.fr/axes/3) du LIMOS. Cet approfondissement fait partie de l'évaluation. L'aspect identifié ici n'est pas exhaustif : votre réalisation peut révéler d'autres aspects théoriques, à approfondir et à signaler également.

## Preuves attendues

- une chronologie du rebalance : détection, redistribution des partitions, reprise ;
- des compteurs métier avant, pendant et après l'opération ;
- la preuve d'absence de perte et de doublon, fondée sur PostgreSQL et pas seulement sur les offsets ;
- une comparaison du comportement selon au moins deux sémantiques de livraison ;
- une procédure de mise à jour rejouable par un autre groupe.

Si vous réalisez l'extension :

- une démonstration de cohabitation des deux formats pendant la mise à jour, avec l'ordre de déploiement justifié et le contre-exemple de l'ordre inverse ;
- le résultat de l'expérience de message invalide pendant la mise à jour, et sa conséquence sur votre procédure.

## Ressources

- [Activité 04 — Groupes de consommateurs](../activite-04-groupes-consommateurs.md)
- [TP 01 à 03 — Sémantiques Kafka](../README.md)
- [Sujet 3 — Évolution des contrats d'événements](sujet-03-contrats-evenements.md)
- [Contrats d'événements](../../docs/contrats-evenements/)
- [Protocole d'arrêt de validation](../../docs/architecture/pix-validation-stop-protocol.md)
- [Guide développeur — reconstruction d'un service](../../docs/developpement/guide-developpeur.md)
