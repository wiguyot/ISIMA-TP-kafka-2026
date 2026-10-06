# Tester le parcours sur une machine moins puissante

Ce protocole sert à vérifier dans quelles conditions les activités et les TP restent réalisables sur une machine disposant de moins de ressources que la machine de référence. Il aide à distinguer une limite de la machine d'un résultat pédagogique attendu.

Le premier passage doit conserver les commandes et les paramètres des fiches. Ne réduisez la charge que lors d'un second essai, si le premier est trop lent ou ne peut pas aboutir. Une variante allégée doit être signalée : elle peut ne plus démontrer la même propriété, notamment pour les essais de panne ou de réplication.

## 1. Décrire la machine

Complétez ces informations avant le test. Distinguez les ressources physiques de celles attribuées à Docker.

| Information | Valeur relevée |
| --- | --- |
| Date du test | |
| Système d'exploitation et version | |
| Processeur | |
| Nombre de cœurs disponibles | |
| Mémoire physique | |
| Mémoire attribuée à Docker, si configurable | |
| Nombre de processeurs attribués à Docker, si configurable | |
| Espace disque libre | |
| Version de Docker | |
| Version de Docker Compose | |
| Applications importantes actives pendant le test | |

Ne déduisez pas les ressources réellement attribuées à Docker à partir des seules caractéristiques de l'ordinateur : relevez-les séparément lorsque l'environnement permet de les configurer.

## 2. Vérifier l'état initial

Depuis la racine du dépôt :

```sh
docker compose version
docker compose ps
```

Lancez ensuite la plateforme selon les [instructions du dépôt](../README.md#démarrage). Vérifiez que les services attendus restent actifs et que les interfaces requises par la première activité répondent. Notez les services qui démarrent lentement, redémarrent ou restent indisponibles.

Pour observer la consommation des conteneurs pendant une étape :

```sh
docker stats --no-stream
```

Cette commande donne une observation ponctuelle. Si le problème est intermittent, relevez plusieurs observations pendant l'étape et notez l'heure correspondante.

Avant de commencer, vérifiez également qu'aucun scénario précédent ni aucune requête ksqlDB persistante ne perturbe l'essai. Suivez la procédure de remise à zéro prévue par le dépôt ; ne supprimez pas manuellement des volumes ou des données sans consigne explicite.

## 3. Rejouer le parcours

Exécutez les activités 01 à 10 dans l'ordre, puis les TP 01 à 04, en suivant leurs paramètres de référence. Remplissez une fiche de relevé pour toute étape qui est lente, échoue ou produit un résultat qui semble inattendu.

| Étape | Résultat pédagogique à vérifier | Statut |
| --- | --- | --- |
| Activité 01 | Architecture et état initial de la plateforme observés | |
| Activité 02 | Messages publiés et lus | |
| Activité 03 | Comportement PUB/SUB et offsets comparés | |
| Activité 04 | Répartition du travail entre consommateurs observée | |
| Activité 05 | Clé et partition associée vérifiées | |
| Activité 06 | Reprise, rejeu et offsets interprétés | |
| Activité 07 | Rejets métier observés et expliqués | |
| Activité 08 | Métriques et état des services consultés | |
| Activité 09 | Charge, lag et temporisation observés | |
| Activité 10 | Parcours d'une transaction reconstitué | |
| TP 01 | Risque de perte avec at-most-once caractérisé | |
| TP 02 | Risque de doublon avec at-least-once caractérisé | |
| TP 03 | Périmètre et limites d'exactly-once établis | |
| TP 04 | Requêtes ksqlDB exécutées et résultats interprétés | |

Valeurs de statut conseillées : `fonctionne`, `lent mais valide`, `saturation probable`, `échec`, `non testé`.

## 4. Fiche de relevé d'une étape

Copiez cette fiche pour chaque difficulté observée.

| Élément | Relevé |
| --- | --- |
| Activité ou TP | |
| Scénario, commande et paramètres utilisés | |
| Résultat attendu selon la fiche | |
| Résultat observé | |
| État des services (`docker compose ps`) | |
| CPU et mémoire des conteneurs (`docker stats`) | |
| Symptôme : lenteur, redémarrage, expiration, interface indisponible… | |
| Action tentée et résultat | |
| Conclusion : valide, saturation probable ou échec reproductible | |

## 5. Interpréter les résultats

Ne cherchez pas nécessairement à reproduire exactement les mêmes compteurs qu'une autre exécution. Certains volumes et répartitions varient. Vérifiez en priorité les invariants demandés dans la fiche : par exemple, si les messages sont finalement traités, si le lag converge, si des doublons sont absorbés, ou si une perte est effectivement constatée.

Un bilan observé avant la fin du traitement peut être incomplet. Avant de conclure à une perte, attendez la stabilisation indiquée par la fiche et vérifiez de nouveau les compteurs. Un retard de traitement (backlog) n'est pas, à lui seul, une perte.

Classez chaque difficulté selon les critères suivants :

- **Fonctionne normalement** : les critères de réussite sont atteints.
- **Lent mais valide** : le résultat attendu arrive, mais l'attente ou la faible réactivité gêne le travail.
- **Saturation locale probable** : les ressources disponibles sont très sollicitées, des services redémarrent ou des commandes expirent pendant que la machine est sous charge. C'est un indice, pas une preuve de la cause.
- **Échec reproductible** : après vérification de l'état des services et respect des étapes de la fiche, le critère de réussite n'est toujours pas atteint.
- **État à remettre au propre** : un essai précédent, des offsets ou des requêtes persistantes influencent le résultat courant.

Une commande qui expire ne suffit pas à conclure à un défaut Kafka. Consignez l'état des conteneurs, les ressources observées et la possibilité de reproduire le problème après remise en état.

## 6. Essayer une variante allégée

Si l'étape de référence est trop lente ou bloque la machine :

1. Conservez le relevé de l'essai de référence.
2. Réduisez un seul paramètre de charge à la fois, par exemple le nombre de messages ou la cadence d'émission, si le scénario le permet.
3. Rejouez l'étape et notez le paramètre modifié, le résultat et les ressources observées.
4. Indiquez explicitement ce que la variante permet encore de vérifier.

Ne réduisez pas silencieusement le nombre de brokers ni le facteur de réplication dans un exercice portant sur les pannes, la disponibilité ou la réplication : le comportement démontré pourrait changer. Une telle variante doit être présentée comme une expérience différente, avec ses limites.

## 7. Bilan du test

À la fin du rejeu, classez chaque étape dans l'une de ces catégories :

| Catégorie | Étapes | Consigne ou adaptation nécessaire |
| --- | --- | --- |
| Réalisable avec les paramètres de référence | | |
| Réalisable, mais lente ou avec une attente à préciser | | |
| Nécessite une variante allégée | | |
| Non concluante ou en échec reproductible | | |

Pour chaque adaptation proposée, consignez le symptôme, les éléments qui suggèrent une limite de ressources, le changement minimal essayé et les conclusions pédagogiques qui restent valides. Ne présentez pas une configuration comme « minimale » ou « compatible » avant de l'avoir testée et d'avoir indiqué les conditions de ce test.
