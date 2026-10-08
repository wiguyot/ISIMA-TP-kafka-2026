# SAÉ Kafka — Instructions rapides

Cette SAÉ prolonge les TP Kafka de `simul-pix`. Vous choisissez un problème d'architecture événementielle, vous développez une solution, puis vous démontrez son comportement par des essais reproductibles.

Les sujets détaillés sont disponibles dans le [portefeuille de SAÉ](SAé/README.md).

## 1. Préparez-vous

1. Réalisez les [activités 01 à 10](README.md) pour maîtriser les bases de Kafka.
2. Si votre sujet porte sur les garanties de livraison, la reconstruction ou l'exactly-once, réalisez aussi les TP 01 à 03.
3. Si votre sujet porte sur la sécurité ou le partage du cluster, lisez la configuration Kafka du projet (`docker-compose.yml`, `infra/kafka/`).
4. Si votre sujet porte sur les modèles de trafic, maîtrisez l'activité 09 et les scénarios de charge avant de modifier le shaper.
5. Démarrez la plateforme et vérifiez qu'un flux nominal fonctionne avant toute modification.

## 2. Choisissez votre sujet

1. Consultez le [portefeuille](SAé/README.md).
2. Choisissez un sujet principal adapté au niveau du groupe, au temps disponible et aux prérequis.
3. Lisez entièrement la problématique associée avant de modifier le code, y compris sa section « Dimension théorique » : l'approfondissement de cet aspect théorique fait partie de l'évaluation. Consultez [l'analyse recherche](SAé/analyse-recherche-limos.md) dès le choix du sujet pour identifier l'aspect, l'axe LIMOS concerné et le travail attendu.
4. Formulez, en une phrase, le problème que votre groupe va résoudre et la garantie recherchée.

Un sujet ne peut être choisi par plusieurs groupes. Un groupe doit être constitué de quatre personnes au minimum ; au-delà, le périmètre du sujet doit augmenter en conséquence.

## 3. Réalisez votre SAÉ

1. Observez l'architecture et établissez une mesure de référence.
2. Concevez votre évolution : contrats, topics, persistance, métriques et comportement en cas d'erreur.
3. Implémentez par petites étapes et ajoutez les tests nécessaires.
4. Exécutez un flux nominal, puis le scénario de charge ou de panne correspondant à votre sujet.
5. Vérifiez le résultat à la fois dans Kafka, PostgreSQL et les outils d'observabilité.

## 4. Constituez vos preuves

Votre rendu doit permettre à une autre personne de reproduire votre conclusion. Conservez :

- la note de conception et les choix techniques ;
- le code, les migrations et la configuration modifiés ;
- les commandes et le protocole d'essai ;
- les résultats de tests et les compteurs observés ;
- une conclusion qui décrit la garantie obtenue et ses limites.

## 5. Publiez votre projet sur GitHub

Dès la première séance, créez un **fork privé** du dépôt de la plateforme [`ISIMA-TP-kafka-2026`](https://github.com/wiguyot/ISIMA-TP-kafka-2026) et travaillez-y : il sert de rendu final et de support à la soutenance.

1. Créez le fork en visibilité **privée** (option de visibilité au moment du fork), puis invitez **williamguyotlenat@icloud.com** comme collaborateur.
2. Publiez la plateforme avec vos évolutions : code, configuration, migrations, scripts, note de conception et preuves. Ajoutez un `README` : sujet, garantie visée, démarche pour lancer la plateforme et rejouer les essais.
3. Committez régulièrement avec des messages explicites : l'historique fait partie de l'évaluation.
4. Vérifiez que le `.gitignore` exclut bien `infra/kafka/secrets/` et `runtime/` avant la première publication.
5. **La veille de la soutenance** : passez le fork en visibilité **publique** et transmettez son URL à l'enseignant — un fork privé, vide ou inaccessible à ce moment équivaut à un rendu manquant.

Les détails (contenu attendu, règles d'évaluation) figurent dans la section « Rendu sur GitHub » du [portefeuille de SAÉ](SAé/README.md).

## Règles importantes

- Un offset Kafka seul ne prouve pas que la donnée métier est correctement persistée.
- Un flux nominal qui fonctionne ne prouve pas la résistance aux rejouements, aux pannes ou à la charge.
- Ne nommez pas une garantie `exactly-once` sans préciser son périmètre : Kafka vers Kafka, Kafka vers PostgreSQL, ou effet métier.
- Toute évolution de contrat, de topic ou de base de données doit être documentée et testée.

Consultez le [portefeuille de SAÉ](SAé/README.md) pour les sujets, la charge prévue, les tailles de groupes conseillées et les ressources de chaque problématique.
