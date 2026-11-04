# Résumé des sujets et affectation des étudiants

Ce document accompagne le [portefeuille de SAÉ](README.md). La première partie résume chaque sujet en quelques lignes pour aider au choix. La seconde partie sert à consigner l'affectation des étudiants.

## Partie 1 — Résumé des sujets

### Sujet 1 — Idempotence entre Kafka et PostgreSQL (★★★★)

Rendre le traitement Kafka → PostgreSQL idempotent et observable. Le point de départ est la zone d'incertitude entre l'écriture PostgreSQL et le commit d'offset : selon le moment d'un arrêt, la donnée est perdue ou rejouée. Il faut mémoriser l'identité et le contenu immuable dans la même transaction que l'effet métier, puis distinguer premier traitement, rejeu identique absorbé et message de contenu différent portant la même identité. Extension : le pattern Inbox/Outbox pour publier un événement après l'écriture.

### Sujet 2 — Gérer un Pix invalide, de la détection à la correction (★★★★)

Construire la chaîne complète de gestion d'un Pix invalide : détection, classification par une taxonomie de rejets, rejet persisté, correction, rejeu contrôlé et décision finale traçable. Il faut distinguer rejet métier et quarantaine technique (message indécodable, identifié par topic, partition et offset), empêcher les boucles de rejeu et les doublons d'effet, et tracer le parcours complet du Pix initial à sa clôture.

### Sujet 3 — Faire évoluer un contrat d'événement sans casser le pipeline (★★★)

Formaliser un contrat d'événement exécutable (les exemples JSON fournis ne sont pas des schémas), le faire évoluer pour un besoin concret, puis adapter le producteur et tous les consommateurs du périmètre. La preuve passe par une matrice ancien/nouveau producteur et consommateur couvrant les compatibilités BACKWARD, FORWARD et FULL, avec vérification du contenu persistant jusqu'à PostgreSQL. Documenter l'ordre de déploiement qui évite la rupture. Extension : registre de schémas.

### Sujet 4 — Préserver le délai de décision d'un paiement Pix (★★★)

Choisir et annoncer l'une des deux garanties : détecter un dépassement du SLA (mesure et alerte configurable, avec faux positifs et dépassements non détectés) ou l'améliorer (action sur la capacité, la priorité ou la charge, comparée avant/après à charge réellement comparable). Les preuves exigent les p95/p99 avec effectifs et un bilan complet des entrées : décisions à temps, tardives et sans décision à l'échéance, rejets compris.

### Sujet 5 — Diagnostiquer un incident à partir des signaux du pipeline (★★★)

Construire une lecture d'incident qui distingue au moins trois situations : retard d'un consommateur, réplication Kafka dégradée, persistance PostgreSQL défaillante. La contribution est une règle ou un outil qui relie les signaux existants (météo, Grafana, sondes) pour lever une ambiguïté mesurée entre deux causes plausibles, avec un cas « indéterminé » et la vérification suivante proposée. Mesurer justesse du diagnostic, délai de détection et faux positifs.

### Sujet 6 — Qualifier un déploiement et identifier ses limites de capacité (★★★★)

Développer un outil de qualification borné (environ 10 minutes) qui lance une campagne par paliers de charge, identifie le premier goulot et produit un tableau de réglages prêts à copier dans l'interface de pilotage, chaque valeur justifiée comme mesurée ou extrapolée avec marge. La preuve finale est la boucle opérationnelle complète : outil lancé, réglages appliqués, scénario tenu sans saturation. L'outil fournit les valeurs de base dont les autres SAé ont besoin.

### Sujet 7 — Tester la résilience réseau et organiser la reprise (★★★★)

Construire une campagne couvrant au moins trois familles de panne réseau (rupture franche, intermittence, erreurs injectées, ralentissement inexpliqué), avec une hypothèse écrite à l'avance et des critères de rétablissement propres à chaque famille. Analyser le rôle de la retransmission TCP, qui rend certaines pertes invisibles au niveau applicatif. La campagne aboutit à un runbook : symptôme → cause probable → vérification → action.

### Sujet 8 — Ajouter un contrôle métier asynchrone au pipeline Pix (★★★★)

Créer un service consommateur autonome qui applique une règle métier justifiée (pas un simple contrôle de format), en choisissant son autorité : contrôle préalable dont le refus empêche l'acceptation finale, ou audit après décision. Câblage Docker, contrats, métriques et tests accepté/refus/rejeu sont attendus. Extension réaliste : simuler des comptes bancaires avec soldes, ce qui introduit ordre par compte, idempotence du solde et causalité temporelle.

### Sujet 9 — Sécuriser l'accès au cluster Kafka (★★★)

Appliquer le principe de moindre privilège : séparer les identités Kafka d'au moins trois services, activer le contrôle d'accès du cluster et définir les ACL couvrant Topics, Groups et TransactionalId. La preuve exige des erreurs d'autorisation explicites (un timeout ne prouve rien) et un flux nominal complet après durcissement, avec une matrice service → droits conforme au fonctionnement réel. Extensions : TLS et rotation des secrets.

### Sujet 10 — Reconstruire PostgreSQL depuis Kafka (★★★★)

Reconstruire la projection PostgreSQL des paiements acceptés et des rejets depuis `validated` et `rejected` vers une base cible vide et séparée, avec des groupes de consommation neufs et des bornes de journal par partition. La preuve est un oracle d'équivalence automatisé sur les identités et les valeurs métier normalisées, l'idempotence d'un second rejeu, et une conclusion sur la rétention et les données non reconstructibles.

### Sujet 11 — Mettre à jour un service avec une continuité métier mesurée (★★★)

Construire un protocole automatisé qui remplace un service consommateur en pleine charge, observe le rebalance, mesure la pause de traitement et le retard maximal, puis vérifie la continuité : aucune perte, aucun doublon d'effet, retard résorbé après reprise. Comparer le comportement selon au moins deux sémantiques de livraison. Le minimum est une « reprise avec continuité bornée », pas un « sans interruption » si une pause est observée. Extensions : déploiement roulant, changement de contrat à chaud.

### Sujet 12 — Partager le cluster entre plusieurs émetteurs (★★★★)

Introduire deux organisations émettrices A et B avec des profils de charge indépendants et simultanés, puis comparer au moins deux organisations de partage (clé métier enrichie, topic dédié, quotas Kafka). Pour chacune, mesurer B seul, A et B nominaux, puis A en pic et B nominal : isolation obtenue, débit global, équilibre des partitions. Un résultat montrant que le mécanisme ne protège pas B est recevable s'il conduit à une conclusion mesurée.

### Sujet 13 — Basculer la chaîne en exactly-once : coût réel et périmètre réel (★★★★)

Activer le mode exactly-once Kafka sur la chaîne, puis produire deux démonstrations mesurées : ce que le mode coûte (débit, délai de décision, comportement sous charge) et ce qu'il ne protège pas (un rejeu de persister reste absorbé par l'UPSERT, sans seconde ligne métier). Ajouter une matrice de fenêtres de panne déterministes, un test de fencing et une conclusion en trois garanties, une par segment de la chaîne. Prérequis indispensable : TP 01 à 03.

### Sujet 14 — Au-delà de Poisson : simuler la variabilité réelle du trafic (★★★★)

Caractériser les dépendances des modèles actuels, puis concevoir au moins deux nouveaux modèles de natures différentes (Poisson non homogène, Hawkes, superposition...), les implémenter dans le shaper, les valider statistiquement hors pipeline et les comparer à la référence à moyenne réalisée comparable, sur plusieurs seeds. Ce sujet comprend un échange scientifique à organiser avec l'enseignant et un chercheur du LIMOS, avec un entretien de remplacement prévu si aucun chercheur n'est disponible.

### Sujet 15 — Du flux à l'état : suivre les paiements avec ksqlDB (★★★★)

Construire avec ksqlDB une application de suivi des paiements acceptés : STREAM des paiements, TABLE du référentiel clients, jointure STREAM–TABLE, tables par identité et par émetteur, agrégation fenêtrée et interface avec requêtes pull et push. Chaque indicateur doit annoncer ce qu'il compte : messages reçus, paiements distincts ou activité sur une période. Les scénarios couvrent doublon, client absent, modification du référentiel, événement tardif et redémarrage. Prérequis : TP 04.

## Partie 2 — Affectation des étudiants

Consigner ci-dessous les noms des étudiants affectés à chaque sujet. Rappel des règles du [portefeuille](README.md) : un groupe compte quatre personnes au minimum, choisit un sujet principal, et un sujet ne peut être pris par plusieurs groupes. Au-delà de quatre étudiants, le périmètre du sujet augmente ou le groupe est scindé.

| SAé                                      | Étudiant 1     | Étudiant 2     | Étudiant 3     | Étudiant 4     |
| ---------------------------------------- | -------------- | -------------- | -------------- | -------------- |
| Sujet 1 — Idempotence Kafka/PostgreSQL   |                |                |                |                |
| Sujet 2 — DLQ et rejeu métier            | LACOTE Raphaël               | Diego CHAOUCHE               | Guilhot Alban               |                |
| Sujet 3 — Contrats d'événements          |                |                |                |                |
| Sujet 4 — SLA de décision                |                |                |                |                |
| Sujet 5 — Observabilité et diagnostic    | LACEB  ania                | DIAKITE Madina               |  Perrin Tom              |  djoumoi anayel              |
| Sujet 6 — Capacité et charge             | Muhamed Hodzic | Oumar KEBE                |                |                |
| Sujet 7 — Résilience réseau              |                |                |                |                |
| Sujet 8 — Contrôle métier asynchrone     |                |                |                |                |
| Sujet 9 — Sécurité du cluster            |                |                |                |                |
| Sujet 10 — Reconstruction PostgreSQL     | aya ahmamou               | ikram mokrani               |       ikram fakhraoui         |                |
| Sujet 11 — Mise à jour sans interruption | ALLILI Ayoub               | BELMOUDEN Saad               | Zakaria taoussi               | ibrahim EL HAOURARI               |
| Sujet 12 — Partage du cluster            |                |                |                |                |
| Sujet 13 — Exactly-once                  |                |                |                |                |
| Sujet 14 — Variabilité du trafic         |                |                |                |                |
| Sujet 15 — ksqlDB                        | yasser mounim               | Nada Ben Ali               |   Illyes Maouda             |                |
