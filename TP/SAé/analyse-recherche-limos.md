# Analyse recherche — dimensions théoriques et ancrage LIMOS

Document de travail enseignant. Il recense, pour chaque sujet du [portefeuille de SAÉ](README.md), les aspects théoriques présents ou implicites, les domaines de recherche associés, et la proximité avec les thèmes des équipes du [laboratoire LIMOS](https://www.limos.fr) (CNRS / Université Clermont Auvergne / Mines Saint-Étienne).

Objectif : identifier, pour chaque sujet, les aspects théoriques qui méritent d'être approfondis, afin de donner aux étudiants l'opportunité d'aller plus loin sur ces dimensions — et d'organiser les rencontres avec les chercheurs concernés. Cet approfondissement théorique fait partie de l'évaluation de la SAé, au même titre que la réalisation technique et les preuves expérimentales.

**Important : les aspects identifiés ici ne constituent pas une liste exhaustive.** Ils sont un point de départ : un groupe peut découvrir, au fil de sa réalisation, un autre aspect théorique de son sujet — ou un sujet du portefeuille peut en révéler un qui n'est pas listé. Les groupes sont invités à signaler ces découvertes à l'enseignant (et, le cas échéant, aux chercheurs rencontrés) afin qu'elles enrichissent ce document pour les promotions suivantes.

## Les trois axes du LIMOS

Chaque axe est porteur de thèmes de recherche dont certains recouvrent directement les sujets du portefeuille. Pour préparer une rencontre, partez de la page de l'axe (et de sa page thèmes) plutôt que d'un membre précis :

- **[MAAD](https://www.limos.fr/axes/1)** — Modèles et algorithmes d'aide à la décision : algorithmique et graphes, optimisation combinatoire, méta-modélisation et simulation (thème MOCA).
- **[SIC](https://www.limos.fr/axes/2)** — Systèmes d'information et communication : thèmes « [Données, services, intelligence](https://www.limos.fr/themes/3) » et « [Réseaux et sécurité](https://www.limos.fr/themes/4) ».
- **[ODPS](https://www.limos.fr/axes/3)** — Outils d'aide à la décision pour la production et les services : optimisation combinatoire, programmation mathématique, processus de décision markoviens, **simulation à événements discrets**, optimisation stochastique. Applications : manufacture, transport, santé.

## Tableau d'analyse des 14 sujets

Critères :
- **Proximité** : proximité du sujet avec les domaines de recherche identifiés (**** = théorie déjà formalisée et directement adossable à des travaux actifs, *** = aspects formalisables avec apport réel d'un chercheur, ** = théorie existante mais surtout appliquée).
- **LIMOS** : proximité avec les thèmes des équipes du laboratoire (**** = équipe ou thème directement concerné, *** = expertise présente sur un volet, ** = proximité indirecte, * = pas d'équipe visible).

| Sujet | Aspects théoriques | Domaines de recherche | Proximité | LIMOS (axe, thème, ancrage) |
|---|---|---|:---:|---|
| [1. Idempotence](sujet-01-idempotence-kafka-postgresql.md) | Sémantiques de livraison, Inbox/Outbox, `effectively once`, identité métier | Transactions, systèmes distribués fiables (idempotency keys) | *** | **[SIC](https://www.limos.fr/axes/2)** — thème « [Données, services, intelligence](https://www.limos.fr/themes/3) » : exécution fiable de services, tests — **\*\*** |
| [2. DLQ et rejeu métier](sujet-02-dlq-rejeu-metier.md) | Politiques de retry/backoff, limite de tentatives, boucles | Sûreté de fonctionnement, fiabilité logicielle | ** | **ODPS** — politiques de réessai formalisables en processus de décision markovien (méthodologie de l'axe) — **\*\*** |
| [3. Contrats d'événements](sujet-03-contrats-evenements.md) | Compatibilité de schéma, versionnement, transition de déploiement | Évolution de schémas, interopérabilité, génie logiciel | ** | **[SIC](https://www.limos.fr/axes/2)** — thème « [Données, services, intelligence](https://www.limos.fr/themes/3) » : interopérabilité, qualité logicielle — **\*\*** |
| [4. SLA de décision](sujet-04-sla-decision.md) | Loi de Little, lag ↔ délai, percentiles vs moyenne, surcharge | **Théorie des files d'attente**, évaluation de performances, capacity planning | **** | **[ODPS](https://www.limos.fr/axes/3)** — files d'attente, écoulements de flux, modélisation de la congestion dans les projets de l'axe — **\*\*\*\*** |
| [5. Observabilité d'incidents](sujet-05-observabilite-incidents.md) | Symptôme/cause/vérification, détection d'anomalie implicite | Détection d'anomalies, root cause analysis, AIOps | *** | **[SIC](https://www.limos.fr/axes/2)** — thème « [Données, services, intelligence](https://www.limos.fr/themes/3) » : détection d'anomalies en flux, séries temporelles ; aussi **[MAAD](https://www.limos.fr/axes/1)** — **\*\*\*** |
| [6. Capacité et charge](sujet-06-capacite-charge.md) | Saturation, goulots, drainage, limites du parallélisme (Amdahl) | **Évaluation de performances**, benchmarking méthodologique, théorie des files | **** | **[ODPS](https://www.limos.fr/axes/3)** — simulation à événements discrets et optimisation stochastique (cœur méthodologique de l'axe) — **\*\*\*\*** |
| [7. Résilience réseau](sujet-07-resilience-reseau.md) | Réplication, ISR, quorum d'acks, chronologie de dégradation, canal à erreurs groupées (Gilbert-Elliott) pour l'intermittence | Chaos engineering, tolérance aux pannes, fiabilité réseau, modèles de canal | *** | **[SIC](https://www.limos.fr/axes/2)** — thème « [Réseaux et sécurité](https://www.limos.fr/themes/4) » : protocoles, performance réseau — **\*\*\*** |
| [8. Contrôle métier asynchrone](sujet-08-controle-metier-asynchrone.md) | Ordre partiel, cohérence de décisions concurrentes, chorégraphie ; extension bancaire : cohérence d'un agrégat mutable (solde) sous événements concurrents | Architectures événementielles, coordination distribuée, Saga | *** | **[SIC](https://www.limos.fr/axes/2)** — thème « [Données, services, intelligence](https://www.limos.fr/themes/3) » : systèmes multi-agents, coordination — **\*\*** |
| [9. Sécurité d'accès](sujet-09-securite-acces-cluster.md) | Moindre privilège, authentification vs autorisation | Modèles de contrôle d'accès (RBAC/ABAC), sécurité des systèmes | ** | **[SIC](https://www.limos.fr/axes/2)** — thème « [Réseaux et sécurité](https://www.limos.fr/themes/4) » : contrôle d'accès, authentification, protection des données — **\*\*\*\*** |
| [10. Reconstruction PostgreSQL](sujet-10-reconstruction-postgres-kafka.md) | Event sourcing, état dérivable du journal, fenêtre de rétention ; sous-journaux dérivables, témoin de persistance extérieur (archive immuable) | Event sourcing / CQRS, state machine replication | *** | **[SIC](https://www.limos.fr/axes/2)** — « données massives, bases distribuées » (thème [Données, services](https://www.limos.fr/themes/3)) mais pas d'équipe journal/state machine visible — **\*\*** |
| [11. Mise à jour sans interruption](sujet-11-mise-a-jour-sans-interruption.md) | Rebalance, message en cours, fenêtre de risque, rolling update ; extension : compatibilité de schéma et fenêtres d'incompatibilité pendant un déploiement | Group membership, protocoles de rebalance, déploiement continu, compatibilité de schéma | *** | **[ODPS](https://www.limos.fr/axes/3)** — politiques de maintenance, continuité d'activité des systèmes de production — **\*\*** |
| [12. Partage entre émetteurs](sujet-12-partage-cluster-emetteurs.md) | Équité d'allocation, isolation, « noisy neighbor », quotas | **Équité max-min**, allocation de ressources, théorie des jeux | **** | **[MAAD](https://www.limos.fr/axes/1)** — optimisation multi-objectif, problèmes d'équité ; **[ODPS](https://www.limos.fr/axes/3)** — allocation de ressources — **\*\*\*** |
| [13. Exactly-once](sujet-13-exactly-once-cout-et-limites.md) | Transactions distribuées, isolation, fencing des producteurs zombies | **Consensus**, traitement transactionnel, systèmes tolérants aux fautes | **** | Pas d'axe LIMOS clairement dédié aux systèmes distribués ; le plus proche : **[MAAD](https://www.limos.fr/axes/1)** (algorithmique et complexité) — **\*** |
| [14. Variabilité du trafic](sujet-14-variabilite-trafic.md) | Processus ponctuels (Poisson non homogène, Hawkes), files, validation statistique | **Processus stochastiques**, modélisation du trafic, évaluation de performances | **** | **[ODPS](https://www.limos.fr/axes/3)** — simulation à événements discrets et optimisation stochastique ; **[MAAD](https://www.limos.fr/axes/1)** — thème [MOCA](https://www.limos.fr/themes/7) (simulation, méta-modélisation) — **\*\*\*\*** |

## Synthèse : quel axe LIMOS pour quels sujets

| Axe / thème LIMOS | Page | Sujets concernés | Angle de collaboration |
|---|---|---|---|
| **ODPS** | [axes/3](https://www.limos.fr/axes/3) | 4, 6, 14 — puis 2, 11 | Le partenaire naturel des sujets de charge et de trafic : simulation à événements discrets et optimisation stochastique recouvrent exactement ces thématiques |
| **SIC — Réseaux et sécurité** | [themes/4](https://www.limos.fr/themes/4) | 9, 7 | Contrôle d'accès, authentification, performance réseau : le sujet 9 est quasi sur mesure |
| **SIC — Données, services, intelligence** | [themes/3](https://www.limos.fr/themes/3) | 5, 3, 1, 8 | Détection d'anomalies en flux, interopérabilité, exécution fiable de services |
| **MAAD** | [axes/1](https://www.limos.fr/axes/1) | 12, 14 | Optimisation multi-objectif et équité d'allocation ; simulation et méta-modélisation (thème [MOCA](https://www.limos.fr/themes/7)) |

Remarques :

1. **Le sujet 13 est le paradoxe du tableau** : le plus riche théoriquement (consensus, transactions distribuées), le moins aligné sur le LIMOS. Sa dimension théorique passera par des références littéraires plutôt que par une rencontre.
2. **Le sujet 14 est le pionnier du mécanisme** : il intègre déjà la rencontre avec les chercheurs comme livrable (compte rendu exigé, restitution des conclusions aux chercheurs). Les sujets \*\*\*\* pourraient suivre le même schéma.
3. **Un sujet \*\* n'est pas un sujet faible** : le ranking mesure la proximité potentielle avec la recherche, pas la valeur du sujet. Un sujet d'ingénierie peut rester purement technique sans perte.
4. **Les points d'entrée sont les pages d'axe et de thème**, pas des personnes : partir de [axes/3](https://www.limos.fr/axes/3) (ODPS), [themes/4](https://www.limos.fr/themes/4) et [themes/3](https://www.limos.fr/themes/3) (SIC), [axes/1](https://www.limos.fr/axes/1) (MAAD), et repérer sur la page de l'axe les membres dont les travaux s'approchent le plus.

## Familles théoriques transversales

Les aspects identifiés se regroupent en quatre familles, qui structurent l'approfondissement théorique proposé aux étudiants :

1. **Théorie des files et surcharge** (sujets 4, 6, 12, 14) — loi de Little, taux d'occupation, variabilité, équité max-min. La famille la plus proche des chercheurs ODPS.
2. **Sémantiques et transactions distribuées** (sujets 1, 13, partiellement 11) — formalisation des garanties, périmètre, fencing. Amorcée par la note [`kafka-sub-exactly-once-limit.md`](../../docs/architecture/kafka-sub-exactly-once-limit.md).
3. **Ordre, cohérence et réplication** (sujets 7, 8, 11) — ordre partiel, ISR, quorum, rebalance.
4. **Journal d'événements et évolution** (sujets 2, 3, 10) — event sourcing, compatibilité de schéma, projections.

La sécurité (sujet 9) forme un cinquième cas, moins théorique au sens mathématique mais bien alignée avec l'équipe SIC dédiée.

## Suivi de l'approfondissement théorique

Pour chaque famille, la décision porte sur la manière dont l'approfondissement est proposé aux étudiants et évalué :

| Famille | Décision | Statut |
|---|---|---|
| Files et surcharge | B — section « Dimension théorique » dans chaque fiche | Appliqué (14/14 fiches) |
| Sémantiques et transactions | B — section « Dimension théorique » dans chaque fiche | Appliqué (14/14 fiches) |
| Ordre et cohérence | B — section « Dimension théorique » dans chaque fiche | Appliqué (14/14 fiches) |
| Journal d'événements | B — section « Dimension théorique » dans chaque fiche | Appliqué (14/14 fiches) |
| Sécurité | B — section « Dimension théorique » dans chaque fiche | Appliqué (14/14 fiches) |

L'option **B a été retenue et appliquée** : chaque fiche du portefeuille comporte désormais une section « Dimension théorique » qui identifie l'aspect théorique, pointe vers ce document et l'axe LIMOS concerné, et rappelle que l'approfondissement fait partie de l'évaluation. Elle n'exclut pas les autres formes, qui restent possibles en extension :

- **A — Fiches théoriques annexes** : une fiche courte par famille, référencée depuis chaque sujet (modèle : [`kafka-sub-exactly-once-limit.md`](../../docs/architecture/kafka-sub-exactly-once-limit.md) référencé par le sujet 1) ;
- **C — Un sujet de plus** : une SAé « théorie des files appliquée au pipeline », pendant purement mathématique des sujets 4 et 6.

Quel que soit le format retenu, l'attente d'évaluation est la même : la profondeur de l'approfondissement théorique (justification du modèle, formulation de la garantie, dialogue avec la recherche) fait partie des critères de notation de la SAé.
