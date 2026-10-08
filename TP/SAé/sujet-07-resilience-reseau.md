# Sujet 7 — Tester la résilience réseau et organiser la reprise

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Kafka fonctionne avec plusieurs brokers, c'est-à-dire plusieurs serveurs qui se répartissent et répliquent les messages. Une latence réseau, une perte de paquets ou l'arrêt d'un broker ne produisent pas les mêmes effets. Le pipeline peut continuer à recevoir des messages tout en dégradant la réplication, le lag, les délais de décision ou la persistance.

Une perturbation ne prouve donc pas automatiquement une perte de données. Votre travail est de distinguer ce qui est temporairement en attente, ce qui est rejeté pour raison métier, et ce qui serait réellement perdu.

Les incidents réseau réels ne se réduisent pas à une panne franche. Une liaison peut se couper nettement, fonctionner par intermittence, injecter silencieusement des erreurs ou devenir plus lente sans raison apparente. Chacune de ces familles laisse une signature différente dans les métriques : savoir la reconnaître est la première étape du diagnostic.

## Votre mission

Construisez une campagne qui couvre plusieurs familles de panne réseau — rupture franche, intermittence, erreurs injectées, ralentissement inexpliqué — et démontrez, famille par famille, le comportement du pipeline et son retour à la normale. Votre campagne aboutit à un runbook : un document qui part d'un symptôme observé, propose une cause probable et indique l'action à mener.

## Socle fourni et contribution nouvelle

Les profils de perturbation, l'isolement d'un broker et les scripts de rétablissement existent. Votre contribution est un oracle automatisé de reprise, une campagne documentée et un runbook fondé sur les résultats.

Le `tc netem` actuel agit sur le trafic sortant de l'interface du conteneur ciblé, sans filtre spécifique au protocole Kafka. Il peut affecter PostgreSQL ou les sondes sur cette interface : relevez précisément conteneurs, interface, sens du trafic et règles appliquées. Sous TCP, perte ou corruption de paquets peut devenir retard et retransmission, sans produire un JSON invalide.

## Réalisation minimale attendue

Ajoutez un scénario ou un script automatisé qui applique une perturbation réseau, observe les critères de reprise puis échoue explicitement si le pipeline ne revient pas à l'état attendu. La campagne doit couvrir **au moins trois familles de panne** (rupture, intermittence, erreurs injectées) avec, pour chacune, une hypothèse écrite à l'avance et des critères de rétablissement propres. Le script doit conserver les données permettant d'interpréter l'essai : profil de perturbation, durée, métriques Kafka, compteurs métier et état de la persistance.

Les profils existants suffisent au minimum. Utilisez un manifeste des entrées et vérifiez les identités ainsi que le contenu après drainage avec timeout. Un lag nul ne suffit pas à prouver l'absence de perte. Recueillez les compteurs du transport ou de `tc` nécessaires au diagnostic ; deux signatures applicatives semblables peuvent imposer une conclusion indéterminée.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Cataloguez les familles de panne réseau que vous allez étudier (rupture franche, intermittence, erreurs injectées, ralentissement inexpliqué) et associez à chacune un levier réellement disponible : profils de perturbation de l'interface de pilotage (`kafka_latency`, `kafka_loss`, `kafka_slow_link`), isolement d'un broker (`./scripts/inject-fault.sh broker_isolate`), ou alternance perturbation/rétablissement (`network-perturb.sh` / `network-reset.sh`). Justifiez ce choix de couverture.
2. Pour chaque famille, écrivez une hypothèse sur les métriques qui doivent évoluer et celles qui doivent rester cohérentes.
3. Lancez un flux suffisamment long, appliquez la perturbation pendant le flux, puis retirez-la. Pour l'intermittence, enchainez plusieurs cycles perturbation/rétablissement : un pipeline peut survivre à une coupure et pourtant dégrader à chaque reconnexion.
4. Observez la réplication Kafka, le lag consommateur, les compteurs métier et PostgreSQL, pendant la perturbation et pendant le rattrapage.
5. Vérifiez le périmètre réel des perturbations et les effets sur le transport, les services et les sondes.
6. Analysez le rôle de la retransmission TCP : le pipeline ne parle qu'en TCP. Expliquez pourquoi certaines pertes de paquets n'apparaissent pas au niveau applicatif, et quelles métriques les révèlent quand même (retransmissions, latence de rattrapage, lag).
7. Identifiez les cas où vos mesures distinguent les causes et ceux qui demandent un signal complémentaire ; justifiez la vérification proposée.
8. Décrivez la chronologie de chaque essai : état nominal, perturbation, dégradation, rattrapage et retour à la normale.
9. Définissez, pour chaque famille, les conditions qui permettent de déclarer le système rétabli.
10. Comparez, pour chaque famille, le résultat avec la garantie de livraison retenue par le scénario.
11. Consolidez le tout en un runbook : symptôme → cause probable → vérification → action. Chaque entrée doit s'appuyer sur une signature mesurée dans votre campagne, pas sur une intuition.

## Extensions facultatives

- Ajoutez un régime de corruption, duplication ou réordonnancement avec `tc netem`, et mesurez ses effets TCP.
- Modélisez l'intermittence par un canal à deux états, par exemple Gilbert-Elliott, et calibrez ses paramètres.
- Organisez un diagnostic à l'aveugle entre groupes. Autorisez « indéterminé » lorsque les symptômes ne discriminent pas la cause ; la réalisation du minimum ne dépend pas d'un groupe partenaire.

## Questions de conception

- Quels symptômes indiquent un problème de réplication plutôt qu'un consommateur lent ?
- Comment distinguer une intermittence, une perte de paquets et un lien lent à partir des seuls symptômes observables ? Quel essai permettrait de trancher entre deux familles qui se ressemblent ?
- Pourquoi la retransmission TCP rend-elle une perte de paquets « invisible » au-dessus de la couche transport ? Qu'est-ce que cette invisibilité change pour le diagnostic applicatif ?
- Que change un isolement silencieux (broker vivant mais injoignable) par rapport à un arrêt propre, pour la réplication et pour la détection de la panne ?
- Quand un lag est-il un état temporaire acceptable et quand devient-il un risque métier ?
- Comment vérifiez-vous qu'un message en attente sera traité après le rétablissement du réseau ?
- Quelles données faut-il contrôler dans PostgreSQL avant de conclure qu'il n'y a pas de perte ?
- Quelle alerte ou quelle procédure aiderait un opérateur à décider quoi faire pendant l'incident ?

## Dimension théorique

Reliez les hypothèses de panne à l'ISR, aux acks et aux garanties de livraison. `acks=all` attend l'ISR, tandis que `min.insync.replicas` fixe une condition d'acceptation ; ce n'est pas simplement un quorum majoritaire. Expliquez l'effet de TCP sur les symptômes. Un canal à deux états tel que Gilbert-Elliott est une extension facultative pour l'intermittence. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- une hypothèse écrite avant chaque essai, pour chaque famille de panne étudiée ;
- une chronologie de la perturbation et du rattrapage, par famille ;
- des mesures couvrant Kafka, le métier et PostgreSQL ;
- une matrice des familles de panne étudiées et de leur signature observée (quelles métriques bougent, lesquelles restent stables) ;
- un diagnostic justifié et ses limites lorsque plusieurs causes ont des symptômes semblables ;
- une conclusion qui distingue indisponibilité, retard, rejet métier et perte réelle ;
- une procédure de retour à la normale reproductible, avec ses critères par famille ;
- un runbook réseau reliant chaque symptôme observé à une cause probable, une vérification et une action.

## Ressources

- [Perturbation réseau Kafka](../pour-aller-plus-loin/partie-6-perturbation-reseau-kafka.md)
- [Retard de réplication Kafka](../pour-aller-plus-loin/partie-4-retard-de-replication-kafka.md)
- [Protocole d'arrêt de validation](../../docs/architecture/pix-validation-stop-protocol.md)
