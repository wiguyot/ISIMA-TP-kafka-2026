# Sujet 12 — Partager le cluster entre plusieurs émetteurs

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Le générateur de `simulpix` émet pour une population de clients unique. Dans la réalité, plusieurs établissements émettent des paiements Pix vers la même plateforme : la situation du [pic de match de football](../pour-aller-plus-loin/partie-5-pic-de-paiements-football.md) montre qu'un seul émetteur peut saturer le pipeline.

Question du sujet : si l'émetteur A subit un pic, l'émetteur B subit-il les conséquences ? Le cluster Kafka est une ressource partagée, et le partage non régulé peut conduire à une situation où un flux affame l'autre.

## Votre mission

Introduisez deux émetteurs dans la simulation, choisissez et justifiez une organisation du partage (clé métier enrichie, topic dédié par émetteur, quotas), puis démontrez par la mesure ce que votre organisation garantit — et ce qu'elle ne garantit pas.

## Socle fourni et contribution nouvelle

La clé `emitter_tax_id` désigne un client payeur, pas l'établissement qui envoie les paiements. Votre contribution introduit une identité d'organisation, par exemple `issuer_id`, deux flux simultanés et une mesure séparée de leur isolation.

Les générateurs peuvent produire les mêmes identifiants locaux ; rendez les `transaction_id` globalement uniques. Préservez l'identité d'organisation dans chaque contrat, notamment les constructions à liste fixe de `decision` et `outcome`. Deux appels de `run-scenario.sh` ne créent pas deux flux indépendants : ce script réinitialise la campagne.

## Réalisation minimale attendue

Distinguez deux organisations A et B, produisez leurs flux simultanément sans réinitialisation mutuelle et comparez **au moins deux configurations de partage des ressources**. Pour chacune, mesurez B seul, A et B nominaux, puis A en pic et B nominal. Relevez la charge réellement confirmée, p95/p99 et effectifs par organisation, décisions manquantes, débit global et drainage borné.

Déclarez le critère d'isolation avant les essais et justifiez la comparaison. Un résultat montrant que le mécanisme ne protège pas B est recevable ; il doit conduire à une conclusion mesurée sur ses limites.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Choisissez le mécanisme d'identification de l'émetteur : champ du message, clé de partitionnement, ou topic dédié. Justifiez.
2. Modifiez le générateur pour émettre deux flux distincts, avec des profils de charge indépendants.
3. Vérifiez l'impact sur les services en aval : un champ nouveau dans le message passe-t-il sans rupture ? (inspiration possible : sujet 3)
4. Écrivez l'hypothèse que vous testez : « avec notre organisation, un pic de A ne dégrade pas le délai de décision de B de plus de X % ».
5. Exécutez l'essai : pic sur A, flux nominal sur B, mesures simultanées des deux délais.
6. Comparez au moins deux organisations (par exemple clé enrichie contre topic dédié) et leurs effets sur l'isolation et le débit global.
7. Explorez les quotas Kafka comme mécanisme de régulation, si votre organisation ne suffit pas.

Un quota doit cibler une identité Kafka ou un `client.id` contrôlé ; ajouter un champ JSON ne crée pas un quota. Des topics séparés partagent encore brokers, consommateurs et PostgreSQL : décrivez les ressources réellement isolées. Justifiez aussi l'ordre par client lorsque vous changez la clé de partition.

## Extensions facultatives

Testez les pics simultanés de A et B, ajoutez une troisième organisation ou comparez plusieurs politiques de quotas. Les campagnes peuvent partir d'une référence provisoire locale ; elles n'attendent pas la livraison du sujet 6.

## Questions de conception

- Pourquoi la clé métier, telle qu'elle est utilisée aujourd'hui, ne protège-t-elle pas contre l'affamement entre émetteurs ?
- Un topic dédié par émetteur garantit-il l'isolation ? À quelle condition côté consommateur ?
- Que devient le parallélisme du pipeline si chaque émetteur a son topic ? Combien de partitions faut-il ?
- Un quota Kafka limite-t-il le débit d'un émetteur ou d'un consommateur ?
- Quel est le coût de l'isolation : complexité d'exploitation, sous-utilisation du cluster, équilibre des partitions ?

## Dimension théorique

Définissez l'équité et l'isolation entre organisations, avec une métrique par organisation et une référence B seul. Situez votre règle par rapport à une allocation max-min ou à des quotas seulement si ses propriétés correspondent au modèle. Explicitez les ressources encore partagées et les effets sur l'ordre par client. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- des mesures simultanées des deux émetteurs, sur un même protocole rejouable ;
- un verdict documenté pour chaque organisation testée : isolation obtenue, débit global, équilibre des partitions ;
- la comparaison entre au moins deux organisations ;
- une chronologie montrant le pic de A et la stabilité (ou dégradation) de B ;
- une conclusion sur le compromis retenu et les limites des charges réellement testées ; les pics simultanés relèvent de l'extension.

## Ressources

- [Activité 05 — Clé métier et partitionnement](../activite-05-partitionnement-cle-emetteur.md)
- [Pic de paiements football](../pour-aller-plus-loin/partie-5-pic-de-paiements-football.md)
- [Sujet 6 — Qualification de capacité](sujet-06-capacite-charge.md) (méthodologie de campagne)
- [Modèles de trafic](../../docs/architecture/traffic-models.md)
