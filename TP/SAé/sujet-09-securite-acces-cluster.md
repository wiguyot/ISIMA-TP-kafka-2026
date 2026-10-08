# Sujet 9 — Sécuriser l'accès au cluster Kafka

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Aujourd'hui, tous les services de `simulpix` utilisent le même identifiant Kafka (`simulpix`) et aucune liste de contrôle d'accès n'est définie : chaque conteneur peut lire tous les topics, y compris ceux des autres, et écrire partout. Les secrets TLS de `infra/kafka/secrets/` sont générés mais non utilisés.

Dans une application bancaire réelle, cette situation est inacceptable : un service compromis peut intercepter tous les paiements ou injecter de faux événements. Le principe de **moindre privilège** impose qu'un service n'ait que les droits strictement nécessaires à son rôle.

## Votre mission

Appliquez le principe de moindre privilège au périmètre choisi : chaque service retenu dispose d'une identité et de droits limités aux ressources nécessaires. Votre durcissement doit être prouvé par des erreurs d'autorisation explicites et un flux nominal autorisé.

## Socle fourni et contribution nouvelle

L'authentification SASL existe déjà avec un compte partagé ; elle n'applique pas le moindre privilège. Votre contribution porte sur des identités distinctes, l'autorisation effective et les preuves d'accès autorisé/refusé. L'activation de TLS est une extension distincte de confidentialité du transport.

## Réalisation minimale attendue

Séparez les identités d'au moins trois services choisis et documentez le traitement des services hors de ce périmètre. Définissez une matrice couvrant **Topics, Groups et, en mode transactionnel, TransactionalId**, ainsi que les droits supplémentaires nécessaires aux sondes et à l'administration. Automatisez au moins deux accès interdits et un flux nominal complet après durcissement.

Les groupes sont notamment suffixés par `RUN_ID` et les producteurs transactionnels par worker. Préférez les préfixes propres au rôle aux droits globaux sur tous les groupes ou toutes les transactions. Un timeout ou une panne réseau ne prouve pas un refus d'autorisation : exigez une erreur d'accès explicite.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Cartographiez les flux réels : quel service produit quel topic, quel service consomme quel topic.
2. Écrivez la matrice : identités, topics, groupes, identifiants transactionnels et opérations autorisées, y compris les outils de diagnostic.
3. Créez un identifiant par service retenu dans le périmètre minimal et remplacez l'identifiant partagé dans sa configuration ; documentez les identités restantes.
4. Activez le contrôle d'accès du cluster et définissez les ACL correspondant à votre matrice.
5. Vérifiez que chaque service fonctionne avec ses seuls droits, puis qu'une tentative hors périmètre est rejetée.
6. Documentez la procédure d'ajout d'un nouveau service : création de l'identifiant, attribution des ACL, test de validation.

## Extensions facultatives

Étendez la politique à tous les services, activez TLS et vérifiez la validation des certificats, ou étudiez la rotation des secrets. Déclarez séparément les garanties d'authentification, d'autorisation et de confidentialité.

## Questions de conception

- Quels topics un persister a-t-il réellement le droit de lire, et pourquoi pas les autres ?
- Un rejet d'accès est-il un incident ou la preuve que la sécurité fonctionne ?
- Comment un service authentifié diffère-t-il d'un service autorisé ?
- Que devient la matrice de droits lorsqu'un nouveau service est ajouté au pipeline ?
- Quel impact ce durcissement a-t-il sur les scripts de diagnostic et d'administration ?

## Dimension théorique

Formalisez une relation entre identités, ressources et opérations autorisées. Distinguez authentification, autorisation et confidentialité. Reliez la matrice de droits aux tests positifs/négatifs ; discutez précisément les limites de la couverture du minimum et les permissions d'administration. Cet approfondissement est encouragé pour mieux comprendre vos choix et interpréter vos résultats. Consultez le [guide des pistes de recherche](analyse-recherche-limos.md) pour trouver des idées de lecture et préparer un échange avec l'enseignant ou le LIMOS.

## Preuves attendues

- une matrice « service → droits » documentée et conforme au fonctionnement réel ;
- des journaux de rejet d'accès pour au moins deux tentatives hors périmètre ;
- un flux nominal complet après durcissement, avec compteurs métier et persistance cohérents ;
- un test automatisé rejouable qui échoue si les ACL sont trop permissives ;
- une conclusion précisant ce que la solution protège et ce qu'elle ne protège pas (réseau interne, conteneur compromis, administrateur).

## Ressources

- [Guide développeur](../../docs/developpement/guide-developpeur.md)
- [Activité 03 — Sémantique pub/sub](../activite-03-semantique-pub-sub.md)
- Configuration Kafka : `docker-compose.yml` et `infra/kafka/secrets/`
- [Anatomie des interactions entre conteneurs](../../docs/architecture/container-interactions.md)
