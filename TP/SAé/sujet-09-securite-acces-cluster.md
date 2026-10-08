# Sujet 9 — Sécuriser l'accès au cluster Kafka

Ce sujet fait partie du [portefeuille de SAÉ](README.md).

## Situation

Aujourd'hui, tous les services de `simulpix` utilisent le même identifiant Kafka (`simulpix`) et aucune liste de contrôle d'accès n'est définie : chaque conteneur peut lire tous les topics, y compris ceux des autres, et écrire partout. Les secrets TLS de `infra/kafka/secrets/` sont générés mais non utilisés.

Dans une application bancaire réelle, cette situation est inacceptable : un service compromis peut intercepter tous les paiements ou injecter de faux événements. Le principe de **moindre privilège** impose qu'un service n'ait que les droits strictement nécessaires à son rôle.

## Votre mission

Appliquez le principe de moindre privilège au cluster : chaque service doit disposer de son propre identifiant, avec des droits limités aux topics qu'il produit et consomme réellement. Votre durcissement doit être prouvé par un rejet d'accès effectif, pas par une convention documentée.

## Réalisation minimale attendue

Activez l'authentification par identité distincte pour au moins trois services, définissez des ACL par topic (producteur, consommateur), et documentez la matrice « service → droits ». Un test automatisé doit vérifier qu'une tentative d'accès non autorisée échoue explicitement et que le pipeline nominal continue de fonctionner après durcissement.

## Actions à réaliser

Avant toute modification, formulez votre garantie cible en une phrase, sous la forme : « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] ».

Ensuite, vous devez :

1. Cartographiez les flux réels : quel service produit quel topic, quel service consomme quel topic.
2. Écrivez la matrice de droits cible : pour chaque service, ses topics de production, de consommation et les opérations autorisées.
3. Créez un identifiant par service et remplacez l'identifiant partagé dans la configuration des conteneurs.
4. Activez le contrôle d'accès du cluster et définissez les ACL correspondant à votre matrice.
5. Vérifiez que chaque service fonctionne avec ses seuls droits, puis qu'une tentative hors périmètre est rejetée.
6. Documentez la procédure d'ajout d'un nouveau service : création de l'identifiant, attribution des ACL, test de validation.

## Questions de conception

- Quels topics un persister a-t-il réellement le droit de lire, et pourquoi pas les autres ?
- Un rejet d'accès est-il un incident ou la preuve que la sécurité fonctionne ?
- Comment un service authentifié diffère-t-il d'un service autorisé ?
- Que devient la matrice de droits lorsqu'un nouveau service est ajouté au pipeline ?
- Quel impact ce durcissement a-t-il sur les scripts de diagnostic et d'administration ?

## Dimension théorique

Votre sujet porte un aspect théorique formalisable : le moindre privilège et les modèles de contrôle d'accès (qui peut faire quoi, sur quelle ressource, et comment le vérifier). Approfondissez-le : formalisez votre matrice de droits comme un modèle et prouvez son invariant (aucun service hors périmètre). Consultez [l'analyse recherche](analyse-recherche-limos.md) pour la référence détaillée : la sécurité (authentification, contrôle d'accès, protection des données) est le thème « Réseaux et sécurité » de l'axe [SIC](https://www.limos.fr/axes/2) du LIMOS. Cet approfondissement fait partie de l'évaluation. L'aspect identifié ici n'est pas exhaustif : votre réalisation peut révéler d'autres aspects théoriques, à approfondir et à signaler également.

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
