# Limites du socle pour les preuves SAé

Cette note accompagne les [consignes communes](README.md#attendus-communs). Elle décrit des limites à prendre en compte dans les expériences SAé. Les activités 1 à 10, les TP et leur socle d'exécution conservent leur version de référence.

## Comptage des rejeux PostgreSQL

Le trigger de [toggle-unique.sh](../../scripts/toggle-unique.sh) trace BEFORE INSERT et BEFORE UPDATE. Un UPSERT en conflit peut produire les deux traces pour une seule tentative. Le script compte actuellement toutes les lignes d'audit puis soustrait les lignes métier : ce calcul peut surestimer les conflits et dépend de l'état initial de la base.

Pour les preuves des sujets 1 et 13, relevez séparément les traces INSERT et UPDATE, avec une borne de début de mesure. Dans le chemin UPSERT fourni, INSERT représente la tentative et UPDATE la branche de conflit ; une mise à jour directe aurait un autre sens. L'audit est dans la transaction PostgreSQL et ne conserve pas les tentatives annulées. Il ne vérifie pas l'identité du contenu.

## Volume linéaire demandé

Dans le [modèle du shaper](../../services/pix-traffic-shaper/traffic_model.py), la branche `linear` retourne le volume arrondi d'un bucket sans le plafonner au reste d'une phase finie. Le dernier lot peut donc dépasser le total configuré. La copie de cette logique dans le générateur présente la même limite.

Pour les sujets 6 et 14, comparez total configuré, lots demandés et publications observées. Construisez le manifeste à partir des entrées effectives ; ne supposez pas que le total demandé est un compteur de livraison.

## Confirmation des publications

Les fonctions `produce_and_confirm` des services du pipeline vérifient actuellement le nombre de messages restant en file après `flush()`. Une file vidée ne prouve pas que chaque publication a réussi : une erreur de livraison peut aussi retirer un message de la file.

Pour une garantie de livraison étudiée en SAé, utilisez les rapports de livraison, les messages effectivement visibles et les données durables. Distinguez aussi la portée des acks et celle d'un commit transactionnel. Ne concluez pas à une réussite à partir du seul retour de `flush()`.

## Mesures et exemples documentaires

- `estimated_oldest_lag_seconds` est un ratio du lag agrégé sur le débit récent de raw ; les libellés existants ne le transforment pas en âge du plus ancien message ni en borne SLA.
- Les moyennes affichées ne donnent pas p95/p99 et les décisions absentes doivent rester dans le bilan.
- Le mode `poisson` contient déjà des oscillations, du bruit lissé, une correction du budget fini et un tirage approché au-dessus d'un seuil ; caractérisez-le avant de comparer des modèles.
- Les JSON de `docs/contrats-evenements/` sont des exemples, parfois incomplets par rapport aux constructeurs. Relevez les champs dans le code, notamment pour checked, decision, les identités de rejeu et `final_topic` ; ne les traitez pas comme des schémas exécutables.

Une évolution du socle entreprise par un groupe doit être justifiée par son sujet, réalisée dans son dépôt dérivé et accompagnée de ses propres preuves. La présente mise à jour des consignes SAé ne modifie pas ce socle.
