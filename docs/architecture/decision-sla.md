# Decision SLA

Cette note décrit l'évolution introduite pour porter un contrat de décision sur les paiements Pix.

## Besoin visé

Chaque paiement doit recevoir une décision finale dans un délai borné, par exemple `5` secondes :

- `ACCEPTED`
- `REJECTED`

La décision doit aussi être intelligible côté client. On ne veut donc pas seulement un détail technique interne, mais un motif exploitable :

- rejet banque ;
- délai de traitement dépassé ;
- incident technique ;
- cause précise affichable ou journalisable côté client.

## Ce qui est désormais implémenté

Les messages `raw` portent maintenant :

- `decision_sla_seconds`
- `decision_deadline`

Les messages `validated` et `rejected` portent désormais une décision client structurée :

- `decision_status`
- `decision_reason_code`
- `decision_reason_label`
- `decision_origin`
- `decision_at`
- `decision_deadline`
- `decision_sla_seconds`
- `decision_latency_ms`
- `decision_within_sla`
- `client_message`

Le `pix-decision-engine` rejette explicitement un paiement quand la deadline est dépassée au moment de la décision, via :

- `rejection_type=TECHNICAL_TIMEOUT`
- `decision_reason_code=PROCESSING_TIMEOUT`

La persistance PostgreSQL conserve ces champs pour les décisions acceptées comme rejetées, et `service-health` expose désormais le nombre de décisions hors SLA.

Un topic final `simulpix.transactions.outcome` est aussi publié pour chaque décision acceptée ou rejetée.

## Impact sur l'architecture

Cette évolution ne remet pas en cause l'architecture événementielle actuelle.

Le socle reste valide :

- `generator` produit ;
- `pix-validator` contrôle ;
- `pix-decision-engine` décide ;
- `pix-outcome-publisher` publie la décision finale ;
- les persisters stockent ;
- `service-health` observe.

En revanche, le modèle fonctionnel change réellement. On ne manipule plus seulement :

- un flux validé ;
- un flux rejeté ;

mais une vraie décision client avec un contrat temporel.

## Limites de l'implémentation actuelle

La décision client est maintenant modélisée et publiée dans Kafka, mais pas encore complètement externalisée.

Il manque encore, si l'on veut aller au bout :

- un service de notification ou d'exposition client ;
- une séparation plus fine entre causes bancaires, techniques, réseau et dépendances externes ;
- un watchdog dédié si l'on veut détecter des messages "perdus" sans attendre qu'ils repassent dans `pix-decision-engine`.

## Prochaine étape recommandée

Si l'objectif devient "un client externe doit toujours pouvoir récupérer la décision finale et son motif", la prochaine extension naturelle est :

1. exposer les événements `outcome` via un endpoint de consultation ou un service de notification ;
2. séparer plus finement les causes bancaires, techniques, réseau et dépendances externes ;
3. ajouter un composant de surveillance qui force un `TIMED_OUT` quand aucune décision n'est produite dans le délai ;
4. définir un contrat de consommation externe pour `simulpix.transactions.outcome`.

Autrement dit : l'architecture n'est pas à refaire, mais elle peut encore être étendue autour de l'exposition et de la surveillance de la décision finale.
