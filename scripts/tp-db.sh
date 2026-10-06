#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

docker compose exec postgres psql -U simulpix -d simulpix -c "
select count(*) as validated_count from validated_transactions;
select count(*) as rejected_count from rejected_transactions;
select rejection_reason, count(*) as occurrences
from rejected_transactions
group by rejection_reason
order by occurrences desc, rejection_reason asc;
"
