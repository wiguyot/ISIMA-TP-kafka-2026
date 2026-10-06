#!/bin/sh
#
# toggle-unique.sh — outil pédagogique pour TP 02 (at-least-once SUB)
#
# Le persister utilise un UPSERT (INSERT ... ON CONFLICT DO UPDATE) protégé par
# la PRIMARY KEY sur validated_transactions.transaction_id. Lors d'un rejeu
# Kafka, le 2ème INSERT du même transaction_id est absorbé silencieusement par
# la contrainte d'unicité — aucun doublon en base, mais la tentative a bien eu
# lieu.
#
# Ce script ne désactive pas la contrainte (sinon le ON CONFLICT casse). Il
# installe à la place une table d'audit et un trigger qui logue CHAQUE
# tentative d'INSERT et d'UPDATE-via-ON-CONFLICT. On compte ensuite :
#   - lignes en validated_transactions   = N
#   - tentatives en tp_audit_attempts    = N + K
#   - K = nombre de doublons absorbés par la PK
#
# Usage : ./scripts/toggle-unique.sh <off|on|status|count|reset>
#
# off    : installe la table d'audit et le trigger
# on     : retire le trigger et la table d'audit
# status : indique l'état (audit actif ou non)
# count  : affiche les compteurs N / N+K / K (doublons absorbés)
# reset  : vide la table d'audit sans la supprimer
#
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

ACTION="${1:-status}"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

psql_exec() {
  docker_compose exec -T postgres psql -U simulpix -d simulpix -v ON_ERROR_STOP=1 "$@"
}

# Libère les locks idle in transaction pour permettre l'attache/détache du
# trigger. Ces transactions inactives sont créées par les SELECT count(*)
# périodiques de service-health ; les terminer ne perturbe pas le service,
# il rouvre une session à la prochaine requête.
release_idle_locks() {
  psql_exec -tA -c "
    select pg_terminate_backend(pid)
    from pg_stat_activity
    where datname = 'simulpix'
      and pid <> pg_backend_pid()
      and state = 'idle in transaction';
  " >/dev/null 2>&1 || true
}

case "$ACTION" in
  off)
    echo "[simulpix] installation de l'audit des tentatives sur validated_transactions"
    release_idle_locks
    psql_exec <<'SQL'
set lock_timeout = '8s';
drop table if exists tp_fault_markers;
create table if not exists tp_audit_attempts (
  attempt_id bigserial primary key,
  transaction_id text not null,
  attempt_kind text not null,
  attempted_at timestamptz not null default now()
);

create or replace function tp_audit_validated_attempt() returns trigger as $$
begin
  insert into tp_audit_attempts (transaction_id, attempt_kind)
  values (new.transaction_id, tg_op);
  return new;
end;
$$ language plpgsql;

drop trigger if exists tp_audit_validated_attempts_trigger on validated_transactions;
create trigger tp_audit_validated_attempts_trigger
  before insert or update on validated_transactions
  for each row execute function tp_audit_validated_attempt();
SQL
    echo "[simulpix] audit installé — chaque tentative INSERT / UPDATE est tracée"
    ;;
  on)
    echo "[simulpix] retrait de l'audit"
    release_idle_locks
    psql_exec <<'SQL'
set lock_timeout = '8s';
drop trigger if exists tp_audit_validated_attempts_trigger on validated_transactions;
drop function if exists tp_audit_validated_attempt();
drop table if exists tp_audit_attempts;
drop table if exists tp_fault_markers;
SQL
    echo "[simulpix] audit retiré — état nominal restauré"
    ;;
  status)
    state="$(psql_exec -tA -c "select coalesce(string_agg(tgname, ','), 'absent') from pg_trigger where tgname = 'tp_audit_validated_attempts_trigger';")"
    if [ "$state" = "absent" ]; then
      echo "[simulpix] audit : INACTIF (mode nominal, doublons invisibles)"
    else
      echo "[simulpix] audit : ACTIF — doublons absorbés observables via 'count'"
    fi
    ;;
  count)
    audit_table="$(psql_exec -tA -c "select coalesce(to_regclass('public.tp_audit_attempts')::text, 'absent');")"
    if [ "$audit_table" = "absent" ]; then
      psql_exec <<'SQL'
\echo
\echo === Compteurs de tentatives sur validated_transactions ===
select
  (select count(*) from validated_transactions) as lignes_en_base,
  0 as tentatives_totales,
  0 as doublons_absorbes;
\echo
select 'audit inactif : lancez toggle-unique.sh off avant une mesure SUB/PG si vous voulez compter les tentatives' as etat_audit;
SQL
      exit 0
    fi
    psql_exec <<'SQL'
\echo
\echo === Compteurs de tentatives sur validated_transactions ===
select
  (select count(*) from validated_transactions) as lignes_en_base,
  (select count(*) from tp_audit_attempts) as tentatives_totales,
  (select count(*) from tp_audit_attempts)
    - (select count(*) from validated_transactions) as doublons_absorbes;
\echo
\echo === Détail par transaction_id (top 10 doublons) ===
select transaction_id, count(*) as tentatives
from tp_audit_attempts
group by transaction_id
having count(*) > 1
order by count(*) desc, transaction_id asc
limit 10;
SQL
    ;;
  reset)
    psql_exec -c "truncate tp_audit_attempts;" >/dev/null 2>&1 || true
    echo "[simulpix] audit vidé"
    ;;
  *)
    echo "Usage : $0 <off|on|status|count|reset>" >&2
    exit 1
    ;;
esac
