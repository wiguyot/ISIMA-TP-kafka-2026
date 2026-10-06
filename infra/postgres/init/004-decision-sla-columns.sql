alter table validated_transactions
  add column if not exists decision_deadline timestamptz;

alter table validated_transactions
  add column if not exists decision_sla_seconds integer not null default 5;

alter table validated_transactions
  add column if not exists decision_status text not null default 'ACCEPTED';

alter table validated_transactions
  add column if not exists decision_reason_code text not null default 'ACCEPTED';

alter table validated_transactions
  add column if not exists decision_reason_label text not null default 'Paiement Pix accepte';

alter table validated_transactions
  add column if not exists decision_origin text not null default 'BANK';

alter table validated_transactions
  add column if not exists decision_latency_ms integer;

alter table validated_transactions
  add column if not exists decision_within_sla boolean not null default true;

alter table validated_transactions
  add column if not exists client_message text;

update validated_transactions
set decision_deadline = coalesce(decision_deadline, event_time + make_interval(secs => coalesce(decision_sla_seconds, 5))),
    decision_sla_seconds = coalesce(decision_sla_seconds, 5),
    decision_status = coalesce(decision_status, 'ACCEPTED'),
    decision_reason_code = coalesce(decision_reason_code, 'ACCEPTED'),
    decision_reason_label = coalesce(decision_reason_label, 'Paiement Pix accepte'),
    decision_origin = coalesce(decision_origin, 'BANK'),
    decision_latency_ms = coalesce(
      decision_latency_ms,
      case
        when validated_at is not null then greatest((extract(epoch from (validated_at - event_time)) * 1000)::integer, 0)
        else null
      end
    ),
    decision_within_sla = coalesce(
      decision_within_sla,
      case
        when validated_at is not null and decision_deadline is not null then validated_at <= decision_deadline
        else true
      end
    ),
    client_message = coalesce(client_message, decision_reason_label, 'Paiement Pix accepte')
where decision_deadline is null
   or client_message is null;

alter table validated_transactions
  alter column decision_deadline set not null;

alter table rejected_transactions
  add column if not exists decision_deadline timestamptz;

alter table rejected_transactions
  add column if not exists decision_sla_seconds integer not null default 5;

alter table rejected_transactions
  add column if not exists decision_status text not null default 'REJECTED';

alter table rejected_transactions
  add column if not exists decision_reason_code text not null default 'TECHNICAL_REJECTED';

alter table rejected_transactions
  add column if not exists decision_reason_label text not null default 'Paiement Pix rejete';

alter table rejected_transactions
  add column if not exists decision_origin text not null default 'SYSTEM';

alter table rejected_transactions
  add column if not exists decision_latency_ms integer;

alter table rejected_transactions
  add column if not exists decision_within_sla boolean not null default true;

alter table rejected_transactions
  add column if not exists client_message text;

update rejected_transactions
set decision_deadline = coalesce(decision_deadline, event_time + make_interval(secs => coalesce(decision_sla_seconds, 5))),
    decision_sla_seconds = coalesce(decision_sla_seconds, 5),
    decision_status = coalesce(decision_status, 'REJECTED'),
    decision_reason_code = coalesce(decision_reason_code, 'TECHNICAL_REJECTED'),
    decision_reason_label = coalesce(decision_reason_label, 'Paiement Pix rejete'),
    decision_origin = coalesce(decision_origin, 'SYSTEM'),
    decision_latency_ms = coalesce(
      decision_latency_ms,
      case
        when rejected_at is not null and event_time is not null then greatest((extract(epoch from (rejected_at - event_time)) * 1000)::integer, 0)
        else null
      end
    ),
    decision_within_sla = coalesce(
      decision_within_sla,
      case
        when rejected_at is not null and decision_deadline is not null then rejected_at <= decision_deadline
        else true
      end
    ),
    client_message = coalesce(client_message, decision_reason_label, 'Paiement Pix rejete')
where decision_deadline is null
   or client_message is null;

alter table rejected_transactions
  alter column decision_deadline set not null;
