alter table validated_transactions
  add column if not exists source_transaction_id text;

alter table validated_transactions
  add column if not exists retry_attempt integer not null default 0;
