alter table rejected_transactions
  add column if not exists source_transaction_id text;

alter table rejected_transactions
  add column if not exists retry_attempt integer not null default 0;

alter table rejected_transactions
  add column if not exists rejection_fingerprint text;

update rejected_transactions
set source_transaction_id = coalesce(source_transaction_id, transaction_id),
    retry_attempt = coalesce(retry_attempt, 0),
    rejection_fingerprint = coalesce(
      rejection_fingerprint,
      concat(coalesce(source_transaction_id, transaction_id, 'unknown'), ':', coalesce(retry_attempt, 0), ':', rejection_reason)
    )
where source_transaction_id is null
   or rejection_fingerprint is null;

alter table rejected_transactions
  alter column rejection_fingerprint set not null;

create unique index if not exists rejected_transactions_rejection_fingerprint_idx
  on rejected_transactions (rejection_fingerprint);
