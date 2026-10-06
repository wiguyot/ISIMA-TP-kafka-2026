-- TP 04 — requêtes minimales pour la démonstration de garantie de traitement.
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM IF NOT EXISTS pix_valides (
  transaction_id VARCHAR,
  emitter_tax_id VARCHAR,
  amount DOUBLE
) WITH (KAFKA_TOPIC = 'simulpix.transactions.validated', VALUE_FORMAT = 'JSON');
CREATE TABLE IF NOT EXISTS pix_par_emetteur
  WITH (KAFKA_TOPIC = 'simulpix.ksql.pix_par_emetteur', PARTITIONS = 6, REPLICAS = 3) AS
  SELECT emitter_tax_id, COUNT(*) AS nb_pix, ROUND(SUM(amount), 2) AS montant_total
  FROM pix_valides
  GROUP BY emitter_tax_id
  EMIT CHANGES;
