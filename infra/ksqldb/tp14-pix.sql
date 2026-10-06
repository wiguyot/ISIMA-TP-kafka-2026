-- TP 04 — ksqlDB sur le flux Pix
-- Chaque bloc illustre un cas d'usage. Exécution : ./scripts/ksql.sh puis
-- RUN SCRIPT '/sql/tp14-pix.sql'; (ou copier-coller bloc par bloc).

-- Lire les topics depuis le début (sinon seules les nouvelles données sont vues).
SET 'auto.offset.reset' = 'earliest';

-- 0. Déclarer un STREAM sur un topic existant : ksqlDB ne copie rien, il décrit
--    le format des messages JSON publiés par pix-decision-engine.
CREATE STREAM IF NOT EXISTS pix_valides (
  transaction_id VARCHAR,
  event_time VARCHAR,
  emitter_tax_id VARCHAR,
  beneficiary_tax_id VARCHAR,
  amount DOUBLE,
  currency VARCHAR,
  decision_latency_ms INT
) WITH (KAFKA_TOPIC = 'simulpix.transactions.validated', VALUE_FORMAT = 'JSON');

-- 1. Filtrage et routage : les Pix de montant élevé partent dans un nouveau topic.
CREATE STREAM IF NOT EXISTS pix_montant_eleve
  WITH (KAFKA_TOPIC = 'simulpix.ksql.pix_montant_eleve', PARTITIONS = 6, REPLICAS = 3) AS
  SELECT transaction_id, emitter_tax_id, beneficiary_tax_id, amount
  FROM pix_valides
  WHERE amount >= 150
  EMIT CHANGES;

-- 2. Agrégation continue : nombre et montant total des Pix par émetteur.
--    Une TABLE garde l'état courant par clé ; il est stocké dans Kafka.
CREATE TABLE IF NOT EXISTS pix_par_emetteur
  WITH (KAFKA_TOPIC = 'simulpix.ksql.pix_par_emetteur', PARTITIONS = 6, REPLICAS = 3) AS
  SELECT emitter_tax_id, COUNT(*) AS nb_pix, ROUND(SUM(amount), 2) AS montant_total
  FROM pix_valides
  GROUP BY emitter_tax_id
  EMIT CHANGES;

-- 3. Fenêtrage et détection d'anomalie : émetteurs trop actifs sur 1 minute.
--    Le seuil est à adapter au débit du scénario (6 émetteurs dans le référentiel).
CREATE TABLE IF NOT EXISTS pix_rafales_emetteur
  WITH (KAFKA_TOPIC = 'simulpix.ksql.pix_rafales_emetteur', PARTITIONS = 6, REPLICAS = 3) AS
  SELECT emitter_tax_id, COUNT(*) AS nb_pix_minute
  FROM pix_valides
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY emitter_tax_id
  HAVING COUNT(*) > 50
  EMIT CHANGES;

-- 4. Enrichissement par jointure : ajouter le nom du client émetteur.
--    Le référentiel est chargé dans Kafka puis lu comme une TABLE.
--    Le topic source simulpix.ksql.clients doit exister avant de déclarer le STREAM.
--    scripts/ksql.sh le crée avec 6 partitions et une réplication de 3 ; le STREAM
--    sert ensuite à y écrire les 6 clients et la TABLE lit ce même topic.
CREATE STREAM clients_flux (
  tax_id VARCHAR KEY,
  pix_client_id VARCHAR,
  name VARCHAR,
  status VARCHAR
) WITH (KAFKA_TOPIC = 'simulpix.ksql.clients', VALUE_FORMAT = 'JSON', PARTITIONS = 6, REPLICAS = 3);

CREATE TABLE clients (
  tax_id VARCHAR PRIMARY KEY,
  pix_client_id VARCHAR,
  name VARCHAR,
  status VARCHAR
) WITH (KAFKA_TOPIC = 'simulpix.ksql.clients', VALUE_FORMAT = 'JSON');

INSERT INTO clients_flux (tax_id, pix_client_id, name, status) VALUES ('TAX-0001', 'pixc-0001', 'Client 0001', 'ACTIVE');
INSERT INTO clients_flux (tax_id, pix_client_id, name, status) VALUES ('TAX-0002', 'pixc-0002', 'Client 0002', 'ACTIVE');
INSERT INTO clients_flux (tax_id, pix_client_id, name, status) VALUES ('TAX-0003', 'pixc-0003', 'Client 0003', 'ACTIVE');
INSERT INTO clients_flux (tax_id, pix_client_id, name, status) VALUES ('TAX-0004', 'pixc-0004', 'Client 0004', 'ACTIVE');
INSERT INTO clients_flux (tax_id, pix_client_id, name, status) VALUES ('TAX-0005', 'pixc-0005', 'Client 0005', 'ACTIVE');
INSERT INTO clients_flux (tax_id, pix_client_id, name, status) VALUES ('TAX-0006', 'pixc-0006', 'Client 0006', 'ACTIVE');

-- La jointure porte sur une colonne de la valeur : ksqlDB repartitionne le flux
-- par emitter_tax_id avant de joindre (les services Python et ksqlDB, en Java,
-- n'utilisent pas le même algorithme de partitionnement).
CREATE STREAM IF NOT EXISTS pix_valides_enrichis
  WITH (KAFKA_TOPIC = 'simulpix.ksql.pix_valides_enrichis', PARTITIONS = 6, REPLICAS = 3) AS
  SELECT p.transaction_id, p.emitter_tax_id, c.name AS emetteur, p.amount
  FROM pix_valides p
  JOIN clients c ON p.emitter_tax_id = c.tax_id
  EMIT CHANGES;

-- 5. Vue interrogeable à la demande (pull query) : l'état actuel d'un émetteur.
--    À lancer à la main :
--    SELECT * FROM pix_par_emetteur WHERE emitter_tax_id = 'TAX-0003';
