-- TP 04 — supprime toutes les requêtes et tous les objets créés par tp14-pix.sql,
-- ainsi que leurs topics. Exécution : ./scripts/ksql.sh --file /sql/tp14-reset.sql
TERMINATE ALL;
DROP STREAM IF EXISTS pix_valides_enrichis DELETE TOPIC;
DROP TABLE IF EXISTS clients;
DROP STREAM IF EXISTS clients_flux DELETE TOPIC;
DROP TABLE IF EXISTS pix_rafales_emetteur DELETE TOPIC;
DROP TABLE IF EXISTS pix_par_emetteur DELETE TOPIC;
DROP STREAM IF EXISTS pix_montant_eleve DELETE TOPIC;
DROP STREAM IF EXISTS pix_valides;
