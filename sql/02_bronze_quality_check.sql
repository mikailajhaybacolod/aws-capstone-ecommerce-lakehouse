-- Bronze quality verification — proves the real data is genuinely clean.
-- Confirms failed:0 in the ETL was real, not a broken ruleset.
-- Scans ~1.62 GB (raw gzip CSV) — run in mikaila-dev (5 GB cap), NOT the 10 MB workgroup.
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
  SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS negative_price
FROM mikaila_bronze.ecommerce_data
WHERE year = '2019' AND month = '10';