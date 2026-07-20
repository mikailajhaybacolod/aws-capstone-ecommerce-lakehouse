-- Bronze sample query
-- LIMIT short-circuits: scans ~1 MB despite the table being 110M rows.
-- Workgroup: mikaila-dev
SELECT * FROM mikaila_bronze.ecommerce_data
WHERE year = '2019' AND month = '10'
LIMIT 10;