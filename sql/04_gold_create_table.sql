-- Gold layer — aggregated, business-ready Iceberg table.
-- table_type=ICEBERG is what enables MERGE, snapshots, and time travel.
-- Deliberately NO user_id column: the sensitive field is protected by not existing in the layer analysts can reach (governance boundary).
CREATE TABLE mikaila_gold.daily_revenue_summary (
  event_date      date,
  category_code   string,
  total_revenue   double,
  purchase_count  bigint,
  unique_users    bigint,
  year            string,
  month           string
)
PARTITIONED BY (year, month)
LOCATION 's3://mikaila-aws-capstone/03-gold/daily_revenue_summary/'
TBLPROPERTIES (
  'table_type'        = 'ICEBERG',
  'format'            = 'parquet',
  'write_compression' = 'snappy'
);