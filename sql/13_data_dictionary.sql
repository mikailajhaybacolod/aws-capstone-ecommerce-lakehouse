-- Data Dictionary — column comments on the Gold table (Metadata Test).
-- Stored IN the Glue Catalog
-- Verify with: DESCRIBE mikaila_gold.daily_revenue_summary;  (comment column populated)
--
-- NOTE: table-level comment via SET TBLPROPERTIES ('comment'=...) is NOT
-- supported on Iceberg tables in Athena ("Unsupported table property key: comment").
-- Column comments use ALTER TABLE ... CHANGE COLUMN, which works.
-- A table-level description can be added via the Glue console UI if needed.

ALTER TABLE mikaila_gold.daily_revenue_summary
  CHANGE COLUMN event_date event_date date
  COMMENT 'Calendar date of the purchase events (derived from event_time).';

ALTER TABLE mikaila_gold.daily_revenue_summary
  CHANGE COLUMN category_code category_code string
  COMMENT 'Product category. "unknown" where the source category was null.';

ALTER TABLE mikaila_gold.daily_revenue_summary
  CHANGE COLUMN total_revenue total_revenue double
  COMMENT 'Sum of price for purchase events in this date/category. Currency units.';

ALTER TABLE mikaila_gold.daily_revenue_summary
  CHANGE COLUMN purchase_count purchase_count bigint
  COMMENT 'Number of purchase events in this date/category.';

ALTER TABLE mikaila_gold.daily_revenue_summary
  CHANGE COLUMN unique_users unique_users bigint
  COMMENT 'Distinct buyers (COUNT DISTINCT user_id). Aggregated only; no raw IDs stored.';