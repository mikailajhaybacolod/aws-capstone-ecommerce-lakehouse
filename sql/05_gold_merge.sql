-- Silver -> Gold incremental load via Iceberg MERGE.
-- Run once per month: change month = '10' then month = '11'.
-- Oct creates snapshot 1, Nov creates snapshot 2 (incremental, no rewrite).
--
-- COALESCE(category_code, 'unknown') keeps the MERGE key stable — SQL treats
-- NULL = NULL as unknown, so null categories would never match on re-run.
MERGE INTO mikaila_gold.daily_revenue_summary AS t
USING (
  SELECT
    CAST(SUBSTR(event_time, 1, 10) AS date)      AS event_date,
    COALESCE(category_code, 'unknown')            AS category_code,
    SUM(price)                                    AS total_revenue,
    COUNT(*)                                      AS purchase_count,
    COUNT(DISTINCT user_id)                       AS unique_users,
    year,
    month
  FROM mikaila_silver.silver_events
  WHERE year = '2019'
    AND month = '10'          -- change to '11' for the November load
    AND event_type = 'purchase'
  GROUP BY
    CAST(SUBSTR(event_time, 1, 10) AS date),
    COALESCE(category_code, 'unknown'),
    year, month
) AS s
ON  t.event_date    = s.event_date
AND t.category_code = s.category_code
WHEN MATCHED THEN UPDATE SET
  total_revenue  = s.total_revenue,
  purchase_count = s.purchase_count,
  unique_users   = s.unique_users
WHEN NOT MATCHED THEN INSERT
  (event_date, category_code, total_revenue, purchase_count, unique_users, year, month)
VALUES
  (s.event_date, s.category_code, s.total_revenue, s.purchase_count, s.unique_users, s.year, s.month);