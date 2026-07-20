-- Gold sample — top categories by revenue per day.
-- Sanity check: unique_users <= purchase_count (a user can buy more than once).
SELECT * FROM mikaila_gold.daily_revenue_summary
ORDER BY event_date, total_revenue DESC
LIMIT 10;