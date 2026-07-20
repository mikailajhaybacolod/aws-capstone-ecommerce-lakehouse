CREATE VIEW mikaila_gold.monthly_revenue_audit_literal AS
SELECT
  year,
  month,
  SUM(price) AS monthly_revenue_all_events
FROM mikaila_silver.silver_events
GROUP BY year, month;