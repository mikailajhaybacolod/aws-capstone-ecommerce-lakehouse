-- Monthly Revenue Audit view (capstone deliverable).
-- Rolls the daily Gold table up to monthly grain.
-- Revenue is already purchase-only (filtered in the Silver->Gold MERGE),
-- so no event_type filter is needed here.
-- Runs under the mikaila-capstone-10mb workgroup (scans KB, well under 10 MB).
CREATE VIEW mikaila_gold.monthly_revenue_audit AS
SELECT
  year,
  month,
  SUM(total_revenue)  AS monthly_revenue,
  SUM(purchase_count) AS total_purchases,
  SUM(unique_users)   AS total_unique_users
FROM mikaila_gold.daily_revenue_summary
GROUP BY year, month;