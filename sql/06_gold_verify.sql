-- Gold verification — both months present after incremental load.
SELECT month, COUNT(*) AS rows, ROUND(SUM(total_revenue), 2) AS revenue
FROM mikaila_gold.daily_revenue_summary
GROUP BY month
ORDER BY month;