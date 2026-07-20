-- Time travel — query Gold as it existed AFTER October but BEFORE November.
-- Uses snapshot 1's id (the October load).
-- Expected: only month=10 (3,261 rows). November does not exist at this snapshot.
-- Compare against 06_gold_verify.sql (current state = both months) to show
-- the table moved forward in time while the past remains queryable.
SELECT month, COUNT(*) AS rows
FROM mikaila_gold.daily_revenue_summary
FOR VERSION AS OF 4719384887684611311
GROUP BY month
ORDER BY month;