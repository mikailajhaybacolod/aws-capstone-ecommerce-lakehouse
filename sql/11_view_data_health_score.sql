-- Data Health Score view (capstone deliverable).
-- Formula: (valid_records / total_records) * 100
-- Reads live counts from Silver (passed) + Quarantine (failed), which
-- reconcile to the ETL's verified total (reconciles: True on every run).
-- LEFT JOIN + COALESCE handles October having no quarantine partition (-> 0).
-- Runs under the mikaila-capstone-10mb workgroup.
CREATE VIEW mikaila_gold.data_health_score AS
WITH silver AS (
  SELECT year, month, COUNT(*) AS valid_records
  FROM mikaila_silver.silver_events
  GROUP BY year, month
),
quarantine AS (
  SELECT year, month, COUNT(*) AS quarantined_records
  FROM mikaila_quarantine.quarantine_events
  GROUP BY year, month
)
SELECT
  s.year,
  s.month,
  s.valid_records,
  COALESCE(q.quarantined_records, 0) AS quarantined_records,
  s.valid_records + COALESCE(q.quarantined_records, 0) AS total_records,
  ROUND(
    s.valid_records * 100.0
    / (s.valid_records + COALESCE(q.quarantined_records, 0)),
    6
  ) AS data_health_score
FROM silver s
LEFT JOIN quarantine q
  ON s.year = q.year AND s.month = q.month;