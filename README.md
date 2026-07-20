# AWS Capstone — Governed Medallion Data Lake

A governed e-commerce data lake built on AWS: raw files in, a certified business
dataset out. Ingests ~110M rows of e-commerce behavior data, enforces quality rules,
quarantines bad records, and serves aggregated metrics from an Apache Iceberg Gold layer
under a strict query-cost cap.

**Submitted by:** Mikaila Jhay Bacolod

**Program:** Stratpoint AWS Data Engineering Bench Training

**Dataset:** [rees46 e-commerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (Oct + Nov 2019)

---

## Architecture

Raw gzip → Bronze → Glue ETL (quality split) → Silver + Quarantine → Iceberg Gold → Views

- **Bronze** — raw events, chunked gzip, Hive-partitioned by year/month
- **Silver** — rows that passed 6 DQDL quality rules; Snappy Parquet
- **Quarantine** — rows that failed, kept with metadata explaining why
- **Gold** — aggregated daily revenue, Apache Iceberg (ACID, snapshots, time travel)
- **Governance** — IAM least-privilege; Athena workgroup with a 10 MB scan cap

The Glue ETL job splits every row three ways: clean → Silver, failed → Quarantine
(with `dq_failed_rules`, `dq_timestamp`, `dq_source_path`, `dq_job_run_id`), and a
rule-outcome summary → DQ results in S3.

---

## AWS environment

| | |
|---|---|
| Region | `ap-southeast-1` |
| Bucket | `mikaila-aws-capstone` (prefixes `01-bronze` … `07-glue-assets`) |
| Databases | `mikaila_bronze` · `mikaila_silver` · `mikaila_quarantine` · `mikaila_gold` |
| Workgroups | `mikaila-dev` (5 GB cap) · `mikaila-capstone-10mb` (10 MB, Gold deliverable) |
| Glue job | `mikaila-capstone-bronze-to-silver` (PySpark, parameterized `--YEAR/--MONTH`) |

---

## Repository structure

```
scripts/
  glue_etl_bronze_to_silver.py     # Glue PySpark ETL: DQDL + 3-way split
sql/
  01_bronze_sample.sql             # sample Bronze rows
  02_bronze_quality_check.sql      # confirm raw data is clean
  03_quarantine_check.sql          # verify quarantined rows + metadata
  04_gold_create_table.sql         # create Iceberg Gold table
  05_gold_merge.sql                # MERGE Silver → Gold (upsert, purchases only)
  06_gold_verify.sql               # row counts / no-bad-rows check
  07_gold_sample.sql               # sample Gold rows
  08_snapshots.sql                 # Iceberg snapshot history
  09_time_travel.sql               # FOR VERSION AS OF query
  10_view_monthly_revenue_audit.sql   # primary revenue view (Gold, <10 MB)
  11_view_data_health_score.sql       # (valid / total) * 100
  12_view_revenue_literal.sql         # literal SUM(price) per brief
  13_data_dictionary.sql              # column comments (Metadata Test)
iam/
  capstone-s3-access-policy.json               # Glue ETL role: least-privilege S3
  capstone-restricted-analyst-policy.json      # analyst role: deny Silver / allow Gold
```

*Note: `raw/` and `chunks/` (the dataset and its gzip splits) are gitignored — they
live in S3, not Git.*

---

## How to reproduce

1. **Chunk & upload** — split each month's gzip into ~115 MB pieces (3M rows each),
   upload to `s3://mikaila-aws-capstone/01-bronze/` via `aws s3 sync`.
2. **Crawl Bronze** — run the Glue crawler to catalog `mikaila_bronze.ecommerce_data`.
3. **Run the ETL** — `mikaila-capstone-bronze-to-silver`, once per month
   (`--YEAR 2019 --MONTH 10`, then `11`). Produces Silver, Quarantine, DQ results.
4. **Crawl Silver & Quarantine** — catalog `silver_events` and `quarantine_events`.
5. **Build Gold** — `sql/04` (create Iceberg table), then `sql/05` (MERGE, per month).
6. **Views & dictionary** — run `sql/10`–`13`.
7. **Verify** — `sql/06` (no bad rows), `sql/08` (snapshots), `sql/09` (time travel).

---

## Quality rules (DQDL)

Six rules via `EvaluateDataQuality`; the two required ones:

- `IsComplete "user_id"` — no missing user IDs
- `ColumnValues "price" >= 0` — no negative prices

No completeness rule on `category_code` / `brand` — they are legitimately null for many
rows, so a rule there would quarantine valid data.

---

## Business metrics

- **Data Health Score** = (valid ÷ total) × 100 → Oct 100.000000%, Nov 99.999994%
- **Monthly Revenue Audit** = SUM(price) for **purchase** events → Oct ~$229.96M,
  Nov ~$275.19M

A literal `SUM(price)` over *all* events (`12_view_revenue_literal.sql`) is also provided
to match the brief exactly — it comes out ~54× larger (~$12.32B Oct) because view and cart
events also carry a price. The purchase-filtered view is the meaningful business number.

---

## The three tests

- **Quality** — injected bad rows (negative price, null user_id) are quarantined and never
  reach Gold.
- **Metadata** — `DESCRIBE mikaila_gold.daily_revenue_summary` shows column comments.
- **Governance** — a restricted analyst role is denied on Silver and allowed on Gold
  (policy in `iam/`, confirmed via IAM Policy Simulator at the S3 layer).

Governance is table/prefix-level (IAM), not column-level — column masking needs Lake
Formation, which was out of scope. The sensitive `user_id` is protected by not existing in
the Gold layer at all.

---

## Architecture Diagram
<img width="3018" height="1368" alt="image" src="https://github.com/user-attachments/assets/592e9e2b-7a3a-4a4f-8412-175d66a1da64" />
