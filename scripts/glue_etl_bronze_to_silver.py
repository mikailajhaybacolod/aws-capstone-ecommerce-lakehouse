"""
Bronze -> Silver ETL with data quality checks.

Reads one month of raw events, checks them against my DQ rules,
then splits into 3: Silver (good rows), Quarantine (bad rows),
DQ Results (summary).

Params: --YEAR --MONTH --BUCKET --SOURCE_DB --SOURCE_TABLE
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ---------- setup ----------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "YEAR", "MONTH", "BUCKET", "SOURCE_DB", "SOURCE_TABLE"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

YEAR = args["YEAR"]
MONTH = args["MONTH"]
BUCKET = args["BUCKET"]
SOURCE_DB = args["SOURCE_DB"]
SOURCE_TABLE = args["SOURCE_TABLE"]

SILVER_PATH = f"s3://{BUCKET}/02-silver/events/"
QUARANTINE_PATH = f"s3://{BUCKET}/04-quarantine/events/"
DQ_RESULTS_PATH = f"s3://{BUCKET}/05-dq-results/"
SOURCE_PATH = f"s3://{BUCKET}/01-bronze/ecommerce_data/year={YEAR}/month={MONTH}/"

# snappy is splittable, gzip isn't
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

print(f"=== year={YEAR} month={MONTH} ===")

# ---------- 1. read bronze ----------

# push_down_predicate = only read the month I want, skip the other folder
bronze_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE,
    push_down_predicate=f"(year == '{YEAR}' and month == '{MONTH}')",
    transformation_ctx="bronze_dyf",
)

total_count = bronze_dyf.count()
print(f"bronze rows: {total_count:,}")

if total_count == 0:
    raise ValueError(f"no rows for year={YEAR} month={MONTH}")

# ---------- 2. dq rules ----------

# required: user_id complete, price >= 0. rest are mine.
# no rule on category_code/brand - they're empty on purpose
RULESET = """
Rules = [
    IsComplete "user_id",
    ColumnValues "price" >= 0,
    IsComplete "event_time",
    IsComplete "event_type",
    IsComplete "product_id",
    ColumnValues "event_type" in ["view", "cart", "purchase", "remove_from_cart"]
]
"""

dq_results = EvaluateDataQuality().process_rows(
    frame=bronze_dyf,
    ruleset=RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "capstone_bronze_to_silver",
        "enableDataQualityResultsPublishing": True,
        "enableDataQualityCloudWatchMetrics": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# my rows + a Passed/Failed column
row_outcomes = SelectFromCollection.apply(
    dfc=dq_results, key="rowLevelOutcomes", transformation_ctx="row_outcomes"
)

# summary per rule -> this is the DQ results deliverable
rule_outcomes = SelectFromCollection.apply(
    dfc=dq_results, key="ruleOutcomes", transformation_ctx="rule_outcomes"
)

# cache = keep the checked rows in memory so the counts below don't re-read everything 3 times
df = row_outcomes.toDF().cache()

# ---------- 3. split ----------

passed_df = df.filter(F.col("DataQualityEvaluationResult") == "Passed")
failed_df = df.filter(F.col("DataQualityEvaluationResult") == "Failed")

passed_count = passed_df.count()
failed_count = failed_df.count()

print(f"passed: {passed_count:,}")
print(f"failed: {failed_count:,}")

# if this says False I'm losing rows somewhere
print(f"reconciles: {passed_count + failed_count == total_count}")

# capstone formula
health_score = (passed_count / total_count) * 100 if total_count else 0
print(f"health score: {health_score:.6f}%")

# ---------- 4. silver ----------

# drop the dq columns, silver = clean data only
silver_df = passed_df.drop(
    "DataQualityRulesPass",
    "DataQualityRulesFail",
    "DataQualityRulesSkip",
    "DataQualityEvaluationResult",
)

silver_df.write.mode("overwrite").partitionBy("year", "month").parquet(SILVER_PATH)
print(f"silver -> {SILVER_PATH}")

# ---------- 5. quarantine ----------

# keep bad rows + why they failed, don't just delete them
if failed_count > 0:
    quarantine_df = (
        failed_df
        .withColumn("dq_failed_rules", F.col("DataQualityRulesFail").cast("string"))
        .withColumn("dq_timestamp", F.lit(datetime.utcnow().isoformat()))
        .withColumn("dq_source_path", F.lit(SOURCE_PATH))
        .withColumn("dq_job_run_id", F.lit(args["JOB_NAME"]))
        .drop("DataQualityRulesPass", "DataQualityRulesSkip", "DataQualityRulesFail")
    )

    # coalesce(1) = one file, not a bunch of tiny ones
    (
        quarantine_df.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(QUARANTINE_PATH)
    )
    print(f"quarantine -> {QUARANTINE_PATH}")
else:
    print("nothing quarantined")

# ---------- 6. dq results ----------

rule_outcomes_df = (
    rule_outcomes.toDF()
    .withColumn("run_year", F.lit(YEAR))
    .withColumn("run_month", F.lit(MONTH))
    .withColumn("run_timestamp", F.lit(datetime.utcnow().isoformat()))
    .withColumn("total_records", F.lit(total_count))
    .withColumn("valid_records", F.lit(passed_count))
    .withColumn("quarantined_records", F.lit(failed_count))
    .withColumn("data_health_score", F.lit(health_score))
)

(
    rule_outcomes_df.coalesce(1)
    .write.mode("append")
    .parquet(f"{DQ_RESULTS_PATH}year={YEAR}/month={MONTH}/")
)
print(f"dq results -> {DQ_RESULTS_PATH}")

print(f"=== done: {total_count:,} total / {passed_count:,} passed / {failed_count:,} quarantined / {health_score:.6f}% ===")

job.commit()