# Databricks notebook source
# MAGIC %md
# MAGIC # Create User Metrics
# MAGIC
# MAGIC This notebook joins inference tables (what was shown/predicted) with application events
# MAGIC (what users actually did) to calculate per-user, per-experiment metrics.
# MAGIC
# MAGIC Flow:
# MAGIC 1. Read inference tables to get experiment assignments and predictions
# MAGIC 2. Read application events to get actual user behavior (impressions, clicks)
# MAGIC 3. Join them together to calculate CTR and other metrics
# MAGIC 4. Aggregate to user-experiment level

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("user_events_table_path", "")
dbutils.widgets.text("user_metrics_table_path", "")
dbutils.widgets.text("experiments_table_path", "")
dbutils.widgets.text("inference_table_path", "")

user_events_table_path = dbutils.widgets.get("user_events_table_path")
user_metrics_table_path = dbutils.widgets.get("user_metrics_table_path")
experiments_table_path = dbutils.widgets.get("experiments_table_path")
inference_table_path = dbutils.widgets.get("inference_table_path")

print(f"User events table: {user_events_table_path}")
print(f"User metrics table: {user_metrics_table_path}")
print(f"Experiments table: {experiments_table_path}")
print(f"Inference table: {inference_table_path}")

# COMMAND ----------

# DBTITLE 1,Read Application Events
events_df = spark.table(user_events_table_path)

print(f"Total application events: {events_df.count():,}")
print(f"Date range: {events_df.agg(F.min('date'), F.max('date')).first()}")

# Show event breakdown
print("\nEvent breakdown:")
events_df.groupBy("event_name").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Read Inference Table

inference_df = (
    spark.table(inference_table_path)
    .select(
        F.col("databricks_request_id"),
        F.col("date"),
        F.col("timestamp_ms"),
        F.get_json_object(F.col("request"), "$.user_id").alias("user_id"),
        F.get_json_object(F.col("request"), "$.device").alias("device"),
        F.get_json_object(F.col("request"), "$.region").alias("region"),
        F.get_json_object(F.col("response"), "$.experiment_id").alias("experiment_id"),
        F.get_json_object(F.col("response"), "$.variant").alias("variant"),
    )
    .filter(F.col("experiment_id").isNotNull())  # Only include requests with active experiments
)

print(f"\nInference records with experiment assignments: {inference_df.count():,}")

# COMMAND ----------

# DBTITLE 1,Read Completed Experiments Not Yet Processed
# Read experiments that are Published AND completed but not yet in user_metrics table

# Get all published experiments that have ended
experiments_df = (
    spark.table(experiments_table_path)
    .filter(F.col("status") == "Published")
    .filter(F.col("end_date") < F.current_date())  # Completed experiments only
    .select(
        "experiment_id",
        "experiment_name",
        "start_date",
        "end_date",
        "primary_kpi_metric",
        "treatment_allocation",
    )
)

# Get experiments that have already been processed
try:
    processed_experiments = (
        spark.table(user_metrics_table_path).select("experiment_id").distinct().rdd.flatMap(lambda x: x).collect()
    )
    print(f"Found {len(processed_experiments)} already processed experiments")
except Exception:
    processed_experiments = []
    print("No previously processed experiments found (table may not exist yet)")

# Filter to only unprocessed experiments
experiments_to_process = experiments_df.filter(~F.col("experiment_id").isin(processed_experiments))

if experiments_to_process.count() == 0:
    dbutils.notebook.exit("No new experiments to process")

# COMMAND ----------

# DBTITLE 1,Join Inference with Experiments
# Add experiment metadata to inference records (only for experiments we're processing)
inference_with_exp_df = (
    inference_df.join(experiments_to_process, on="experiment_id", how="inner")
    .filter(
        # Filter to experiment date ranges
        (F.col("date") >= F.col("start_date")) & ((F.col("end_date").isNull()) | (F.col("date") <= F.col("end_date")))
    )
    .withColumn("inference_date", F.col("date"))
)

# COMMAND ----------

# DBTITLE 1,Calculate Impressions (from Application Events)
# Get ad impressions from application events
impressions_df = (
    events_df.filter(F.col("event_name") == "ad_impression")
    .select("user_id", "date", "session_id", "ad_id", "device", "region", "event_timestamp")
    .withColumnRenamed("date", "impression_date")
)

# COMMAND ----------

# DBTITLE 1,Calculate Clicks (from Application Events)
# Get ad clicks from application events
clicks_df = (
    events_df.filter(F.col("event_name") == "ad_click")
    .select("user_id", "date", "session_id", "ad_id", "event_timestamp")
    .withColumnRenamed("date", "click_date")
    .withColumnRenamed("event_timestamp", "click_timestamp")
)

# COMMAND ----------

# DBTITLE 1,Join Impressions with Clicks to Get User Daily Activity
# Join impressions with clicks (left join because not all impressions lead to clicks)
impressions_with_clicks_df = (
    impressions_df.alias("imp")
    .join(
        clicks_df.alias("clk"),
        (F.col("imp.user_id") == F.col("clk.user_id"))
        & (F.col("imp.session_id") == F.col("clk.session_id"))
        & (F.col("imp.ad_id") == F.col("clk.ad_id"))
        # Click happened after impression (within same day)
        & (F.col("clk.click_timestamp") >= F.col("imp.event_timestamp"))
        & (F.col("clk.click_timestamp") <= F.col("imp.event_timestamp") + F.expr("INTERVAL 1 DAY")),
        how="left",
    )
    .select(
        F.col("imp.user_id"),
        F.col("imp.impression_date").alias("date"),
        F.col("imp.device"),
        F.col("imp.region"),
        F.col("imp.ad_id"),
        F.when(F.col("clk.user_id").isNotNull(), 1).otherwise(0).alias("clicked"),
    )
)

# COMMAND ----------

# DBTITLE 1,Link Application Events with Experiment Assignments
# Join user activity (impressions/clicks) with experiment assignments from inference tables
# Match on user_id and date to determine which experiment each user was in

user_activity_with_experiments_df = (
    impressions_with_clicks_df.alias("activity")
    .join(
        inference_with_exp_df.select(
            "user_id",
            "inference_date",
            "experiment_id",
            "experiment_name",
            "variant",
            "primary_kpi_metric",
            "start_date",
            "end_date",
        )
        .distinct()
        .alias("exp"),
        on=(F.col("activity.user_id") == F.col("exp.user_id"))
        & (F.col("activity.date") == F.col("exp.inference_date")),
        how="inner",
    )
    .select(
        F.col("activity.user_id"),
        F.col("activity.date"),
        F.col("activity.device"),
        F.col("activity.region"),
        F.col("activity.ad_id"),
        F.col("activity.clicked"),
        F.col("exp.experiment_id"),
        F.col("exp.experiment_name"),
        F.col("exp.variant"),
        F.col("exp.primary_kpi_metric"),
        F.col("exp.start_date"),
        F.col("exp.end_date"),
    )
)

# COMMAND ----------

# DBTITLE 1,Calculate User-Level Metrics by Experiment

# Aggregate to user-experiment level
user_metrics_df = (
    user_activity_with_experiments_df.groupBy(
        "user_id",
        "experiment_id",
        "experiment_name",
        "variant",
        "primary_kpi_metric",
        "start_date",
        "end_date",
    )
    .agg(
        # Core CTR metrics
        F.count("*").alias("impression_count"),
        F.sum("clicked").alias("click_count"),
        # Daily averages
        F.countDistinct("date").alias("active_days"),
        # Device breakdown
        F.collect_set("device").alias("devices_used"),
        F.sum(F.when(F.col("device") == "mobile", 1).otherwise(0)).alias("mobile_impressions"),
        F.sum(F.when(F.col("device") == "desktop", 1).otherwise(0)).alias("desktop_impressions"),
        F.sum(F.when(F.col("device") == "tablet", 1).otherwise(0)).alias("tablet_impressions"),
        F.min("date").alias("first_activity_date"),
        F.max("date").alias("last_activity_date"),
    )
    .withColumn("click_through_rate", F.col("click_count") / F.col("impression_count"))
    .withColumn("avg_daily_impressions", F.col("impression_count") / F.col("active_days"))
    .withColumn("avg_daily_clicks", F.col("click_count") / F.col("active_days"))
    .withColumn("has_treatment", F.when(F.col("variant") == "treatment", True).otherwise(False))
    .withColumn("experiment_duration_days", F.datediff(F.col("end_date"), F.col("start_date")))
    .withColumn("days_active_in_experiment", F.datediff(F.col("last_activity_date"), F.col("first_activity_date")) + 1)
)

# COMMAND ----------

# DBTITLE 1,Write User Metrics Table

user_metrics_df.write.format("delta").mode("append").saveAsTable(user_metrics_table_path)

# COMMAND ----------

# DBTITLE 1,Sample User Metrics
spark.table(user_metrics_table_path).limit(100).display()
