# Databricks notebook source
# MAGIC %md
# MAGIC # Generate User Features
# MAGIC
# MAGIC Creates user features from user dimension + aggregated user events.
# MAGIC Computes rolling 30-day metrics for each user.

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("user_features_table_path", "")
dbutils.widgets.text("user_dim_table_path", "")
dbutils.widgets.text("user_events_table_path", "")

user_features_table_path = dbutils.widgets.get("user_features_table_path")
user_dim_table_path = dbutils.widgets.get("user_dim_table_path")
user_events_table_path = dbutils.widgets.get("user_events_table_path")

print(f"User features table: {user_features_table_path}")
print(f"User dimension table: {user_dim_table_path}")
print(f"User events table: {user_events_table_path}")

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Load User Dimension
user_dim_df = spark.table(user_dim_table_path)

# COMMAND ----------

# DBTITLE 1,Calculate Event-Based Metrics (Last 30 Days)
# Get events from last 30 days for current user behavior
thirty_days_ago = date.today() - timedelta(days=30)

user_events_df = spark.table(user_events_table_path)

# Filter to last 30 days
recent_events = user_events_df.filter(F.col("date") >= F.lit(thirty_days_ago))

# COMMAND ----------

# DBTITLE 1,Aggregate User Metrics
# Count impressions and clicks per user
user_metrics = recent_events.groupBy("user_id").agg(
    F.countDistinct(F.when(F.col("event_name") == "session_start", F.col("session_id"))).alias("sessions_30d"),
    F.sum(F.when(F.col("event_name") == "ad_impression", 1).otherwise(0)).alias("impressions_30d"),
    F.sum(F.when(F.col("event_name") == "ad_click", 1).otherwise(0)).alias("clicks_30d"),
    F.sum(F.when(F.col("event_name") == "ad_conversion", 1).otherwise(0)).alias("conversions_30d"),
    F.countDistinct("date").alias("active_days_30d"),
)

# Calculate CTR
user_metrics = user_metrics.withColumn(
    "historical_ctr_30d",
    F.when(F.col("impressions_30d") > 0, F.col("clicks_30d") / F.col("impressions_30d")).otherwise(0.0),
)

# COMMAND ----------

# DBTITLE 1,Join with User Dimension
# Join user dimension with event-based metrics
user_features_df = (
    user_dim_df.join(user_metrics, on="user_id", how="left")
    # Fill nulls for users with no recent activity
    .fillna(
        0,
        subset=[
            "sessions_30d",
            "impressions_30d",
            "clicks_30d",
            "conversions_30d",
            "active_days_30d",
            "historical_ctr_30d",
        ],
    )
    .select(
        "user_id",
        "age_group",
        "device",
        "region",
        "signup_date",
        "sessions_30d",
        "impressions_30d",
        "clicks_30d",
        "conversions_30d",
        "active_days_30d",
        "historical_ctr_30d",
        F.when(F.col("sessions_30d") > 0, F.col("impressions_30d") / F.col("sessions_30d"))
        .otherwise(0.0)
        .alias("avg_impressions_per_session"),
        F.datediff(F.current_date(), F.col("signup_date")).alias("days_since_signup"),
    )
)

# COMMAND ----------

# DBTITLE 1,Write to Table
user_features_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    user_features_table_path
)

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed
spark.sql(f"ALTER TABLE {user_features_table_path} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

print("✓ Change Data Feed enabled")
