# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Training Data
# MAGIC
# MAGIC Creates training dataset by joining user events with user/ad features.
# MAGIC Each row represents an ad impression with whether it was clicked.

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("training_features_table_path", "")
dbutils.widgets.text("user_events_table_path", "")
dbutils.widgets.text("user_features_table_path", "")
dbutils.widgets.text("ad_features_table_path", "")

training_features_table_path = dbutils.widgets.get("training_features_table_path")
user_events_table_path = dbutils.widgets.get("user_events_table_path")
user_features_table_path = dbutils.widgets.get("user_features_table_path")
ad_features_table_path = dbutils.widgets.get("ad_features_table_path")

print(f"Training features table: {training_features_table_path}")
print(f"User events table: {user_events_table_path}")
print(f"User features table: {user_features_table_path}")
print(f"Ad features table: {ad_features_table_path}")

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Load Tables
user_events_df = spark.table(user_events_table_path)
user_features_df = spark.table(user_features_table_path)
ad_features_df = spark.table(ad_features_table_path)

# COMMAND ----------

# DBTITLE 1,Get Impression and Click Events
# Get all ad impressions
impressions = user_events_df.filter(F.col("event_name") == "ad_impression").select(
    "event_id", "user_id", "ad_id", "event_timestamp", "device", "region", "session_id"
)

# Get all clicks
clicks = user_events_df.filter(F.col("event_name") == "ad_click").select(
    F.col("event_id").alias("click_event_id"),
    F.col("session_id").alias("click_session_id"),
    F.col("ad_id").alias("click_ad_id"),
    F.col("user_id").alias("click_user_id"),
)

# COMMAND ----------

# DBTITLE 1,Join Impressions with Clicks
# Left join to get clicked = 1 for impressions that resulted in clicks
training_base = impressions.join(
    clicks,
    (impressions.user_id == clicks.click_user_id)
    & (impressions.ad_id == clicks.click_ad_id)
    & (impressions.session_id == clicks.click_session_id),
    "left",
).select(impressions["*"], F.when(F.col("click_event_id").isNotNull(), 1).otherwise(0).alias("clicked"))

# COMMAND ----------

# DBTITLE 1,Add Temporal Features
training_with_time = (
    training_base.withColumn("hour", F.hour("event_timestamp"))
    .withColumn("day_of_week", F.dayofweek("event_timestamp"))
    .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0))
)

# COMMAND ----------

# DBTITLE 1,Join with User Features
training_with_user = training_with_time.join(
    user_features_df.select(
        "user_id",
        "age_group",
        "historical_ctr_30d",
        "sessions_30d",
        "impressions_30d",
        "clicks_30d",
        "days_since_signup",
    ),
    on="user_id",
    how="left",
)

# COMMAND ----------

# DBTITLE 1,Join with Ad Features
training_full = training_with_user.join(
    ad_features_df.select("ad_id", "category", "_ad_quality_for_rank"), on="ad_id", how="left"
)

# COMMAND ----------

# DBTITLE 1,Select Final Training Features
training_final = training_full.select(
    # Target
    "clicked",
    # Temporal features
    "hour",
    "day_of_week",
    "is_weekend",
    # User features
    "device",
    "region",
    "age_group",
    "historical_ctr_30d",
    "sessions_30d",
    "impressions_30d",
    "clicks_30d",
    "days_since_signup",
    # Ad features
    "category",
    F.col("_ad_quality_for_rank").alias("ad_quality"),
    # IDs (for debugging/analysis)
    "user_id",
    "ad_id",
    "event_timestamp",
).na.drop()  # Drop any rows with nulls

# COMMAND ----------

# DBTITLE 1,Write to Table
training_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    training_features_table_path
)

# Show statistics after write
print(f"\n✓ Training features table written to: {training_features_table_path}")

training_features_final = spark.table(training_features_table_path)
print(f"✓ Total rows: {training_features_final.count():,}")

print("\nTarget distribution:")
training_features_final.groupBy("clicked").count().show()

ctr = training_features_final.filter(F.col("clicked") == 1).count() / training_features_final.count()
print(f"Overall CTR: {ctr*100:.2f}%")
