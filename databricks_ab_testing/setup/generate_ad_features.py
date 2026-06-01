# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Ad Features
# MAGIC
# MAGIC Creates ad features from the ad dimension table.
# MAGIC Only includes **currently active** ads (available for production serving).

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("ad_features_table_path", "")
dbutils.widgets.text("ad_dim_table_path", "")

ad_features_table_path = dbutils.widgets.get("ad_features_table_path")
ad_dim_table_path = dbutils.widgets.get("ad_dim_table_path")

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Load Ad Dimension
ad_dim_df = spark.table(ad_dim_table_path)

# COMMAND ----------

# DBTITLE 1,Create Ad Features
# Include ALL ads (active + historical) for training purposes
# The model will learn quality scores for all ads, even retired ones
ad_features_df = ad_dim_df.select(
    F.col("ad_id"),
    F.col("category"),
    F.col("advertiser_id"),
    F.col("ad_quality_score").alias("_ad_quality_for_rank"),  # Used in CTRPyFunc
    F.col("active_from"),
    F.col("active_to"),
)

# COMMAND ----------

# DBTITLE 1,Write to Table
ad_features_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    ad_features_table_path
)

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed
spark.sql(f"ALTER TABLE {ad_features_table_path} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
