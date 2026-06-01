# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Ad Dimension Table
# MAGIC
# MAGIC Creates a synthetic ad catalog with realistic lifecycle patterns.
# MAGIC
# MAGIC **Ad Buckets**:
# MAGIC - Retired ads (20%): Historical only, ended before demo period
# MAGIC - Long-running ads (50%): Active throughout demo + production
# MAGIC - Recent ads (30%): Started recently, active for production

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("ad_dim_table_path", "")

ad_dim_table_path = dbutils.widgets.get("ad_dim_table_path")

# COMMAND ----------

# DBTITLE 1,Imports and Setup
import random
from datetime import datetime, timedelta, date
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType

random.seed(42)

# COMMAND ----------

# DBTITLE 1,Define Parameters
NUM_ADS = 1000
today = date.today()
start_date = today - timedelta(days=90)

# Ad categories with realistic distribution
CATEGORIES = [
    ("gaming", 0.40),
    ("shopping", 0.30),
    ("travel", 0.20),
    ("finance", 0.10),
]

ADVERTISERS = [f"advertiser_{i:03d}" for i in range(1, 51)]  # 50 advertisers

# COMMAND ----------

# DBTITLE 1,Generate Ad Data
ads_data = []


# Helper function to pick category based on distribution
def pick_category():
    r = random.random()
    cumulative = 0
    for category, prob in CATEGORIES:
        cumulative += prob
        if r < cumulative:
            return category
    return CATEGORIES[-1][0]


# Bucket 1: Retired ads (20% = 200 ads) - Historical only
for i in range(200):
    active_from = start_date - timedelta(days=random.randint(30, 90))
    active_to = today - timedelta(days=random.randint(14, 120))  # Ended in past

    ads_data.append(
        {
            "ad_id": f"ad_{i:04d}",
            "category": pick_category(),
            "advertiser_id": random.choice(ADVERTISERS),
            "ad_quality_score": max(0.0, min(1.0, random.gauss(0.6, 0.2))),  # Normal dist, clipped
            "active_from": active_from,
            "active_to": active_to,
            "created_date": datetime.combine(active_from, datetime.min.time()),
            "ad_status": "retired",
        }
    )

# Bucket 2: Long-running ads (50% = 500 ads) - Historical + Production
for i in range(200, 700):
    active_from = start_date - timedelta(days=random.randint(0, 60))

    ads_data.append(
        {
            "ad_id": f"ad_{i:04d}",
            "category": pick_category(),
            "advertiser_id": random.choice(ADVERTISERS),
            "ad_quality_score": max(0.0, min(1.0, random.gauss(0.6, 0.2))),
            "active_from": active_from,
            "active_to": None,  # Active indefinitely
            "created_date": datetime.combine(active_from, datetime.min.time()),
            "ad_status": "active",
        }
    )

# Bucket 3: Recent ads (30% = 300 ads) - Some demo + Production
for i in range(700, 1000):
    active_from = today - timedelta(days=random.randint(30, 90))

    ads_data.append(
        {
            "ad_id": f"ad_{i:04d}",
            "category": pick_category(),
            "advertiser_id": random.choice(ADVERTISERS),
            "ad_quality_score": max(0.0, min(1.0, random.gauss(0.6, 0.2))),
            "active_from": active_from,
            "active_to": None,  # Active indefinitely
            "created_date": datetime.combine(active_from, datetime.min.time()),
            "ad_status": "active",
        }
    )

# COMMAND ----------

# DBTITLE 1,Create DataFrame
schema = StructType(
    [
        StructField("ad_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("advertiser_id", StringType(), False),
        StructField("ad_quality_score", DoubleType(), False),
        StructField("active_from", DateType(), False),
        StructField("active_to", DateType(), True),
        StructField("created_date", TimestampType(), False),
        StructField("ad_status", StringType(), False),
    ]
)

ads_df = spark.createDataFrame(ads_data, schema=schema)

# COMMAND ----------

# DBTITLE 1,Write to Table
ads_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(ad_dim_table_path)
