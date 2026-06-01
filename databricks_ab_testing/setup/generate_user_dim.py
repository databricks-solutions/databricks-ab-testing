# Databricks notebook source
# MAGIC %md
# MAGIC # Generate User Dimension Table
# MAGIC
# MAGIC Creates a synthetic user catalog with realistic demographic distributions.
# MAGIC
# MAGIC **User Attributes**:
# MAGIC - Device distribution: 60% mobile, 30% desktop, 10% tablet
# MAGIC - Age groups: Realistic pyramid (more 25-34 than 55+)
# MAGIC - Regions: US states with population weighting
# MAGIC - Signup dates: Spread over past 2 years

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("user_dim_table_path", "")

user_dim_table_path = dbutils.widgets.get("user_dim_table_path")

print(f"User dimension table: {user_dim_table_path}")

# COMMAND ----------

# DBTITLE 1,Imports and Setup
import random
from datetime import datetime, timedelta, date
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType

random.seed(42)

# COMMAND ----------

# DBTITLE 1,Define Parameters
NUM_USERS = 1000
today = date.today()
two_years_ago = today - timedelta(days=730)

# Device distribution
DEVICES = [
    ("mobile", 0.60),
    ("desktop", 0.30),
    ("tablet", 0.10),
]

# Age group distribution (realistic demographic pyramid)
AGE_GROUPS = [
    ("18-24", 0.20),
    ("25-34", 0.35),
    ("35-44", 0.25),
    ("45-54", 0.12),
    ("55+", 0.08),
]

# Region distribution (top 10 US states by population + others)
REGIONS = [
    ("CA", 0.15),  # California
    ("TX", 0.10),  # Texas
    ("FL", 0.08),  # Florida
    ("NY", 0.08),  # New York
    ("PA", 0.05),  # Pennsylvania
    ("IL", 0.05),  # Illinois
    ("OH", 0.04),  # Ohio
    ("GA", 0.04),  # Georgia
    ("NC", 0.04),  # North Carolina
    ("MI", 0.04),  # Michigan
    ("OTHER", 0.33),  # All other states combined
]

print(f"Generating {NUM_USERS} users")
print(f"Signup date range: {two_years_ago} to {today}")

# COMMAND ----------


# DBTITLE 1,Helper Functions
def pick_weighted(distribution):
    """Pick item from weighted distribution"""
    r = random.random()
    cumulative = 0
    for item, prob in distribution:
        cumulative += prob
        if r < cumulative:
            return item
    return distribution[-1][0]


# COMMAND ----------

# DBTITLE 1,Generate User Data
users_data = []

print("Generating user records...")

for i in range(NUM_USERS):
    user_id = f"user_{i:06d}"

    # Random signup date (more recent users)
    # Use beta distribution to skew toward recent signups
    signup_days_ago = int(random.betavariate(2, 5) * 730)
    signup_date = today - timedelta(days=signup_days_ago)

    users_data.append(
        {
            "user_id": user_id,
            "age_group": pick_weighted(AGE_GROUPS),
            "device": pick_weighted(DEVICES),
            "region": pick_weighted(REGIONS),
            "signup_date": signup_date,
            "created_at": datetime.combine(signup_date, datetime.min.time()),
            "user_status": "active",
        }
    )

    if (i + 1) % 2000 == 0:
        print(f"  Generated {i + 1}/{NUM_USERS} users...")

print(f"✓ Generated {len(users_data)} user records")

# COMMAND ----------

# DBTITLE 1,Create DataFrame
schema = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("age_group", StringType(), False),
        StructField("device", StringType(), False),
        StructField("region", StringType(), False),
        StructField("signup_date", DateType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("user_status", StringType(), False),
    ]
)

users_df = spark.createDataFrame(users_data, schema=schema)

# COMMAND ----------

# DBTITLE 1,Write to Table
users_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(user_dim_table_path)
