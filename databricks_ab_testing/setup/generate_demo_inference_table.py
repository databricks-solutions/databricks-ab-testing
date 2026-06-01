# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Demo Inference Table
# MAGIC
# MAGIC Creates synthetic inference table data with **controlled outcomes** for demo purposes.
# MAGIC
# MAGIC **Purpose**: Showcase A/B testing framework with three different statistical outcomes:
# MAGIC - Experiment 1: **Treatment WINS** (+15% CTR lift) → Deploy treatment ✓
# MAGIC - Experiment 2: **No Significant Difference** (+2% CTR lift) → Inconclusive ⚠
# MAGIC - Experiment 3: **Control WINS** (-20% CTR drop) → Don't deploy ✗
# MAGIC
# MAGIC **Note**: This is independent of user_events - we control the outcomes directly.

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("demo_inference_table_path", "")
dbutils.widgets.text("experiments_table_path", "")
dbutils.widgets.text("user_dim_table_path", "")
dbutils.widgets.text("ad_features_table_path", "")
dbutils.widgets.text("registered_model_path", "")

demo_inference_table_path = dbutils.widgets.get("demo_inference_table_path")
experiments_table_path = dbutils.widgets.get("experiments_table_path")
user_dim_table_path = dbutils.widgets.get("user_dim_table_path")
ad_features_table_path = dbutils.widgets.get("ad_features_table_path")
registered_model_path = dbutils.widgets.get("registered_model_path")

print(f"Demo inference table: {demo_inference_table_path}")
print(f"Experiments table: {experiments_table_path}")
print(f"User dimension: {user_dim_table_path}")
print(f"Ad features: {ad_features_table_path}")
print(f"Registered model: {registered_model_path}")

# COMMAND ----------

# DBTITLE 1,Imports
import hashlib
import json
import random
import uuid
from datetime import datetime, timedelta, date
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, MapType, TimestampType

random.seed(42)

# COMMAND ----------

# DBTITLE 1,Load Published Experiments
experiments_df = (
    spark.table(experiments_table_path)
    .filter(F.col("status") == "Published")
    .filter(F.col("end_date") < F.current_date())  # Completed experiments only
    .select("experiment_id", "experiment_name", "start_date", "end_date", "treatment_allocation")
)

experiments = experiments_df.collect()
print(f"Found {len(experiments)} published experiments to generate inference data for")

if not experiments:
    dbutils.notebook.exit("No published experiments found")

for exp in experiments:
    print(f"  - {exp.experiment_name}: {exp.start_date} to {exp.end_date}")

# COMMAND ----------

# DBTITLE 1,Load User and Ad Data
users_df = spark.table(user_dim_table_path).select("user_id", "device")
ads_df = spark.table(ad_features_table_path).select("ad_id", "_ad_quality_for_rank")

users_list = users_df.collect()
ads_list = ads_df.collect()

print(f"Loaded {len(users_list):,} users")
print(f"Loaded {len(ads_list):,} ads")

# COMMAND ----------

# DBTITLE 1,Define Outcome Configurations
# Map experiment names to desired outcomes
EXPERIMENT_OUTCOMES = {
    "CTR Boost - Temperature Test": {
        "control_base_ctr": 0.05,
        "treatment_base_ctr": 0.0575,
        "description": "Treatment WINS (+15% CTR)",
    },
    "Mobile Interaction Boost": {
        "control_base_ctr": 0.05,
        "treatment_base_ctr": 0.051,
        "description": "No significant difference (+2% CTR)",
    },
    "Aggressive Floor Test": {
        "control_base_ctr": 0.05,
        "treatment_base_ctr": 0.040,
        "description": "Control WINS (-20% CTR in treatment)",
    },
}

print("\n=== Configured Outcomes ===")
for exp_name, config in EXPERIMENT_OUTCOMES.items():
    print(f"{exp_name}:")
    print(f"  Control CTR: {config['control_base_ctr']*100:.2f}%")
    print(f"  Treatment CTR: {config['treatment_base_ctr']*100:.2f}%")
    print(f"  → {config['description']}")

# COMMAND ----------


# DBTITLE 1,Helper Functions
def hash_user_to_variant(user_id, experiment_id, treatment_allocation):
    """Deterministic assignment using SHA-256 hash"""
    key = f"{experiment_id}:{user_id}"
    hash_value = int(hashlib.sha256(key.encode()).hexdigest(), 16) / (2**256)
    return "treatment" if hash_value < treatment_allocation else "control"


def should_click(base_ctr, device):
    """Determine if user clicks with device-specific variation"""
    # Add device-based variation
    device_mult = {"mobile": 1.1, "desktop": 1.0, "tablet": 0.95}.get(device, 1.0)

    # Add random noise
    noise = random.gauss(0, 0.01)

    effective_ctr = base_ctr * device_mult + noise
    effective_ctr = max(0.01, min(0.15, effective_ctr))  # Clip to reasonable range

    return random.random() < effective_ctr


# COMMAND ----------

# DBTITLE 1,Generate Inference Data for Each Experiment
all_inference_records = []

for exp in experiments:
    experiment_id = exp.experiment_id
    experiment_name = exp.experiment_name
    start_date = exp.start_date
    end_date = exp.end_date
    treatment_allocation = exp.treatment_allocation

    print(f"\n{'='*80}")
    print(f"Generating inference data for: {experiment_name}")
    print(f"Period: {start_date} to {end_date}")

    # Get outcome configuration for this experiment
    outcome_config = EXPERIMENT_OUTCOMES.get(experiment_name)
    if not outcome_config:
        print(f"WARNING: No outcome configuration for {experiment_name}, using defaults")
        outcome_config = {"control_base_ctr": 0.05, "treatment_base_ctr": 0.05}

    control_base_ctr = outcome_config["control_base_ctr"]
    treatment_base_ctr = outcome_config["treatment_base_ctr"]

    print(f"Target: Control CTR = {control_base_ctr*100:.2f}%, Treatment CTR = {treatment_base_ctr*100:.2f}%")

    # Generate inference records for this experiment period
    num_days = (end_date - start_date).days + 1
    requests_per_day = 5000  # ~70K total per experiment

    experiment_clicks_control = 0
    experiment_impressions_control = 0
    experiment_clicks_treatment = 0
    experiment_impressions_treatment = 0

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)

        # Sample users for this day
        daily_users = random.sample(users_list, k=min(requests_per_day, len(users_list)))

        for user in daily_users:
            # Assign to variant
            variant = hash_user_to_variant(user.user_id, experiment_id, treatment_allocation)
            base_ctr = treatment_base_ctr if variant == "treatment" else control_base_ctr

            # Random time during the day
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)

            request_time = datetime.combine(current_date, datetime.min.time()).replace(
                hour=hour, minute=minute, second=second
            )

            # Sample ads for this request
            num_ads = random.randint(5, 15)
            ad_candidates = random.sample(ads_list, k=min(num_ads, len(ads_list)))

            # Rank ads (simple ranking by quality + random noise)
            ranked_ads = sorted(
                ad_candidates, key=lambda ad: ad._ad_quality_for_rank + random.gauss(0, 0.1), reverse=True
            )[:10]  # Top 10

            # Determine if user clicks any ad
            clicked = should_click(base_ctr, user.device)
            clicked_ad_id = ranked_ads[0].ad_id if clicked and ranked_ads else None

            # Track metrics for verification
            if variant == "control":
                experiment_impressions_control += 1
                if clicked:
                    experiment_clicks_control += 1
            else:
                experiment_impressions_treatment += 1
                if clicked:
                    experiment_clicks_treatment += 1

            # Create inference record matching Databricks Model Serving schema
            request_id = str(uuid.uuid4())

            request_json = json.dumps(
                {"user_id": user.user_id, "device": user.device, "ad_candidates": [ad.ad_id for ad in ad_candidates]}
            )

            response_json = json.dumps(
                {
                    "ranked_ads": [ad.ad_id for ad in ranked_ads],
                    "experiment_id": experiment_id,
                    "variant": variant,
                    "clicked": clicked,
                    "clicked_ad_id": clicked_ad_id,
                }
            )

            all_inference_records.append(
                {
                    "databricks_request_id": request_id,
                    "client_request_id": None,
                    "date": current_date,
                    "timestamp_ms": int(request_time.timestamp() * 1000),
                    "status_code": 200,
                    "sampling_fraction": 1.0,
                    "execution_time_ms": random.randint(50, 200),
                    "request": request_json,
                    "response": response_json,
                    "request_metadata": {
                        "endpoint_name": "ctr_model",
                        "model_name": registered_model_path.split(".")[-1],
                        "model_version": "1",
                    },
                }
            )

    # Print actual achieved CTR for this experiment
    actual_control_ctr = (
        experiment_clicks_control / experiment_impressions_control if experiment_impressions_control > 0 else 0
    )
    actual_treatment_ctr = (
        experiment_clicks_treatment / experiment_impressions_treatment if experiment_impressions_treatment > 0 else 0
    )

    print(f"✓ Generated {experiment_impressions_control + experiment_impressions_treatment:,} inference records")
    print(f"  Control: {experiment_impressions_control:,} impressions, CTR = {actual_control_ctr*100:.2f}%")
    print(f"  Treatment: {experiment_impressions_treatment:,} impressions, CTR = {actual_treatment_ctr*100:.2f}%")
    print(
        f"  Relative lift: {((actual_treatment_ctr / actual_control_ctr) - 1)*100:+.1f}%"
        if actual_control_ctr > 0
        else ""
    )

print(f"\n{'='*80}")
print(f"Total inference records generated: {len(all_inference_records):,}")

# COMMAND ----------

# DBTITLE 1,Create DataFrame
schema = StructType(
    [
        StructField("databricks_request_id", StringType(), False),
        StructField("client_request_id", StringType(), True),
        StructField("date", StringType(), False),
        StructField("timestamp_ms", LongType(), False),
        StructField("status_code", IntegerType(), False),
        StructField("sampling_fraction", StringType(), False),
        StructField("execution_time_ms", LongType(), False),
        StructField("request", StringType(), False),
        StructField("response", StringType(), False),
        StructField("request_metadata", MapType(StringType(), StringType()), False),
    ]
)

inference_df = spark.createDataFrame(all_inference_records, schema=schema)

# COMMAND ----------

# DBTITLE 1,Write to Table
inference_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    demo_inference_table_path
)

spark.table(demo_inference_table_path).display()
