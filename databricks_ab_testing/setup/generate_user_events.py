# Databricks notebook source
# MAGIC %md
# MAGIC # Generate User Events (Application Events)
# MAGIC
# MAGIC This notebook generates mock application events that simulate user behavior.
# MAGIC These are the events your application would send when users interact with ads.
# MAGIC
# MAGIC **Event types**:
# MAGIC - `session_start`: User started a new session
# MAGIC - `ad_impression`: User was shown an ad
# MAGIC - `ad_click`: User clicked on an ad
# MAGIC - `ad_conversion`: User completed desired action after clicking
# MAGIC - `session_end`: User ended their session
# MAGIC
# MAGIC **Volume**: ~10K events/day × 90 days = ~900K events

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("user_events_table_path", "")
dbutils.widgets.text("user_dim_table_path", "")
dbutils.widgets.text("ad_dim_table_path", "")

user_events_table_path = dbutils.widgets.get("user_events_table_path")
user_dim_table_path = dbutils.widgets.get("user_dim_table_path")
ad_dim_table_path = dbutils.widgets.get("ad_dim_table_path")

print(f"User events table: {user_events_table_path}")
print(f"User dimension table: {user_dim_table_path}")
print(f"Ad dimension table: {ad_dim_table_path}")

# COMMAND ----------

# DBTITLE 1,Imports and Setup
import random
import uuid
import json
from datetime import datetime, timedelta, date
from pyspark.sql import functions as F, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

random.seed(42)

# COMMAND ----------

# DBTITLE 1,Define Parameters

EVENTS_PER_DAY_TARGET = 10_000
DAYS_IN_PERIOD = 90
BASELINE_CTR = 0.05
AVG_SESSIONS_PER_ACTIVE_USER = 2
AVG_ADS_PER_SESSION = 4
USER_ACTIVITY_RATE = 0.05

today = date.today()
six_months_ago = today - timedelta(days=DAYS_IN_PERIOD)

users_df = spark.table(user_dim_table_path).select("user_id", "device", "region", "signup_date")
ads_df = spark.table(ad_dim_table_path).select("ad_id", "active_from", "active_to")

schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_name", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("device", StringType(), False),
        StructField("region", StringType(), False),
        StructField("ad_id", StringType(), True),
        StructField("ad_category", StringType(), True),
        StructField("event_properties", StringType(), True),
    ]
)

# Create table fresh
spark.sql(f"DROP TABLE IF EXISTS {user_events_table_path}")

users_list = users_df.collect()
ads_rows = ads_df.collect()
ads_by_date = {}


def get_available_ads(d):
    if d not in ads_by_date:
        ads_by_date[d] = [r.ad_id for r in ads_rows if r.active_from <= d and (r.active_to is None or r.active_to >= d)]
    return ads_by_date[d]


def should_click(device, hour):
    device_mult = {"mobile": 1.2, "desktop": 1.0, "tablet": 0.9}.get(device, 1.0)
    time_mult = 1.3 if 18 <= hour <= 23 else 1.0
    return random.random() < (BASELINE_CTR * device_mult * time_mult)


def gen_session_events(user, current_date, available_ads, session_idx):
    hour = random.choices(range(24), weights=[0.5] * 6 + [1.0] * 12 + [1.5] * 6)[0]
    minute, second = random.randint(0, 59), random.randint(0, 59)

    session_start = datetime.combine(current_date, datetime.min.time()).replace(hour=hour, minute=minute, second=second)
    session_id = str(uuid.uuid4())

    events = []
    events.append(
        {
            "event_id": str(uuid.uuid4()),
            "user_id": user.user_id,
            "session_id": session_id,
            "event_name": "session_start",
            "event_timestamp": session_start,
            "device": user.device,
            "region": user.region,
            "ad_id": None,
            "ad_category": None,
            "event_properties": json.dumps({"session_number": session_idx + 1}),
        }
    )

    t = session_start
    num_ads = max(1, int(random.gauss(AVG_ADS_PER_SESSION, AVG_ADS_PER_SESSION * 0.3)))

    for ad_idx in range(num_ads):
        t += timedelta(seconds=random.randint(2, 30))
        ad_id = random.choice(available_ads)

        impression_id = str(uuid.uuid4())
        events.append(
            {
                "event_id": impression_id,
                "user_id": user.user_id,
                "session_id": session_id,
                "event_name": "ad_impression",
                "event_timestamp": t,
                "device": user.device,
                "region": user.region,
                "ad_id": ad_id,
                "ad_category": None,
                "event_properties": json.dumps({"position": ad_idx + 1}),
            }
        )

        if should_click(user.device, hour):
            click_time = t + timedelta(seconds=random.randint(1, 10))
            click_id = str(uuid.uuid4())
            events.append(
                {
                    "event_id": click_id,
                    "user_id": user.user_id,
                    "session_id": session_id,
                    "event_name": "ad_click",
                    "event_timestamp": click_time,
                    "device": user.device,
                    "region": user.region,
                    "ad_id": ad_id,
                    "ad_category": None,
                    "event_properties": json.dumps({"impression_id": impression_id}),
                }
            )

            if random.random() < 0.15:
                conv_time = click_time + timedelta(seconds=random.randint(5, 120))
                events.append(
                    {
                        "event_id": str(uuid.uuid4()),
                        "user_id": user.user_id,
                        "session_id": session_id,
                        "event_name": "ad_conversion",
                        "event_timestamp": conv_time,
                        "device": user.device,
                        "region": user.region,
                        "ad_id": ad_id,
                        "ad_category": None,
                        "event_properties": json.dumps(
                            {"click_id": click_id, "value": round(random.uniform(5, 200), 2)}
                        ),
                    }
                )

    session_duration = random.randint(60, 1800)
    events.append(
        {
            "event_id": str(uuid.uuid4()),
            "user_id": user.user_id,
            "session_id": session_id,
            "event_name": "session_end",
            "event_timestamp": t + timedelta(seconds=session_duration),
            "device": user.device,
            "region": user.region,
            "ad_id": None,
            "ad_category": None,
            "event_properties": json.dumps({"duration_seconds": session_duration, "ads_shown": num_ads}),
        }
    )
    return events


for day_offset in range(DAYS_IN_PERIOD):
    current_date = six_months_ago + timedelta(days=day_offset)
    available_ads = get_available_ads(current_date)
    if not available_ads:
        continue

    active_users = random.sample(users_list, k=max(1, int(len(users_list) * USER_ACTIVITY_RATE)))

    day_events = []
    for user in active_users:
        if user.signup_date > current_date:
            continue

        num_sessions = random.randint(1, AVG_SESSIONS_PER_ACTIVE_USER * 2)
        for s in range(num_sessions):
            day_events.extend(gen_session_events(user, current_date, available_ads, s))
            if len(day_events) >= EVENTS_PER_DAY_TARGET:
                break
        if len(day_events) >= EVENTS_PER_DAY_TARGET:
            break

    events_df = spark.createDataFrame(day_events[:EVENTS_PER_DAY_TARGET], schema=schema).withColumn(
        "date", F.lit(current_date)
    )

    (events_df.write.format("delta").mode("append").saveAsTable(user_events_table_path))

    if (day_offset + 1) % 30 == 0:
        print(f"Day {day_offset + 1}/{DAYS_IN_PERIOD}")
