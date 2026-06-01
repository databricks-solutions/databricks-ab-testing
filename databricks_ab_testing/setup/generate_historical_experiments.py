# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Historical Experiments
# MAGIC
# MAGIC This notebook inserts historical experiment records into the Lakebase experiments table for testing the results pipeline.

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install -r ./../requirements.txt
# MAGIC %restart_python

# COMMAND ----------

import sys

dbutils.widgets.text("root_path", "")
root_path = dbutils.widgets.get("root_path")
if root_path and root_path not in sys.path:
    sys.path.append(root_path)
# COMMAND ----------

import json
import uuid
from datetime import date, timedelta
from databricks.sdk import WorkspaceClient
from databricks_ab_testing.lakebase.utils import pg_cursor

# COMMAND ----------

dbutils.widgets.text("lakebase_instance_name", "")
dbutils.widgets.text("lakebase_experiments_table_path", "")

lakebase_instance_name = dbutils.widgets.get("lakebase_instance_name")
lakebase_database_name = dbutils.widgets.get("lakebase_database_name")
lakebase_experiments_table_path = dbutils.widgets.get("lakebase_experiments_table_path")

print(f"Lakebase instance: {lakebase_instance_name}")
print(f"Lakebase database: {lakebase_database_name}")

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

today = date.today()

experiments = [
    {
        "experiment_id": str(uuid.uuid4()),
        "experiment_name": "CTR Boost - Temperature Test",
        "status": "Published",
        "start_date": today - timedelta(days=90),
        "end_date": today - timedelta(days=61),
        "treatment_allocation": 0.5,
        "control_config": json.dumps({"temperature": 1.0}),
        "treatment_config": json.dumps({"temperature": 1.2}),
        "mde": 0.0075,
        "power": 0.8,
        "significance_level": 0.05,
        "control_sample_size": 2000,
        "treatment_sample_size": 2000,
        "primary_kpi_metric": "click_through_rate",
    },
    {
        "experiment_id": str(uuid.uuid4()),
        "experiment_name": "Mobile Interaction Boost",
        "status": "Published",
        "start_date": today - timedelta(days=60),
        "end_date": today - timedelta(days=31),
        "treatment_allocation": 0.5,
        "control_config": json.dumps({"use_interaction_boost": False}),
        "treatment_config": json.dumps({"use_interaction_boost": True, "interaction_boost_strength": 1.05}),
        "mde": 0.001,
        "power": 0.8,
        "significance_level": 0.05,
        "control_sample_size": 2000,
        "treatment_sample_size": 2000,
        "primary_kpi_metric": "click_through_rate",
    },
    {
        "experiment_id": str(uuid.uuid4()),
        "experiment_name": "Aggressive Floor Test",
        "status": "Published",
        "start_date": today - timedelta(days=30),
        "end_date": today - timedelta(days=16),
        "treatment_allocation": 0.5,
        "control_config": json.dumps({"ctr_floor": 0.01}),
        "treatment_config": json.dumps({"ctr_floor": 0.10}),
        "mde": 0.01,
        "power": 0.8,
        "significance_level": 0.05,
        "control_sample_size": 2000,
        "treatment_sample_size": 2000,
        "primary_kpi_metric": "click_through_rate",
    },
]

print(f"\nPrepared {len(experiments)} historical experiments to insert:")
for exp in experiments:
    print(f"  - {exp['experiment_name']}: {exp['start_date']} to {exp['end_date']}")

# COMMAND ----------

insert_query = f"""
INSERT INTO {lakebase_experiments_table_path} (
    experiment_id, experiment_name, status, start_date, end_date,
    treatment_allocation, control_config, treatment_config,
    mde, power, significance_level, control_sample_size, treatment_sample_size,
    primary_kpi_metric
)
VALUES (
    %(experiment_id)s, %(experiment_name)s, %(status)s, %(start_date)s, %(end_date)s,
    %(treatment_allocation)s, %(control_config)s, %(treatment_config)s,
    %(mde)s, %(power)s, %(significance_level)s, %(control_sample_size)s, %(treatment_sample_size)s,
    %(primary_kpi_metric)s
)
"""

with pg_cursor(w, lakebase_instance_name, lakebase_database_name) as cur:
    for exp in experiments:
        cur.execute(f"delete from {lakebase_experiments_table_path} where experiment_name='{exp['experiment_name']}'")
        cur.execute(insert_query, exp)

print(f"\n✓ Inserted {len(experiments)} historical experiments")
