# Databricks notebook source
# MAGIC %md
# MAGIC # Create Experiments Table in Lakebase
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Creates the experiments table in Lakebase Postgres (if not exists)
# MAGIC 2. Grants permissions to the Experiment Manager App service principal and Model Serving service principal

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

from databricks.sdk import WorkspaceClient
from databricks_ab_testing.lakebase.utils import (
    pg_cursor,
    ensure_database_instance_role,
    grant_schema_and_table_permissions,
)
from databricks_ab_testing.utils import get_app_service_principal_id

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("lakebase_instance_name", "")
dbutils.widgets.text("lakebase_experiments_table_path", "")
dbutils.widgets.text("model_serving_service_principal_id", "")
dbutils.widgets.text("experiment_manager_app_name", "")

lakebase_instance_name = dbutils.widgets.get("lakebase_instance_name")
lakebase_experiments_table_path = dbutils.widgets.get("lakebase_experiments_table_path")
model_serving_service_principal_id = dbutils.widgets.get("model_serving_service_principal_id")
experiment_manager_app_name = dbutils.widgets.get("experiment_manager_app_name")

# COMMAND ----------

# DBTITLE 1,Initialize Workspace Client
w = WorkspaceClient()

# COMMAND ----------

DATABASE, SCHEMA, TABLE = lakebase_experiments_table_path.split(".")
print(f"Lakebase experiments table path: {lakebase_experiments_table_path}")
print(f"Database: {DATABASE}")
print(f"Schema: {SCHEMA}")
print(f"Table: {TABLE}")

# COMMAND ----------

# DBTITLE 1,Get App Service Principal
# Get App Service Principal (optional - may not exist yet)
app_service_principal_id = get_app_service_principal_id(w, experiment_manager_app_name)

if app_service_principal_id:
    print(f"✓ Found app service principal: {app_service_principal_id}")
else:
    print("⚠ App service principal not found - will skip app grants")

# COMMAND ----------

# DBTITLE 1,Create Schema and Experiments Table
# Schema matches the one in experiment_manager_app/db.py

create_table_ddl = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};

CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    experiment_id           CHAR(36) PRIMARY KEY,
    experiment_name         VARCHAR(100) NOT NULL,
    status                  VARCHAR(20) NOT NULL,
    start_date              DATE,
    end_date                DATE,
    treatment_allocation    NUMERIC(5,3),
    control_config          JSONB,
    treatment_config        JSONB,
    mde                     NUMERIC(5,3),
    power                   NUMERIC(5,3),
    significance_level      NUMERIC(5,3),
    control_sample_size     NUMERIC(5,0),
    treatment_sample_size   NUMERIC(5,0),
    primary_kpi_metric      TEXT
);
"""

with pg_cursor(w, lakebase_instance_name, DATABASE) as cur:
    cur.execute(create_table_ddl)

print(f"✓ Created {SCHEMA}.{TABLE} table")

# COMMAND ----------

# DBTITLE 1,Ensure Database Instance Roles
# Create database instance roles for service principals
print("\nEnsuring database instance roles:")

ensure_database_instance_role(w, lakebase_instance_name, model_serving_service_principal_id)

if app_service_principal_id:
    ensure_database_instance_role(w, lakebase_instance_name, app_service_principal_id)

# COMMAND ----------

# DBTITLE 1,Grant Permissions
print("\nGranting permissions:")

with pg_cursor(w, lakebase_instance_name, DATABASE) as cur:
    # Grant to App Service Principal (full CRUD access)
    if app_service_principal_id:
        try:
            grant_schema_and_table_permissions(
                cur, SCHEMA, TABLE, app_service_principal_id, permissions=["SELECT", "INSERT", "UPDATE", "DELETE"]
            )
            print(f"✓ Granted full access to app service principal")
        except Exception as e:
            print(f"⚠ Could not grant to app: {e}")

    # Grant to Model Serving Service Principal (read-only)
    try:
        grant_schema_and_table_permissions(
            cur, SCHEMA, TABLE, model_serving_service_principal_id, permissions=["SELECT"]
        )
        print(f"✓ Granted SELECT to model serving service principal")
    except Exception as e:
        print(f"⚠ Could not grant to model serving service principal: {e}")

print("\n✓ Setup complete!")
