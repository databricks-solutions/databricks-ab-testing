# Databricks notebook source

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
from model_serving_ab_testing.lakebase.utils import (
    pg_cursor,
    ensure_database_instance_role,
    grant_schema_and_table_permissions,
)
from model_serving_ab_testing.utils import get_app_service_principal_id

# COMMAND ----------

dbutils.widgets.text("lakebase_instance_name", "")
dbutils.widgets.text("lakebase_table_path", "")
dbutils.widgets.text("experiment_manager_app_name", "")
dbutils.widgets.text("model_serving_service_principal_id", "")

lakebase_instance_name = dbutils.widgets.get("lakebase_instance_name")
lakebase_table_path = dbutils.widgets.get("lakebase_table_path")
experiment_manager_app_name = dbutils.widgets.get("experiment_manager_app_name")
model_serving_service_principal_id = dbutils.widgets.get("model_serving_service_principal_id")

# Parse the table path
catalog, schema, table = lakebase_table_path.split(".")
database = catalog  # For Lakebase, the catalog name is the database name

print(f"Lakebase instance: {lakebase_instance_name}")
print(f"Database: {database}")
print(f"Schema: {schema}")
print(f"Table: {table}")
print(f"Model serving service principal: {model_serving_service_principal_id}")

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Get App Service Principal
app_service_principal_id = get_app_service_principal_id(w, experiment_manager_app_name)

if app_service_principal_id:
    print(f"✓ Found app service principal: {app_service_principal_id}")
else:
    raise Exception(f"App service principal not found for app: {experiment_manager_app_name}")

# COMMAND ----------

# DBTITLE 1,Ensure Database Instance Roles
print("\nEnsuring database instance roles:")
ensure_database_instance_role(w, lakebase_instance_name, app_service_principal_id)
ensure_database_instance_role(w, lakebase_instance_name, model_serving_service_principal_id)

# COMMAND ----------

# DBTITLE 1,Grant Permissions
print("\nGranting permissions:")

with pg_cursor(w, lakebase_instance_name, database) as cursor:
    try:
        grant_schema_and_table_permissions(cursor, schema, table, app_service_principal_id, permissions=["SELECT"])
        print(f"✓ Granted SELECT to app service principal")
    except Exception as e:
        print(f"⚠ Could not grant to app service principal: {e}")

    try:
        grant_schema_and_table_permissions(
            cursor, schema, table, model_serving_service_principal_id, permissions=["SELECT"]
        )
        print(f"✓ Granted SELECT to model serving service principal")
    except Exception as e:
        print(f"⚠ Could not grant to model serving service principal: {e}")

print("\n✓ Setup complete!")
