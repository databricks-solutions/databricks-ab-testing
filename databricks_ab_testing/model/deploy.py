# Databricks notebook source
# MAGIC %pip install -r ./../requirements.txt
# MAGIC %restart_python

# COMMAND ----------

import os

import mlflow
import mlflow.pyfunc
from mlflow.models import infer_signature
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedModelInput,
    ServingModelWorkloadType,
    AutoCaptureConfigInput,
)
from src.CTRPyFunc import CTRPyFunc

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("mlflow_experiment_name", "")
dbutils.widgets.text("registered_model_path", "")
dbutils.widgets.text("lakebase_experiments_table_path", "")
dbutils.widgets.text("lakebase_ad_features_table_path", "")
dbutils.widgets.text("lakebase_user_features_table_path", "")
dbutils.widgets.text("secret_scope_name", "")
dbutils.widgets.text("lakebase_instance_name", "")
dbutils.widgets.text("lakebase_database_name", "")
dbutils.widgets.text("model_serving_service_principal_id", "")
dbutils.widgets.text("endpoint_name", "")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
mlflow_experiment_name = dbutils.widgets.get("mlflow_experiment_name")
registered_model_path = dbutils.widgets.get("registered_model_path")
lakebase_experiments_table_path = dbutils.widgets.get("lakebase_experiments_table_path")
lakebase_ad_features_table_path = dbutils.widgets.get("lakebase_ad_features_table_path")
lakebase_user_features_table_path = dbutils.widgets.get("lakebase_user_features_table_path")
secret_scope_name = dbutils.widgets.get("secret_scope_name")
lakebase_instance_name = dbutils.widgets.get("lakebase_instance_name")
lakebase_database_name = dbutils.widgets.get("lakebase_database_name")
model_serving_service_principal_id = dbutils.widgets.get("model_serving_service_principal_id")
endpoint_name = dbutils.widgets.get("endpoint_name")

# COMMAND ----------

databricks_host = dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get()

# COMMAND ----------

mlflow.set_experiment(mlflow_experiment_name)

# COMMAND ----------

w = WorkspaceClient()
mlflow_client = mlflow.tracking.MlflowClient()

instance = w.database.get_database_instance(name=lakebase_instance_name)
lakebase_host = instance.read_write_dns

# COMMAND ----------

spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog_name} TO `{model_serving_service_principal_id}`")

# COMMAND ----------

DEFAULT_FLAGS = {
    "temperature": 1.0,
    "boost_factor": 1.0,
    "ctr_floor": 0.0,
    "ctr_cap": 1.0,
    "epsilon_explore": 0.0,
    "use_interaction_boost": False,
    "interaction_boost_strength": 1.05,
    "device_uplift": {"mobile": 1.0, "desktop": 1.0, "tablet": 1.0},
}

# COMMAND ----------

os.environ["DATABRICKS_HOST"] = databricks_host
os.environ["DATABRICKS_CLIENT_ID"] = dbutils.secrets.get(secret_scope_name, "DATABRICKS_CLIENT_ID")
os.environ["DATABRICKS_CLIENT_SECRET"] = dbutils.secrets.get(secret_scope_name, "DATABRICKS_CLIENT_SECRET")
os.environ["LAKEBASE_USER"] = model_serving_service_principal_id
os.environ["LAKEBASE_HOST"] = lakebase_host
os.environ["LAKEBASE_DB"] = lakebase_database_name

ctr = CTRPyFunc()

artifacts = {"ctr_model": f"models:/{registered_model_path}@Champion"}

model_config = {
    "ad_features_table_path": lakebase_ad_features_table_path,
    "user_features_table_path": lakebase_user_features_table_path,
    "experiments_table_path": lakebase_experiments_table_path,
    "default_flags": DEFAULT_FLAGS,
}

context = mlflow.pyfunc.PythonModelContext(artifacts=artifacts, model_config=model_config)

ctr.load_context(context=context)

# COMMAND ----------

input_example = pd.DataFrame(
    [
        {
            "user_id": 1,
            "device": "mobile",
            "region": "US",
            "k": 5,
        }
    ]
)

output_example = ctr.predict(model_input=input_example, context=None)

# COMMAND ----------

output_example

# COMMAND ----------

signature = infer_signature(input_example, output_example)

# COMMAND ----------

# Clear env vars so that workspace client initialized for use below (deploy) will run as job run_as entity

os.environ.pop("DATABRICKS_HOST", None)
os.environ.pop("DATABRICKS_CLIENT_ID", None)
os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
os.environ.pop("LAKEBASE_USER", None)
os.environ.pop("LAKEBASE_HOST", None)
os.environ.pop("LAKEBASE_DB", None)

w = WorkspaceClient()

# COMMAND ----------

pyfunc_registered_model_path = f"{registered_model_path}_wrapper"

mlflow.pyfunc.log_model(
    name="model",
    python_model="./src/CTRPyFunc.py",
    registered_model_name=pyfunc_registered_model_path,
    signature=signature,
    model_config=model_config,
    pip_requirements="./../requirements.txt",
    code_paths=["./src"],
    artifacts=artifacts,
)

# COMMAND ----------

latest_ver = max(
    [int(v.version) for v in mlflow_client.search_model_versions(f"name='{pyfunc_registered_model_path}'")]
)

# COMMAND ----------

w.serving_endpoints.create_and_wait(
    name=endpoint_name,
    config=EndpointCoreConfigInput(
        name=endpoint_name,
        auto_capture_config=AutoCaptureConfigInput(
            catalog_name=catalog_name, schema_name=schema_name, table_name_prefix=endpoint_name, enabled=True
        ),
        served_models=[
            ServedModelInput(
                name=f"{endpoint_name}-{latest_ver}",
                model_name=pyfunc_registered_model_path,
                model_version=latest_ver,
                workload_type=ServingModelWorkloadType.CPU,
                workload_size="Small",
                scale_to_zero_enabled=True,
                environment_vars={
                    "DATABRICKS_HOST": databricks_host,
                    "DATABRICKS_CLIENT_ID": f"{{{{secrets/{secret_scope_name}/DATABRICKS_CLIENT_ID}}}}",
                    "DATABRICKS_CLIENT_SECRET": f"{{{{secrets/{secret_scope_name}/DATABRICKS_CLIENT_SECRET}}}}",
                    "LAKEBASE_USER": model_serving_service_principal_id,
                    "LAKEBASE_HOST": lakebase_host,
                    "LAKEBASE_DB": lakebase_database_name,
                },
            )
        ],
    ),
)
