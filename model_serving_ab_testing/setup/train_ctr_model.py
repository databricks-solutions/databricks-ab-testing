# Databricks notebook source
# MAGIC %pip install -r ./../requirements.txt
# MAGIC %restart_python

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

# COMMAND ----------

dbutils.widgets.text("mlflow_experiment_name", "")
dbutils.widgets.text("training_features_table_path", "")
dbutils.widgets.text("registered_model_path", "")

mlflow_experiment_name = dbutils.widgets.get("mlflow_experiment_name")
training_features_table_path = dbutils.widgets.get("training_features_table_path")
registered_model_path = dbutils.widgets.get("registered_model_path")

# COMMAND ----------

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

mlflow.set_experiment(mlflow_experiment_name)

# COMMAND ----------

features = spark.table(training_features_table_path).toPandas()

feature_cols = [
    "hour",
    "device",
    "region",
    "category",
    "ad_quality",
    "age_group",
    "historical_ctr_30d",
    "sessions_30d",
    "impressions_30d",
    "clicks_30d",
    "days_since_signup",
]

label_col = "clicked"

X = features[feature_cols]
y = features[label_col]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# COMMAND ----------

cat_cols = ["device", "region", "category", "age_group"]
num_cols = [
    "hour",
    "ad_quality",
    "historical_ctr_30d",
    "sessions_30d",
    "impressions_30d",
    "clicks_30d",
    "days_since_signup",
]

pre = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False, drop=None), cat_cols),
        ("num", "passthrough", num_cols),
    ]
)

clf = LogisticRegression(max_iter=500, solver="lbfgs")

sk_pipe = Pipeline(steps=[("pre", pre), ("lr", clf)])
sk_pipe.fit(X_train, y_train)

proba = sk_pipe.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, proba)
pr = average_precision_score(y_test, proba)

# COMMAND ----------

mlflow.log_metric("auc", auc)
mlflow.log_metric("pr_area", pr)

input_example = X_train.head(3)
signature = infer_signature(input_example, sk_pipe.predict_proba(input_example)[:, 1])

mlflow.sklearn.log_model(
    sk_pipe, name="model", registered_model_name=registered_model_path, input_example=input_example, signature=signature
)

# COMMAND ----------

client = mlflow.tracking.MlflowClient()
latest_ver = max([int(v.version) for v in client.search_model_versions(f"name='{registered_model_path}'")])
client.set_registered_model_alias(registered_model_path, "Champion", latest_ver)

latest_ver
