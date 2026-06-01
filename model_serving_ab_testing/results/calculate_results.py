# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate A/B Test Results
# MAGIC
# MAGIC This notebook performs statistical testing on completed experiments to determine
# MAGIC if there are significant differences between control and treatment groups.
# MAGIC
# MAGIC **Statistical Tests:**
# MAGIC - Z-test for proportions (CTR and other ratio metrics)
# MAGIC - Welch's t-test for continuous metrics
# MAGIC
# MAGIC **Outputs:**
# MAGIC - Experiment-level test statistics
# MAGIC - P-values and significance indicators
# MAGIC - Human-readable results and recommendations for dashboards

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F, types as T
from scipy import stats
from statsmodels.stats.proportion import proportions_ztest

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("user_metrics_table_path", "")
dbutils.widgets.text("results_table_path", "")
dbutils.widgets.text("experiments_table_path", "")

user_metrics_table_path = dbutils.widgets.get("user_metrics_table_path")
results_table_path = dbutils.widgets.get("results_table_path")
experiments_table_path = dbutils.widgets.get("experiments_table_path")

print(f"User metrics table: {user_metrics_table_path}")
print(f"Results table: {results_table_path}")
print(f"Experiments table: {experiments_table_path}")
# COMMAND ----------

# DBTITLE 1,Get Completed Experiments Not Yet Analyzed
experiments_df = (
    spark.table(experiments_table_path)
    .filter(F.col("status") == "Published")
    .filter(F.col("end_date") < F.current_date())
    .select(
        "experiment_id",
        "experiment_name",
        "primary_kpi_metric",
        "start_date",
        "end_date",
        "significance_level",
    )
)

# Get experiments that have already been analyzed
try:
    analyzed_experiments = (
        spark.table(results_table_path).select("experiment_id").distinct().rdd.flatMap(lambda x: x).collect()
    )
    print(f"Found {len(analyzed_experiments)} already analyzed experiments")
except Exception:
    # Table doesn't exist yet or is empty
    analyzed_experiments = []
    print("No previously analyzed experiments found")

# Filter to only unanalyzed experiments
experiments_to_analyze = experiments_df.filter(~F.col("experiment_id").isin(analyzed_experiments))

num_to_analyze = experiments_to_analyze.count()
print(f"\nFound {num_to_analyze} completed experiments to analyze")

if num_to_analyze == 0:
    dbutils.notebook.exit("No new experiments to analyze")

# COMMAND ----------

# DBTITLE 1,Read User Metrics
user_metrics_df = spark.table(user_metrics_table_path)

# COMMAND ----------

# DBTITLE 1,Define Statistical Test Functions


def run_proportion_test(control_data, treatment_data, metric_col, alpha=0.05):
    """
    Run z-test for proportions (e.g., CTR)

    Args:
        control_data: DataFrame with control group metrics
        treatment_data: DataFrame with treatment group metrics
        metric_col: Name of the metric column
        alpha: Significance level

    Returns:
        Dictionary with test results
    """
    # For proportion tests, we need counts - ensure they're native Python ints
    control_clicks = int(control_data["click_count"].sum())
    control_impressions = int(control_data["impression_count"].sum())
    treatment_clicks = int(treatment_data["click_count"].sum())
    treatment_impressions = int(treatment_data["impression_count"].sum())

    # Validate we have enough data
    if control_impressions == 0 or treatment_impressions == 0:
        raise ValueError("Cannot run proportion test with zero impressions")

    # Calculate proportions
    control_ctr = control_clicks / control_impressions
    treatment_ctr = treatment_clicks / treatment_impressions

    # Run z-test for proportions - use native Python ints for counts
    counts = np.array([treatment_clicks, control_clicks], dtype=np.int64)
    nobs = np.array([treatment_impressions, control_impressions], dtype=np.int64)

    test_stat, p_value = proportions_ztest(counts, nobs, alternative="two-sided")

    # Calculate confidence interval for difference in proportions
    # Using normal approximation
    se_control = np.sqrt(control_ctr * (1 - control_ctr) / control_impressions)
    se_treatment = np.sqrt(treatment_ctr * (1 - treatment_ctr) / treatment_impressions)
    se_diff = np.sqrt(se_control**2 + se_treatment**2)

    z_critical = stats.norm.ppf(1 - alpha / 2)
    diff = treatment_ctr - control_ctr
    ci_lower = diff - z_critical * se_diff
    ci_upper = diff + z_critical * se_diff

    return {
        "test_type": "z-test",
        "control_users": len(control_data),
        "treatment_users": len(treatment_data),
        "control_mean": control_ctr,
        "treatment_mean": treatment_ctr,
        "absolute_difference": diff,
        "relative_difference": diff / control_ctr if control_ctr > 0 else 0,
        "test_statistic": test_stat,
        "p_value": p_value,
        "alpha": alpha,
        "is_significant": p_value < alpha,
        "confidence_interval_lower": ci_lower,
        "confidence_interval_upper": ci_upper,
    }


def run_ttest(control_data, treatment_data, metric_col, alpha=0.05):
    """
    Run t-test for continuous metrics

    Args:
        control_data: DataFrame with control group metrics
        treatment_data: DataFrame with treatment group metrics
        metric_col: Name of the metric column
        alpha: Significance level

    Returns:
        Dictionary with test results
    """
    control_values = control_data[metric_col].dropna()
    treatment_values = treatment_data[metric_col].dropna()

    # Run independent samples t-test
    test_stat, p_value = stats.ttest_ind(treatment_values, control_values, equal_var=False)

    # Calculate means
    control_mean = control_values.mean()
    treatment_mean = treatment_values.mean()
    diff = treatment_mean - control_mean

    # Calculate confidence interval
    # Using Welch's t-test (unequal variances)
    n1, n2 = len(treatment_values), len(control_values)
    s1, s2 = treatment_values.std(ddof=1), control_values.std(ddof=1)
    se_diff = np.sqrt(s1**2 / n1 + s2**2 / n2)

    # Welch-Satterthwaite degrees of freedom
    df = (s1**2 / n1 + s2**2 / n2) ** 2 / ((s1**2 / n1) ** 2 / (n1 - 1) + (s2**2 / n2) ** 2 / (n2 - 1))
    t_critical = stats.t.ppf(1 - alpha / 2, df)

    ci_lower = diff - t_critical * se_diff
    ci_upper = diff + t_critical * se_diff

    return {
        "test_type": "t-test",
        "control_users": len(control_data),
        "treatment_users": len(treatment_data),
        "control_mean": control_mean,
        "treatment_mean": treatment_mean,
        "absolute_difference": diff,
        "relative_difference": diff / control_mean if control_mean != 0 else 0,
        "test_statistic": test_stat,
        "p_value": p_value,
        "alpha": alpha,
        "is_significant": p_value < alpha,
        "confidence_interval_lower": ci_lower,
        "confidence_interval_upper": ci_upper,
    }


def generate_result_text(result):
    """Generate human-readable result text and determine winner"""
    relative_pct = result["relative_difference"] * 100

    if result["is_significant"]:
        direction = "increase" if result["relative_difference"] > 0 else "decrease"
        magnitude = abs(relative_pct)

        test_result = (
            f"✓ Statistically significant {direction} of {magnitude:.1f}% "
            f"(p={result['p_value']:.4f}, α={result['alpha']})"
        )

        if result["relative_difference"] > 0:
            recommendation = "✓ Deploy treatment - shows statistically significant improvement"
            winner = "treatment"
        else:
            recommendation = "✗ Do not deploy - treatment performs worse than control"
            winner = "control"
    else:
        test_result = (
            f"No significant difference detected "
            f"(p={result['p_value']:.4f} ≥ α={result['alpha']}, "
            f"observed difference: {relative_pct:+.1f}%)"
        )
        recommendation = "⚠ Insufficient evidence - consider running longer or increasing sample size"
        winner = "no_winner"

    # Format percentage for dashboards
    relative_lift_pct = f"{relative_pct:+.1f}%"

    return test_result, recommendation, winner, relative_lift_pct


# COMMAND ----------

# DBTITLE 1,Analyze Each Experiment


def analyze_experiment(experiment_pdf):
    """Analyze a single experiment using pandas UDF approach"""

    experiment_id = experiment_pdf["experiment_id"].iloc[0]
    experiment_name = experiment_pdf["experiment_name"].iloc[0]
    metric_name = experiment_pdf["primary_kpi_metric"].iloc[0]

    if "significance_level" in experiment_pdf.columns:
        raw_alpha = experiment_pdf["significance_level"].iloc[0]
        if pd.isna(raw_alpha):
            alpha = 0.05
        else:
            alpha = float(raw_alpha)
    else:
        alpha = 0.05

    print(f"\nAnalyzing experiment: {experiment_name} ({experiment_id})")
    print(f"  Primary metric: {metric_name}")

    # Split into control and treatment
    control_data = experiment_pdf[experiment_pdf["variant"] == "control"]
    treatment_data = experiment_pdf[experiment_pdf["variant"] == "treatment"]

    print(f"  Control users: {len(control_data)}")
    print(f"  Treatment users: {len(treatment_data)}")

    if len(control_data) < 10 or len(treatment_data) < 10:
        print(f"  WARNING: Sample size too small for reliable testing")
        return None

    # Determine test type based on metric
    if metric_name == "click_through_rate":
        result = run_proportion_test(control_data, treatment_data, metric_name, alpha)
    else:
        # Default to t-test for continuous metrics
        result = run_ttest(control_data, treatment_data, metric_name, alpha)

    # Generate human-readable results
    test_result, recommendation, winner, relative_lift_pct = generate_result_text(result)

    # Compile final result with all dashboard-friendly fields
    final_result = {
        "experiment_id": experiment_id,
        "experiment_name": experiment_name,
        "metric": metric_name,
        "control_users": result["control_users"],
        "treatment_users": result["treatment_users"],
        "control_mean": result["control_mean"],
        "treatment_mean": result["treatment_mean"],
        "absolute_difference": result["absolute_difference"],
        "relative_difference": result["relative_difference"],
        "relative_lift_pct": relative_lift_pct,
        "test_type": result["test_type"],
        "test_statistic": result["test_statistic"],
        "p_value": result["p_value"],
        "alpha": result["alpha"],
        "is_significant": result["is_significant"],
        "winner": winner,
        "confidence_interval_lower": result["confidence_interval_lower"],
        "confidence_interval_upper": result["confidence_interval_upper"],
        "test_result": test_result,
        "recommendation": recommendation,
    }

    # Print summary
    print(f"  Result: {test_result}")
    print(f"  Control {metric_name}: {result['control_mean']:.4f}")
    print(f"  Treatment {metric_name}: {result['treatment_mean']:.4f}")
    print(f"  Lift: {relative_lift_pct}")
    print(f"  Winner: {winner}")
    print(f"  Recommendation: {recommendation}")

    return final_result


# COMMAND ----------

# DBTITLE 1,Run Analysis for Each Experiment
# Join user metrics with experiments to analyze
experiments_to_analyze_list = [row.experiment_id for row in experiments_to_analyze.collect()]

# Alias dataframes to avoid ambiguous column references
user_metrics_alias = user_metrics_df.alias("um")
experiments_alias = experiments_to_analyze.alias("exp")

user_metrics_to_analyze = (
    user_metrics_alias.filter(F.col("um.experiment_id").isin(experiments_to_analyze_list))
    .join(
        experiments_alias.select(
            "exp.experiment_id", "exp.experiment_name", "exp.primary_kpi_metric", "exp.significance_level"
        ),
        on=F.col("um.experiment_id") == F.col("exp.experiment_id"),
        how="inner",
    )
    .select(
        F.col("um.experiment_id").alias("experiment_id"),
        F.col("exp.experiment_name").alias("experiment_name"),
        F.col("exp.primary_kpi_metric").alias("primary_kpi_metric"),
        F.col("exp.significance_level").alias("significance_level"),
        F.col("um.user_id").alias("user_id"),
        F.col("um.variant").alias("variant"),
        F.col("um.click_through_rate").alias("click_through_rate"),
        F.col("um.impression_count").alias("impression_count"),
        F.col("um.click_count").alias("click_count"),
    )
)

# Collect and analyze each experiment
results_list = []

for experiment_id in experiments_to_analyze_list:
    experiment_data = user_metrics_to_analyze.filter(F.col("experiment_id") == experiment_id).toPandas()

    if len(experiment_data) == 0:
        continue

    result = analyze_experiment(experiment_data)
    if result:
        results_list.append(result)

if len(results_list) == 0:
    dbutils.notebook.exit("No results generated")

# COMMAND ----------

# DBTITLE 1,Write Results to Table
results_pdf = pd.DataFrame(results_list)
results_pdf["analysis_timestamp"] = pd.Timestamp.now()

results_schema = T.StructType(
    [
        T.StructField("experiment_id", T.StringType(), True),
        T.StructField("experiment_name", T.StringType(), True),
        T.StructField("metric", T.StringType(), True),
        T.StructField("control_users", T.IntegerType(), True),
        T.StructField("treatment_users", T.IntegerType(), True),
        T.StructField("control_mean", T.DoubleType(), True),
        T.StructField("treatment_mean", T.DoubleType(), True),
        T.StructField("absolute_difference", T.DoubleType(), True),
        T.StructField("relative_difference", T.DoubleType(), True),
        T.StructField("relative_lift_pct", T.StringType(), True),
        T.StructField("test_type", T.StringType(), True),
        T.StructField("test_statistic", T.DoubleType(), True),
        T.StructField("p_value", T.DoubleType(), True),
        T.StructField("alpha", T.DoubleType(), True),
        T.StructField("is_significant", T.BooleanType(), True),
        T.StructField("winner", T.StringType(), True),
        T.StructField("confidence_interval_lower", T.DoubleType(), True),
        T.StructField("confidence_interval_upper", T.DoubleType(), True),
        T.StructField("test_result", T.StringType(), True),
        T.StructField("recommendation", T.StringType(), True),
        T.StructField("analysis_timestamp", T.TimestampType(), True),
    ]
)

results_spark_df = spark.createDataFrame(results_pdf, schema=results_schema)

# Append to results table
results_spark_df.write.format("delta").mode("append").saveAsTable(results_table_path)

print(f"\n✓ Results written to {results_table_path}")
print(f"Total experiments analyzed: {len(results_list)}")
