#!/bin/bash
set -e  # Exit on error

################################################################################
# Databricks A/B Testing Framework - Deployment Script
#
# Automates the deployment steps from DEPLOYMENT_GUIDE.md
#
# Prerequisites:
# - Service principal created with UUID in databricks.yml
# - Secret scope created with OAuth credentials
# - Databricks CLI authenticated
#
# Usage: ./deploy.sh [target]
################################################################################

TARGET="${1:-dev}"

if [ -z "${DATABRICKS_HOST}" ]; then
  echo "Error: DATABRICKS_HOST is not set."
  echo "Set it before running deploy, for example:"
  echo "  export DATABRICKS_HOST=\"https://adb-<workspace-id>.<region>.azuredatabricks.net\""
  exit 1
fi

echo "=========================================="
echo "Deploying A/B Testing Framework"
echo "Target: $TARGET"
echo "Workspace host: $DATABRICKS_HOST"
echo "=========================================="
echo ""

# Step 1: Initial deployment
echo "→ Step 1: Initial bundle deployment..."
echo "  (Note: Synced table grants may fail - this is expected)"
set +e  # Temporarily allow errors
databricks bundle deploy -t "$TARGET"
set -e  # Re-enable exit on error
echo "✓ Done"
echo ""

# Step 2: Setup job
echo "→ Step 2: Running setup job (this takes ~10-15 minutes)..."
databricks bundle run ab_testing_setup_job -t "$TARGET"
echo "✓ Done"
echo ""

# Step 3: Synced tables
echo "→ Step 3: Deploying synced tables and granting permissions..."
databricks bundle deploy -t "$TARGET"
databricks bundle run ab_testing_features_synced_table_grants_job -t "$TARGET"
echo "✓ Done"
echo ""

# Step 4: Model wrapper
echo "→ Step 4: Deploying model wrapper..."
databricks bundle run ab_testing_deploy_model_wrapper_job -t "$TARGET"
echo "✓ Done"
echo ""

# Step 5: Start app
echo "→ Step 5: Starting Experiment Manager App..."
databricks bundle run experiment_manager_app -t "$TARGET"
echo "✓ Done"
echo ""

# Step 6: Results pipeline
echo "→ Step 6: Running results pipeline..."
databricks bundle run ab_testing_results_pipeline_job -t "$TARGET"
echo "✓ Done"
echo ""

echo "=========================================="
echo "✓ Deployment Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Access the Experiment Manager App in your workspace"
echo "  2. Review experiment results in the results table"
echo "  3. See DEPLOYMENT_GUIDE.md for more information"
echo ""
