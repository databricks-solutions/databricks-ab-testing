# Databricks Model Serving A/B Testing Framework - Deployment Guide

This guide outlines the deployment steps for the end-to-end A/B testing framework.

> **📝 Note: Demo Setup**  
> This guide deploys the framework with **demo/synthetic data** for validation and learning.  
> For production deployment with real data, see [Transitioning to Production](#transitioning-to-production) after completing the demo setup.
>
> **Databricks Apps Compatibility**  
> The same setup can be used for Databricks Apps flows as well. The Experiment Manager app, Lakebase experiment config, deterministic assignment logic, and results pipeline are reusable whether traffic originates from a Model Serving endpoint or a Databricks App backend.

## Quick Deploy (Automated)

For a fully automated deployment, use the provided script:

```bash
./deploy.sh          # Deploy to dev (default)
./deploy.sh prod     # Deploy to production
```

**What it does:** Runs all 6 deployment steps in sequence automatically.

**Duration:** ~20-30 minutes

**Prerequisites:** Same as manual deployment (see below).

Continue reading for manual step-by-step instructions.

---

## Manual Deployment Steps

## Prerequisites

Before deploying, ensure you have:
- Databricks CLI installed and authenticated
- Access to a Databricks workspace with Unity Catalog enabled
- Account admin permissions (for creating service principals)

## Pre-Deployment: Create Service Principal

**Before deploying the bundle**, you must manually create a service principal for model serving:

### Step 1: Create Service Principal

1. Go to your Databricks **Account Console**
2. Navigate to **User Management** → **Service Principals**
3. Click **Add Service Principal**
4. Set display name (e.g., `model_serving_svc`)
5. Click **Create**
6. **Copy the Application ID (UUID)** - you'll need this for configuration

### Step 2: Generate OAuth Secret

1. In the service principal details, go to **OAuth secrets** tab
2. Click **Generate secret**
3. Set lifetime to **90 days**
4. **Copy both the Client ID and Secret** (shown only once!)

### Step 3: Create Secret Scope and Store Credentials

Using Databricks CLI:

```bash
# Create secret scope
databricks secrets create-scope ab_testing

# Store credentials
databricks secrets put-secret ab_testing DATABRICKS_CLIENT_ID
# Paste the client ID when prompted

databricks secrets put-secret ab_testing DATABRICKS_CLIENT_SECRET
# Paste the client secret when prompted
```

### Step 4: Update Configuration

Edit `databricks.yml` and update the service principal ID:

```yaml
model_serving_service_principal_id:
  description: Model serving service principal application_id (UUID)
  default: YOUR-SERVICE-PRINCIPAL-UUID-HERE  # Replace with actual UUID
```

---

## Deployment Steps

### 1. Initial Bundle Deployment

Deploy all Databricks resources (jobs, experiments, app, lakebase instance):

```bash
databricks bundle deploy
```

**Expected behavior:**
- ✅ Jobs, experiments, and app definitions are created
- ✅ Lakebase instance is provisioned
- ⚠️ Synced table grants will fail (schema/tables don't exist yet - this is expected)

---

### 2. Run Setup Job

This job creates all infrastructure and generates initial data:

```bash
databricks bundle run ab_testing_setup_job
```

**What this job does:**
1. **Creates catalog and schema** (`ab_testing_dev.ab_testing`)
2. **Grants catalog permissions** to the model serving service principal
3. **Creates experiments table in Lakebase** with proper permissions
4. **Generates feature tables** (ad features, user features, training data)
5. **Trains initial CTR model** and registers in Unity Catalog
6. **Inserts historical experiments** for testing
7. **Generates mock inference data** (exposures)
8. **Generates mock application events** (clicks, impressions)

**Duration:** ~10-15 minutes

---

### 3. Deploy Synced Tables and Run Synced Table Grants Job

Now that feature tables exist, run deploy again and grant permissions for Lakebase synced tables:

```bash
databricks bundle deploy
databricks bundle run ab_testing_features_synced_table_grants_job
```

**What the grants job does:**
- Grants SELECT permissions on synced feature tables to:
  - Model serving service principal
  - Experiment manager app service principal

---

### 4. Deploy Model Wrapper

Deploy the CTR model wrapper that integrates A/B testing logic:

```bash
databricks bundle run ab_testing_deploy_model_wrapper_job
```

**What this job does:**
- Wraps the base CTR model with A/B testing assignment logic
- Configures access to Lakebase experiments table
- Registers wrapped model in Unity Catalog
- Ready for model serving endpoint deployment

---

### 5. Start Experiment Manager App

Launch the Streamlit app for managing experiments:

```bash
databricks bundle run experiment_manager_app
```

**What this app does:**
- Provides UI for creating and managing A/B experiments
- Calculates sample sizes based on statistical parameters
- Updates experiment status (Draft → Published → Completed)
- Connects directly to Lakebase experiments table

**Access:** The app URL will be displayed in the output

---

### 6. Run Results Pipeline (Optional)

Process completed experiments and calculate statistical results:

```bash
databricks bundle run ab_testing_results_pipeline_job
```

**What this job does:**
1. **Creates user-level metrics** from inference data and application events
2. **Calculates statistical results** for completed experiments
   - Performs z-tests for proportions (CTR)
   - Generates human-readable recommendations
   - Determines winning variants

**Output tables:**
- `ab_testing_dev.ab_testing.user_metrics` - User-level aggregated metrics
- `ab_testing_dev.ab_testing.results` - Experiment-level statistical results

---

## Configuration

### Key Variables (databricks.yml)

Update these in `databricks.yml` for your environment:

- `catalog_name`: Unity Catalog name (default: `ab_testing_${bundle.target}`)
- `model_serving_service_principal_id`: Service principal UUID (required - see pre-deployment steps)
- `experiment_manager_app_name`: Name of the Streamlit app (default: `experiment_manager_app`)
- `lakebase_instance_name`: Lakebase instance name (default: `ab-testing`)
- `secret_scope_name`: Secret scope for OAuth credentials (default: `ab_testing`)

---

## Important Notes

### Service Principal Management
- **Service principals must be created manually** before deployment
- Use the **application_id (UUID)**, not the display name
- OAuth secrets expire after 90 days - plan to rotate them
- Store credentials securely in Databricks secret scopes

### Lakebase Permissions
- Database instance roles are created automatically during setup
- Grants are managed through the setup notebooks
- The app assumes infrastructure exists (no runtime table creation)

### Mock Data
- The setup job generates historical experiments and mock data for testing
- In production, replace mock event generation with real application events
- Mock inference data simulates model serving endpoint requests/responses

### Results Pipeline
- Designed to run incrementally (only processes new completed experiments)
- Uses fully qualified table paths from `databricks.yml`
- Can be scheduled to run daily/weekly

---

## Troubleshooting

### Service Principal Not Found
**Cause:** UUID not updated in `databricks.yml` or service principal deleted  
**Solution:** Verify the service principal exists and update `model_serving_service_principal_id`

### OAuth Token Expired
**Cause:** 90-day OAuth secret has expired  
**Solution:** Generate a new secret and update the secret scope

### Synced Table Grants Fail
**Cause:** Feature tables don't exist yet  
**Solution:** Run step 2 (setup job) first

### App Cannot Connect to Database
**Cause:** Experiments table not created  
**Solution:** Ensure step 2 (setup job) completed successfully

### Model Wrapper Deploy Fails
**Cause:** Base model or synced tables not available  
**Solution:** Verify steps 2 and 3 completed

---

## Next Steps

After deployment:

### 1. Use the Framework in Production

1. **Access the Experiment Manager App**
   - Navigate to the app in your Databricks workspace
   - Review the demo experiments already created

2. **Create Your First Experiment**
   - Use the app UI to define new experiments
   - Configure feature flags for control and treatment
   - Set MDE and review sample size calculations

3. **Publish and Activate**
   - Publish the experiment to make it active
   - The model serving endpoint will automatically start assigning users

4. **Deploy Model Serving Endpoint** (if not already deployed)
   - Run: `databricks bundle run ab_testing_deploy_model_wrapper_job`
   - Verify the endpoint is serving and capturing inference data

5. **Monitor Exposure**
   - Check the Experiment Manager App for user exposure counts
   - Verify users are being assigned to control/treatment

6. **Analyze Results**
   - When experiment completes, run: `databricks bundle run ab_testing_results_pipeline_job`
   - Review results in the `results` table or via the Experiment Manager App
   - Use statistical recommendations to make decisions

---

## Transitioning to Production

The deployment above sets up the framework with **synthetic demo data**. To use in production with real data, follow these steps:

### 1. Understand What Changes

**🧪 Demo Components (Remove/Replace):**
- `generate_ad_dim.py` → Point to your real ad inventory table
- `generate_user_dim.py` → Point to your real user dimension table
- `generate_user_events.py` → Point to your real application event stream
- `generate_demo_inference_table.py` → Use actual Model Serving inference table
- `generate_historical_experiments.py` → Create experiments via app instead

**🎯 Production Components (Keep):**
- Experiment Manager App
- CTRPyFunc wrapper (`model_serving_ab_testing/model/src/`)
- Results pipeline (`model_serving_ab_testing/results/`)
- Lakebase infrastructure
- All `resources/` configuration

**🔄 Adapt (Point to Real Data):**
- Feature engineering notebooks (`generate_ad_features.py`, `generate_user_features.py`, `generate_training_data.py`)
- Training pipeline (`train_ctr_model.py`)

### 2. Update Configuration

Edit `databricks.yml` for your production target:

```yaml
targets:
  prod:
    mode: production
    variables:
      # Point to YOUR production data tables
      ad_dim_table_path: "prod.inventory.ads"
      user_dim_table_path: "prod.users.user_dim"
      user_events_table_path: "prod.events.user_interactions"
      
      # Use ACTUAL Model Serving inference table
      # Format: catalog.schema.endpoint_name_payload_request
      inference_table_path: "prod.ml.ctr_model_endpoint_payload_request"
      
      # Feature tables (generated from your data)
      ad_features_table_path: "prod.ml.ad_features"
      user_features_table_path: "prod.ml.user_features"
      training_features_table_path: "prod.ml.training_features"
      
      # Model registry
      registered_model_path: "prod.ml.ctr_model"
      
      # Results tables
      user_metrics_table_path: "prod.ml.user_metrics"
      results_table_path: "prod.ml.experiment_results"
```

### 3. Adapt Setup Job

Create a **production setup job** that:

**✅ Runs (Infrastructure Setup):**
- `setup_unity_catalog.py` - Create catalog/schema
- `create_experiments_table.py` - Create Lakebase table
- Feature synced table grants

**❌ Skip (Demo Data Generation):**
- `generate_ad_dim.py` (use your real ad table)
- `generate_user_dim.py` (use your real user table)
- `generate_user_events.py` (use your real events)
- `generate_historical_experiments.py` (create via app)
- `generate_demo_inference_table.py` (use real inference)

**🔄 Adapt (Feature Pipeline):**
- `generate_ad_features.py` → reads from `prod.inventory.ads`
- `generate_user_features.py` → reads from `prod.users.*` and `prod.events.*`
- `generate_training_data.py` → joins your real features
- `train_ctr_model.py` → trains on your real data

### 4. Update Jobs

Edit `resources/jobs.yml`:

```yaml
# Production feature pipeline (scheduled daily/weekly)
prod_feature_pipeline_job:
  name: prod_ab_testing_feature_pipeline
  schedule:
    quartz_cron_expression: "0 0 2 * * ?"  # Daily at 2 AM
    timezone_id: "UTC"
  tasks:
    - task_key: generate_ad_features
      # ... points to prod.inventory.ads
    - task_key: generate_user_features
      # ... points to prod.users.*, prod.events.*
    - task_key: generate_training_data
      # ... joins real features
    - task_key: train_model
      # ... trains on real data

# Results pipeline (scheduled daily)
prod_results_pipeline_job:
  name: prod_ab_testing_results_pipeline
  schedule:
    quartz_cron_expression: "0 0 3 * * ?"  # Daily at 3 AM
    timezone_id: "UTC"
  tasks:
    - task_key: create_user_metrics
      base_parameters:
        inference_table_path: ${var.inference_table_path}  # Real inference table
    - task_key: calculate_results
      # ... processes completed experiments
```

### 5. Deploy to Production

```bash
# Deploy infrastructure and app
databricks bundle deploy -t prod

# Run one-time setup (infrastructure only)
databricks bundle run prod_setup_infrastructure_job -t prod

# Deploy model serving endpoint
databricks bundle run ab_testing_deploy_model_wrapper_job -t prod

# Schedule feature pipeline and results pipeline
# (Jobs will run automatically on schedule)
```

### 6. Production Workflow

**Daily/Weekly (Automated):**
1. Feature pipeline runs → generates features from latest data
2. Model training runs → trains on latest data, registers new version
3. Results pipeline runs → analyzes any completed experiments

**Ad-Hoc (Via App):**
1. Create new experiment via Experiment Manager App
2. Set feature flags, dates, MDE
3. Publish to activate
4. Monitor in app (exposure counts, status)

**When Experiment Completes:**
1. Results pipeline automatically processes it (next scheduled run)
2. Review results in `results` table or via BI dashboard
3. Make decision based on statistical recommendation

### 7. Data Requirements

Ensure your production data has the following:

**Ad Dimension (`ad_dim_table_path`):**
- `ad_id` (STRING) - Unique ad identifier
- `category` (STRING) - Ad category/type
- `ad_quality_score` (DOUBLE) - Quality/relevance score (0-1)
- `active_from` (DATE) - When ad became active
- `active_to` (DATE) - When ad was retired (NULL if still active)

**User Dimension (`user_dim_table_path`):**
- `user_id` (STRING/INT) - Unique user identifier
- `age_group` (STRING) - User age bracket
- `device` (STRING) - Primary device (mobile/desktop/tablet)
- `region` (STRING) - User's geographic region
- `signup_date` (DATE) - User registration date

**User Events (`user_events_table_path`):**
- `user_id` (STRING/INT) - User identifier
- `event_name` (STRING) - Event type (session_start, ad_impression, ad_click, ad_conversion)
- `event_timestamp` (TIMESTAMP) - When event occurred
- `date` (DATE) - Partition column
- `device` (STRING) - Device used
- `region` (STRING) - User location
- `ad_id` (STRING) - Ad ID (for impression/click events)
- `session_id` (STRING) - Session identifier

**Inference Table (Auto-generated by Model Serving):**
- Automatically created when Model Serving endpoint is deployed with `auto_capture_config`
- Format: `{catalog}.{schema}.{endpoint_name}_payload_request`
- Contains: `request` (JSON), `response` (JSON), `timestamp_ms`, `date`

### 8. Validation Checklist

Before going live in production:

- [ ] Real data tables are accessible and have correct schemas
- [ ] Feature engineering notebooks run successfully with real data
- [ ] Model trains and registers successfully
- [ ] Service principal has permissions on all tables
- [ ] Model serving endpoint is deployed and healthy
- [ ] Inference table is being populated (check after a few requests)
- [ ] Experiment Manager App can connect to Lakebase
- [ ] Results pipeline processes real inference data correctly
- [ ] All scheduled jobs have appropriate retry/alert policies

### 9. Monitoring

Set up monitoring for:

**Model Serving:**
- Endpoint health and latency
- Inference table row counts
- Error rates in responses

**Feature Pipeline:**
- Job success/failure
- Data freshness (last update time)
- Feature distribution shifts

**Results Pipeline:**
- Job success/failure
- Number of experiments processed
- Statistical test warnings (low power, etc.)

**Experiments:**
- Exposure balance (control vs treatment)
- Sufficient sample sizes
- Experiment duration vs expected duration

---

**📖 For more details, see:**
- [README - Demo vs. Production](README.md#demo-vs-production)
- [Architecture - Demo vs. Production Setup](ARCHITECTURE.md#demo-vs-production-setup)
