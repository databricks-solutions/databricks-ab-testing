# Databricks A/B Testing Framework

A production-ready A/B testing framework for Databricks workloads that enables teams to run controlled experiments with deterministic user assignment, experiment tracking, and statistical evaluation.

> This framework works across Databricks deployment types: classic ML on Model Serving, agents on Model Serving, agents on Databricks Apps, and Databricks Apps backends.  
> The repository remains opinionated around a Model Serving endpoint as the reference implementation.

## 🚀 Quick Start

**Ready to deploy?** See the complete step-by-step guide: [Deployment Guide](DEPLOYMENT_GUIDE.md)

**Want to understand the architecture?** See the technical overview: [Architecture Documentation](ARCHITECTURE.md)

**Need local environment variables?** Copy `.env.example` to `.env` and fill in your values.  
For deployed environments, configure values in `databricks.yml` (via bundle variables/targets).

## Supported Deployment Patterns

- Classic ML model on Databricks Model Serving
- Agent deployed on Databricks Model Serving
- Agent backend deployed on Databricks Apps
- Databricks Apps backend/service with deterministic assignment and experiment tracking

## Problem Statement

Teams lack an easy, standardized way to perform A/B testing to understand how user behavior changes when new features or model versions are introduced across Databricks-hosted applications.

While Databricks supports traffic splitting between endpoints, it does not natively support sticky user assignments or experiment tracking—critical components for evaluating user-level impact over time. As a result, experimentation today is often ad hoc, manual, and inconsistent, making it difficult to attribute behavioral differences to specific model or feature changes.

Current approaches fall short:

- **Sequential Rollouts (Model A → Model B)**: Fail to control for seasonality, external factors, or shifting user populations, making it hard to isolate the true effect of changes
- **Traffic Splitting Across Endpoints**: Stateless and non-deterministic routing means the same user may hit different models on different requests, making user-level analysis impossible

## Solution Overview

This framework provides an end-to-end A/B testing solution within the Databricks ecosystem that enables deterministic user routing, exposure logging, and experiment metric evaluation. It makes it simple for teams to launch, monitor, and analyze experiments across production Databricks applications.

### Core Components

The reusable framework core is deployment-agnostic:
- Experiment definition and lifecycle management
- Deterministic user assignment
- Variant configuration and feature-flag resolution
- Exposure/event joins and statistical evaluation

This repository uses a Databricks Model Serving endpoint as the opinionated reference implementation for those core patterns.

#### 1. Lakebase Experiment Tables
Central source of truth for experiments, variants, treatment allocations, and status. Stores configuration metadata including:
- Experiment ID and naming
- Feature flag configurations for control and treatment
- Start/end dates and allocation ratios
- Primary KPI metrics and sample size calculations
- Experiment status (Draft, Published, Archived)

Enables queryable experiment lineage and easy integration with other Delta or BI layers.

#### 2. Experiment Manager App
A lightweight Databricks Streamlit application that allows teams to manage experiments through an intuitive interface:
- **Create & Edit Experiments**: Simple dialogs for setting up experiment parameters
- **Allocation Management**: Fixed 50/50 splits with built-in sample size calculator
- **Safety Checks**: Prevents overlapping experiments and validates date ranges
- **Status Management**: Publish, unpublish, and archive experiments with workflow controls
- **KPI-Aware Design**: Support for multiple metric types (watch time, return rates)
- **Real-time Visibility**: Track active, scheduled, and completed experiments

The app provides governance and reproducibility by interfacing directly with Lakebase experiment tables.

#### 3. Serving Integration (PyFunc Reference Implementation)
Drop-in Python functionality that integrates directly with any Databricks model's predict() logic:
- **Deterministic Assignment**: Consistent user-to-variant mapping using hashed user IDs
- **Configuration Retrieval**: Fetches active experiment settings from Lakebase
- **Feature Flag Support**: Applies treatment-specific configurations to model behavior
- **Exposure Logging**: Tracks which users see which variants (via inference tables)
- **Lakehouse Integration**: Pushes results back to Unity Catalog for unified analysis

Supports optional metric computation and enables continuous experimentation without external orchestration. The same assignment/flagging pattern can be reused in Databricks Apps or agent backends.

#### 4. Results Pipeline
Automated statistical analysis of experiment outcomes:
- **User Metrics Aggregation**: Computes per-user KPIs from engagement data
- **Statistical Testing**: Runs t-tests and z-tests based on metric types
- **Results Tables**: Stores test statistics, p-values, and significance determinations
- **Experiment Evaluation**: Automatically processes completed experiments

### Reuse in Other Application Types

Use this pattern when your runtime is not a `CTRPyFunc` model endpoint (for example: Databricks Apps backend service, custom API, or agent runtime):

1. Keep the reusable core pieces:
   - `databricks_ab_testing/model/src/AssignmentService.py`
   - Lakebase experiment table and config schema
   - Results pipeline in `databricks_ab_testing/results/`
2. Replace the runtime adapter:
   - Treat `databricks_ab_testing/model/src/CTRPyFunc.py` as the example adapter
   - Implement your own adapter in app/agent code that calls `AssignmentService`
3. Apply variant flags in your runtime logic:
   - Fetch `(experiment_id, variant, flags)` per request/user
   - Use `flags` to change behavior (ranking params, prompt strategy, retrieval weights, business rules, etc.)
4. Emit exposure metadata:
   - Include `experiment_id` and `variant` in response/log payloads so the results pipeline can join exposures to outcomes

Minimal adapter shape:

```python
from databricks_ab_testing.model.src.AssignmentService import AssignmentService

assigner = AssignmentService(
    db=lakebase_client,
    experiments_table_path=experiments_table_path,
    default_flags=default_flags,
)

def handle_request(user_id: str, request_payload: dict) -> dict:
    experiment_id, variant, flags = assigner.assign_one(user_id)

    # Replace this block with your app/agent/model logic
    result = run_business_logic(request_payload, flags=flags)

    return {
        "experiment_id": experiment_id,
        "variant": variant,
        "flags": flags,
        "result": result,
    }
```

In short: `AssignmentService` is the reusable engine, and `CTRPyFunc` is just one concrete runtime integration.

## What's in This Repository

### Project Structure

```
databricks_ab_testing/
├── experiment_manager_app/      # 🎯 PRODUCTION - Experiment management UI
│   ├── app.py                   # Main application UI and orchestration
│   ├── config.py                # App configuration and KPI definitions
│   ├── db.py                    # Database connectivity and helpers
│   ├── experiment_service.py    # CRUD operations and status transitions
│   ├── benchmark_service.py     # Metric baseline calculations
│   ├── stats.py                 # Sample size calculations
│   └── requirements.txt         # App dependencies
│
├── model/                       # 🎯 PRODUCTION - Model serving integration
│   ├── src/
│   │   ├── AssignmentService.py # Deterministic user-to-variant assignment
│   │   ├── CTRPyFunc.py        # PyFunc wrapper with A/B testing logic
│   │   └── utils/
│   │       └── lakebase.py     # Lakebase database client
│   ├── deploy.py               # Model endpoint deployment
│   └── requirements.txt
│
├── setup/                       # 🧪 DEMO ONLY - Synthetic data generation
│   ├── setup_unity_catalog.py   # UC catalog/schema setup
│   ├── create_experiments_table.py # Lakebase experiments table setup
│   ├── generate_ad_dim.py       # 🧪 DEMO: Synthetic ad catalog
│   ├── generate_user_dim.py     # 🧪 DEMO: Synthetic user base
│   ├── generate_user_events.py  # 🧪 DEMO: Synthetic user behavior
│   ├── generate_ad_features.py  # Feature engineering (uses real data in prod)
│   ├── generate_user_features.py # Feature engineering (uses real data in prod)
│   ├── generate_training_data.py # Training dataset creation
│   ├── train_ctr_model.py       # Model training
│   ├── generate_historical_experiments.py # 🧪 DEMO: Pre-populated experiments
│   └── generate_demo_inference_table.py   # 🧪 DEMO: Mock inference data
│
├── results/                     # 🎯 PRODUCTION - Statistical analysis pipeline
│   ├── create_user_metrics.py   # User-level metric aggregation
│   └── calculate_results.py     # Statistical testing and recommendations
│
├── lakebase/
│   ├── synced_table_grants.py   # Lakebase table permissions management
│   └── utils.py                 # Lakebase helper utilities
│
├── utils.py                     # Shared utilities
│
resources/                       # Databricks Asset Bundles configuration
├── apps.yml                     # Streamlit app deployment
├── experiments.yml              # MLflow experiments
├── jobs.yml                     # Workflow definitions
├── lakebase.yml                 # Database instance and synced tables
├── secrets.yml                  # Secret scope configuration
└── unity_catalog.yml            # UC schema and table definitions

databricks.yml                   # Main bundle configuration
pyproject.toml                   # Python package configuration
DEPLOYMENT_GUIDE.md              # Deployment guide
ARCHITECTURE.md                  # Technical architecture documentation
```

**Legend:**
- 🎯 **PRODUCTION** - Used in production environments
- 🧪 **DEMO ONLY** - Only for demonstration/testing purposes

---

### Demo vs. Production

This repository includes both **production-ready components** and **demo-specific components** for testing and validation.

#### 🧪 Demo Components

The demo setup generates **3 months (90 days) of synthetic data** to showcase the framework end-to-end without requiring real production data:

**What's Generated:**
- **Ad Catalog** (`ad_dim`): 1,000 ads with realistic categories, quality scores, and lifecycles
- **User Base** (`user_dim`): 1,000 users with demographics, devices, and regions
- **User Events** (`user_events`): ~900K events (sessions, impressions, clicks, conversions)
- **Historical Experiments**: 3 pre-configured experiments with different outcomes:
  - ✅ **Temperature Test**: +15% CTR lift (treatment wins)
  - ⚪ **Mobile Interaction**: No significant difference
  - ❌ **Aggressive Floor**: -20% CTR drop (treatment loses)
- **Demo Inference Data** (`demo_inference`): Mock model serving requests with controlled CTR outcomes

**Purpose:** 
- Validate the framework works end-to-end
- Test statistical calculations with known outcomes
- Provide a learning environment before deploying to production

**Demo Job:** `ab_testing_setup_job` generates all demo data

#### 🎯 Production Components

These components are **production-ready** and used in real deployments:

**Core Infrastructure (Reusable Across Deployment Types):**
- **Experiment Manager App**: Create and manage experiments through a UI
- **Assignment Service**: Deterministically assigns users to variants
- **Results Pipeline**: Analyzes completed experiments with statistical rigor
- **Lakebase Integration**: Low-latency experiment configuration lookup

**Reference Runtime in this repository:**
- **CTRPyFunc Wrapper**: Adds A/B testing to a Databricks Model Serving endpoint

**Data Pipeline (Adapted for Production):**
- **Feature Engineering**: Same notebooks, but point to real dimensions/events
- **Model Training**: Trains on your actual historical data
- **Results Analysis**: Processes real runtime exposure/inference tables (Model Serving in this reference setup)

#### 🔄 Transition from Demo to Production

To use in production:

1. **Replace Data Sources:**
   - Point feature engineering to your real `ad_dim`, `user_dim`, `user_events` tables
   - These should come from your production data pipelines

2. **Update Inference Table:**
   - Change `inference_table_path` from `demo_inference` to your actual Model Serving inference table
   - Format: `${catalog}.${schema}.${endpoint_name}_payload_request`

3. **Remove Demo Components:**
   - Skip running `generate_ad_dim.py`, `generate_user_dim.py`, `generate_user_events.py`
   - Skip `generate_historical_experiments.py` (create experiments via the app instead)
   - Skip `generate_demo_inference_table.py`

4. **Keep Production Components:**
   - Continue using Experiment Manager App for experiment creation
   - Deploy model endpoint with `CTRPyFunc` wrapper
   - Run results pipeline on schedule to analyze experiments

**Example Production Configuration:**

```yaml
# In databricks.yml (production target)
inference_table_path: "${var.catalog_name}.${var.schema_name}.ctr_model_endpoint_payload_request"
user_events_table_path: "${var.catalog_name}.app_events.user_events"  # Your real events
user_dim_table_path: "${var.catalog_name}.user_data.users"             # Your real users
ad_dim_table_path: "${var.catalog_name}.ad_data.ads"                   # Your real ads
```

---

### Key Features

#### Experiment Management
- **Intuitive UI**: Clean Streamlit interface for non-technical users
- **Validation & Safety**: Built-in checks prevent configuration errors and experiment overlap
- **Sample Size Calculator**: Automatic power analysis based on KPI type and MDE
- **Flexible KPIs**: Support for continuous (watch time) and binary (return rate) metrics
- **Status Workflow**: Draft → Published → Archived lifecycle with appropriate controls

#### Model Integration
- **Deterministic Hashing**: SHA-256-based consistent assignment ensures user stickiness
- **Feature Flags**: JSON-based configuration allows complex treatment variations
- **Real-time Lookup**: Sub-millisecond experiment configuration retrieval from Lakebase
- **Transparent Interface**: Returns experiment_id, variant, and flags alongside predictions
- **Production Ready**: Handles edge cases, fallbacks, and error conditions

#### Statistical Rigor
- **Proper Testing**: t-tests for continuous metrics, z-tests for proportions
- **Fixed Parameters**: Power = 0.80, Alpha = 0.05 (two-tailed) for consistency
- **Automated Analysis**: Results computed automatically for completed experiments
- **Result Storage**: Test statistics, p-values, and interpretations persisted for reporting

## How It Works

### 1. Experiment Creation
Teams use the Experiment Manager App to:
- Define experiment name and date range
- Select primary KPI metric
- Configure control and treatment feature flags
- Set minimum detectable effect (MDE)
- Review automatically calculated sample sizes

### 2. Experiment Activation
When an experiment is published:
- Configuration is written to the Lakebase experiments table
- Safety checks ensure no date overlap with other active experiments
- Experiment becomes immediately queryable by runtime components (Model Serving endpoint in this reference setup)

### 3. User Assignment & Runtime Execution
When a runtime request arrives (Model Serving in this reference flow):
1. **AssignmentService** queries active experiments from Lakebase
2. User ID is deterministically hashed to assign control/treatment
3. Appropriate feature flags are retrieved for the assigned variant
4. **CTRPyFunc** applies flags to model behavior (temperature, boosts, etc.)
5. Output is returned with experiment metadata

### 4. Exposure & Metrics Tracking
- Inference tables automatically capture experiment_id and variant
- Engagement events are joined with exposure data
- User-level metrics are aggregated per experiment

### 5. Statistical Analysis
After experiment completion:
- User metrics are compared between control and treatment groups
- Statistical tests determine significance
- Results are written to results tables for dashboard consumption

## Technology Stack

- **Databricks Ecosystem**:
  - Unity Catalog for data governance
  - Model Serving for inference endpoints (reference runtime in this repository)
  - Lakebase (PostgreSQL) for low-latency lookups
  - MLflow for model tracking and serving
  - Databricks Apps for UI deployment
  - Asset Bundles for infrastructure-as-code

- **Python Stack**:
  - Streamlit for UI
  - MLflow for model management
  - Scikit-learn for ML
  - Pandas/NumPy for data processing
  - SciPy/Statsmodels for statistical testing
  - psycopg for PostgreSQL connectivity

- **Infrastructure**:
  - Delta Lake for feature and results storage
  - Synced Tables for real-time data access
  - Service Principals for secure authentication
  - Secret Scopes for credential management

## Benefits

- **Fast Experimentation**: Launch experiments in minutes, not days
- **Statistical Confidence**: Built-in power analysis and rigorous testing
- **User-Level Testing**: Sticky assignments enable accurate behavioral analysis
- **Governance**: Centralized tracking with full experiment lineage
- **Scalability**: Handles production-scale traffic with sub-millisecond lookups
- **Flexibility**: Support for any model endpoint and metric type
- **Portability**: Core framework patterns apply across Model Serving, agents, and Databricks Apps backends
- **Integration**: Native Databricks ecosystem—no external tools required

## Example Use Cases

- **Model Comparison**: A/B test two ML model versions
- **Feature Testing**: Compare different ranking algorithms or scoring methods
- **Hyperparameter Tuning**: Test temperature, boost factors, or other parameters
- **Personalization**: Evaluate device-specific or audience-specific treatments
- **Product Changes**: Measure impact of new recommendation strategies

---

## 📦 Deployment

Ready to get started?

### Quick Deploy (Automated)

Use the automated deployment script to deploy the entire framework with one command:

```bash
./deploy.sh
```

This script automates all deployment steps and takes ~20-30 minutes to complete.

### Manual Deploy (Step-by-Step)

**[→ View Deployment Guide](DEPLOYMENT_GUIDE.md)**

The guide includes:
- Prerequisites and setup requirements
- Step-by-step deployment instructions
- Configuration options
- Troubleshooting tips
- Next steps after deployment

**[→ View Architecture Documentation](ARCHITECTURE.md)**

For technical deep-dive:
- System architecture diagrams
- Component interactions and data flow
- Key design decisions
- Performance characteristics
- Extension points

---

## How to get help

Databricks support does not cover this content. For questions or bugs, please open a GitHub issue and the team will help on a best-effort basis.

## License

Copyright 2025 Databricks, Inc. All rights reserved. The source in this repository is provided subject to the [Databricks License](https://databricks.com/db-license-source). All included or referenced third-party libraries are subject to their respective licenses.
