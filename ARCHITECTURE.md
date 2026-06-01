# Architecture Documentation

## Overview

The Databricks A/B Testing Framework is a lakehouse-native system that enables controlled experiments across Databricks-hosted applications. The architecture follows a producer-consumer pattern with clear separation of concerns across experiment management, serving/application execution, and results analysis. This document uses Model Serving as the primary reference implementation.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Databricks Workspace                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────────┐         ┌──────────────────────────────────┐      │
│  │  Experiment      │         │      Lakebase PostgreSQL         │      │
│  │  Manager App     │◄───────►│  ┌────────────────────────────┐  │      │
│  │  (Streamlit)     │  CRUD   │  │    experiments             │  │      │
│  └────────┬─────────┘         │  │  ┌──────────────────────┐  │  │      │
│           │                   │  │  │ experiment_id        │  │  │      │
│           │                   │  │  │ experiment_name      │  │  │      │
│           │                   │  │  │ status               │  │  │      │
│           │ Publish           │  │  │ start_date/end_date  │  │  │      │
│           │                   │  │  │ control_config       │  │  │      │
│           ▼                   │  │  │ treatment_config     │  │  │      │
│  ┌──────────────────┐         │  │  │ treatment_allocation │  │  │      │
│  │  Unity Catalog   │◄────────┤  │  └──────────────────────┘  │  │      │
│  │  (Federated)     │ Synced  │  └────────────────────────────┘  │      │
│  │                  │         └──────────────────────────────────┘      │
│  │  experiments     │                                                   │
│  │  (read-only)     │                                                   │
│  └────────┬─────────┘                                                   │
│           │                                                             │
│           │ Query at inference time                                     │
│           │                                                             │
│  ┌────────▼─────────────────────────────────────────┐                   │
│  │          Model Serving Endpoint                  │                   │
│  │  ┌─────────────────────────────────────────────┐ │                   │
│  │  │         CTRPyFunc (PyFunc Model)            │ │                   │
│  │  │  ┌───────────────────────────────────────┐  │ │                   │
│  │  │  │  1. Load active experiment config     │  │ │                   │
│  │  │  │     from experiments table            │  │ │                   │
│  │  │  │                                       │  │ │                   │
│  │  │  │  2. Hash user_id → variant            │  │ │                   │
│  │  │  │     (AssignmentService)               │  │ │                   │
│  │  │  │                                       │  │ │                   │
│  │  │  │  3. Apply variant flags               │  │ │                   │
│  │  │  │     (temperature, boosts, etc.)       │  │ │                   │
│  │  │  │                                       │  │ │                   |
│  │  │  │  4. Rank ads with base model          │  │ │                   │
│  │  │  │                                       │  │ │                   │
│  │  │  │  5. Log exposure in response          │  │ │                   │
│  │  │  └───────────────────────────────────────┘  │ │                   │
│  │  └─────────────────────────────────────────────┘ │                   │
│  └────────┬─────────────────────────────────────────┘                   │
│           │                                                             │
│           │ Auto-capture                                                │
│           ▼                                                             │
│  ┌──────────────────┐                                                   │
│  │  Inference Table │                                                   │
│  │  (Delta)         │                                                   │
│  │                  │                                                   │
│  │  request JSON    │  Contains:                                        │
│  │  response JSON   │  • user_id                                        │
│  │  metadata        │  • variant assignment                             │
│  │  timestamp       │  • experiment_id                                  │
│  └────────┬─────────┘  • ranked recommendations                         │
│           │                                                             │
│           │                                                             │
│  ┌────────▼───────────────────────────────────────────┐                 │
│  │          Results Pipeline (Jobs)                   │                 │
│  │  ┌───────────────────────────────────────────────┐ │                 │
│  │  │  1. create_user_metrics                       │ │                 │
│  │  │     • Join inference + user events            │ │                 │
│  │  │     • Compute CTR, engagement per user        │ │                 │
│  │  │     • Filter for completed experiments        │ │                 │
│  │  │                                               │ │                 │
│  │  │  2. calculate_results                         │ │                 │
│  │  │     • Run statistical tests (z/t-test)        │ │                 │
│  │  │     • Generate p-values, confidence intervals │ │                 |
│  │  │     • Produce human-readable recommendations  │ │                 │
│  │  └───────────────────────────────────────────────┘ │                 │
│  └────────┬───────────────────────────────────────────┘                 │
│           │                                                             │
│           ▼                                                             │
│  ┌──────────────────┐                                                   │
│  │  Results Tables  │                                                   │
│  │  (Delta)         │                                                   │
│  │                  │                                                   │
│  │  user_metrics    │  • Per-user aggregated KPIs                       │
│  │  results         │  • Experiment-level test statistics               │
│  └──────────────────┘                                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Core Components

The reusable framework core is deployment-agnostic (experiment lifecycle, deterministic assignment, flag resolution, and results analysis). The runtime shown here is a Model Serving reference implementation.

### 1. Experiment Manager App

**Technology**: Streamlit on Databricks Apps  
**Database**: Lakebase (PostgreSQL)  
**Purpose**: UI for experiment lifecycle management

#### Key Responsibilities
- **Experiment CRUD**: Create, read, update experiments with validation
- **Status Management**: Draft → Published → Archived workflow
- **Configuration**: Define feature flags for control/treatment variants
- **Safety Checks**: Prevent overlapping experiments and invalid date ranges
- **Sample Size Calculations**: Built-in power analysis for experiment planning

#### Database Schema
The `experiments` table in Lakebase stores:

```sql
CREATE TABLE experiments (
    experiment_id TEXT PRIMARY KEY,
    experiment_name TEXT NOT NULL,
    description TEXT,
    status TEXT CHECK (status IN ('Draft', 'Published', 'Archived')),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    treatment_allocation REAL DEFAULT 0.5,
    control_config JSONB,      -- Feature flags for control group
    treatment_config JSONB,    -- Feature flags for treatment group
    primary_kpi_metric TEXT,
    sample_size_required INTEGER,
    significance_level REAL DEFAULT 0.05,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Data Sync
- Lakebase experiments table is synced to Unity Catalog via **Federated Catalog**
- Runtime components read from federated catalog (read-only, low-latency)
- Ensures single source of truth while enabling high-performance queries

### 2. Model Serving Integration

**Technology**: MLflow PyFunc, Databricks Model Serving  
**Purpose**: Execute A/B tests at inference time with deterministic assignment

#### Component: CTRPyFunc (PyFunc Model)

The `CTRPyFunc` class wraps a base ML model with A/B testing logic:

```python
class CTRPyFunc(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        # Load base model + A/B testing dependencies
        self._model = mlflow.sklearn.load_model(...)
        self.assigner = AssignmentService(...)
    
    def predict(self, context, model_input):
        # 1. Assign user to variant
        # 2. Apply variant-specific flags
        # 3. Get predictions from base model
        # 4. Apply post-processing based on flags
        # 5. Return results with exposure metadata
```

#### Component: AssignmentService

**Deterministic Assignment Algorithm**:

```python
def assign_variant(user_id: str, experiment_id: str) -> str:
    # SHA-256 hash normalized to [0, 1)
    hash_value = sha256(f"{experiment_id}:{user_id}") / 2^256
    
    # Cumulative allocation check
    if hash_value < treatment_allocation:
        return "treatment"
    else:
        return "control"
```

**Key Properties**:
- ✅ **Deterministic**: Same user always gets same variant for given experiment
- ✅ **Uniform**: Hash function ensures even distribution
- ✅ **Independent**: Different experiments produce independent assignments
- ✅ **Stateless**: No need to store assignments, computed on-demand

#### Feature Flags System

Feature flags enable runtime configuration of model behavior without redeployment:

```json
{
  "temperature": 1.2,              // Logit temperature for exploration
  "ctr_floor": 0.01,               // Minimum CTR threshold
  "ctr_cap": 0.95,                 // Maximum CTR threshold
  "use_interaction_boost": true,   // Enable interaction features
  "interaction_boost_strength": 1.05,
  "epsilon_explore": 0.05,         // ε-greedy exploration rate
  "device_uplift": {               // Device-specific multipliers
    "mobile": 1.1,
    "desktop": 1.0
  }
}
```

**Configuration Merge Strategy**:
- Base model has default flags
- Experiment config overrides defaults (deep merge for nested dicts)
- Control group typically gets defaults, treatment gets modified flags

#### Inference Flow

```
Request arrives
    ↓
Extract user_id
    ↓
Load active experiment ←─────── Query federated experiments table
    ↓
Hash user_id → variant assignment
    ↓
Retrieve variant flags (control or treatment)
    ↓
Apply flags to model input/output
    ↓
Generate predictions with base model
    ↓
Return response with metadata:
    • ranked ads
    • experiment_id
    • assigned variant
    ↓
Auto-logged to inference table
```

### 3. Inference Tables

**Technology**: Delta Lake, Unity Catalog  
**Purpose**: Automatic capture of model serving requests/responses

Databricks Model Serving automatically writes all traffic to inference tables:

```
Schema:
- databricks_request_id: STRING    -- Unique request ID
- timestamp_ms: LONG                -- Request timestamp
- request: STRING                   -- JSON payload (includes user_id)
- response: STRING                  -- JSON response (includes variant, experiment_id)
- request_metadata: MAP<STRING, STRING>  -- Endpoint metadata
- status_code: INT                  -- HTTP response code
- execution_time_ms: LONG           -- Inference latency
```

**Key Benefits**:
- ✅ No instrumentation code required
- ✅ 100% traffic capture
- ✅ Queryable with Spark SQL
- ✅ Time-travel and versioning via Delta

### 4. Results Pipeline

**Technology**: PySpark notebooks, Databricks Jobs  
**Purpose**: Automated statistical analysis of completed experiments

#### Pipeline Stages

##### Stage 1: Create User Metrics (`create_user_metrics.py`)

**Input**: 
- Inference table (exposures)
- User events table (clicks, impressions)
- Experiments table (to identify completed experiments)

**Processing**:
```python
# Join inference exposures with application events
user_metrics = (
    inference_table
    .join(user_events, on=["user_id", "event_date"])
    .groupBy("experiment_id", "user_id", "variant")
    .agg(
        F.sum("impressions").alias("impression_count"),
        F.sum("clicks").alias("click_count"),
        (F.sum("clicks") / F.sum("impressions")).alias("click_through_rate")
    )
)
```

**Output**: `user_metrics` table
```
Schema:
- experiment_id: STRING
- user_id: STRING
- variant: STRING (control/treatment)
- click_through_rate: DOUBLE
- impression_count: LONG
- click_count: LONG
- metric_date: DATE
```

**Incremental Processing**:
- Only processes experiments marked "Published" and past end_date
- Filters out experiments already in results table
- Appends new metrics (idempotent)

##### Stage 2: Calculate Results (`calculate_results.py`)

**Input**: user_metrics table

**Statistical Tests**:

**For Proportion Metrics (CTR)**:
```python
# Z-test for proportions
from statsmodels.stats.proportion import proportions_ztest

counts = [treatment_clicks, control_clicks]
nobs = [treatment_impressions, control_impressions]
z_stat, p_value = proportions_ztest(counts, nobs)
```

**For Continuous Metrics**:
```python
# Welch's t-test (unequal variances)
from scipy.stats import ttest_ind

t_stat, p_value = ttest_ind(
    treatment_values, 
    control_values, 
    equal_var=False
)
```

**Output**: `results` table
```
Schema:
- experiment_id: STRING
- experiment_name: STRING
- metric: STRING (primary KPI)
- control_users: INTEGER
- treatment_users: INTEGER
- control_mean: DOUBLE
- treatment_mean: DOUBLE
- absolute_difference: DOUBLE
- relative_difference: DOUBLE
- relative_lift_pct: STRING          -- Dashboard-ready "+2.5%"
- test_type: STRING                  -- "z-test" or "t-test"
- test_statistic: DOUBLE
- p_value: DOUBLE
- alpha: DOUBLE
- is_significant: BOOLEAN
- winner: STRING                     -- "control", "treatment", "no_winner"
- confidence_interval_lower: DOUBLE
- confidence_interval_upper: DOUBLE
- test_result: STRING                -- Human-readable summary
- recommendation: STRING             -- Action to take
- analysis_timestamp: TIMESTAMP
```

**Human-Readable Outputs**:
```python
# Example outputs for dashboards
test_result = "✓ Statistically significant increase of 12.3% (p=0.0023, α=0.05)"
recommendation = "✓ Deploy treatment - shows statistically significant improvement"
winner = "treatment"
```

---

## Demo vs. Production Setup

The repository includes both **production components** (used in real deployments) and **demo components** (for testing and learning). Understanding this distinction is critical for proper deployment.

### 🧪 Demo Setup

The demo setup is designed to showcase the framework end-to-end **without requiring production data**. It generates 3 months (90 days) of synthetic data with known statistical outcomes.

#### Demo Data Generation

| Component | What It Generates | Purpose |
|-----------|-------------------|---------|
| `generate_ad_dim.py` | 1,000 synthetic ads with categories, quality scores, and lifecycle dates | Ad catalog with active/retired ads |
| `generate_user_dim.py` | 1,000 synthetic users with demographics, devices, regions | User base with realistic distributions |
| `generate_user_events.py` | ~900K events over 90 days (sessions, impressions, clicks, conversions) | User behavior patterns for training |
| `generate_historical_experiments.py` | 3 pre-configured experiments with specific date ranges and feature flags | Historical experiments for demo |
| `generate_demo_inference_table.py` | Mock model serving requests/responses with **controlled CTR outcomes** | Inference data with known results |

#### Demo Experiments (Designed Outcomes)

The demo includes 3 experiments with **intentionally designed outcomes** to validate the statistical pipeline:

1. **CTR Boost - Temperature Test** (90 days ago, 2-week duration)
   - Treatment: `temperature=1.2` (higher exploration)
   - Expected: **+15% CTR lift** (statistically significant)
   - Purpose: Validate detection of positive improvements

2. **Mobile Interaction Boost** (60 days ago, 2-week duration)
   - Treatment: `interaction_boost_strength=1.05` (small boost)
   - Expected: **+2% CTR lift** (NOT statistically significant)
   - Purpose: Validate handling of neutral/marginal changes

3. **Aggressive Floor Test** (30 days ago, 2-week duration)
   - Treatment: `ctr_floor=0.10` (aggressive minimum CTR)
   - Expected: **-20% CTR drop** (statistically significant)
   - Purpose: Validate detection of negative impacts

#### Why Use Demo Data?

- ✅ **No dependencies** on production data pipelines
- ✅ **Known outcomes** validate statistical calculations are correct
- ✅ **Quick validation** of end-to-end flow (< 30 minutes)
- ✅ **Learning environment** before deploying to production
- ✅ **Reproducible** testing with consistent data

#### Demo Job

The `ab_testing_setup_job` orchestrates all demo data generation:

```
1. Setup Unity Catalog (catalog + schema)
2. Create experiments table in Lakebase
3. Generate dimensions (ads, users) ──────┐
4. Generate user events (90 days)          ├─> Feature engineering
5. Generate ad/user features               │
6. Generate training data ─────────────────┘
7. Train CTR model
8. Insert historical experiments
9. Generate demo inference table (controlled outcomes)
```

---

### 🎯 Production Setup

In production, **replace synthetic data sources** with your actual data pipelines while keeping the core framework components.

#### Production Data Sources

| Component | Demo Source | Production Source |
|-----------|-------------|-------------------|
| **Ad Catalog** | `generate_ad_dim.py` → `ad_dim` | Your ad inventory table (e.g., `prod.ads.active_ads`) |
| **User Base** | `generate_user_dim.py` → `user_dim` | Your user dimension (e.g., `prod.users.user_dim`) |
| **User Events** | `generate_user_events.py` → `user_events` | Your application events (e.g., `prod.events.user_actions`) |
| **Inference Data** | `generate_demo_inference_table.py` | **Actual Model Serving inference table** |

#### Production Architecture (What Changes)

**❌ Remove (Demo Only):**
- Synthetic data generation notebooks (`generate_*_dim.py`, `generate_user_events.py`)
- Demo inference table generation
- Historical experiment insertion (create via app instead)

**✅ Keep (Production):**
- Experiment Manager App (for creating experiments)
- CTRPyFunc wrapper (model serving with A/B testing)
- AssignmentService (deterministic user assignment)
- Results pipeline (statistical analysis)
- Lakebase experiments table (configuration store)

**🔄 Adapt (Point to Real Data):**
- Feature engineering notebooks → point to production dimensions/events
- Training pipeline → use real historical data
- Results pipeline → `inference_table_path` points to actual inference table

#### Production Configuration Example

```yaml
# databricks.yml (production target)
variables:
  # Unity Catalog tables (your production data)
  ad_dim_table_path: "prod.inventory.ads"
  user_dim_table_path: "prod.users.user_dim"
  user_events_table_path: "prod.events.user_interactions"
  
  # Model Serving inference table (auto-generated)
  inference_table_path: "prod.ml.ctr_model_endpoint_payload_request"
  
  # Feature/training tables (still generated, but from real data)
  ad_features_table_path: "prod.ml.ad_features"
  user_features_table_path: "prod.ml.user_features"
  training_features_table_path: "prod.ml.training_features"
  
  # Model registry
  registered_model_path: "prod.ml.ctr_model"
  
  # Results tables
  user_metrics_table_path: "prod.ml.user_metrics"
  results_table_path: "prod.ml.experiment_results"
```

#### Production Deployment Flow

```
1. Deploy Infrastructure (one-time)
   ├─ Create Unity Catalog catalog/schema
   ├─ Create Lakebase experiments table
   ├─ Setup service principals & permissions
   └─ Deploy Experiment Manager App

2. Feature Pipeline (scheduled, e.g., daily)
   ├─ generate_ad_features.py      (reads prod.inventory.ads)
   ├─ generate_user_features.py    (reads prod.users, prod.events)
   ├─ generate_training_data.py    (joins features)
   └─ train_ctr_model.py           (trains & registers model)

3. Model Serving (continuous)
   ├─ Deploy model endpoint with CTRPyFunc wrapper
   ├─ Endpoint queries experiments table for active experiments
   ├─ AssignmentService assigns users deterministically
   └─ Inference table captures all requests/responses

4. Experiment Management (ad-hoc via app)
   ├─ Create experiments via Experiment Manager App
   ├─ Configure control/treatment feature flags
   ├─ Publish to activate
   └─ Monitor exposure counts

5. Results Pipeline (scheduled, e.g., daily)
   ├─ create_user_metrics.py       (reads real inference table)
   ├─ calculate_results.py         (statistical tests)
   └─ Results table updated with recommendations
```

---

### Key Differences Summary

| Aspect | Demo | Production |
|--------|------|------------|
| **Data Source** | Synthetic (generated) | Real (from pipelines) |
| **Inference Table** | Mock with controlled outcomes | Actual Model Serving table |
| **Experiments** | Pre-inserted historical | Created via app as needed |
| **Purpose** | Validation & learning | Real experimentation |
| **Timeline** | Historical (90 days) | Real-time & future |
| **Outcomes** | Known/designed | Unknown (to be discovered) |
| **Setup Time** | ~30 minutes (full demo) | Ongoing (depends on data) |

---

## Data Flow

### Experiment Lifecycle

```
1. CREATION
   User → Streamlit App → Lakebase experiments table
   Status: Draft
   
2. PUBLICATION
   User clicks "Publish" → Status: Published
   Lakebase → Federated Catalog (sync)
   
3. EXPOSURE
   Model Serving queries federated experiments table
   Assigns users to variants
   Logs exposures to inference table
   
4. COMPLETION
   end_date passes → Experiment "complete" but still Published
   
5. ANALYSIS
   Results pipeline:
   • Identifies completed experiments
   • Joins inference + events
   • Computes user metrics
   • Runs statistical tests
   • Writes results
   
6. ARCHIVAL
   User reviews results → Archives experiment
   Status: Archived (prevents re-analysis)
```

### Real-Time vs Batch Processing

| Component | Processing Mode | Latency |
|-----------|----------------|---------|
| Model Serving | Real-time | ~50-200ms |
| Inference Table | Streaming writes | ~1-5 minutes |
| User Metrics | Batch (daily) | Daily job |
| Statistical Analysis | Batch (on-completion) | Triggered post-experiment |

## Key Design Decisions

### 1. Why Lakebase for Experiment Config?

**Decision**: Use Lakebase (PostgreSQL) instead of Delta tables

**Rationale**:
- ✅ **ACID transactions**: Critical for experiment status changes
- ✅ **Low latency**: Sub-millisecond queries for model serving
- ✅ **Relational integrity**: Constraints and foreign keys
- ✅ **Concurrent writes**: Multiple users managing experiments
- ✅ **Streamlit compatibility**: Native SQL support

**Trade-offs**:
- ❌ Requires federated catalog sync for Unity Catalog access
- ❌ Additional infrastructure (managed by Databricks)

### 2. Why Hash-Based Assignment?

**Decision**: Use deterministic hashing instead of lookup tables

**Rationale**:
- ✅ **Stateless**: No storage required for assignments
- ✅ **Scalable**: No bottleneck on assignment lookups
- ✅ **Consistent**: Same input always produces same output
- ✅ **Auditable**: Assignments can be recomputed
- ✅ **Multi-experiment**: Independent assignments per experiment

**Trade-offs**:
- ❌ Cannot change allocation mid-experiment
- ❌ No explicit control over specific user assignments

### 3. Why PyFunc Instead of Custom Serving?

**Decision**: Wrap logic in MLflow PyFunc model

**Rationale**:
- ✅ **Native integration**: Databricks Model Serving first-class support
- ✅ **Inference tables**: Auto-logging without custom code
- ✅ **Versioning**: MLflow tracks model versions
- ✅ **Deployment**: Standard deployment workflows
- ✅ **Monitoring**: Built-in metrics and alerts

**Trade-offs**:
- ❌ Some overhead from MLflow layer
- ❌ Less flexibility than custom REST API

### 4. Why Separate User Metrics Step?

**Decision**: Compute user-level aggregations before statistical tests

**Rationale**:
- ✅ **Reusability**: Same metrics for multiple analyses
- ✅ **Performance**: Pre-aggregate before pulling to Pandas
- ✅ **Auditability**: Transparent metric definitions
- ✅ **Incremental**: Only compute for new experiments

**Trade-offs**:
- ❌ Additional storage (minimal - aggregated data)
- ❌ Two-stage pipeline

### 5. Why Batch Analysis Instead of Real-Time?

**Decision**: Run statistical tests as batch jobs after experiment completion

**Rationale**:
- ✅ **Statistical validity**: Wait for sufficient sample size
- ✅ **Cost-effective**: No continuous compute
- ✅ **Correctness**: Avoid multiple testing issues
- ✅ **Idempotency**: Easy to rerun if needed

**Trade-offs**:
- ❌ Not suitable for continuous monitoring during experiment
- ❌ Delayed results (acceptable for week+ experiments)

## Security & Governance

### Service Principal Architecture

```
Model Serving Service Principal
    ↓
Grants:
    • READ on experiments (federated catalog)
    • READ on feature tables
    • WRITE on inference table (automatic)
    ↓
OAuth Secret (90-day expiration)
    • Stored in Databricks Secret Scope
    • Accessed by model serving endpoint
```

### Unity Catalog Permissions

| Principal | Catalog | Schema | Experiments | Inference | Features |
|-----------|---------|--------|-------------|-----------|----------|
| User | MANAGE | MANAGE | via Lakebase | MANAGE | MANAGE |
| Model Serving SP | ALL_PRIVILEGES | ALL_PRIVILEGES | READ | WRITE | READ |
| Results Pipeline | - | - | READ | READ | - |

## Deployment Architecture

### Databricks Asset Bundle Structure

```yaml
bundle:
  name: databricks-ab-testing
  
resources:
  apps:
    experiment_manager_app:  # Streamlit app
  
  models:
    ctr_model:               # PyFunc model with A/B logic
  
  model_serving_endpoints:
    ctr_model:               # Serving endpoint
      config:
        auto_capture_config:
          enabled: true       # Enable inference tables
  
  jobs:
    ab_testing_setup_job:    # Initial setup
    ab_testing_results_pipeline_job:  # Scheduled analysis
  
  lakebase:
    ab-testing:              # PostgreSQL instance
```

### Infrastructure Components

| Component | Type | Purpose |
|-----------|------|---------|
| Lakebase | Postgres | Experiment metadata storage |
| Unity Catalog | Catalog | Data governance + federated tables |
| Model Serving | Compute | Real-time inference |
| Jobs Cluster | Compute | Batch processing (results) |
| Secret Scope | Vault | Service principal credentials |

## Performance Characteristics

### Model Serving Latency

| Operation | Latency | Notes |
|-----------|---------|-------|
| Base model inference | 20-50ms | Scikit-learn CTR model |
| Experiment config load | 5-10ms | Cached, federated query |
| Assignment calculation | <1ms | SHA-256 hash |
| Feature flag application | <5ms | In-memory operations |
| **Total p95 latency** | **~100ms** | Acceptable for most use cases |

### Results Pipeline Performance

| Stage | Processing Time | Notes |
|-------|----------------|-------|
| User metrics (1M rows) | ~2-3 minutes | PySpark aggregation |
| Statistical tests | ~10-30 seconds | Pandas UDFs |
| **Total for typical experiment** | **~5 minutes** | Single-node cluster |

## Extension Points

### Adding New Metrics

1. **Define in app config** (`databricks_ab_testing/experiment_manager_app/config.py`):
```python
KPI_METRICS = {
    "my_new_metric": {
        "display_name": "My New Metric",
        "type": "continuous"  # or "proportion"
    }
}
```

2. **Compute in user metrics** (`databricks_ab_testing/results/create_user_metrics.py`):
```python
.agg(
    # ... existing metrics
    F.avg("session_duration").alias("my_new_metric")
)
```

3. **Results pipeline auto-detects** metric type and applies appropriate test

### Adding New Feature Flags

1. **Define in model code** (`databricks_ab_testing/model/src/CTRPyFunc.py`):
```python
my_new_flag = flags.get("my_new_flag", default_value)
# Apply flag logic in _apply_flags_np or predict
```

2. **Configure in Experiment Manager App** via JSON editor

### Supporting Multiple Experiments

Currently: Single active experiment at a time (safety constraint)

To enable multiple simultaneous experiments:
1. Remove uniqueness check in `experiment_service.py`
2. Update `AssignmentService` to handle multiple experiments
3. Add experiment priority/hierarchy logic
4. Update results pipeline to handle multi-experiment users

## Testing Strategy

### Unit Tests
- Assignment determinism
- Flag merging logic
- Statistical test calculations

### Integration Tests
- End-to-end experiment flow (create → publish → expose → analyze)
- Model serving with mock experiments table
- Results pipeline with synthetic data

### A/A Tests
- Run experiments with identical control/treatment configs
- Validate no significant differences detected (p-value distribution check)
- Confirms assignment randomization and test validity

## Monitoring & Observability

### Key Metrics to Track

**Model Serving**:
- Request latency (p50, p95, p99)
- Assignment distribution (50/50 split maintained?)
- Inference table write lag
- Error rates by variant

**Results Pipeline**:
- Job success rate
- Processing time
- Sample size warnings
- Statistical power achieved

**Experiment Health**:
- Exposure balance (control vs treatment)
- User overlap between experiments (if running multiple)
- Missing data rates (clicks, impressions)

## References

- [Databricks Model Serving Docs](https://docs.databricks.com/machine-learning/model-serving/index.html)
- [Inference Tables Schema](https://docs.databricks.com/machine-learning/model-serving/inference-tables.html)
- [Lakebase Documentation](https://docs.databricks.com/lakehouse-federation/postgresql.html)
- [Unity Catalog Federated Catalogs](https://docs.databricks.com/data-governance/unity-catalog/federated-catalogs.html)
- [MLflow PyFunc](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html)

