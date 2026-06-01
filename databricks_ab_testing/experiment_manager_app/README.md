# Experiment Manager App (Streamlit)

Streamlit application for managing A/B experiments end-to-end: create, edit, publish/unpublish, and archive experiments, with built‑in sample size estimates and safety checks to avoid overlapping live tests.

## Highlights

- Clean, opinionated workflow for experiments
  - Create and edit experiments with a minimal set of fields
  - Publish with overlap safety checks; unpublish before start; archive at any time
- Built‑in design assumptions and sample‑size calculator
  - Allocation fixed at 50/50
  - Power fixed at 0.80; Alpha fixed at 0.05 (two‑sided)
  - KPI‑aware MDE input
    - Watch Time: seconds (continuous)
    - Return KPIs: absolute probability in 0–1 (e.g., 0.03 = +3pp)
- UX details that help operators
  - Simple dialog: Name, dates, KPI, Treatment (radio: Personalization), MDE (slider)
  - Only Treatment Sample Size is shown (auto‑calculated); control size is hidden
  - Status badges: Published (Running) in green; Published (Queued) in blue when future‑dated
  - Save disabled until the form is valid; clear validation errors

## Quick Start

### 1) Install

```bash
pip install -r databricks_ab_testing/experiment_manager_app/requirements.txt
```

### 2) Configure environment

Set the following environment variables (examples shown):

```bash
# Environment label shown in the app header
export APP_ENV=dev

# Postgres/Lakebase connectivity (db.py reads the standard libpq variable names)
export PGHOST=your-host
export PGPORT=5432
export PGDATABASE=your-db
export PGUSER=oauth

# Table to store experiments
export EXPERIMENTS_TABLE_NAME=public.experiments

# Databricks profile (assumes you've already run: databricks auth login --profile <name>)
export DATABRICKS_CONFIG_PROFILE=your-profile
```

### 3) Run the app

From the repo root:

```bash
streamlit run databricks_ab_testing/experiment_manager_app/app.py
```

## Data Model

The app reads and writes records to the table named in `EXPERIMENTS_TABLE_NAME`.

Required columns (names must match):

- `experiment_id` (TEXT/UUID)
- `experiment_name` (TEXT)
- `status` (TEXT: Draft | Published | Archived)
- `start_date` (DATE)
- `end_date` (DATE, nullable)
- `treatment_allocation` (FLOAT) — fixed at 0.5 in this app
- `control_config` (TEXT/JSON)
- `treatment_config` (TEXT/JSON)
- `mde` (FLOAT)
- `power` (FLOAT) — fixed to 0.80 by the app
- `significance_level` (FLOAT) — fixed to 0.05 by the app
- `control_sample_size` (INT)
- `treatment_sample_size` (INT)
- `primary_kpi_metric` (TEXT)

Example DDL (adjust for your environment):

```sql
CREATE TABLE IF NOT EXISTS public.experiments (
  experiment_id TEXT PRIMARY KEY,
  experiment_name TEXT NOT NULL,
  status TEXT NOT NULL,
  start_date DATE,
  end_date DATE,
  treatment_allocation DOUBLE PRECISION,
  control_config TEXT,
  treatment_config TEXT,
  mde DOUBLE PRECISION,
  power DOUBLE PRECISION,
  significance_level DOUBLE PRECISION,
  control_sample_size INT,
  treatment_sample_size INT,
  primary_kpi_metric TEXT
);
```

## KPIs and Units

KPI select drives how MDE is interpreted:

- `avg_daily_watched_seconds_capped` (Watch Time): MDE is seconds (continuous)
- Return KPIs: MDE is absolute probability 0–1
  - `is_return_7days`
  - `is_return_5days`
  - `is_return_3days`

Baseline and design assumptions used by the calculator:

- Allocation: 50/50 (fixed)
- Power: 0.80; Alpha: 0.05 (two‑sided) (fixed)
- Baselines (from config):
  - Watch Time: σ = 15.0 seconds (default)
  - Return KPIs: p₀ = 0.05 (default)

## Workflow and Rules

### Table

- Toggle “Show archived” to include/exclude archived experiments.
- Sorted by Status group (Draft, Published, Archived), then by Start Date ascending.
- Click an experiment name to open its dialog.
- Publish button appears for Draft experiments with a start date.
  - Published (Running) shows green badge when date window includes today.
  - Published (Queued) shows blue badge when start date is in the future.

### Create/Edit Dialog

Fields (visible):

- Experiment Name
- Start Date (must be > today)
- End Date (must be > start date)
- Primary KPI Metric
- Treatment: radio (Personalization)
- MDE (KPI‑aware units)
- Treatment Sample Size (read‑only)

Actions:

- Save: Enabled only when all validations pass
- Unpublish: Only for Published experiments that have not yet started
- Archive: Available for any non‑archived experiment (including running)

Overlap safety:

- Live Published date ranges are displayed as help on the date inputs
- Save is prevented if the chosen dates overlap an existing Published window
- Publish is blocked server‑side against overlapping Published windows

## Architecture

- `databricks_ab_testing/experiment_manager_app/app.py`: Streamlit UI and orchestration
- `databricks_ab_testing/experiment_manager_app/db.py`: DB engine, cached connection, query helpers, and bootstrap:
  - `ensure_lakebase_table_exists()` – creates schema/table in Lakebase Postgres if missing
  - `ensure_uc_privileges()` – optional UC grants via SQL Warehouse (if run with an admin identity)
- `databricks_ab_testing/experiment_manager_app/stats.py`: Normal quantiles and sample size math
- `databricks_ab_testing/experiment_manager_app/experiment_service.py`: CRUD and status transitions (publish/unpublish), overlap checks

Notable implementation details:

- Connection pooling via SQLAlchemy; token acquired per physical connection using `WorkspaceClient().oauth_token()`.
- Streamlit `@st.cache_resource` around the engine; button to clear the cache is available in the debug expander (if enabled in code).
- Sample size is recomputed client‑side based on current UI inputs.

## Configuration

See `APP_CONFIG` in `app.py` for defaults:

- `kpi_options` mapping display labels to column names
- Defaults: `watch_time_sigma_default`, `return_rate_default`, `power_default`, `alpha_default`

You can safely change KPI names/columns (e.g., `is_return_7days`) so long as your reporting stack matches those column names.

## Deployment

This app can be deployed like any Streamlit app. On Databricks Apps, either:

- Deploy via bundle (preferred) with an `apps` resource in `resources/apps.yml`, or
- Deploy from the Apps UI by selecting the Workspace source path that contains this folder (it must include `app.yml`, `requirements.txt`, and `app.py`).

Ensure the runtime can obtain an OAuth token and reach your Lakebase Postgres endpoint.

## Troubleshooting

- Import/path issues when running from subdirectories
  - `app.py` prepends the parent directory to `sys.path` for reliable package imports.
- Databricks OAuth failures
  - Ensure your workspace SDK profile is configured (for example, `DATABRICKS_CONFIG_PROFILE` is set correctly).
- “Selected dates overlap another published experiment”
  - Adjust your window or unpublish/archive the conflicting experiment.
- MDE input disabled or invalid
  - Watch Time uses seconds; Return KPIs require absolute probability in 0–1.

## Roadmap / Ideas

- Optional unequal allocations and multi‑arm support
- Richer KPI catalog and per‑KPI baselines
- Experiment cloning and templates
- Role‑based edit/publish permissions

---

Maintainers: update this README when adding fields, changing defaults, or altering publish/overlap rules so operators have up‑to‑date guidance.

