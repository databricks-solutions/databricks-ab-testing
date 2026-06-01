import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime

import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")

from benchmark_service import get_benchmarks  # noqa: E402
from config import APP_CONFIG  # noqa: E402
from db import execute_insert_query, execute_query, get_database_connection, get_experiment_table_name  # noqa: E402
from experiment_service import (  # noqa: E402
    publish_experiment,
    unpublish_experiment,
    update_experiment,
    update_experiment_status,
)
from stats import compute_sample_sizes  # noqa: E402


def _configure_root_logger():
    """Ensure logs print to stdout even under Streamlit reruns."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    has_stream_handler = any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers)
    if not has_stream_handler:
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)


_configure_root_logger()

logger = logging.getLogger(__name__)

st.markdown(
    """
    <style>
    .stButton > button {
        background-color: #003998 !important;
        border-color: #003998 !important;
        color: #ffffff !important;
    }
    .stButton > button:hover {
        background-color: #003080 !important;
        border-color: #003080 !important;
    }
    .status-running { color: #16a34a; font-weight: 600; }
    .status-queued { color: #003998; font-weight: 600; }
    /* Clickable experiment name as text */
    a.exp-name { color: inherit; text-decoration: none; }
    a.exp-name:hover { color: #003998; text-decoration: underline; cursor: pointer; }
    /* Use full width */
    .main .block-container { max-width: 100% !important; padding-left: 1.5rem; padding-right: 1.5rem; }
    .section-spacer { height: 16px; }
    </style>
    """,
    unsafe_allow_html=True,
)

header_title_col, header_toggle_col, header_button_col = st.columns([7, 2, 2])
with header_title_col:
    env = os.getenv("APP_ENV", "dev").lower()
    st.title(f"Click Through Rate A/B Testing Experiments | {env}")
with header_toggle_col:
    st.toggle("Show archived", value=st.session_state.get("show_archived_toggle", False), key="show_archived_toggle")
with header_button_col:
    if st.button("Create Experiment", key="create_experiment_button"):
        st.session_state["open_create_experiment_dialog"] = True
        logger.info("Create Experiment dialog opened")

logger = logging.getLogger(__name__)


db_engine = get_database_connection()
query = f"SELECT * FROM {get_experiment_table_name()}"
records = execute_query(db_engine, query)

benchmarks = get_benchmarks()


@st.dialog("Create New Experiment")
def create_experiment_dialog():
    """Dialog for creating a new experiment"""
    st.write("Enter experiment details:")

    experiment_name = st.text_input("Experiment Name*", placeholder="Enter experiment name")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=None)
    with col2:
        end_date = st.date_input("End Date", value=None)

    primary_kpi_display = st.selectbox(
        "Primary KPI Metric",
        options=list(APP_CONFIG["kpi_options"].keys()),
        index=0,
    )
    primary_kpi_metric = APP_CONFIG["kpi_options"][primary_kpi_display]
    if "name" not in primary_kpi_metric or "type" not in primary_kpi_metric or "benchmark" not in primary_kpi_metric:
        raise Exception("Primary KPI metric must be defined with both name and type")

    primary_kpi_metric_name = primary_kpi_metric["name"]
    primary_kpi_metric_type = primary_kpi_metric["type"]
    primary_kpi_metric_benchmark = primary_kpi_metric["benchmark"]

    st.caption(
        "Assumptions: 50/50 allocation, power 0.80, alpha 0.05. "
        f"{primary_kpi_display} Baseline (calculated): {round(float(primary_kpi_metric_benchmark), 2)}. "
        "MDE must match KPI units."
    )
    treatment_allocation = 0.5

    cfg_left, cfg_right = st.columns(2)
    with cfg_left:
        control_config_text = st.text_area(
            "Control Config (JSON)",
            value="{}",
            height=200,
            help="Enter a JSON object for the control configuration.",
        )
    with cfg_right:
        treatment_config_text = st.text_area(
            "Treatment Config (JSON)",
            value="{}",
            height=200,
            help="Enter a JSON object for the treatment configuration.",
        )

    col1, col2, col3 = st.columns(3)
    with col1:
        is_continuous_kpi = primary_kpi_metric_type == "continuous"
        if is_continuous_kpi:
            mde = st.number_input(
                "MDE (seconds)",
                min_value=0.0,
                value=5.0,
                step=0.5,
                help="Absolute change in seconds for Watch Time.",
            )
        else:
            mde = st.number_input(
                "MDE (absolute, 0–1)",
                min_value=0.0,
                max_value=1.0,
                value=0.05,
                step=0.01,
                help="Absolute change in probability (e.g., 0.03 = +3pp).",
            )
    with col2:
        st.caption("Power fixed at 0.80, Significance fixed at 0.05")
        power_val = APP_CONFIG["defaults"]["power_default"]
    with col3:
        st.write("")
        sig_level = APP_CONFIG["defaults"]["alpha_default"]

    st.markdown("---")

    try:
        n_c, n_t = compute_sample_sizes(
            primary_kpi_metric_type,
            float(primary_kpi_metric_benchmark),
            float(mde),
            float(power_val),
            float(sig_level),
            float(treatment_allocation),
        )
    except Exception as e:
        logger.warning(f"Failed to compute sample sizes: {e}")
        n_c, n_t = 0, 0
    control_sample_size = int(n_c)
    derived_treatment_size = int(n_t)
    st.text(f"Treatment Sample Size (derived): {derived_treatment_size:,}")

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()

    today_ = datetime.now().date()
    is_continuous_kpi = primary_kpi_metric_type == "continuous"
    is_name_ok = bool(experiment_name.strip())
    are_dates_present = bool(start_date) and bool(end_date)
    are_dates_ok = are_dates_present and (start_date >= today_) and (end_date >= start_date)
    is_kpi_ok = bool(primary_kpi_metric_name)
    if is_continuous_kpi:
        is_mde_ok = (mde is not None) and (float(mde) > 0)
    else:
        is_mde_ok = (mde is not None) and (0.0 < float(mde) <= 1.0)
    is_valid = all([is_name_ok, are_dates_ok, is_kpi_ok, is_mde_ok])

    with col3:
        if st.button("Save", use_container_width=True, type="primary", disabled=not is_valid):
            if not experiment_name.strip():
                st.error("Experiment name is required!")
                return
            if not start_date or not end_date:
                st.error("Start date and end date are required")
                return
            if start_date < today_:
                st.error("Start date must be today or later")
                return
            if end_date < start_date:
                st.error("End date must be on or after the start date")
                return
            try:
                control_cfg = json.loads(control_config_text) if control_config_text.strip() else {}
            except Exception:
                st.error("Control Config must be valid JSON")
                return
            if not isinstance(control_cfg, dict):
                st.error("Control Config must be a JSON object")
                return

            try:
                treatment_cfg = json.loads(treatment_config_text) if treatment_config_text.strip() else {}
            except Exception:
                st.error("Treatment Config must be valid JSON")
                return
            if not isinstance(treatment_cfg, dict):
                st.error("Treatment Config must be a JSON object")
                return

            if primary_kpi_metric_type == "continuous":
                if mde is None or float(mde) <= 0:
                    st.error("MDE must be > 0")
                    return
            else:
                if mde is None or not (0.0 < float(mde) <= 1.0):
                    st.error("MDE must be > 0 and <= 1")
                    return
            if power_val is None or not (0.0 < float(power_val) <= 1.0):
                st.error("Power must be > 0 and <= 1")
                return
            if sig_level is None or not (0.0 < float(sig_level) <= 1.0):
                st.error("Significance level must be > 0 and <= 1")
                return

            experiment_id = str(uuid.uuid4())

            insert_query = f"""
                INSERT INTO {get_experiment_table_name()}
                (experiment_id, experiment_name, status, start_date, end_date,
                treatment_allocation, control_config, treatment_config,
                mde, power, significance_level, control_sample_size, treatment_sample_size,
                primary_kpi_metric)
                VALUES
                (:experiment_id, :experiment_name, :status, :start_date, :end_date,
                 :treatment_allocation, :control_config, :treatment_config,
                 :mde, :power, :significance_level, :control_sample_size, :treatment_sample_size,
                 :primary_kpi_metric)
            """

            params = {
                'experiment_id': experiment_id,
                'experiment_name': experiment_name.strip(),
                'status': 'Draft',
                'start_date': start_date,
                'end_date': end_date,
                'treatment_allocation': treatment_allocation,
                'control_config': json.dumps(control_cfg),
                'treatment_config': json.dumps(treatment_cfg),
                'mde': mde,
                'power': power_val,
                'significance_level': sig_level,
                'control_sample_size': int(control_sample_size) if control_sample_size else None,
                'treatment_sample_size': int(derived_treatment_size) if derived_treatment_size else None,
                'primary_kpi_metric': primary_kpi_metric_name.strip() if primary_kpi_metric_name else None,
            }

            success = execute_insert_query(db_engine, insert_query, params)

            if success:
                st.success("Experiment created successfully!")
                logger.info("Experiment %s created: %s", experiment_id, experiment_name)
                time.sleep(1)
                st.rerun()
            else:
                st.error("Failed to create experiment. Please try again.")
                logger.error("Experiment create failed: %s", experiment_name)


@st.dialog("Edit Experiment")
def edit_experiment_dialog(experiment_data: dict, is_view_only: bool = False):
    """Dialog for editing or viewing an experiment"""
    title = "View Experiment" if is_view_only else "Edit Experiment"
    st.write(f"{title}:")

    experiment_name = st.text_input(
        "Experiment Name", value=experiment_data.get('experiment_name', ''), disabled=is_view_only
    )

    col1, col2 = st.columns(2)
    with col1:
        start_date_value = experiment_data.get('start_date')
        if start_date_value and isinstance(start_date_value, str):
            try:
                start_date_value = pd.to_datetime(start_date_value).date()
            except Exception:
                start_date_value = None
        start_date = st.date_input(
            "Start Date",
            value=start_date_value,
            disabled=is_view_only,
        )

    with col2:
        end_date_value = experiment_data.get('end_date')
        if end_date_value and isinstance(end_date_value, str):
            try:
                end_date_value = pd.to_datetime(end_date_value).date()
            except Exception:
                end_date_value = None
        end_date = st.date_input(
            "End Date",
            value=end_date_value,
            disabled=is_view_only,
        )

    current_kpi_val = experiment_data.get('primary_kpi_metric')
    if not current_kpi_val:
        # Default to first KPI option
        first_display = list(APP_CONFIG["kpi_options"].keys())[0]
        current_kpi_val = APP_CONFIG["kpi_options"][first_display]["name"]

    # Build reverse map from metric name to display name
    rev_map = {v["name"]: k for k, v in APP_CONFIG["kpi_options"].items()}
    current_display = rev_map.get(current_kpi_val, list(APP_CONFIG["kpi_options"].keys())[0])
    primary_kpi_display = st.selectbox(
        "Primary KPI Metric",
        options=list(APP_CONFIG["kpi_options"].keys()),
        index=list(APP_CONFIG["kpi_options"].keys()).index(current_display),
        disabled=is_view_only,
    )
    primary_kpi_metric = APP_CONFIG["kpi_options"][primary_kpi_display]
    primary_kpi_metric_name = primary_kpi_metric["name"]
    primary_kpi_metric_type = primary_kpi_metric["type"]
    primary_kpi_metric_benchmark = primary_kpi_metric["benchmark"]

    treatment_allocation = 0.5

    try:
        existing_control_cfg = experiment_data.get('control_config')
        if isinstance(existing_control_cfg, str):
            existing_control_cfg = json.loads(existing_control_cfg)
    except Exception:
        existing_control_cfg = {}
    try:
        existing_treatment_cfg = experiment_data.get('treatment_config')
        if isinstance(existing_treatment_cfg, str):
            existing_treatment_cfg = json.loads(existing_treatment_cfg)
    except Exception:
        existing_treatment_cfg = {}

    st.subheader("Configs")
    cfg_left, cfg_right = st.columns(2)
    with cfg_left:
        control_config_text = st.text_area(
            "Control Config (JSON)",
            value=json.dumps(existing_control_cfg or {}, indent=2),
            height=200,
            disabled=is_view_only,
            help="Enter a JSON object for the control configuration.",
        )
    with cfg_right:
        treatment_config_text = st.text_area(
            "Treatment Config (JSON)",
            value=json.dumps(existing_treatment_cfg or {}, indent=2),
            height=200,
            disabled=is_view_only,
            help="Enter a JSON object for the treatment configuration.",
        )

    col1, col2 = st.columns(2)
    with col1:
        is_continuous_kpi = primary_kpi_metric_type == "continuous"
        _mde_raw = experiment_data.get('mde')
        if is_continuous_kpi:
            try:
                _init = float(_mde_raw)
                if pd.isna(_init) or _init <= 0:
                    _init = 5.0
            except Exception:
                _init = 5.0
            mde = st.number_input(
                "MDE (seconds)",
                min_value=0.0,
                value=_init,
                step=0.5,
                help="Absolute change in seconds for Watch Time.",
                disabled=is_view_only,
            )
        else:
            try:
                _init = float(_mde_raw)
                if pd.isna(_init) or _init <= 0 or _init > 1:
                    _init = 0.05
            except Exception:
                _init = 0.05
            mde = st.number_input(
                "MDE (absolute, 0–1)",
                min_value=0.0,
                max_value=1.0,
                value=_init,
                step=0.01,
                help="Absolute change in probability (e.g., 0.03 = +3pp).",
                disabled=is_view_only,
            )
    with col2:
        st.caption("Power fixed at 0.80, Significance fixed at 0.05")
    power_val = APP_CONFIG["defaults"]["power_default"]
    sig_level = APP_CONFIG["defaults"]["alpha_default"]

    st.markdown("---")
    try:
        n_c, n_t = compute_sample_sizes(
            primary_kpi_metric_type,
            float(primary_kpi_metric_benchmark),
            float(mde),
            float(power_val),
            float(sig_level),
            float(treatment_allocation),
        )
    except Exception as e:
        logger.warning(f"Failed to compute sample sizes: {e}")
        n_c, n_t = (
            int(experiment_data.get('control_sample_size') or 0),
            int(experiment_data.get('treatment_sample_size') or 0),
        )
    control_sample_size = int(n_c)
    derived_treatment_size = int(n_t)
    st.text(f"Treatment Sample Size (derived): {derived_treatment_size:,}")

    if is_view_only:
        col1, col2 = st.columns([1, 1])
        with col2:
            if st.button("Close", use_container_width=True):
                st.rerun()
    else:
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            if st.button("Cancel", use_container_width=True):
                st.rerun()

        status_val_for_actions = (experiment_data.get('status') or '').strip()
        can_unpublish = False
        try:
            can_unpublish = (
                status_val_for_actions == 'Published'
                and (start_date is not None)
                and (datetime.now().date() < start_date)
            )
        except Exception:
            can_unpublish = False

        with col2:
            if can_unpublish:
                if st.button("Unpublish", use_container_width=True):
                    success, msg = unpublish_experiment(experiment_data)
                    if success:
                        st.success("Experiment unpublished (reverted to Draft)")
                        logger.info("Experiment %s unpublished", experiment_data.get('experiment_id'))
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg or "Failed to unpublish experiment")
                        logger.error("Unpublish failed for %s: %s", experiment_data.get('experiment_id'), msg)

        status_val_for_archive = (experiment_data.get('status') or '').strip()
        can_archive = status_val_for_archive != 'Archived'
        with col3:
            if can_archive:
                if st.button("Archive", use_container_width=True):
                    exp_id = experiment_data.get('experiment_id')
                    if exp_id:
                        success = update_experiment_status(exp_id, "Archived")
                        if success:
                            st.success("Experiment archived successfully!")
                            logger.info("Experiment %s archived", exp_id)
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Failed to archive experiment")
                            logger.error("Archive failed for %s", exp_id)

        with col4:
            if st.button("Save", use_container_width=True, type="primary"):
                if not experiment_name.strip():
                    st.error("Experiment name is required!")
                    return
                if not start_date or not end_date:
                    st.error("Start date and end date are required")
                    return
                today_ = datetime.now().date()
                if start_date < today_:
                    st.error("Start date must be today or later")
                    return
                if end_date < start_date:
                    st.error("End date must be on or after the start date")
                    return
                if not primary_kpi_metric_name:
                    st.error("Primary KPI metric is required")
                    return
                if primary_kpi_metric_type == "continuous":
                    if mde is None or float(mde) <= 0:
                        st.error("MDE must be > 0 seconds")
                        return
                else:
                    if mde is None or not (0.0 < float(mde) <= 1.0):
                        st.error("MDE must be > 0 and <= 1")
                        return
                if power_val is None or not (0.0 < float(power_val) <= 1.0):
                    st.error("Power must be > 0 and <= 1")
                    return
                if sig_level is None or not (0.0 < float(sig_level) <= 1.0):
                    st.error("Significance level must be > 0 and <= 1")
                    return

                try:
                    control_cfg = json.loads(control_config_text) if control_config_text.strip() else {}
                except Exception:
                    st.error("Control Config must be valid JSON")
                    return
                if not isinstance(control_cfg, dict):
                    st.error("Control Config must be a JSON object")
                    return

                try:
                    treatment_cfg = json.loads(treatment_config_text) if treatment_config_text.strip() else {}
                except Exception:
                    st.error("Treatment Config must be valid JSON")
                    return
                if not isinstance(treatment_cfg, dict):
                    st.error("Treatment Config must be a JSON object")
                    return

                success = update_experiment(
                    experiment_data['experiment_id'],
                    experiment_name.strip(),
                    start_date,
                    end_date,
                    float(treatment_allocation),
                    control_cfg,
                    treatment_cfg,
                    float(mde) if mde is not None else None,
                    float(power_val) if power_val is not None else None,
                    float(sig_level) if sig_level is not None else None,
                    int(control_sample_size) if control_sample_size else None,
                    int(derived_treatment_size) if derived_treatment_size else None,
                    primary_kpi_metric_name.strip() if primary_kpi_metric_name else None,
                )

                if success:
                    st.success("Experiment updated successfully!")
                    logger.info("Experiment %s updated", experiment_data.get('experiment_id'))
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to update experiment. Please try again.")
                    logger.error("Experiment update failed: %s", experiment_data.get('experiment_id'))


def _action_options_for_row(row: dict):
    status = (row.get('status') or '').strip()
    start_date = row.get('start_date')
    try:
        if isinstance(start_date, str) and start_date:
            start_date = pd.to_datetime(start_date).date()
    except Exception:
        start_date = None
    today = datetime.now().date()

    if status == 'Draft':
        return ['Edit', 'Publish', 'Archive']
    if status == 'Published':
        if start_date and today < start_date:
            return ['Edit', 'Unpublish', 'Archive']
        return ['Archive']
    if status == 'Archived':
        return []
    return []


def display_experiments_table(df: pd.DataFrame):
    """Display a single unified experiments table with per-row actions."""

    if 'status' in df.columns and not st.session_state.get("show_archived_toggle", False):
        df = df[df['status'] != 'Archived']

    if df.empty:
        st.info("No experiments found")
        st.markdown("---")
        return

    _open_id = st.query_params.get('open')

    df = df.copy()
    if 'start_date' in df.columns:
        df['sort_start_date'] = pd.to_datetime(df['start_date'], errors='coerce').dt.date
    else:
        df['sort_start_date'] = None
    if 'end_date' in df.columns:
        df['sort_end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date
    else:
        df['sort_end_date'] = None

    today = datetime.now().date()
    status_order_map = {'Draft': 0, 'Published': 1, 'Archived': 2}
    df['status_order'] = df['status'].map(status_order_map).fillna(99)
    df['is_running'] = (
        (df['status'] == 'Published')
        & (df['sort_start_date'].notna())
        & (df['sort_start_date'] <= today)
        & (df['sort_end_date'].isna() | (df['sort_end_date'] >= today))
    )
    df['is_queued'] = (df['status'] == 'Published') & (df['sort_start_date'].notna()) & (df['sort_start_date'] > today)
    df = df.sort_values(by=['status_order', 'sort_start_date'], ascending=[True, True])
    df = df.reset_index(drop=True)

    st.markdown("<div class='section-spacer'></div>", unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns([6, 2, 2, 2, 2])
    with col1:
        st.write("**Experiment Name:**")
    with col2:
        st.write("**Status:**")
    with col3:
        st.write("**Start Date:**")
    with col4:
        st.write("**End Date:**")
    with col5:
        st.write("**Actions:**")

    for row_pos, row in df.iterrows():
        col1, col2, col3, col4, col5 = st.columns([6, 2, 2, 2, 2])

        with col1:
            exp_name = row.get('experiment_name', '')
            exp_id = str(row.get('experiment_id', row_pos))
            st.markdown(
                f"<a class='exp-name' href='?open={exp_id}#top' target='_self'>" + (exp_name or 'Untitled') + "</a>",
                unsafe_allow_html=True,
            )
            if _open_id == exp_id:
                try:
                    del st.query_params['open']
                except Exception:
                    pass
                experiment_data = row.to_dict()
                status_val = (experiment_data.get('status') or '').strip()
                start_date_val = experiment_data.get('start_date')
                try:
                    if isinstance(start_date_val, str) and start_date_val:
                        start_date_val = pd.to_datetime(start_date_val).date()
                except Exception:
                    start_date_val = None
                today_btn = datetime.now().date()
                is_editable = False
                if status_val == 'Draft':
                    is_editable = True
                elif status_val == 'Published' and start_date_val and today_btn < start_date_val:
                    is_editable = True
                edit_experiment_dialog(experiment_data, is_view_only=not is_editable)
        with col2:
            status_text = row.get('status', '')
            try:
                running = bool(row.get('is_running'))
            except Exception:
                running = False
            if status_text == 'Published' and running:
                st.markdown("**Published** <span class='status-running'>(Running)</span>", unsafe_allow_html=True)
            elif status_text == 'Published' and bool(row.get('is_queued')):
                st.markdown("**Published** <span class='status-queued'>(Queued)</span>", unsafe_allow_html=True)
            else:
                st.text(status_text)
        with col3:
            start_date = row.get('start_date', '')
            st.text(str(start_date) if start_date else '')
        with col4:
            end_date = row.get('end_date', '')
            st.text(str(end_date) if end_date else '')
        with col5:
            experiment_data = row.to_dict()
            status_val = (experiment_data.get('status') or '').strip()
            start_date_val = experiment_data.get('start_date')
            try:
                if isinstance(start_date_val, str) and start_date_val:
                    start_date_val = pd.to_datetime(start_date_val).date()
            except Exception:
                start_date_val = None
            can_publish = status_val == 'Draft' and start_date_val is not None
            if can_publish:
                unique_key = f"publish_{row.get('experiment_id', row_pos)}_{row_pos}"
                if st.button("Publish", key=unique_key):
                    success, msg = publish_experiment(experiment_data)
                    if success:
                        st.success(
                            f"Experiment '{experiment_data.get('experiment_name','Unknown')}' published successfully!"
                        )
                        logger.info("Experiment %s published", experiment_data.get('experiment_id'))
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(
                            msg or f"Failed to publish experiment '{experiment_data.get('experiment_name','Unknown')}'"
                        )
                        logger.error("Publish failed for %s: %s", experiment_data.get('experiment_id'), msg)


def display_experiment_section(df, section_title, section_type, create_button_key=None):
    """Display a section of experiments with consistent formatting"""
    header_col1, header_col2 = st.columns([8, 1])
    with header_col1:
        st.markdown(f'<p style="font-size: 20px;">{section_title}:</p>', unsafe_allow_html=True)
    with header_col2:
        if create_button_key:
            if st.button("Create", key=create_button_key):
                create_experiment_dialog()

    if df.empty:
        st.info(f"No experiments found for {section_title.lower()}")
        st.markdown("---")
        return

    action_options = {
        'draft': ['Edit', 'Publish', 'Archive'],
        'published_unstarted': ['Edit', 'Unpublish', 'Archive'],
        'published_started': ['View', 'Archive'],
        'archived': ['View'],
    }

    first_row = True
    for idx, row in df.iterrows():
        col1, col2, col3, col4, col5, col6 = st.columns([3, 1, 1, 1, 1, 1])

        with col1:
            if first_row:
                st.write("**Experiment Name:**")
            st.text(row.get('experiment_name', ''))
        with col2:
            if first_row:
                st.write("**Status:**")
            st.text(row.get('status', ''))
        with col3:
            if first_row:
                st.write("**Start Date:**")
            start_date = row.get('start_date', '')
            st.text(str(start_date) if start_date else '')
        with col4:
            if first_row:
                st.write("**End Date:**")
            end_date = row.get('end_date', '')
            st.text(str(end_date) if end_date else '')
        with col5:
            if first_row:
                st.write("**Action:**")
            actions = action_options.get(section_type, ['View'])
            selected_action = st.selectbox('', actions, key=f"action_{section_type}_{idx}")
        with col6:
            if first_row:
                st.write("")
                st.write("")
            else:
                st.write("")
            if st.button("GO", key=f"go_{section_type}_{idx}"):
                experiment_data = row.to_dict()
                experiment_id = experiment_data.get('experiment_id')
                experiment_name = experiment_data.get('experiment_name', 'Unknown')

                if selected_action == "Edit":
                    edit_experiment_dialog(experiment_data, is_view_only=False)

                elif selected_action == "View":
                    edit_experiment_dialog(experiment_data, is_view_only=True)

                elif selected_action == "Publish":
                    if experiment_id:
                        success, msg = publish_experiment(experiment_data)
                        if success:
                            st.success(f"Experiment '{experiment_name}' published successfully!")
                            logger.info("Experiment %s published", experiment_id)
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg or f"Failed to publish experiment '{experiment_name}'")
                            logger.error("Publish failed for %s: %s", experiment_id, msg)

                elif selected_action == "Unpublish":
                    if experiment_id:
                        success, msg = unpublish_experiment(experiment_data)
                        if success:
                            st.success(f"Experiment '{experiment_name}' unpublished successfully!")
                            logger.info("Experiment %s unpublished", experiment_id)
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg or f"Failed to unpublish experiment '{experiment_name}'")
                            logger.error("Unpublish failed for %s: %s", experiment_id, msg)

                elif selected_action == "Archive":
                    if experiment_id:
                        success = update_experiment_status(experiment_id, "Archived")
                        if success:
                            st.success(f"Experiment '{experiment_name}' archived successfully!")
                            logger.info("Experiment %s archived", experiment_id)
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"Failed to archive experiment '{experiment_name}'")
                            logger.error("Archive failed for %s", experiment_id)

        first_row = False

    st.markdown("---")


if records.empty:
    st.info("No experiments found in the database. Create your first experiment to get started!")

    col1, col2, col3 = st.columns([2, 1, 2])
    with col2:
        if st.button(
            "Create First Experiment", key="create_first_experiment", use_container_width=True, type="primary"
        ):
            create_experiment_dialog()

elif 'status' in records.columns:
    display_experiments_table(records)

else:
    st.warning("No status column found in the data. Displaying all records:")
    display_experiments_table(records)

if st.session_state.get("open_create_experiment_dialog"):
    st.session_state["open_create_experiment_dialog"] = False
    create_experiment_dialog()
