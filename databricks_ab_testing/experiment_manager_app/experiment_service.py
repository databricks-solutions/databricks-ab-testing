import json
import logging
from datetime import datetime
from typing import Tuple

import pandas as pd
from db import execute_insert_query, get_database_connection, get_experiment_table_name
from sqlalchemy import text

logger = logging.getLogger(__name__)


def update_experiment_status(experiment_id: str, new_status: str) -> bool:
    """Update experiment status in database"""
    db_engine = get_database_connection()
    update_query = f"""
        UPDATE {get_experiment_table_name()}
        SET status = :status
        WHERE experiment_id = :experiment_id
    """
    params = {'status': new_status, 'experiment_id': experiment_id}
    ok = execute_insert_query(db_engine, update_query, params)
    if ok:
        logger.info("Experiment %s status updated to %s", experiment_id, new_status)
    else:
        logger.error("Failed to update status for experiment %s to %s", experiment_id, new_status)
    return ok


def update_experiment_design_params(experiment_id: str, mde: float, power: float, significance_level: float) -> bool:
    """Update just the design parameters (MDE, power, significance) regardless of other fields."""
    db_engine = get_database_connection()
    update_query = f"""
        UPDATE {get_experiment_table_name()}
        SET mde = :mde,
            power = :power,
            significance_level = :significance_level
        WHERE experiment_id = :experiment_id
    """
    params = {
        'mde': mde,
        'power': power,
        'significance_level': significance_level,
        'experiment_id': experiment_id,
    }
    return execute_insert_query(db_engine, update_query, params)


def find_overlapping_published(start_date, end_date, exclude_experiment_id: str | None = None) -> pd.DataFrame:
    """Return published experiments that overlap given date range (excluding id if provided)."""
    db_engine = get_database_connection()
    query = text(
        f"""
        SELECT experiment_id, experiment_name, start_date, end_date
        FROM {get_experiment_table_name()}
        WHERE status = 'Published'
          AND (:exclude_id IS NULL OR experiment_id <> :exclude_id)
          AND start_date <= :new_end
          AND (end_date IS NULL OR end_date >= :new_start)
        """
    )
    params = {
        'exclude_id': exclude_experiment_id,
        'new_start': start_date,
        'new_end': end_date if end_date else start_date,
    }
    try:
        return pd.read_sql(query, db_engine, params=params)
    except Exception:
        logger.exception("Failed to check overlapping experiments")
        return pd.DataFrame()


def publish_experiment(experiment_row: dict) -> Tuple[bool, str]:
    """Publish experiment after enforcing constraints (dates present, no overlap).

    Returns (success, error_message). error_message is empty on success.
    """
    experiment_id = experiment_row.get('experiment_id')
    start_date = experiment_row.get('start_date')
    end_date = experiment_row.get('end_date')

    if not start_date:
        return False, "Start date is required to publish an experiment"

    # Convert potential string dates
    try:
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
        if end_date and isinstance(end_date, str):
            end_date = pd.to_datetime(end_date).date()
    except Exception:
        return False, "Invalid date format on experiment; fix dates before publishing"

    logger.info("Attempting to publish experiment %s (start=%s, end=%s)", experiment_id, start_date, end_date)
    overlaps = find_overlapping_published(start_date, end_date, exclude_experiment_id=experiment_id)
    if not overlaps.empty:
        return False, "Cannot publish: overlapping published experiment exists in the selected dates"

    ok = update_experiment_status(experiment_id, "Published")
    return ok, ("" if ok else "Failed to update status during publishing")


def unpublish_experiment(experiment_row: dict) -> Tuple[bool, str]:
    """Revert to Draft only if current_date < start_date. Returns (success, message)."""
    experiment_id = experiment_row.get('experiment_id')
    start_date = experiment_row.get('start_date')
    try:
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
    except Exception:
        return False, "Invalid start date; cannot unpublish"

    if not start_date:
        # No start date means unstarted; allow
        logger.info("Unpublishing experiment %s (no start_date set)", experiment_id)
        ok = update_experiment_status(experiment_id, "Draft")
        return ok, ("" if ok else "Failed to update status during unpublish")

    if datetime.now().date() < start_date:
        logger.info("Unpublishing experiment %s (before start date %s)", experiment_id, start_date)
        ok = update_experiment_status(experiment_id, "Draft")
        return ok, ("" if ok else "Failed to update status during unpublish")

    return False, "Cannot revert to Draft after the start date"


def update_experiment(
    experiment_id: str,
    experiment_name: str,
    start_date,
    end_date,
    treatment_allocation: float,
    control_config: dict,
    treatment_config: dict,
    mde: float | None,
    power: float | None,
    significance_level: float | None,
    control_sample_size: int | None,
    treatment_sample_size: int | None,
    primary_kpi_metric: str | None,
) -> bool:
    """Update experiment details in database"""
    db_engine = get_database_connection()
    update_query = f"""
        UPDATE {get_experiment_table_name()}
        SET experiment_name = :experiment_name,
            start_date = :start_date,
            end_date = :end_date,
            treatment_allocation = :treatment_allocation,
            control_config = :control_config,
            treatment_config = :treatment_config,
            mde = :mde,
            power = :power,
            significance_level = :significance_level,
            control_sample_size = :control_sample_size,
            treatment_sample_size = :treatment_sample_size,
            primary_kpi_metric = :primary_kpi_metric
        WHERE experiment_id = :experiment_id
    """

    params = {
        'experiment_name': experiment_name,
        'start_date': start_date,
        'end_date': end_date,
        'treatment_allocation': treatment_allocation,
        'control_config': json.dumps(control_config) if isinstance(control_config, dict) else control_config,
        'treatment_config': json.dumps(treatment_config) if isinstance(treatment_config, dict) else treatment_config,
        'mde': mde,
        'power': power,
        'significance_level': significance_level,
        'control_sample_size': control_sample_size,
        'treatment_sample_size': treatment_sample_size,
        'primary_kpi_metric': primary_kpi_metric,
        'experiment_id': experiment_id,
    }

    ok = execute_insert_query(db_engine, update_query, params)
    if ok:
        logger.info(
            "Updated experiment %s (name=%s, start=%s, end=%s)", experiment_id, experiment_name, start_date, end_date
        )
    else:
        logger.error("Failed to update experiment %s", experiment_id)
    return ok
