import logging

import pandas as pd
from config import APP_CONFIG
from db import get_benchmark_table_name, get_database_connection
from sqlalchemy import text

logger = logging.getLogger(__name__)


def get_benchmarks() -> dict:
    """Return metric benchmarks."""
    benchmark_keys = [f"{v['name']}_benchmark" for v in APP_CONFIG.get("kpi_options").values()]

    db_engine = get_database_connection()
    query = text(
        f"""
        SELECT {", ".join(benchmark_keys)}
        FROM {get_benchmark_table_name()}
        """
    )
    try:
        data = pd.read_sql(query, db_engine)

        if data.empty:
            # Fallback to benchmarks from kpi_options config
            return {f"{v['name']}_benchmark": v['benchmark'] for v in APP_CONFIG.get("kpi_options").values()}

        return data.iloc[0].to_dict()

    except Exception:
        logger.exception("Failed to retrieve benchmarks values")
        # Fallback to benchmarks from kpi_options config
        return {f"{v['name']}_benchmark": v['benchmark'] for v in APP_CONFIG.get("kpi_options").values()}
