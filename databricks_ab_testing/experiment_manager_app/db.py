import logging
import os
from typing import Any, Dict

import pandas as pd
import psycopg
import streamlit as st
from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


@st.cache_resource(ttl=3300)
def get_database_connection():
    """Get Lakebase connection as a SQLAlchemy engine (cached with TTL, fresh token per connection)"""
    postgres_username = os.getenv("PGUSER")
    postgres_host = os.getenv("PGHOST")
    postgres_port = os.getenv("PGPORT")
    postgres_database = os.getenv("PGDATABASE")
    sslmode = os.getenv("PGSSLMODE", "require")
    app_env = os.getenv("APP_ENV", "dev").lower()
    app_name = f"experiment_manager_app_{app_env}"

    def _get_oauth_token() -> str:
        wc = WorkspaceClient()
        try:
            cfg = getattr(wc, "config", None)
            if cfg is not None and callable(getattr(cfg, "oauth_token", None)):
                tok = cfg.oauth_token()
                if tok and getattr(tok, "access_token", None):
                    return tok.access_token
        except Exception:
            pass

        val = os.getenv("DATABRICKS_OAUTH_TOKEN")
        if val:
            return val
        raise RuntimeError("Unable to obtain Databricks OAuth token for database connection")

    def _creator():
        postgres_password = _get_oauth_token()
        if not postgres_password:
            raise RuntimeError("Unable to obtain Databricks Postgres password for database connection")
        return psycopg.connect(
            host=postgres_host,
            port=postgres_port,
            dbname=postgres_database,
            user=postgres_username,
            password=postgres_password,
            sslmode=sslmode,
            application_name=app_name,
        )

    engine = create_engine(
        "postgresql+psycopg://",
        creator=_creator,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=1800,
        echo=False,
    )

    return engine


def clear_database_connection_cache():
    """Clear the cached database connection and dispose pooled connections."""
    try:
        eng = get_database_connection()
        eng.dispose()
    except Exception:
        pass
    get_database_connection.clear()


def execute_query(engine, query: str) -> pd.DataFrame:
    """Execute SQL query and return DataFrame"""
    try:
        return pd.read_sql(query, engine)
    except Exception:
        logger.exception(f"Database query failed: {query}")
        return pd.DataFrame()


def execute_insert_query(engine, query: str, params: Dict[str, Any] | None = None) -> bool:
    """Execute INSERT/UPDATE query and return success status"""
    try:
        with engine.begin() as conn:
            conn.execute(text(query), params or {})
        return True
    except Exception:
        logger.exception(f"Database insert failed: {query}")
        return False


def get_experiment_table_name(split: bool = False):
    catalog = os.getenv("PGDATABASE")
    schema = os.getenv("AB_TESTING_SCHEMA_NAME")
    table = os.getenv("EXPERIMENTS_TABLE_NAME")

    if split:
        return catalog, schema, table
    else:
        return f"{catalog}.{schema}.{table}"


def get_benchmark_table_name(split: bool = False):
    catalog = os.getenv("PGDATABASE")
    schema = os.getenv("AB_TESTING_SCHEMA_NAME")
    table = os.getenv("BENCHMARKS_TABLE_NAME")

    if split:
        return catalog, schema, table
    else:
        return f"{catalog}.{schema}.{table}"
