import os
import time

import psycopg
from databricks.sdk import WorkspaceClient
from psycopg.rows import dict_row


def _get_oauth_token() -> str:
    wc = WorkspaceClient()

    cfg = getattr(wc, "config", None)
    if cfg is not None and callable(getattr(cfg, "oauth_token", None)):
        tok = cfg.oauth_token()
        if tok and getattr(tok, "access_token", None):
            return tok.access_token


class LakebaseClient:
    def __init__(self):
        self.conn = None

    def _connect(self, max_retries: int = 3, retry_delay: float = 0.3):
        for attempt in range(1, max_retries + 1):
            try:
                conn = psycopg.connect(
                    host=os.environ["LAKEBASE_HOST"],
                    port=os.environ.get("LAKEBASE_PORT", "5432"),
                    dbname=os.environ["LAKEBASE_DB"],
                    user=os.environ["LAKEBASE_USER"],
                    password=_get_oauth_token(),
                    sslmode=os.environ.get("LAKEBASE_SSLMODE", "require"),
                    autocommit=True,
                )
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                self.conn = conn
                return conn
            except Exception as e:
                if attempt == max_retries:
                    raise e
                time.sleep(retry_delay * attempt)

    def get_conn(self):
        """Return a healthy connection (reconnects if needed)."""
        if self.conn is None or self.conn.closed:
            return self._connect()

        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1;")
            return self.conn
        except Exception:
            return self._connect()

    def fetch(self, sql: str, params=None):
        conn = self.get_conn()
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or [])
            return cur.fetchall()
