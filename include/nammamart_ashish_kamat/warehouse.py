"""
DuckDB helpers shared by both pipelines.

DuckDB allows one writing process per database file. Airflow's LocalExecutor
runs parallel tasks as separate processes, so every connection goes through
`connect()`, which waits and retries while another task holds the lock.
"""

from __future__ import annotations

import logging
import random
import re
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from .config import SQL_DIR

log = logging.getLogger(__name__)

DQ_COLUMNS = ["run_id", "dag_id", "check_name", "dimension", "severity", "rows_checked",
              "rows_failed", "status", "checked_at", "source_file", "population", "details"]


@contextmanager
def connect(path: Path, read_only: bool = False, timeout_s: float = 180.0):
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            con = duckdb.connect(str(path), read_only=read_only)
            break
        except duckdb.IOException as exc:  # lock held by another task process
            if "lock" not in str(exc).lower() or time.monotonic() > deadline:
                raise
            time.sleep(0.5 + random.random())
    try:
        yield con
    finally:
        con.close()


@contextmanager
def transaction(con: duckdb.DuckDBPyConnection):
    con.execute("BEGIN TRANSACTION")
    try:
        yield con
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def read_sql(relative: str) -> str:
    return (SQL_DIR / relative).read_text()


IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def run_sql_file(con: duckdb.DuckDBPyConnection, relative: str, params: dict | None = None,
                 idents: dict | None = None) -> None:
    """Execute a .sql file. `{{ name }}` placeholders are replaced by SQL literals
    (params) or by validated table identifiers (idents)."""
    sql = read_sql(relative)
    for k, v in (idents or {}).items():
        if not IDENT_RE.match(v):
            raise ValueError(f"unsafe identifier {v!r}")
        sql = sql.replace("{{ " + k + " }}", v)
    for k, v in (params or {}).items():
        sql = sql.replace("{{ " + k + " }}", sql_literal(v))
    if "{{" in sql:
        raise ValueError(f"{relative}: unresolved placeholder near {sql[sql.index('{{'):][:40]!r}")
    log.info("running %s", relative)
    con.execute(sql)


def sql_literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, datetime):
        v = v.isoformat(sep=" ")
    return "'" + str(v).replace("'", "''") + "'"


def ensure_audit_tables(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "common/ddl_audit.sql")


def write_dq_results(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    """Upsert check results; a retried task overwrites its own rows instead of duplicating them."""
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    for c in DQ_COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[DQ_COLUMNS]
    df["details"] = df["details"].fillna("").astype(str)
    con.register("dq_in", df)
    con.execute("""
        INSERT INTO dq_results
        SELECT run_id, dag_id, check_name, dimension, severity, CAST(rows_checked AS BIGINT),
               CAST(rows_failed AS BIGINT), status, CAST(checked_at AS TIMESTAMP),
               source_file, population, details
        FROM dq_in
        ON CONFLICT (run_id, dag_id, check_name, source_file) DO UPDATE SET
            dimension = excluded.dimension, severity = excluded.severity,
            rows_checked = excluded.rows_checked, rows_failed = excluded.rows_failed,
            status = excluded.status, checked_at = excluded.checked_at,
            population = excluded.population, details = excluded.details
    """)
    con.unregister("dq_in")
    return len(df)


def check_status(severity: str, rows_failed: int) -> str:
    if rows_failed == 0:
        return "PASS"
    return "FAIL" if severity == "Critical" else "WARN"
