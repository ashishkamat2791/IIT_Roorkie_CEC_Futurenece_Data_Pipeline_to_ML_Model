"""
ELT pipeline steps: Python only orchestrates. Every transformation is a .sql
file in include/sql/elt/ executed inside DuckDB; no pandas touches the data.
"""

from __future__ import annotations

import csv
import json
import logging
from datetime import datetime

from .config import INCREMENT_PATTERN, SOURCES, RunConfig, increment_cutoff
from .extract import check_schema, file_md5
from .warehouse import connect, ensure_audit_tables, run_sql_file, transaction

log = logging.getLogger(__name__)

STAGING_MASTERS = ("stg_customers", "stg_products", "stg_stores")
MARTS = ("int_fact_orders", "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_reconciliation", "mart_top_customers", "mart_payment_mode_revenue", "mart_rejected_rows")


def now() -> str:
    return datetime.now().isoformat(sep=" ", timespec="seconds")


def start_run(cfg: RunConfig) -> dict:
    with connect(cfg.warehouse) as con:
        ensure_audit_tables(con)
        run_sql_file(con, "elt/raw/ddl_raw.sql")
        if cfg.orders_baseline is None:
            row = con.execute("""
                SELECT median(rows_checked) FROM (
                  SELECT rows_checked FROM dq_results
                  WHERE dag_id = ? AND check_name = 'orders_volume_vs_baseline' AND status = 'PASS'
                  ORDER BY checked_at DESC LIMIT 7)""", [cfg.dag_id]).fetchone()
            cfg.orders_baseline = int(row[0]) if row and row[0] is not None else None
    out = cfg.to_dict()
    out["start_time"] = now()
    log.info("[start] %s", json.dumps(out))
    return out


def extract_check(cfg: RunConfig, source: str, file_name: str | None = None) -> dict:
    """Extract = confirm the file exists and honours its column contract. DuckDB reads it in load_raw."""
    spec = SOURCES["orders" if source == "orders_increment" else source]
    file_name = file_name or spec["file"]
    path = cfg.data_dir / file_name
    if not path.exists():
        raise FileNotFoundError(f"required source file missing: {path}")
    check_schema(path, spec["columns"])
    with open(path, newline="") as f:
        rows = sum(1 for _ in csv.reader(f)) - 1
    meta = {"source": source, "file": file_name, "path": str(path), "rows": rows, "md5": file_md5(path)}
    log.info("[extract] %s", meta)
    return meta


def load_raw(cfg: RunConfig, meta: dict) -> dict:
    table = "raw_orders" if meta["source"] == "orders_increment" else f"raw_{meta['source']}"
    with connect(cfg.warehouse) as con:
        with transaction(con):
            run_sql_file(con, "elt/raw/load_raw.sql", idents={"table": table},
                         params={"run_id": cfg.run_id, "source_file": meta["file"], "loaded_at": now(),
                                 "file_md5": meta["md5"], "path": meta["path"]})
        loaded = con.execute(f"SELECT count(*) FROM {table} WHERE _run_id = ? AND _source_file = ?",
                             [cfg.run_id, meta["file"]]).fetchone()[0]
        total = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    if loaded != meta["rows"]:
        raise ValueError(f"{table}: loaded {loaded} rows but {meta['file']} has {meta['rows']}")
    log.info("[raw] %s += %s rows from %s (table now %s rows, all history kept)", table, loaded, meta["file"], total)
    return {"table": table, "file": meta["file"], "rows_loaded": loaded, "table_rows": total}


def build_staging(cfg: RunConfig, name: str) -> dict:
    with connect(cfg.warehouse) as con:
        with transaction(con):
            run_sql_file(con, f"elt/staging/{name}.sql", params={"cutoff": cfg.as_of, "run_id": cfg.run_id})
        stats = dict(con.execute(f"SELECT dq_status, count(*) FROM {name} GROUP BY 1").fetchall())
    log.info("[staging] %s %s", name, stats)
    return {"table": name, "dq_status": stats}


def build_orders_staging(cfg: RunConfig) -> dict:
    """stg_orders_versions (every row of every order file version) then stg_orders via MERGE."""
    with connect(cfg.warehouse) as con:
        inc_files = [r[0] for r in con.execute(
            "SELECT DISTINCT _source_file FROM raw_orders WHERE _source_file <> 'orders.csv' ORDER BY 1").fetchall()]
        inc_files = [f for f in inc_files if INCREMENT_PATTERN.match(f)]
        with transaction(con):
            run_sql_file(con, "elt/staging/stg_orders_versions.sql", params={"cutoff": cfg.as_of})
            for f in inc_files:
                run_sql_file(con, "elt/staging/stg_orders_versions_increment.sql",
                             params={"source_file": f, "cutoff": increment_cutoff(f)})
            run_sql_file(con, "elt/staging/stg_orders_dq.sql",
                         params={"cutoff": cfg.as_of, "baseline": cfg.orders_baseline or 0,
                                 "expect_reduced_volume": cfg.expect_reduced_volume})
            run_sql_file(con, "elt/staging/stg_orders_base.sql")
            merges = []
            for f in inc_files:  # file names sort by date, so increments replay in order
                run_sql_file(con, "elt/staging/stg_orders_merge.sql", params={"source_file": f, "run_id": cfg.run_id})
                merges.append(dict(zip(
                    ["source_file", "rows_in_file", "inserted", "updated", "status_changed", "skipped_older", "extra_copies"],
                    con.execute("SELECT source_file, rows_in_file, inserted, updated, status_changed, skipped_older, "
                                "extra_copies FROM elt_merge_log WHERE run_id = ? AND source_file = ?",
                                [cfg.run_id, f]).fetchone())))
        stats = dict(con.execute("SELECT dq_status, count(*) FROM stg_orders GROUP BY 1").fetchall())
        versions = con.execute("SELECT count(*) FROM stg_orders_versions").fetchone()[0]
        dup = con.execute("SELECT count(*) - count(DISTINCT order_id) FROM stg_orders").fetchone()[0]
    if dup:
        raise ValueError(f"stg_orders has {dup} duplicate order_id rows after MERGE")
    log.info("[staging] stg_orders_versions=%s stg_orders=%s merges=%s", versions, stats, merges)
    return {"table": "stg_orders", "versions": versions, "dq_status": stats, "merges": merges}


def record_dq_results(cfg: RunConfig) -> dict:
    with connect(cfg.warehouse) as con:
        with transaction(con):
            run_sql_file(con, "elt/dq/dq_results.sql",
                         params={"run_id": cfg.run_id, "dag_id": cfg.dag_id, "checked_at": now()})
        rows = con.execute("""SELECT check_name, dimension, status, rows_failed, rows_checked, source_file, details
                              FROM dq_results WHERE run_id = ? AND dag_id = ? ORDER BY source_file, check_name""",
                           [cfg.run_id, cfg.dag_id]).fetchall()
    for r in rows:
        if r[2] != "PASS":
            log.info("[dq] %-45s %-12s %-7s %s/%s %s %s", *r)
    warnings = [f"{r[0]}: {r[6]}" for r in rows if r[2] == "WARN" and r[6]]
    return {"checks": len(rows), "failed": sum(r[2] == "FAIL" for r in rows),
            "warned": sum(r[2] == "WARN" for r in rows), "dataset_warnings": warnings}


def build_mart(cfg: RunConfig, name: str) -> dict:
    with connect(cfg.warehouse) as con:
        with transaction(con):
            run_sql_file(con, f"elt/mart/{name}.sql", params={"run_id": cfg.run_id})
        n = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    log.info("[mart] %s rows=%s", name, n)
    return {"table": name, "rows": n}


def customer_scd2(cfg: RunConfig) -> dict:
    """Bonus: SCD Type 2 customer dimension (loyalty-tier history) in SQL."""
    with connect(cfg.warehouse) as con:
        with transaction(con):
            run_sql_file(con, "elt/mart/dim_customer_scd2.sql", params={"as_of": cfg.as_of, "run_id": cfg.run_id})
        r = con.execute("""SELECT count(*) FILTER (WHERE change_type = 'NEW' AND loaded_run_id = ?),
                                  count(*) FILTER (WHERE change_type = 'CHANGED' AND loaded_run_id = ?),
                                  count(*), count(*) FILTER (WHERE is_current) FROM elt_dim_customer_scd2""",
                        [cfg.run_id, cfg.run_id]).fetchone()
    stats = dict(zip(["new", "changed", "versions_total", "current_total"], r))
    log.info("[scd2] elt_dim_customer_scd2 %s", stats)
    return stats


def report(cfg: RunConfig) -> dict:
    out = {}
    with connect(cfg.warehouse, read_only=False) as con:
        for t in MARTS[1:]:
            df = con.execute(f"SELECT * FROM {t}").df()
            shown = df.tail(20) if len(df) > 20 else df
            log.info("[report] %s (%s rows)%s\n%s", t, len(df), " - last 20 shown" if len(df) > 20 else "",
                     shown.to_string(index=False))
            out[t] = len(df)
        tot = con.execute("""SELECT sum(delivered_orders), sum(net_revenue), sum(gross_margin)
                             FROM mart_daily_store_kpis WHERE order_date < DATE '2026-10-01'""").fetchone()
        quarantined = con.execute("""
            SELECT (SELECT count(*) FROM stg_orders_versions WHERE dq_status IN ('FAILED','FAILED_ATTRIBUTE'))
                 + (SELECT count(*) FROM stg_payments  WHERE dq_status IN ('FAILED','FAILED_ATTRIBUTE'))
                 + (SELECT count(*) FROM stg_customers WHERE dq_status IN ('FAILED','FAILED_ATTRIBUTE'))
                 + (SELECT count(*) FROM stg_products  WHERE dq_status IN ('FAILED','FAILED_ATTRIBUTE'))
                 + (SELECT count(*) FROM stg_stores    WHERE dq_status IN ('FAILED','FAILED_ATTRIBUTE'))""").fetchone()[0]
    out.update(september_delivered_orders=int(tot[0] or 0), september_net_revenue=float(tot[1] or 0),
               september_gross_margin=float(tot[2] or 0), rows_failed_in_staging=int(quarantined))
    log.info("[report] September totals %s", out)
    return out
