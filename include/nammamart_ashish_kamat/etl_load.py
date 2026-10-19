"""
ETL load, reconciliation and reporting against include/warehouse_ashish_kamat.duckdb.

Idempotency:
  * dimensions are full snapshots -> replaced inside one transaction;
  * the fact is upserted by order_id. A row is only rewritten when the incoming
    version is from the same or a newer batch AND something changed, so a rerun
    inserts 0 and updates 0 rows, and re-running September never undoes an
    October correction;
  * reconciliation and report tables are rebuilt from the fact on every run.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from decimal import Decimal

import pandas as pd

from .config import AMOUNT_TOLERANCE
from .validate import dec, map_payment_mode, s
from .warehouse import run_sql_file, transaction

log = logging.getLogger(__name__)

FACT_COLS = [
    "order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id", "quantity",
    "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel",
    "delivery_minutes", "net_revenue", "cost_amount", "gross_margin", "within_15_min",
    "dq_status", "dq_warnings", "source_file", "source_row_num", "batch_cutoff", "loaded_run_id",
]
FACT_CASTS = {
    "order_ts": "TIMESTAMP", "order_date": "DATE", "quantity": "INTEGER", "unit_price": "DECIMAL(12,2)",
    "discount_pct": "DECIMAL(5,2)", "order_amount": "DECIMAL(12,2)", "delivery_minutes": "INTEGER",
    "net_revenue": "DECIMAL(12,2)", "cost_amount": "DECIMAL(12,2)", "gross_margin": "DECIMAL(12,2)",
    "within_15_min": "BOOLEAN", "source_row_num": "INTEGER", "batch_cutoff": "TIMESTAMP",
}
# Columns whose change counts as an update (lineage columns excluded).
COMPARE_COLS = [c for c in FACT_COLS if c not in
                ("order_id", "source_file", "source_row_num", "batch_cutoff", "loaded_run_id")]


def _typed_select(cols: list[str], casts: dict, src: str) -> str:
    parts = []
    for c in cols:
        t = casts.get(c)
        parts.append(f"CAST({src}.{c} AS {t}) AS {c}" if t else f"{src}.{c}")
    return ", ".join(parts)


def _stringify(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        out[c] = out[c].map(lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v))
    return out


def load_dimensions(con, dims: dict[str, pd.DataFrame], run_id: str) -> dict:
    casts = {
        "etl_dim_customer": {"signup_date": "DATE", "is_active": "BOOLEAN", "source_row_num": "INTEGER"},
        "etl_dim_product": {"mrp": "DECIMAL(12,2)", "cost_price": "DECIMAL(12,2)", "is_perishable": "BOOLEAN",
                            "launch_date": "DATE", "source_row_num": "INTEGER"},
        "etl_dim_store": {"open_date": "DATE", "capacity_orders_per_day": "INTEGER", "source_row_num": "INTEGER"},
    }
    counts = {}
    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    with transaction(con):
        for table, df in dims.items():
            con.register("dim_in", _stringify(df))
            cols = list(df.columns)
            con.execute(f"DELETE FROM {table}")
            con.execute(f"INSERT INTO {table} ({', '.join(cols)}, loaded_run_id, loaded_at) "
                        f"SELECT {_typed_select(cols, casts[table], 'dim_in')}, ?, CAST(? AS TIMESTAMP) FROM dim_in",
                        [run_id, now])
            con.unregister("dim_in")
            counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    return counts


def load_fact(con, fact: pd.DataFrame, rejected: pd.DataFrame, run_id: str) -> dict:
    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    con.register("fact_in", _stringify(fact[FACT_COLS]))
    con.register("rej_in", _stringify(rejected))
    changed = " OR ".join(f"s.{c} IS DISTINCT FROM f.{c}" for c in COMPARE_COLS)
    with transaction(con):
        con.execute(f"CREATE OR REPLACE TEMP TABLE stage_fact AS SELECT {_typed_select(FACT_COLS, FACT_CASTS, 'fact_in')} FROM fact_in")
        stats = con.execute(f"""
            SELECT
              count(*) FILTER (WHERE f.order_id IS NULL)                                   AS inserted,
              count(*) FILTER (WHERE f.order_id IS NOT NULL AND s.batch_cutoff >= f.batch_cutoff AND ({changed})) AS updated,
              count(*) FILTER (WHERE f.order_id IS NOT NULL AND s.batch_cutoff >= f.batch_cutoff AND NOT ({changed})) AS unchanged,
              count(*) FILTER (WHERE f.order_id IS NOT NULL AND s.batch_cutoff < f.batch_cutoff) AS skipped_older
            FROM stage_fact s LEFT JOIN etl_fact_orders f USING (order_id)
        """).fetchone()
        set_cols = ", ".join(f"{c} = s.{c}" for c in COMPARE_COLS + ["source_file", "source_row_num", "batch_cutoff"])
        con.execute(f"""
            UPDATE etl_fact_orders AS f
            SET {set_cols}, updated_run_id = ?, previous_status = f.order_status
            FROM stage_fact s
            WHERE s.order_id = f.order_id AND s.batch_cutoff >= f.batch_cutoff AND ({changed})
        """, [run_id])
        con.execute(f"""
            INSERT INTO etl_fact_orders ({', '.join(FACT_COLS)}, loaded_at)
            SELECT {', '.join('s.' + c for c in FACT_COLS)}, CAST(? AS TIMESTAMP)
            FROM stage_fact s WHERE s.order_id NOT IN (SELECT order_id FROM etl_fact_orders)
        """, [now])
        # A newer version of an order failed a critical check: remove the stale valid version.
        to_delete = con.execute("""
            SELECT f.order_id, r.failed_checks, to_json(f)::VARCHAR
            FROM etl_fact_orders f JOIN rej_in r ON r.order_id = f.order_id
            WHERE CAST(r._cutoff AS TIMESTAMP) >= f.batch_cutoff
        """).fetchall()
        for oid, checks, old in to_delete:
            con.execute("INSERT INTO etl_fact_orders_removed VALUES (?, ?, CAST(? AS TIMESTAMP), ?, ?)",
                        [oid, run_id, now, checks, old])
            con.execute("DELETE FROM etl_fact_orders WHERE order_id = ?", [oid])
        result = dict(zip(["inserted", "updated", "unchanged", "skipped_older"], stats))
        result["deleted"] = len(to_delete)
        sources = ",".join(sorted(fact["source_file"].unique())) if len(fact) else ""
        con.execute("""INSERT INTO etl_load_log VALUES (?, 'etl_fact_orders', ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP))
                       ON CONFLICT (run_id, table_name, source_file) DO UPDATE SET
                       inserted = excluded.inserted, updated = excluded.updated, unchanged = excluded.unchanged,
                       skipped_older = excluded.skipped_older, deleted = excluded.deleted, loaded_at = excluded.loaded_at""",
                    [run_id, sources, result["inserted"], result["updated"], result["unchanged"],
                     result["skipped_older"], result["deleted"], now])
        result["fact_rows_after"] = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
    con.unregister("fact_in")
    con.unregister("rej_in")
    log.info("[load] etl_fact_orders %s", json.dumps(result))
    return result


# ---------------------------------------------------------- reconciliation
def reconcile(fact: pd.DataFrame, payments: pd.DataFrame, source_order_ids: set, run_id: str) -> pd.DataFrame:
    """One row per order/payment mismatch.

    fact: current etl_fact_orders (valid orders). payments: validated payments
    (all rows, with dq columns). Payment rows that failed critical checks are
    still compared here, because reconciliation exists to surface exactly those.
    """
    tol = Decimal(str(AMOUNT_TOLERANCE))
    out = []

    def add(order_id, payment_ids, order_status, payment_status, mode, expected, actual, reason, detail=""):
        exp = None if expected is None else Decimal(str(expected))
        act = Decimal(str(actual or 0))
        diff = act - (exp or Decimal(0))
        out.append(dict(order_id=order_id, payment_ids=payment_ids, order_status=order_status,
                        payment_status=payment_status, payment_mode=mode,
                        expected_amount=None if exp is None else str(exp), actual_amount=str(act),
                        difference=str(diff), unreconciled_amount=str(abs(diff)), reason=reason,
                        detail=detail, run_id=run_id))

    p = payments.copy()
    p["payment_id"], p["order_id"] = s(p, "payment_id"), s(p, "order_id")
    p["status_std"] = s(p, "payment_status").str.upper()
    p["mode_std"] = map_payment_mode(p["payment_mode"])
    p["amt"] = p["amount_paid"].map(dec)
    is_extra = p["failed_checks"].str.split(";").map(lambda l: "payments_payment_id_unique" in l)
    unusable = (p["payment_id"] == "") | (p["order_id"] == "") | p["amt"].isna()

    # Money actually captured by the gateway: only SUCCESS rows move money.
    p["captured"] = [a if (st == "SUCCESS" and a is not None) else Decimal(0)
                     for a, st in zip(p["amt"], p["status_std"])]
    for _, r in p[is_extra].iterrows():
        add(r["order_id"], r["payment_id"], None, r["status_std"], r["mode_std"], 0, r["captured"],
            "DUPLICATE_PAYMENT_RECORD", f"extra copy at source row {r['_row_num']} would double count")
    for _, r in p[unusable & ~is_extra].iterrows():
        add(r["order_id"] or None, r["payment_id"] or None, None, r["status_std"], r["mode_std"], None, r["captured"],
            "UNUSABLE_PAYMENT_RECORD", r["failed_checks"])
    pu = p[~is_extra & ~unusable]
    fact_ids = set(fact["order_id"])
    for _, r in pu[~pu["order_id"].isin(fact_ids)].iterrows():
        reason = "PAYMENT_FOR_REJECTED_ORDER" if r["order_id"] in source_order_ids else "ORPHAN_PAYMENT"
        add(r["order_id"], r["payment_id"], None, r["status_std"], r["mode_std"], None, r["captured"], reason,
            "order quarantined; payment not matched to revenue" if reason == "PAYMENT_FOR_REJECTED_ORDER"
            else "order_id not present in any order file")

    by_order = {k: g for k, g in pu[pu["order_id"].isin(fact_ids)].groupby("order_id")}
    for _, o in fact.iterrows():
        oid, st = o["order_id"], o["order_status"]
        amount = Decimal(str(o["order_amount"]))
        g = by_order.get(oid)
        succ = g[g["status_std"] == "SUCCESS"] if g is not None else None
        ids = ",".join(sorted(g["payment_id"])) if g is not None else None
        statuses = ",".join(sorted(set(g["status_std"]))) if g is not None else None
        paid = sum(succ["amt"], Decimal(0)) if succ is not None and len(succ) else Decimal(0)
        if st == "DELIVERED":
            if g is None:
                add(oid, None, st, None, o["payment_mode"], amount, 0, "MISSING_PAYMENT")
            elif len(succ) == 0:
                reason = "PAYMENT_FAILED" if "FAILED" in set(g["status_std"]) else "NO_SUCCESS_PAYMENT"
                add(oid, ids, st, statuses, o["payment_mode"], amount, 0, reason)
            elif len(succ) > 1:
                add(oid, ids, st, statuses, o["payment_mode"], amount, paid, "DUPLICATE_PAYMENT",
                    f"{len(succ)} SUCCESS payments")
            else:
                pr = succ.iloc[0]
                if abs(pr["amt"] - amount) > tol:
                    add(oid, ids, st, statuses, o["payment_mode"], amount, pr["amt"], "AMOUNT_MISMATCH")
                elif pr["mode_std"] != o["payment_mode"]:
                    add(oid, ids, st, statuses, o["payment_mode"], amount, pr["amt"], "PAYMENT_MODE_MISMATCH",
                        f"gateway mode {pr['mode_std']}")
        elif st == "CANCELLED" and paid > 0:
            add(oid, ids, st, statuses, o["payment_mode"], 0, paid, "PAYMENT_ON_CANCELLED_ORDER")
        elif st == "RETURNED" and paid > 0 and "REFUNDED" not in set(g["status_std"]):
            add(oid, ids, st, statuses, o["payment_mode"], 0, paid, "RETURNED_NOT_REFUNDED",
                "order returned but gateway shows no refund")
    cols = ["order_id", "payment_ids", "order_status", "payment_status", "payment_mode", "expected_amount",
            "actual_amount", "difference", "unreconciled_amount", "reason", "detail", "run_id"]
    return pd.DataFrame(out, columns=cols)


def write_reconciliation(con, recon: pd.DataFrame) -> int:
    con.register("recon_in", _stringify(recon))
    with transaction(con):
        con.execute("""
            CREATE OR REPLACE TABLE etl_payment_reconciliation AS
            SELECT order_id, payment_ids, order_status, payment_status, payment_mode,
                   CAST(expected_amount AS DECIMAL(12,2)) AS expected_amount,
                   CAST(actual_amount AS DECIMAL(12,2)) AS actual_amount,
                   CAST(difference AS DECIMAL(12,2)) AS difference,
                   CAST(unreconciled_amount AS DECIMAL(12,2)) AS unreconciled_amount,
                   reason, detail, run_id, current_timestamp AS reconciled_at
            FROM recon_in ORDER BY reason, order_id
        """)
    con.unregister("recon_in")
    return len(recon)


def write_rejected_rows(con, rejected: list[dict], run_id: str) -> None:
    with transaction(con):
        con.execute("DELETE FROM etl_rejected_rows WHERE run_id = ?", [run_id])
        for r in rejected:
            con.execute("INSERT INTO etl_rejected_rows VALUES (?, ?, ?, ?, ?, ?)",
                        [run_id, r["source_file"], r["check_name"], r["severity"], r["rows"], r["quarantine_file"]])


def build_reports(con, run_id: str) -> dict:
    run_sql_file(con, "etl/reports.sql", {"run_id": run_id})
    tables = ["etl_daily_zone_revenue", "etl_category_margin", "etl_delivery_sla",
              "etl_payment_mode_revenue", "etl_top_customers"]
    return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}
