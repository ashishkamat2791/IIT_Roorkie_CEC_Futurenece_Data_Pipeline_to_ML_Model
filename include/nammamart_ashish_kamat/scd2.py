"""
Bonus: SCD Type 2 customer dimension for the ETL pipeline (pandas).

Tracked attributes: loyalty_tier and is_active. For each customer in the
validated snapshot:
  * no current version            -> insert NEW version valid_from = batch cut-off
  * current version, newer batch,
    tracked attributes changed    -> close it (valid_to = cut-off) and insert CHANGED
  * same values, or a batch that is not newer than the current version -> no-op
The ELT pipeline implements the same rules in include/sql/elt/mart/dim_customer_scd2.sql.
"""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from .warehouse import transaction

log = logging.getLogger(__name__)
TRACKED = ["loyalty_tier", "is_active"]


def _norm(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return str(v).lower() if isinstance(v, bool) else str(v)


def plan_changes(snapshot: pd.DataFrame, current: pd.DataFrame, as_of: datetime) -> tuple[list[dict], list[str]]:
    """Returns (rows to insert, customer_sk values to close). Pure function, unit-tested."""
    cur = {r["customer_id"]: r for r in current.to_dict("records")}
    inserts, closes = [], []
    for r in snapshot.to_dict("records"):
        cid = r["customer_id"]
        c = cur.get(cid)
        row = {"customer_id": cid, "full_name_masked": r.get("full_name_masked"),
               "loyalty_tier": _norm(r.get("loyalty_tier")), "is_active": _norm(r.get("is_active")),
               "valid_from": as_of}
        if c is None:
            inserts.append({**row, "change_type": "NEW"})
        elif pd.Timestamp(as_of) > pd.Timestamp(c["valid_from"]) and any(
                _norm(row[k]) != _norm(c[k]) for k in TRACKED):
            closes.append(c["customer_sk"])
            inserts.append({**row, "change_type": "CHANGED"})
    for i in inserts:
        i["customer_sk"] = f"{i['customer_id']}@{pd.Timestamp(as_of):%Y-%m-%dT%H:%M:%S}"
    return inserts, closes


def load_customer_scd2(con, snapshot: pd.DataFrame, as_of: datetime, run_id: str) -> dict:
    current = con.execute("""SELECT customer_sk, customer_id, loyalty_tier, CAST(is_active AS VARCHAR) AS is_active,
                                    valid_from FROM etl_dim_customer_scd2 WHERE is_current""").df()
    inserts, closes = plan_changes(snapshot, current, as_of)
    with transaction(con):
        for sk in closes:
            con.execute("UPDATE etl_dim_customer_scd2 SET valid_to = ?, is_current = FALSE WHERE customer_sk = ?",
                        [as_of, sk])
        for r in inserts:
            con.execute("""INSERT INTO etl_dim_customer_scd2 VALUES (?, ?, ?, ?, CAST(? AS BOOLEAN), ?, NULL, TRUE, ?, ?)""",
                        [r["customer_sk"], r["customer_id"], r["full_name_masked"], r["loyalty_tier"],
                         r["is_active"], as_of, r["change_type"], run_id])
    stats = {"new": sum(r["change_type"] == "NEW" for r in inserts), "changed": len(closes),
             "versions_total": con.execute("SELECT count(*) FROM etl_dim_customer_scd2").fetchone()[0],
             "current_total": con.execute("SELECT count(*) FROM etl_dim_customer_scd2 WHERE is_current").fetchone()[0]}
    log.info("[scd2] etl_dim_customer_scd2 %s", stats)
    return stats
