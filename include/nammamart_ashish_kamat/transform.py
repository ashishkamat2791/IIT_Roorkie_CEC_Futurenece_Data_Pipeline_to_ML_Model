"""
ETL transformations (pandas): standardise, deduplicate, mask, conform and
derive the star schema rows. Money stays in Decimal end-to-end and is written
as text, then cast to DECIMAL(12,2) by DuckDB, so ETL and ELT round identically.
"""

from __future__ import annotations

from decimal import Decimal

import pandas as pd

from .config import WITHIN_SLA_MINUTES
from .masking import mask_email, mask_name, mask_phone
from .validate import DATE_FMT, PINCODE_RE, TS_FMT, dec, map_payment_mode, s

LOADABLE = ("PASSED", "WARNING")


def _nullified(row, col: str) -> bool:
    return col in str(row.get("nullify_columns", "")).split(";")


def _clean(v):
    v = str(v).strip()
    return v if v else None


def _date(v, fmt=DATE_FMT):
    t = pd.to_datetime(str(v).strip(), format=fmt, errors="coerce")
    return None if pd.isna(t) else t.strftime("%Y-%m-%d")


def dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    """One row per customer_id (identity failures already excluded). No raw PII leaves this function."""
    c = customers[customers["dq_status"] != "FAILED"]
    rows = []
    for _, r in c.iterrows():
        pin = r["pincode"].strip()
        rows.append({
            "customer_id": r["customer_id"].strip(),
            "full_name_masked": mask_name(r["full_name"]),
            "email_masked": mask_email(r["email"]),
            "phone_masked": mask_phone(r["phone"]),
            "city": _clean(r["city"]),
            "area": _clean(r["area"]),
            "pincode": pin if pd.Series([pin]).str.match(PINCODE_RE).iloc[0] else None,
            "signup_date": None if _nullified(r, "signup_date") else _date(r["signup_date"]),
            "loyalty_tier": None if _nullified(r, "loyalty_tier") else r["loyalty_tier"].strip().title(),
            "is_active": {"Y": "true", "N": "false"}.get(r["is_active"].strip().upper()),
            "dq_status": r["dq_status"],
            "dq_issues": "; ".join(x for x in [r["failed_checks"], _check_names(r["warning_reason"])] if x),
            "source_file": r["_source_file"],
            "source_row_num": int(r["_row_num"]),
        })
    return pd.DataFrame(rows)


def _check_names(reason: str) -> str:
    return ";".join(sorted({p.split(":")[0].strip() for p in str(reason).split("|") if p.strip()}))


def dim_product(products: pd.DataFrame) -> pd.DataFrame:
    p = products[products["dq_status"] != "FAILED"]
    rows = []
    for _, r in p.iterrows():
        rows.append({
            "product_id": r["product_id"].strip(),
            "product_name": _clean(r["product_name"]),
            "category": "Unknown" if _nullified(r, "category") else r["category"].strip(),
            "sub_category": _clean(r["sub_category"]),
            "brand": _clean(r["brand"]),
            "mrp": None if _nullified(r, "mrp") or dec(r["mrp"]) is None else str(dec(r["mrp"])),
            "cost_price": None if _nullified(r, "cost_price") or dec(r["cost_price"]) is None
            else str(dec(r["cost_price"])),
            "unit": _clean(r["unit"]),
            "is_perishable": {"TRUE": "true", "FALSE": "false"}.get(r["is_perishable"].strip().upper()),
            "launch_date": None if _nullified(r, "launch_date") else _date(r["launch_date"]),
            "dq_status": r["dq_status"],
            "dq_issues": "; ".join(x for x in [r["failed_checks"], _check_names(r["warning_reason"])] if x),
            "source_file": r["_source_file"],
            "source_row_num": int(r["_row_num"]),
        })
    return pd.DataFrame(rows)


def dim_store(stores: pd.DataFrame) -> pd.DataFrame:
    """manager_name (employee personal data) is deliberately not published."""
    st = stores[stores["dq_status"] != "FAILED"]
    rows = []
    for _, r in st.iterrows():
        rows.append({
            "store_id": r["store_id"].strip(),
            "store_name": _clean(r["store_name"]),
            "zone": "Unknown" if _nullified(r, "zone") else r["zone"].strip().title(),
            "area": _clean(r["area"]),
            "pincode": _clean(r["pincode"]),
            "open_date": None if _nullified(r, "open_date") else _date(r["open_date"]),
            "capacity_orders_per_day": None if _nullified(r, "capacity_orders_per_day")
            else int(float(r["capacity_orders_per_day"])),
            "dq_status": r["dq_status"],
            "dq_issues": "; ".join(x for x in [r["failed_checks"], _check_names(r["warning_reason"])] if x),
            "source_file": r["_source_file"],
            "source_row_num": int(r["_row_num"]),
        })
    return pd.DataFrame(rows)


def latest_order_versions(*validated: pd.DataFrame) -> pd.DataFrame:
    """Combine base and increment rows; the newest batch wins per order_id.

    Extra copies of a duplicated order_id are not versions of the order and are
    ignored here (they are quarantined). If the newest version failed a critical
    check, the order is reported as rejected so the warehouse drops its older,
    previously valid version instead of serving stale revenue.
    """
    frames = []
    for i, df in enumerate(validated):
        if df is None or df.empty:
            continue
        x = df.copy()
        x["_batch_seq"] = i
        frames.append(x)
    allv = pd.concat(frames, ignore_index=True)
    is_extra_copy = allv["failed_checks"].str.split(";").map(lambda l: "orders_order_id_unique" in l)
    allv = allv[~is_extra_copy & (s(allv, "order_id") != "")]
    allv["order_id"] = s(allv, "order_id")
    allv = allv.sort_values(["order_id", "_cutoff", "_batch_seq", "_row_num"])
    return allv.groupby("order_id", as_index=False).tail(1)


def fact_orders(latest: pd.DataFrame, dim_prod: pd.DataFrame, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (fact rows to upsert, rejected latest versions to remove from the fact)."""
    ok = latest[latest["dq_status"].isin(LOADABLE)]
    rejected = latest[~latest["dq_status"].isin(LOADABLE)][["order_id", "_cutoff", "failed_checks"]]
    cost = dict(zip(dim_prod["product_id"], dim_prod["cost_price"]))
    rows = []
    for _, r in ok.iterrows():
        status = r["order_status"].strip().upper()
        delivered = status == "DELIVERED"
        qty = int(r["quantity"].strip())
        amount = dec(r["order_amount"])
        dm = r["delivery_minutes"].strip()
        dm_i = int(float(dm)) if dm else None
        c = cost.get(r["product_id"].strip())
        cogs = (Decimal(qty) * Decimal(c)) if (delivered and c is not None) else None
        net = amount if delivered else Decimal("0")
        if delivered:
            margin = (net - cogs) if cogs is not None else None
        else:
            margin = Decimal("0")
        ots = pd.to_datetime(r["order_ts"].strip(), format=TS_FMT)
        rows.append({
            "order_id": r["order_id"],
            "order_ts": ots.strftime("%Y-%m-%d %H:%M:%S"),
            "order_date": ots.strftime("%Y-%m-%d"),
            "customer_id": r["customer_id"].strip(),
            "store_id": r["store_id"].strip(),
            "product_id": r["product_id"].strip(),
            "quantity": qty,
            "unit_price": str(dec(r["unit_price"])),
            "discount_pct": str(dec(r["discount_pct"])),
            "order_amount": str(amount),
            "payment_mode": map_payment_mode(pd.Series([r["payment_mode"]])).iloc[0],
            "order_status": status,
            "channel": _clean(r["channel"]),
            "delivery_minutes": dm_i,
            "net_revenue": str(net),
            "cost_amount": None if cogs is None else str(cogs),
            "gross_margin": None if margin is None else str(margin),
            "within_15_min": None if not delivered or dm_i is None else ("true" if dm_i <= WITHIN_SLA_MINUTES else "false"),
            "dq_status": r["dq_status"],
            "dq_warnings": _check_names(r["warning_reason"]),
            "source_file": r["_source_file"],
            "source_row_num": int(r["_row_num"]),
            "batch_cutoff": r["_cutoff"],
            "loaded_run_id": run_id,
        })
    return pd.DataFrame(rows), rejected
