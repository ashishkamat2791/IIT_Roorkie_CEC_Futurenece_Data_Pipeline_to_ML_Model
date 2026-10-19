"""
scripts/profile_nammamart.py
----------------------------
Reproducible Part 0 profiling of the six NammaMart CSV exports.

Reads every file as text (nothing is coerced on read), profiles each column,
checks the four relationships and every client business rule, and writes:

  docs/profiling/profile_report.md     human-readable profile
  docs/profiling/column_profile.csv    one row per file x column
  docs/profiling/data_issues_log.csv   one row per (file, row_key, column, problem)

PII rule: values of full_name, email and phone are never written to the
reports. Customer issues are identified by customer_id and a description of the
defect pattern only.

Run:  .venv/bin/python scripts/profile_nammamart.py
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "include" / "data" / "nammamart"
OUT_DIR = ROOT / "docs" / "profiling"

FILES = {
    "orders.csv": "order_id",
    "payments.csv": "payment_id",
    "customers.csv": "customer_id",
    "products.csv": "product_id",
    "stores.csv": "store_id",
    "orders_2026-10-01_increment.csv": "order_id",
}
PII_COLUMNS = {"full_name", "email", "phone", "manager_name"}
CATEGORICAL = {
    "order_status", "payment_mode", "channel", "payment_status", "loyalty_tier",
    "is_active", "city", "zone", "category", "sub_category", "unit",
    "is_perishable", "brand", "area", "quantity", "discount_pct",
    "capacity_orders_per_day",
}
NUMERIC = {
    "quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes",
    "amount_paid", "mrp", "cost_price", "capacity_orders_per_day",
}
DATES = {"order_ts", "payment_ts", "signup_date", "launch_date", "open_date"}
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"

ALLOWED_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
ALLOWED_PAYMENT_MODE = {"UPI", "Card", "Cash on Delivery", "Wallet"}
ALLOWED_PAYMENT_STATUS = {"SUCCESS", "FAILED", "REFUNDED"}
ALLOWED_TIER = {"Bronze", "Silver", "Gold", "Platinum"}
# The September snapshot is consumed by the 2026-10-01 09:00 IST run.
AS_OF = pd.Timestamp("2026-10-01 09:00:00")

issues: list[dict] = []


def log(file, key, column, problem, dimension, rule, severity, status="CONFIRMED"):
    issues.append(dict(file=file, row_key=key, column=column, problem=problem,
                       quality_dimension=dimension, proposed_rule=rule,
                       severity=severity, evidence=status))


def read(name: str) -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / name, dtype=str, keep_default_na=False)


def parse_ts(s: pd.Series, fmt: str) -> pd.Series:
    return pd.to_datetime(s, format=fmt, errors="coerce")


def profile_columns(name: str, df: pd.DataFrame) -> list[dict]:
    rows = []
    for col in df.columns:
        s = df[col]
        blank = s.str.strip().eq("") | s.str.upper().isin(["N/A", "NA", "NULL", "NONE"])
        rec = {
            "file": name, "column": col, "rows": len(s),
            "null_or_blank": int(blank.sum()),
            "null_pct": round(100 * blank.mean(), 2),
            "distinct": int(s.nunique()),
            "leading_trailing_ws": int((s != s.str.strip()).sum()),
            "inferred_type": "text", "min": "", "max": "", "mean": "",
            "p50": "", "unparseable": 0, "top_values": "",
        }
        if col in NUMERIC:
            num = pd.to_numeric(s.where(~blank), errors="coerce")
            rec.update(inferred_type="integer" if (num.dropna() % 1 == 0).all() else "decimal",
                       min=num.min(), max=num.max(), mean=round(num.mean(), 2),
                       p50=num.median(), unparseable=int((num.isna() & ~blank).sum()))
        elif col in DATES:
            fmt = TS_FORMAT if col.endswith("_ts") else DATE_FORMAT
            dt = parse_ts(s.where(~blank), fmt)
            rec.update(inferred_type="timestamp" if fmt == TS_FORMAT else "date",
                       min=str(dt.min()), max=str(dt.max()),
                       unparseable=int((dt.isna() & ~blank).sum()))
        if col in CATEGORICAL and col not in PII_COLUMNS:
            vc = s.value_counts()
            rec["top_values"] = "; ".join(f"{k!r}={v}" for k, v in vc.head(12).items())
        if col in PII_COLUMNS:
            rec["top_values"] = "(PII - values not reported)"
        rows.append(rec)
    return rows


def check_orders(name: str, o: pd.DataFrame, cust, prod, stores, is_increment=False):
    key = "order_id"
    dup = o[o[key].duplicated(keep="first")]
    for _, r in dup.iterrows():
        exact = (o[o[key] == r[key]].drop_duplicates().shape[0] == 1)
        log(name, r[key], key, f"duplicate order_id ({'exact duplicate row' if exact else 'conflicting values'})",
            "Uniqueness", "order_id unique; keep first exact copy, quarantine the extra copy", "Critical")
    ts = parse_ts(o["order_ts"], TS_FORMAT)
    for _, r in o[ts.isna()].iterrows():
        log(name, r[key], "order_ts", f"unparseable timestamp {r['order_ts']!r}",
            "Validity", "order_ts must parse as YYYY-MM-DD HH:MM:SS", "Critical")
    as_of = AS_OF + pd.Timedelta(days=1) if is_increment else AS_OF
    for _, r in o[ts > as_of].iterrows():
        log(name, r[key], "order_ts", f"future timestamp {r['order_ts']} (batch cut-off {as_of})",
            "Timeliness", "order_ts <= batch cut-off", "Critical")
    for _, r in o[o["customer_id"].str.strip() == ""].iterrows():
        log(name, r[key], "customer_id", "customer_id is blank", "Completeness",
            "customer_id not null", "Critical")
    for col, master, mkey in [("customer_id", cust, "customer_id"), ("product_id", prod, "product_id"),
                              ("store_id", stores, "store_id")]:
        bad = o[(o[col].str.strip() != "") & ~o[col].str.strip().isin(master[mkey].str.strip())]
        for _, r in bad.iterrows():
            log(name, r[key], col, f"{col} {r[col]!r} not found in master",
                "Consistency", f"{col} must exist in {mkey.split('_')[0]} master", "Critical")
    q = pd.to_numeric(o["quantity"], errors="coerce")
    up = pd.to_numeric(o["unit_price"], errors="coerce")
    dp = pd.to_numeric(o["discount_pct"], errors="coerce")
    amt = pd.to_numeric(o["order_amount"], errors="coerce")
    dm = pd.to_numeric(o["delivery_minutes"].replace("", None), errors="coerce")
    for _, r in o[q <= 0].iterrows():
        log(name, r[key], "quantity", f"non-positive quantity {r['quantity']} (status {r['order_status']})",
            "Accuracy", "quantity > 0", "Critical")
    for _, r in o[(dp < 0) | (dp > 100)].iterrows():
        log(name, r[key], "discount_pct", f"discount_pct {r['discount_pct']} outside 0-100",
            "Validity", "0 <= discount_pct <= 100", "Critical")
    exp = (q * up * (1 - dp / 100)).round(2)
    for i in o.index[(exp - amt).abs() > 0.01]:
        r = o.loc[i]
        log(name, r[key], "order_amount",
            f"order_amount {r['order_amount']} != quantity*unit_price*(1-discount/100) = {exp[i]:.2f}",
            "Accuracy", "order_amount = round(qty*unit_price*(1-discount_pct/100),2) +/-0.01", "Critical")
    status = o["order_status"].str.strip().str.upper()
    for _, r in o[~status.isin(ALLOWED_STATUS)].iterrows():
        log(name, r[key], "order_status", f"invalid order_status {r['order_status']!r}",
            "Validity", "order_status in DELIVERED/CANCELLED/RETURNED", "Critical")
    for _, r in o[status.isin(ALLOWED_STATUS) & (o["order_status"] != status)].iterrows():
        log(name, r[key], "order_status", f"non-canonical casing {r['order_status']!r}",
            "Consistency", "upper-case order_status during standardisation", "Warning")
    mode_map = {"upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
                "cod": "Cash on Delivery", "wallet": "Wallet"}
    mapped = o["payment_mode"].str.strip().str.lower().map(mode_map)
    for _, r in o[mapped.isna()].iterrows():
        log(name, r[key], "payment_mode", f"unmappable payment_mode {r['payment_mode']!r}",
            "Validity", "payment_mode in UPI/Card/Cash on Delivery/Wallet after mapping", "Critical")
    for _, r in o[mapped.notna() & (mapped != o["payment_mode"])].iterrows():
        log(name, r[key], "payment_mode", f"non-canonical payment_mode {r['payment_mode']!r} -> {mode_map[r['payment_mode'].strip().lower()]}",
            "Consistency", "trim + map payment_mode synonyms", "Warning")
    delivered = status == "DELIVERED"
    for _, r in o[delivered & dm.isna()].iterrows():
        log(name, r[key], "delivery_minutes", "DELIVERED order without delivery_minutes",
            "Completeness", "DELIVERED orders must have delivery_minutes", "Critical")
    for _, r in o[dm >= 120].iterrows():
        log(name, r[key], "delivery_minutes", f"delivery_minutes {r['delivery_minutes']} (>= 120, hours not minutes)",
            "Accuracy", "delivery_minutes < 120", "Critical")
    for _, r in o[(dm < 120) & ((dm < 8) | (dm > 35))].iterrows():
        log(name, r[key], "delivery_minutes", f"delivery_minutes {r['delivery_minutes']} outside normal 8-35",
            "Accuracy", "flag delivery outside 8-35 minutes", "Warning")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dfs = {n: read(n) for n in FILES}
    o, p, c, pr, s = (dfs[n] for n in ["orders.csv", "payments.csv", "customers.csv",
                                        "products.csv", "stores.csv"])
    inc = dfs["orders_2026-10-01_increment.csv"]

    col_rows = []
    for n, df in dfs.items():
        col_rows += profile_columns(n, df)
    colprof = pd.DataFrame(col_rows)
    colprof.to_csv(OUT_DIR / "column_profile.csv", index=False)

    # ---- orders + increment
    check_orders("orders.csv", o, c, pr, s)
    check_orders("orders_2026-10-01_increment.csv", inc, c, pr, s, is_increment=True)

    # ---- payments
    n = "payments.csv"
    for _, r in p[p["payment_id"].duplicated(keep="first")].iterrows():
        log(n, r["payment_id"], "payment_id", "duplicate payment_id (exact duplicate row)", "Uniqueness",
            "payment_id unique; quarantine extra copy", "Critical")
    for _, r in p[~p["order_id"].isin(o["order_id"])].iterrows():
        log(n, r["payment_id"], "order_id", f"orphan payment: order_id {r['order_id']} not in orders",
            "Consistency", "payments.order_id must exist in orders", "Critical")
    pts = parse_ts(p["payment_ts"], TS_FORMAT)
    for _, r in p[pts > AS_OF].iterrows():
        log(n, r["payment_id"], "payment_ts", f"future payment_ts {r['payment_ts']}", "Timeliness",
            "payment_ts <= batch cut-off", "Critical")
    paid = pd.to_numeric(p["amount_paid"], errors="coerce")
    for _, r in p[(paid <= 0) & (p["payment_status"] == "SUCCESS")].iterrows():
        log(n, r["payment_id"], "amount_paid", f"SUCCESS payment with non-positive amount {r['amount_paid']}",
            "Accuracy", "SUCCESS amount_paid > 0", "Critical")
    od = o.drop_duplicates("order_id").set_index("order_id")
    succ = p[(p["payment_status"] == "SUCCESS") & p["order_id"].isin(od.index)]
    oamt = pd.to_numeric(od.loc[succ["order_id"], "order_amount"]).values
    for (_, r), ea in zip(succ.iterrows(), oamt):
        if abs(float(r["amount_paid"]) - ea) > 0.01:
            log(n, r["payment_id"], "amount_paid",
                f"SUCCESS amount_paid {r['amount_paid']} != order_amount {ea} for {r['order_id']}",
                "Consistency", "SUCCESS amount_paid = order_amount (reconciliation)", "Critical")
    mode_map = {"upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
                "cod": "Cash on Delivery", "wallet": "Wallet"}
    mapped = p["payment_mode"].str.strip().str.lower().map(mode_map)
    for _, r in p[mapped.isna()].iterrows():
        log(n, r["payment_id"], "payment_mode", f"unmappable payment_mode {r['payment_mode']!r}",
            "Validity", "payment_mode allowed list after mapping", "Critical")
    for _, r in p[mapped.notna() & (mapped != p["payment_mode"])].iterrows():
        log(n, r["payment_id"], "payment_mode", f"non-canonical payment_mode {r['payment_mode']!r}",
            "Consistency", "trim + map payment_mode synonyms", "Warning")
    nogw = p[(p["gateway_ref"].str.strip() == "")]
    log(n, f"{len(nogw)} rows (all payment_mode=Cash on Delivery: "
           f"{(nogw['payment_mode'] == 'Cash on Delivery').all()})", "gateway_ref",
        "gateway_ref blank", "Completeness",
        "gateway_ref required for non-COD payments only", "Warning")
    nopay = od[~od.index.isin(p["order_id"]) & (od["order_status"] == "DELIVERED")]
    for oid in nopay.index:
        log("orders.csv / payments.csv", oid, "order_id", "DELIVERED order has no payment record",
            "Consistency", "every DELIVERED order has a SUCCESS payment (reconciliation)", "Warning")

    # ---- customers (no PII values written)
    n = "customers.csv"
    for _, r in c[c["customer_id"].duplicated(keep="first")].iterrows():
        g = c[c["customer_id"] == r["customer_id"]]
        diff = [col for col in c.columns if g[col].nunique() > 1]
        log(n, r["customer_id"], "customer_id", f"duplicate customer_id; conflicting columns: {', '.join(diff)}",
            "Uniqueness", "one row per customer_id; keep first file occurrence, quarantine the other", "Critical")
    em_ok = c["email"].str.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
    for _, r in c[~em_ok].iterrows():
        what = "email placeholder 'N/A'" if r["email"] == "N/A" else (
            "email missing '@'" if "@" not in r["email"] else "email domain has no TLD")
        log(n, r["customer_id"], "email", what, "Validity", "email matches name@domain.tld", "Warning")
    for _, r in c[~c["phone"].str.fullmatch(r"[6-9]\d{9}")].iterrows():
        log(n, r["customer_id"], "phone", f"phone has {len(r['phone'])} digits (expected 10)",
            "Validity", "phone is 10 digits starting 6-9", "Warning")
    for _, r in c[~c["pincode"].str.fullmatch(r"56\d{4}")].iterrows():
        log(n, r["customer_id"], "pincode", f"invalid pincode {r['pincode']!r}", "Validity",
            "pincode is 6 digits starting 56 (Bengaluru)", "Warning")
    for _, r in c[c["city"].str.strip() == ""].iterrows():
        log(n, r["customer_id"], "city", "city blank", "Completeness", "default city from area/pincode or flag", "Warning")
    sd = parse_ts(c["signup_date"], DATE_FORMAT)
    for _, r in c[sd > AS_OF].iterrows():
        log(n, r["customer_id"], "signup_date", f"future signup_date {r['signup_date']}", "Timeliness",
            "signup_date <= batch cut-off", "Critical")
    for _, r in c[~c["loyalty_tier"].isin(ALLOWED_TIER)].iterrows():
        log(n, r["customer_id"], "loyalty_tier", f"non-canonical casing {r['loyalty_tier']!r}",
            "Consistency", "title-case loyalty_tier", "Warning")
    ap = c.drop_duplicates("customer_id").groupby("area")["pincode"].nunique()
    for area in ap[ap > 1].index:
        log(n, f"area={area}", "pincode", f"area maps to {ap[area]} different pincodes",
            "Consistency", "area->pincode should be 1:1 (incl. invalid pincodes)", "Warning")

    # ---- products
    n = "products.csv"
    for _, r in pr[pr["product_id"].duplicated(keep=False)].iterrows():
        if r["product_name"].endswith("(old)"):
            log(n, r["product_id"], "product_id", f"duplicate product_id; superseded row '{r['product_name']}' mrp {r['mrp']}",
                "Uniqueness", "one row per product_id; keep current row, quarantine '(old)' row", "Critical")
    mrp = pd.to_numeric(pr["mrp"], errors="coerce")
    cp = pd.to_numeric(pr["cost_price"], errors="coerce")
    for _, r in pr[mrp <= 0].iterrows():
        log(n, r["product_id"], "mrp", f"non-positive mrp {r['mrp']} (orders sell at {abs(float(r['mrp']))})",
            "Accuracy", "mrp > 0", "Critical")
    for _, r in pr[(mrp > 0) & (cp > mrp)].iterrows():
        log(n, r["product_id"], "cost_price", f"cost_price {r['cost_price']} > mrp {r['mrp']} (ratio {float(r['cost_price'])/float(r['mrp']):.2f})",
            "Accuracy", "cost_price <= mrp", "Critical")
    for _, r in pr[pr["category"].str.strip() == ""].iterrows():
        log(n, r["product_id"], "category", f"category blank (sub_category {r['sub_category']})",
            "Completeness", "category not null", "Critical")

    # ---- stores (manager_name not reported)
    n = "stores.csv"
    for _, r in s[s["zone"].str.strip() == ""].iterrows():
        log(n, r["store_id"], "zone", "zone blank", "Completeness", "zone in North/South/East/West", "Critical")
    capn = pd.to_numeric(s["capacity_orders_per_day"], errors="coerce")
    for _, r in s[capn <= 0].iterrows():
        log(n, r["store_id"], "capacity_orders_per_day", f"capacity_orders_per_day {r['capacity_orders_per_day']}",
            "Accuracy", "capacity_orders_per_day > 0", "Critical")
    unknown_store_ids = sorted(set(o["store_id"]) - set(s["store_id"]))
    log(n, ",".join(unknown_store_ids), "store_id", "store_ids used in orders but missing from store master",
        "Consistency", "store master must contain every operating store", "Critical")

    issues_df = pd.DataFrame(issues)
    issues_df.to_csv(OUT_DIR / "data_issues_log.csv", index=False)

    # ---- relationship summary
    rel = []
    for child, ccol, parent, pcol in [("orders.csv", "customer_id", "customers.csv", "customer_id"),
                                      ("orders.csv", "product_id", "products.csv", "product_id"),
                                      ("orders.csv", "store_id", "stores.csv", "store_id"),
                                      ("payments.csv", "order_id", "orders.csv", "order_id"),
                                      ("orders_2026-10-01_increment.csv", "customer_id", "customers.csv", "customer_id"),
                                      ("orders_2026-10-01_increment.csv", "product_id", "products.csv", "product_id"),
                                      ("orders_2026-10-01_increment.csv", "store_id", "stores.csv", "store_id")]:
        cv = dfs[child][ccol]
        nonblank = cv[cv.str.strip() != ""]
        orphan = nonblank[~nonblank.isin(dfs[parent][pcol])]
        rel.append(dict(relationship=f"{child}.{ccol} -> {parent}.{pcol}", child_rows=len(cv),
                        blank_keys=int((cv.str.strip() == "").sum()), orphan_rows=len(orphan),
                        orphan_keys=", ".join(sorted(orphan.unique()))))
    rel_df = pd.DataFrame(rel)

    od = o.drop_duplicates("order_id")
    inc_existing = inc[inc["order_id"].isin(o["order_id"])]
    inc_new = inc[~inc["order_id"].isin(o["order_id"])]
    j = inc_existing.merge(od, on="order_id", suffixes=("_inc", "_orig"))
    changed_cols = {col: int((j[f"{col}_inc"] != j[f"{col}_orig"]).sum()) for col in o.columns[1:]}

    with open(OUT_DIR / "profile_report.md", "w") as f:
        f.write("# NammaMart data profile\n\nGenerated by `scripts/profile_nammamart.py` from "
                "`include/data/nammamart/` (byte-identical copies of `assignment/nammamart_dataset/`). "
                f"Timeliness checks use the batch cut-off {AS_OF} IST (run that consumes the September file).\n\n")
        f.write("## Files\n\n| File | Rows | Columns | Distinct primary key | Duplicate key rows | Exact duplicate rows |\n|---|---|---|---|---|---|\n")
        for nme, k in FILES.items():
            df = dfs[nme]
            f.write(f"| {nme} | {len(df)} | {df.shape[1]} | {df[k].nunique()} | "
                    f"{int(df[k].duplicated().sum())} | {int(df.duplicated().sum())} |\n")
        f.write(f"\nTotal rows across six files: {sum(len(d) for d in dfs.values())}.\n\n")
        f.write("## Relationships\n\n" + rel_df.to_markdown(index=False) + "\n\n")
        f.write("## Increment file\n\n"
                f"- {len(inc)} rows: {len(inc_new)} new order_ids "
                f"({inc_new['order_id'].min()}..{inc_new['order_id'].max()}), {len(inc_existing)} existing order_ids.\n"
                f"- Columns changed on existing orders: { {k: v for k, v in changed_cols.items() if v} }\n"
                f"- Corrections: {', '.join(f'{r.order_id} {r.order_status_orig}->{r.order_status_inc}' for r in j.itertuples())}\n\n")
        f.write("## Business-rule summary\n\n")
        summary = issues_df.groupby(["file", "column", "problem" if False else "quality_dimension", "severity"]).size()
        f.write(issues_df.groupby(["file", "quality_dimension", "severity"]).size()
                .rename("issues").reset_index().to_markdown(index=False) + "\n\n")
        f.write("## Column profile\n\nNull = blank, `N/A`, `NA`, `NULL` or `NONE`.\n\n")
        cols = ["file", "column", "inferred_type", "null_or_blank", "null_pct", "distinct",
                "leading_trailing_ws", "unparseable", "min", "max", "mean", "p50", "top_values"]
        f.write(colprof[cols].to_markdown(index=False) + "\n")
    print(f"issues logged: {len(issues_df)}")
    print(issues_df.groupby(["file", "severity"]).size().to_string())


if __name__ == "__main__":
    main()
