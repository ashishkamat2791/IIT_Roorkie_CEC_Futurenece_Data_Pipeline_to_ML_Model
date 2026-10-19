"""
scripts/experiments.py
----------------------
Part F observability experiments. Every experiment works on a disposable copy:

  include/experiments/<exp>/data/                  modified copy of the six CSVs
  include/experiments/<exp>/<etl|elt>/warehouse.duckdb   isolated warehouse
  include/experiments/<exp>/<etl|elt>/quarantine/        isolated quarantine

so the production warehouse (include/warehouse_ashish_kamat.duckdb) and the
source files in include/data/nammamart/ are never modified.

  prepare <exp>      build the isolated input for one experiment
  conf <exp> <p>     print the Airflow --conf JSON for pipeline p (etl|elt)
  evidence <exp> <p> print dq_results / pipeline_runs / quarantine evidence
  restore <exp>      restore the experiment's data copy to the originals
  verify             prove include/data/nammamart is byte-identical to the originals
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ORIG = ROOT / "assignment" / "nammamart_dataset"
PROD = ROOT / "include" / "data" / "nammamart"
EXP = ROOT / "include" / "experiments"
FILES = ["orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv",
         "orders_2026-10-01_increment.csv"]


def md5(p: Path) -> str:
    return hashlib.md5(p.read_bytes()).hexdigest()


def fresh_copy(exp: str) -> Path:
    d = EXP / exp / "data"
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    for f in FILES:
        shutil.copy2(ORIG / f, d / f)
    return d


def read(p: Path) -> pd.DataFrame:
    return pd.read_csv(p, dtype=str, keep_default_na=False)


def prepare(exp: str) -> None:
    d = fresh_copy(exp)
    o = d / "orders.csv"
    run_name, exp = exp, exp.removesuffix("_v2")   # _v2 = same input, re-run after a fix
    if exp == "exp04_missing_file":
        (d / "products.csv").rename(d / "products.csv.renamed")
    elif exp == "exp05_bad_rows":
        df = read(o)
        base = df[df["order_id"] == "ORD000001"].iloc[0].to_dict()
        bad = [
            {**base, "order_id": "ORD900001", "customer_id": ""},                                   # null customer
            {**base, "order_id": "ORD900002", "quantity": "-2", "order_amount": str(-2 * float(base["unit_price"]) * (1 - float(base["discount_pct"]) / 100))},
            {**base, "order_id": "ORD900003", "store_id": "S42"},                                   # unknown store
            {**base, "order_id": "ORD900004", "order_status": "SHIPPED"},                           # invalid status
            {**base, "order_id": "ORD900005", "order_ts": "2027-03-15 10:00:00"},                   # 2027 timestamp
        ]
        pd.concat([df, pd.DataFrame(bad)], ignore_index=True).to_csv(o, index=False)
    elif exp == "exp06_schema_change":
        read(o).rename(columns={"order_amount": "total"}).to_csv(o, index=False)
    elif exp == "exp07_volume_drop":
        read(o).head(50).to_csv(o, index=False)
    elif exp in ("exp08_drift", "exp08b_drift_consistent"):
        df = read(o)
        m = df["store_id"] == "S05"
        df.loc[m, "unit_price"] = (pd.to_numeric(df.loc[m, "unit_price"]) * 100).map(lambda x: f"{x:.1f}")
        if exp == "exp08b_drift_consistent":   # also recompute order_amount so the formula check passes
            q, up, dp = (pd.to_numeric(df.loc[m, c]) for c in ["quantity", "unit_price", "discount_pct"])
            df.loc[m, "order_amount"] = (q * up * (1 - dp / 100)).round(2).map(lambda x: f"{x}")
        df.to_csv(o, index=False)
    elif exp == "exp09_freshness":
        df = read(o)
        ts = pd.to_datetime(df["order_ts"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
        shifted = (ts - pd.Timedelta(days=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
        df["order_ts"] = shifted.where(ts.notna(), df["order_ts"])   # unparseable values stay as they were
        df.to_csv(o, index=False)
    elif exp == "exp11_scd2_day2":
        # Next-day CRM export: three tier changes and one deactivation (bonus SCD Type 2 test).
        c = read(d / "customers.csv")
        changes = {"C00136": ("loyalty_tier", "Gold"), "C00236": ("loyalty_tier", "Gold"),
                   "C00071": ("loyalty_tier", "Gold"), "C00001": ("is_active", "N")}
        for cid, (col, val) in changes.items():
            c.loc[c["customer_id"] == cid, col] = val
        c.to_csv(d / "customers.csv", index=False)
    elif exp == "exp00_control":
        pass
    else:
        raise SystemExit(f"unknown experiment {exp}")
    for p in ("etl", "elt"):
        shutil.rmtree(EXP / run_name / p, ignore_errors=True)
        (EXP / run_name / p).mkdir(parents=True)
    print(f"prepared {d}")
    for f in sorted(d.iterdir()):
        orig = ORIG / f.name
        state = "unchanged" if orig.exists() and md5(orig) == md5(f) else "MODIFIED/RENAMED"
        print(f"  {f.name:40s} {state}")


def conf(exp: str, p: str, **extra) -> dict:
    c = {"data_dir": f"include/experiments/{exp}/data",
         "warehouse_path": f"include/experiments/{exp}/{p}/warehouse.duckdb",
         "quarantine_dir": f"include/experiments/{exp}/{p}/quarantine"}
    c.update(extra)
    return c


def evidence(exp: str, p: str) -> None:
    wh = EXP / exp / p / "warehouse.duckdb"
    con = duckdb.connect(str(wh), read_only=True)
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.max_colwidth", 120)
    print(f"== {exp} / {p} ({wh.relative_to(ROOT)})")
    print(con.execute("SELECT run_id, status, rows_extracted, rows_loaded, rows_quarantined, failed_tasks, "
                      "warnings, end_time - start_time AS duration FROM pipeline_runs").df().to_string(index=False))
    print("\n-- dq_results that did not pass")
    print(con.execute("SELECT source_file, check_name, dimension, severity, rows_checked, rows_failed, status, details "
                      "FROM dq_results WHERE status <> 'PASS' ORDER BY source_file, status, check_name").df().to_string(index=False))
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    fact = "etl_fact_orders" if p == "etl" else "int_fact_orders"
    if fact in tables:
        print(f"\n-- {fact}: rows / September delivered revenue by store S05 vs all")
        print(con.execute(f"""SELECT count(*) AS fact_rows,
                 sum(net_revenue) FILTER (WHERE order_status='DELIVERED') AS net_revenue,
                 sum(net_revenue) FILTER (WHERE order_status='DELIVERED' AND store_id='S05') AS s05_revenue,
                 count(*) FILTER (WHERE store_id='S05') AS s05_orders,
                 max(order_ts) AS latest_order_ts FROM {fact}""").df().to_string(index=False))
    for t in ("etl_payment_reconciliation", "mart_payment_reconciliation"):
        if t in tables:
            print(f"\n-- {t} by reason")
            print(con.execute(f"SELECT reason, count(*) n, sum(unreconciled_amount) amount FROM {t} GROUP BY 1 ORDER BY 1").df().to_string(index=False))
    if p == "elt" and "stg_orders_versions" in tables:
        print("\n-- stg_orders_versions rows ORD9000xx")
        print(con.execute("SELECT order_id, dq_status, failure_reason FROM stg_orders_versions "
                          "WHERE order_id LIKE 'ORD9000%' ORDER BY 1").df().to_string(index=False))
    q = EXP / exp / p / "quarantine"
    for f in sorted(q.glob("orders_failed_*.csv")):
        df = read(f)
        print(f"\n-- quarantine {f.relative_to(ROOT)}: {len(df)} rows")
        sel = df[df["order_id"].str.startswith("ORD9000")]
        if len(sel):
            print(sel[["order_id", "source_row_num", "failed_checks", "failure_reason"]].to_string(index=False))


def restore(exp: str) -> None:
    d = EXP / exp / "data"
    for f in FILES:
        shutil.copy2(ORIG / f, d / f)
    for extra in d.glob("*.renamed"):
        extra.unlink()
    ok = all(md5(ORIG / f) == md5(d / f) for f in FILES) and len(list(d.iterdir())) == len(FILES)
    print(f"restored {d}: {'byte-identical to originals' if ok else 'MISMATCH'}")


def verify() -> int:
    bad = [f for f in FILES if md5(ORIG / f) != md5(PROD / f)]
    print("include/data/nammamart vs assignment/nammamart_dataset:",
          "all 6 files byte-identical" if not bad else f"DIFFER: {bad}")
    for f in FILES:
        print(f"  {md5(PROD / f)}  {f}")
    return 1 if bad else 0


if __name__ == "__main__":
    cmd, *args = sys.argv[1:]
    if cmd == "prepare":
        prepare(args[0])
    elif cmd == "conf":
        extra = json.loads(args[2]) if len(args) > 2 else {}
        print(json.dumps(conf(args[0], args[1], **extra)))
    elif cmd == "evidence":
        evidence(args[0], args[1])
    elif cmd == "restore":
        restore(args[0])
    elif cmd == "verify":
        sys.exit(verify())
