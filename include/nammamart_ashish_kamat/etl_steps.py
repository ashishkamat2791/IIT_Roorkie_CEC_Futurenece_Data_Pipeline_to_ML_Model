"""
ETL pipeline steps. Each function is one Airflow task in
dags/nammamart_etl_ashish_kamat.py and can also be called directly in tests.

Data moves between tasks as run-scoped CSV work files under include/tmp/;
XCom carries only small metadata dicts.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import etl_load, transform, validate
from .config import SOURCES, RunConfig, increment_cutoff
from .extract import read_work, work_path, write_work
from .masking import mask_email, mask_name, mask_phone
from .warehouse import connect, ensure_audit_tables, run_sql_file, write_dq_results

log = logging.getLogger(__name__)

PII_MASKERS = {"full_name": mask_name, "email": mask_email, "phone": mask_phone, "manager_name": mask_name}


def now() -> str:
    return datetime.now().isoformat(sep=" ", timespec="seconds")


def orders_baseline(con, dag_id: str) -> int | None:
    """Median orders.csv row count of the last 7 runs whose volume check passed."""
    row = con.execute("""
        SELECT median(rows_checked) FROM (
            SELECT rows_checked FROM dq_results
            WHERE dag_id = ? AND check_name = 'orders_volume_vs_baseline' AND status = 'PASS'
            ORDER BY checked_at DESC LIMIT 7)
    """, [dag_id]).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def start_run(cfg: RunConfig) -> dict:
    with connect(cfg.warehouse) as con:
        ensure_audit_tables(con)
        run_sql_file(con, "etl/ddl_etl.sql")
        if cfg.orders_baseline is None:
            cfg.orders_baseline = orders_baseline(con, cfg.dag_id)
    cfg.run_work_dir.mkdir(parents=True, exist_ok=True)
    out = cfg.to_dict()
    out["start_time"] = now()
    log.info("[start] %s", json.dumps(out))
    return out


def _dump_results(cfg: RunConfig, name: str, results: list[dict]) -> str:
    p = cfg.run_work_dir / f"dq_{name}.json"
    p.write_text(json.dumps(results, default=str))
    return str(p)


def _summary(df: pd.DataFrame) -> dict:
    return {k: int(v) for k, v in df["dq_status"].value_counts().items()}


def validate_master(cfg: RunConfig, source: str, extract_meta: dict) -> dict:
    df = read_work(extract_meta["work_file"])
    fn = {"customers": validate.validate_customers, "products": validate.validate_products,
          "stores": validate.validate_stores}[source]
    out, results = fn(df, source_file=extract_meta["file"], run_id=cfg.run_id, dag_id=cfg.dag_id, cutoff=cfg.as_of)
    path = write_work(out, work_path(cfg, f"validated_{source}"))
    log.info("[validate] %s %s", source, _summary(out))
    return {"source": source, "file": extract_meta["file"], "rows": len(out), "work_file": path,
            "dq_file": _dump_results(cfg, source, results), "summary": _summary(out)}


def validate_orders(cfg: RunConfig, orders_meta: dict, inc_meta: dict, masters: dict[str, dict]) -> dict:
    m = validate.kept_masters(read_work(masters["customers"]["work_file"]),
                              read_work(masters["products"]["work_file"]),
                              read_work(masters["stores"]["work_file"]))
    base = read_work(orders_meta["work_file"])
    out, results = validate.validate_orders(
        base, source_file=orders_meta["file"], run_id=cfg.run_id, dag_id=cfg.dag_id, cutoff=cfg.as_of,
        baseline=cfg.orders_baseline, expect_reduced_volume=cfg.expect_reduced_volume, **m)
    meta = {"source": "orders", "file": orders_meta["file"], "rows": len(out),
            "work_file": write_work(out, work_path(cfg, "validated_orders")), "summary": _summary(out)}
    if inc_meta.get("applied"):
        inc = read_work(inc_meta["work_file"])
        inc_out, inc_results = validate.validate_orders(
            inc, source_file=inc_meta["file"], run_id=cfg.run_id, dag_id=cfg.dag_id,
            cutoff=increment_cutoff(inc_meta["file"]), baseline=None, is_increment=True, **m)
        results += inc_results
        meta["increment"] = {"file": inc_meta["file"], "rows": len(inc_out), "summary": _summary(inc_out),
                             "work_file": write_work(inc_out, work_path(cfg, "validated_orders_increment"))}
    meta["dq_file"] = _dump_results(cfg, "orders", results)
    log.info("[validate] orders %s increment %s", meta["summary"], meta.get("increment", {}).get("summary"))
    return meta


def _order_versions(orders_meta: dict) -> pd.DataFrame:
    frames = [read_work(orders_meta["work_file"])]
    if "increment" in orders_meta:
        frames.append(read_work(orders_meta["increment"]["work_file"]))
    return frames


def validate_payments(cfg: RunConfig, payments_meta: dict, orders_meta: dict) -> dict:
    frames = _order_versions(orders_meta)
    src = pd.concat(frames, ignore_index=True)
    src["order_id"] = src["order_id"].str.strip()
    # Latest source version per order_id (increment overrides base; extra duplicate copies are not
    # versions); used for the payment FK and amount checks.
    src = src[~src["failed_checks"].str.split(";").map(lambda l: "orders_order_id_unique" in l)]
    src = src.sort_values(["_cutoff", "_row_num"]).drop_duplicates("order_id", keep="last")
    df = read_work(payments_meta["work_file"])
    out, results = validate.validate_payments(df, source_file=payments_meta["file"], run_id=cfg.run_id,
                                              dag_id=cfg.dag_id, cutoff=cfg.as_of, orders_src=src)
    path = write_work(out, work_path(cfg, "validated_payments"))
    log.info("[validate] payments %s", _summary(out))
    return {"source": "payments", "file": payments_meta["file"], "rows": len(out), "work_file": path,
            "dq_file": _dump_results(cfg, "payments", results), "summary": _summary(out)}


def quarantine(cfg: RunConfig, validated: list[dict]) -> dict:
    """Write every row that failed a critical check to include/quarantine/ashish_kamat/ and record dq_results."""
    cfg.quarantine_dir.mkdir(parents=True, exist_ok=True)
    files, rejected, dq_rows, total = {}, [], [], 0
    items = []
    for meta in validated:
        items.append((meta["source"], meta["file"], meta["work_file"]))
        if "increment" in meta:
            items.append(("orders_increment", meta["increment"]["file"], meta["increment"]["work_file"]))
        dq_rows += json.loads(Path(meta["dq_file"]).read_text())
    for source, file_name, wf in items:
        df = read_work(wf)
        bad = df[df["dq_status"].isin(["FAILED", "FAILED_ATTRIBUTE"])].copy()
        stem = Path(file_name).stem
        qpath = cfg.quarantine_dir / f"{stem}_failed_{cfg.safe_run_id}.csv"
        if bad.empty:
            if qpath.exists():
                qpath.unlink()  # a retry after a fix must not leave a stale file behind
            continue
        # Raw PII stays in the restricted source / raw layer; the quarantine copy is masked
        # and points back to the exact source row via source_file + source_row_num.
        for col, fn in PII_MASKERS.items():
            if col in bad.columns:
                bad[col] = bad[col].map(fn)
        bad.insert(0, "run_id", cfg.run_id)
        bad = bad.rename(columns={"_row_num": "source_row_num", "_source_file": "source_file"})
        keep = [c for c in bad.columns if c not in ("warning_reason", "nullify_columns", "_cutoff")]
        bad[keep].to_csv(qpath, index=False)
        files[file_name] = str(qpath)
        total += len(bad)
        exploded = bad.assign(check=bad["failed_checks"].str.split(";")).explode("check")
        for chk, n in exploded.groupby("check").size().items():
            rejected.append(dict(source_file=file_name, check_name=chk, severity="Critical", rows=int(n),
                                 quarantine_file=qpath.name))
        rejected.append(dict(source_file=file_name, check_name="ALL (distinct rows)", severity="Critical",
                             rows=len(bad), quarantine_file=qpath.name))
        log.info("[quarantine] %s -> %s (%s rows)", file_name, qpath, len(bad))
    with connect(cfg.warehouse) as con:
        write_dq_results(con, dq_rows)
        etl_load.write_rejected_rows(con, rejected, cfg.run_id)
    fails = [r for r in dq_rows if r["status"] in ("FAIL", "WARN")]
    for r in fails:
        log.info("[dq] %-45s %-12s %-8s %s/%s %s", r["check_name"], r["dimension"], r["status"],
                 r["rows_failed"], r["rows_checked"], r.get("details", ""))
    warnings = [f"{r['check_name']}: {r['details']}" for r in dq_rows
                if r["status"] == "WARN" and r["population"].startswith(("dataset", "orders file", "stores", "store-days"))]
    return {"rows_quarantined": total, "files": files, "dq_checks": len(dq_rows),
            "dataset_warnings": warnings}


def transform_step(cfg: RunConfig, validated: dict[str, dict]) -> dict:
    cust = read_work(validated["customers"]["work_file"])
    prod = read_work(validated["products"]["work_file"])
    stor = read_work(validated["stores"]["work_file"])
    dc, dp, ds = transform.dim_customer(cust), transform.dim_product(prod), transform.dim_store(stor)
    om = validated["orders"]
    latest = transform.latest_order_versions(*[read_work(f) for f in
                                               [om["work_file"]] + ([om["increment"]["work_file"]] if "increment" in om else [])])
    fact, rejected = transform.fact_orders(latest, dp, cfg.run_id)
    out = {}
    for name, df in [("dim_customer", dc), ("dim_product", dp), ("dim_store", ds), ("fact_orders", fact),
                     ("fact_rejected_versions", rejected)]:
        out[name] = {"rows": len(df), "work_file": write_work(df, work_path(cfg, f"transformed_{name}"))}
    log.info("[transform] %s", {k: v["rows"] for k, v in out.items()})
    return out


def load_step(cfg: RunConfig, transformed: dict) -> dict:
    def rd(name):
        df = pd.read_csv(transformed[name]["work_file"], dtype=str, keep_default_na=False, na_values=[""])
        return df.astype(object).where(df.notna(), None)
    dims = {"etl_dim_customer": rd("dim_customer"), "etl_dim_product": rd("dim_product"),
            "etl_dim_store": rd("dim_store")}
    fact = rd("fact_orders")
    rej = rd("fact_rejected_versions")
    with connect(cfg.warehouse) as con:
        dim_counts = etl_load.load_dimensions(con, dims, cfg.run_id)
        fact_stats = etl_load.load_fact(con, fact, rej, cfg.run_id)
    return {"dimensions": dim_counts, "fact": fact_stats, "rows_loaded": int(len(fact)) + sum(dim_counts.values())}


def reconcile_step(cfg: RunConfig, payments_meta: dict, orders_meta: dict) -> dict:
    pays = read_work(payments_meta["work_file"])
    src_ids = set()
    for f in [orders_meta["work_file"]] + ([orders_meta["increment"]["work_file"]] if "increment" in orders_meta else []):
        src_ids |= set(read_work(f)["order_id"].str.strip())
    with connect(cfg.warehouse) as con:
        fact = con.execute("SELECT order_id, order_status, CAST(order_amount AS VARCHAR) AS order_amount, "
                           "payment_mode FROM etl_fact_orders").df()
        recon = etl_load.reconcile(fact, pays, src_ids, cfg.run_id)
        n = etl_load.write_reconciliation(con, recon)
        summary = con.execute("SELECT reason, count(*) AS n, sum(unreconciled_amount) AS amount "
                              "FROM etl_payment_reconciliation GROUP BY reason ORDER BY reason").fetchall()
    log.info("[reconcile] %s mismatches: %s", n, summary)
    return {"mismatches": n, "by_reason": {r: {"rows": c, "amount": float(a)} for r, c, a in summary}}


def report_step(cfg: RunConfig) -> dict:
    with connect(cfg.warehouse) as con:
        counts = etl_load.build_reports(con, cfg.run_id)
        for t in ["etl_daily_zone_revenue", "etl_category_margin", "etl_delivery_sla",
                  "etl_payment_mode_revenue", "etl_top_customers"]:
            df = con.execute(f"SELECT * FROM {t}").df()
            shown = df if t != "etl_daily_zone_revenue" else df.tail(20)
            log.info("[report] %s (%s rows)%s\n%s", t, len(df), " - last 20 shown" if len(df) > 20 else "",
                     shown.to_string(index=False))
        rej = con.execute("SELECT source_file, check_name, rows_rejected FROM etl_rejected_rows "
                          "WHERE run_id = ? ORDER BY source_file, rows_rejected DESC", [cfg.run_id]).df()
        log.info("[report] rejected rows this run\n%s", rej.to_string(index=False))
        totals = con.execute("""SELECT count(*) FILTER (WHERE order_status='DELIVERED'),
                                       sum(net_revenue), sum(gross_margin)
                                FROM etl_fact_orders WHERE order_date < DATE '2026-10-01'""").fetchone()
    counts["september_delivered_orders"] = totals[0]
    counts["september_net_revenue"] = float(totals[1] or 0)
    counts["september_gross_margin"] = float(totals[2] or 0)
    log.info("[report] September totals %s", counts)
    return counts


def cleanup_work_dir(cfg: RunConfig) -> None:
    """Work files contain raw extracts (including customer PII); delete them once a run succeeded.
    Failed runs keep them so a cleared/retried task can resume."""
    import shutil
    if cfg.run_work_dir.exists():
        shutil.rmtree(cfg.run_work_dir)
        log.info("[cleanup] removed %s", cfg.run_work_dir)


def customer_scd2_step(cfg: RunConfig, transformed: dict) -> dict:
    """Bonus: maintain the SCD Type 2 customer dimension (loyalty-tier history)."""
    from .scd2 import load_customer_scd2
    snap = pd.read_csv(transformed["dim_customer"]["work_file"], dtype=str, keep_default_na=False, na_values=[""])
    snap = snap.astype(object).where(snap.notna(), None)
    with connect(cfg.warehouse) as con:
        return load_customer_scd2(con, snap, cfg.as_of, cfg.run_id)
