"""
scripts/run_pipeline_local.py
-----------------------------
Runs the ETL or ELT step functions in the same order as the Airflow DAGs,
without Airflow. Used by the tests and for quick local reproduction; the
graded runs are the Airflow DAG runs.

  .venv/bin/python scripts/run_pipeline_local.py etl --run-id local_1 [--increment]
      [--warehouse include/tmp/x.duckdb] [--data-dir ...] [--quarantine-dir ...]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from include.nammamart_ashish_kamat import audit, elt_steps, etl_steps, extract  # noqa: E402
from include.nammamart_ashish_kamat.config import ELT_DAG_ID, ETL_DAG_ID, RunConfig  # noqa: E402

BASE = ["orders", "payments", "customers", "products", "stores"]


def run_etl(cfg: RunConfig) -> dict:
    st = etl_steps.start_run(cfg)
    cfg = RunConfig.from_dict({k: v for k, v in st.items() if k != "start_time"})
    states, out = {}, {"start_time": st["start_time"]}
    try:
        ex = {s: extract.extract_source(cfg, s) for s in BASE}
        inc = extract.extract_increment(cfg)
        v = {s: etl_steps.validate_master(cfg, s, ex[s]) for s in ["customers", "products", "stores"]}
        v["orders"] = etl_steps.validate_orders(cfg, ex["orders"], inc, v)
        v["payments"] = etl_steps.validate_payments(cfg, ex["payments"], v["orders"])
        q = etl_steps.quarantine(cfg, list(v.values()))
        tr = etl_steps.transform_step(cfg, v)
        ld = etl_steps.load_step(cfg, tr)
        out["scd2"] = etl_steps.customer_scd2_step(cfg, tr)
        rc = etl_steps.reconcile_step(cfg, v["payments"], v["orders"])
        rp = etl_steps.report_step(cfg)
        out.update(extract={k: x["rows"] for k, x in ex.items()}, increment=inc["rows"],
                   validate={k: x["summary"] for k, x in v.items()},
                   increment_validate=v["orders"].get("increment", {}).get("summary"),
                   quarantine=q, load=ld, reconcile=rc, report=rp)
        states["pipeline"] = "success"
    except Exception as exc:
        states["pipeline"] = "failed"
        out["error"] = f"{type(exc).__name__}: {exc}"
    rows_ex = sum(out.get("extract", {}).values()) + out.get("increment", 0) if "extract" in out else None
    out["audit"] = audit.write_pipeline_run(
        cfg.warehouse, run_id=cfg.run_id, dag_id=cfg.dag_id, start_time=out["start_time"],
        rows_extracted=rows_ex, rows_loaded=out.get("load", {}).get("rows_loaded"),
        rows_quarantined=out.get("quarantine", {}).get("rows_quarantined"), task_states=states,
        warnings=out.get("quarantine", {}).get("dataset_warnings", []), params=cfg.to_dict(),
        audit_task_id="audit")
    return out


def run_elt(cfg: RunConfig) -> dict:
    st = elt_steps.start_run(cfg)
    cfg = RunConfig.from_dict({k: v for k, v in st.items() if k != "start_time"})
    states, out = {}, {"start_time": st["start_time"]}
    try:
        metas = [elt_steps.extract_check(cfg, s) for s in BASE]
        if cfg.apply_increment:
            metas.append(elt_steps.extract_check(cfg, "orders_increment", cfg.increment_file))
        raw = [elt_steps.load_raw(cfg, m) for m in metas]
        stg = {s: elt_steps.build_staging(cfg, s)["dq_status"] for s in elt_steps.STAGING_MASTERS}
        o = elt_steps.build_orders_staging(cfg)
        stg["stg_orders"] = o["dq_status"]
        stg["stg_payments"] = elt_steps.build_staging(cfg, "stg_payments")["dq_status"]
        dq = elt_steps.record_dq_results(cfg)
        marts = {m: elt_steps.build_mart(cfg, m)["rows"] for m in elt_steps.MARTS}
        out["scd2"] = elt_steps.customer_scd2(cfg)
        rp = elt_steps.report(cfg)
        out.update(raw={r["table"] + ":" + r["file"]: (r["rows_loaded"], r["table_rows"]) for r in raw},
                   staging=stg, versions=o["versions"], merges=o["merges"], dq=dq, marts=marts, report=rp,
                   rows_extracted=sum(m["rows"] for m in metas), rows_loaded=sum(r["rows_loaded"] for r in raw))
        states["pipeline"] = "success"
    except Exception as exc:
        states["pipeline"] = "failed"
        out["error"] = f"{type(exc).__name__}: {exc}"
    out["audit"] = audit.write_pipeline_run(
        cfg.warehouse, run_id=cfg.run_id, dag_id=cfg.dag_id, start_time=out["start_time"],
        rows_extracted=out.get("rows_extracted"), rows_loaded=out.get("rows_loaded"),
        rows_quarantined=out.get("report", {}).get("rows_failed_in_staging"), task_states=states,
        warnings=out.get("dq", {}).get("dataset_warnings", []), params=cfg.to_dict(), audit_task_id="audit")
    return out


def main(argv=None) -> dict:
    ap = argparse.ArgumentParser()
    ap.add_argument("pipeline", choices=["etl", "elt"])
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--increment", action="store_true")
    ap.add_argument("--warehouse")
    ap.add_argument("--data-dir")
    ap.add_argument("--quarantine-dir")
    ap.add_argument("--work-dir")
    ap.add_argument("--as-of")
    ap.add_argument("--expect-reduced-volume", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.WARNING if a.quiet else logging.INFO, format="%(message)s")
    dag_id = ETL_DAG_ID if a.pipeline == "etl" else ELT_DAG_ID
    cfg = RunConfig.from_params(a.run_id, dag_id, {
        "apply_increment": a.increment, "warehouse_path": a.warehouse, "data_dir": a.data_dir,
        "quarantine_dir": a.quarantine_dir, "as_of": a.as_of, "expect_reduced_volume": a.expect_reduced_volume})
    if a.work_dir:
        cfg.work_dir = Path(a.work_dir)
    out = run_etl(cfg) if a.pipeline == "etl" else run_elt(cfg)
    print(json.dumps(out, indent=1, default=str))
    return out


if __name__ == "__main__":
    main()
