"""
dags/nammamart_elt_ashish_kamat.py
----------------------------------
NammaMart ELT pipeline: Extract -> Load raw -> Transform in DuckDB with SQL.

  RAW      raw_orders, raw_payments, raw_customers, raw_products, raw_stores
           every source row as text + _row_num/_source_file/_loaded_at/_run_id, append-only
  STAGING  stg_customers, stg_products, stg_stores, stg_orders(_versions), stg_payments
           SQL-only typing, standardising, dedupe, PII masking, quality rules, dq_status/failure_reason,
           MERGE on order_id for incremental order files
  MART     int_fact_orders -> mart_daily_store_kpis, mart_category_margin, mart_delivery_sla,
           mart_payment_reconciliation, mart_top_customers (+ mart_payment_mode_revenue, mart_rejected_rows)

Every transformation is a .sql file in include/sql/elt/; Python only orchestrates.
Schedule: 09:00 Asia/Kolkata, Monday to Saturday (cron "0 9 * * 1-6").
Trigger with {"apply_increment": true} to land orders_2026-10-01_increment.csv in raw_orders.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from include.nammamart_ashish_kamat import audit, elt_steps  # noqa: E402
from include.nammamart_ashish_kamat.config import ELT_DAG_ID, RunConfig  # noqa: E402
from include.nammamart_ashish_kamat.extract import SchemaChangeError  # noqa: E402

BASE_SOURCES = ["orders", "payments", "customers", "products", "stores"]

default_args = {
    "owner": "ashish_kamat",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": audit.notify_failure,
}

PARAMS = {
    "apply_increment": Param(False, type="boolean", description="Land the next-day incremental order file in raw"),
    "increment_file": Param("orders_2026-10-01_increment.csv", type="string"),
    "as_of": Param("2026-10-01 09:00:00", type="string",
                   description="Batch cut-off (IST) for timeliness checks of the September files"),
    "expect_reduced_volume": Param(False, type="boolean"),
    "orders_baseline": Param(None, type=["null", "integer"]),
    "data_dir": Param(None, type=["null", "string"]),
    "warehouse_path": Param(None, type=["null", "string"]),
    "quarantine_dir": Param(None, type=["null", "string"]),
}


def _cfg(cfg_dict: dict) -> RunConfig:
    return RunConfig.from_dict({k: v for k, v in cfg_dict.items() if k != "start_time"})


@dag(
    dag_id=ELT_DAG_ID,
    description="NammaMart ELT: raw history in DuckDB, SQL staging and marts",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    default_args=default_args,
    params=PARAMS,
    tags=["nammamart", "elt", "ashish_kamat"],
    doc_md=__doc__,
)
def nammamart_elt_ashish_kamat():

    @task
    def start_run() -> dict:
        ctx = get_current_context()
        cfg = RunConfig.from_params(ctx["run_id"], ELT_DAG_ID, dict(ctx["params"]))
        return elt_steps.start_run(cfg)

    @task
    def extract_source(cfg: dict, source: str) -> dict:
        try:
            return elt_steps.extract_check(_cfg(cfg), source)
        except SchemaChangeError as exc:
            # Deterministic: retrying cannot fix a renamed column (experiment 6), so fail at once.
            raise AirflowFailException(str(exc)) from exc

    @task
    def extract_order_increment(cfg: dict) -> dict:
        c = _cfg(cfg)
        if not c.apply_increment:
            return {"source": "orders_increment", "rows": 0, "applied": False}
        try:
            return {**elt_steps.extract_check(c, "orders_increment", c.increment_file), "applied": True}
        except SchemaChangeError as exc:
            raise AirflowFailException(str(exc)) from exc

    @task
    def load_raw(cfg: dict, meta: dict) -> dict:
        if meta.get("applied") is False:
            return {"table": "raw_orders", "file": None, "rows_loaded": 0, "skipped": True}
        return elt_steps.load_raw(_cfg(cfg), meta)

    @task
    def build_staging(cfg: dict, name: str) -> dict:
        return elt_steps.build_staging(_cfg(cfg), name)

    @task
    def stg_orders(cfg: dict) -> dict:
        return elt_steps.build_orders_staging(_cfg(cfg))

    @task
    def record_dq_results(cfg: dict) -> dict:
        return elt_steps.record_dq_results(_cfg(cfg))

    @task
    def build_mart(cfg: dict, name: str) -> dict:
        return elt_steps.build_mart(_cfg(cfg), name)

    @task
    def dim_customer_scd2(cfg: dict) -> dict:
        """Bonus: SCD Type 2 customer dimension (loyalty-tier history)."""
        return elt_steps.customer_scd2(_cfg(cfg))

    @task
    def report(cfg: dict) -> dict:
        return elt_steps.report(_cfg(cfg))

    @task(trigger_rule="all_done", retries=0)
    def run_audit() -> dict:
        ctx = get_current_context()
        ti, run_id = ctx["ti"], ctx["run_id"]
        pull = lambda tid: ti.xcom_pull(task_ids=tid)
        cfg_dict = pull("start_run")
        cfg = _cfg(cfg_dict) if cfg_dict else RunConfig.from_params(run_id, ELT_DAG_ID, dict(ctx["params"]))
        extracted = [pull(f"extract_{s}") for s in BASE_SOURCES] + [pull("extract_order_increment")]
        loaded = [pull(f"load_raw_{s}") for s in BASE_SOURCES] + [pull("load_raw_order_increment")]
        rep, dq = pull("report"), pull("record_dq_results")
        states = audit.fetch_task_states(ELT_DAG_ID, run_id)
        row = audit.write_pipeline_run(
            cfg.warehouse, run_id=run_id, dag_id=ELT_DAG_ID, start_time=(cfg_dict or {}).get("start_time"),
            rows_extracted=sum(m["rows"] for m in extracted if m) if any(extracted) else None,
            rows_loaded=sum(m["rows_loaded"] for m in loaded if m) if any(loaded) else None,
            rows_quarantined=(rep or {}).get("rows_failed_in_staging"), task_states=states,
            warnings=(dq or {}).get("dataset_warnings", []), params=cfg.to_dict(), audit_task_id="run_audit")
        if row["status"] == "FAILED":
            raise AirflowFailException(f"pipeline failed; failed tasks: {row['failed_tasks']}")
        return {k: row[k] for k in ("status", "rows_extracted", "rows_loaded", "rows_quarantined", "warnings")}

    cfg = start_run()
    # Extract (parallel) -> load raw (one task per file)
    ex = {s: extract_source.override(task_id=f"extract_{s}")(cfg, s) for s in BASE_SOURCES}
    raw = {s: load_raw.override(task_id=f"load_raw_{s}")(cfg, ex[s]) for s in BASE_SOURCES}
    raw_inc = load_raw.override(task_id="load_raw_order_increment")(cfg, extract_order_increment(cfg))
    # Staging: masters, then orders (versions + MERGE), then payments (needs orders)
    s_c = build_staging.override(task_id="stg_customers")(cfg, "stg_customers")
    s_p = build_staging.override(task_id="stg_products")(cfg, "stg_products")
    s_s = build_staging.override(task_id="stg_stores")(cfg, "stg_stores")
    s_o = stg_orders(cfg)
    s_pay = build_staging.override(task_id="stg_payments")(cfg, "stg_payments")
    dq = record_dq_results(cfg)
    raw["customers"] >> s_c
    raw["products"] >> s_p
    raw["stores"] >> s_s
    [raw["orders"], raw_inc, s_c, s_p, s_s] >> s_o
    [raw["payments"], s_o] >> s_pay >> dq
    # Marts
    marts = {m: build_mart.override(task_id=m)(cfg, m) for m in [
        "int_fact_orders", "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
        "mart_top_customers", "mart_payment_reconciliation", "mart_payment_mode_revenue", "mart_rejected_rows"]}
    dq >> marts["int_fact_orders"] >> [marts["mart_daily_store_kpis"], marts["mart_category_margin"],
                                       marts["mart_delivery_sla"], marts["mart_top_customers"],
                                       marts["mart_payment_reconciliation"]]
    marts["mart_payment_reconciliation"] >> marts["mart_payment_mode_revenue"]
    dq >> marts["mart_rejected_rows"]
    rp = report(cfg)
    scd2 = dim_customer_scd2(cfg)
    s_c >> scd2 >> rp
    [marts[m] for m in marts if m != "int_fact_orders"] >> rp
    rp >> run_audit()


nammamart_elt_ashish_kamat()
