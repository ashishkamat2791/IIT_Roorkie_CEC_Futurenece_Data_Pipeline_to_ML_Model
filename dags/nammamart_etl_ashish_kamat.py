"""
dags/nammamart_etl_ashish_kamat.py
----------------------------------
NammaMart ETL pipeline: Extract -> Validate -> Quarantine -> Transform -> Load
-> Reconcile -> Report -> Audit. Transformations run in pandas BEFORE the
warehouse, so only rows that passed every critical check reach the finance
star schema in include/warehouse_ashish_kamat.duckdb.

  extract_orders ─┐                           ┌ validate_customers ┐
  extract_payments│  extract_customers ───────┤ validate_products  ├─ validate_orders ─ validate_payments ─┐
  extract_* (x5) ─┤  extract_products  ───────┤ validate_stores    ┘                                       │
  extract_order_increment ───────────────────────────────────────────────────────────────────────────────┘
      -> quarantine_failed_rows -> transform -> load_warehouse -> reconcile_payments -> build_reports
      -> run_audit (trigger_rule=all_done, records pipeline_runs even when something failed)

Schedule: 09:00 Asia/Kolkata, Monday to Saturday (cron "0 9 * * 1-6").
Trigger with {"apply_increment": true} to load orders_2026-10-01_increment.csv.
Helpers: include/nammamart_ashish_kamat/. Business rules: SUBMISSION.md.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from include.nammamart_ashish_kamat import audit, etl_steps, extract  # noqa: E402
from include.nammamart_ashish_kamat.config import ETL_DAG_ID, RunConfig  # noqa: E402

BASE_SOURCES = ["orders", "payments", "customers", "products", "stores"]

default_args = {
    "owner": "ashish_kamat",
    "retries": 2,                                  # transient issues (file landing late, DuckDB lock)
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),    # a hung task must not eat the 09:00-09:30 window
    "on_failure_callback": audit.notify_failure,  # logs, and posts to Slack if the Variable exists
}

PARAMS = {
    "apply_increment": Param(False, type="boolean", description="Also load the next-day incremental order file"),
    "increment_file": Param("orders_2026-10-01_increment.csv", type="string"),
    "as_of": Param("2026-10-01 09:00:00", type="string",
                   description="Batch cut-off (IST) for timeliness checks of the September files"),
    "expect_reduced_volume": Param(False, type="boolean",
                                   description="Suppress the volume-drop alert for an intentional small test run"),
    "orders_baseline": Param(None, type=["null", "integer"], description="Override the volume baseline"),
    # Isolated experiments only; must stay under include/.
    "data_dir": Param(None, type=["null", "string"]),
    "warehouse_path": Param(None, type=["null", "string"]),
    "quarantine_dir": Param(None, type=["null", "string"]),
}


def _cfg(cfg_dict: dict) -> RunConfig:
    return RunConfig.from_dict({k: v for k, v in cfg_dict.items() if k != "start_time"})


@dag(
    dag_id=ETL_DAG_ID,
    description="NammaMart ETL: validated, reconciled finance star schema (pandas -> DuckDB)",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,               # one writer per DuckDB file; runs never overlap
    dagrun_timeout=timedelta(minutes=60),
    default_args=default_args,
    params=PARAMS,
    tags=["nammamart", "etl", "ashish_kamat"],
    doc_md=__doc__,
)
def nammamart_etl_ashish_kamat():

    @task
    def start_run() -> dict:
        ctx = get_current_context()
        cfg = RunConfig.from_params(ctx["run_id"], ETL_DAG_ID, dict(ctx["params"]))
        return etl_steps.start_run(cfg)

    @task
    def extract_source(cfg: dict, source: str) -> dict:
        try:
            return extract.extract_source(_cfg(cfg), source)
        except extract.SchemaChangeError as exc:
            # Deterministic: retrying cannot fix a renamed column (experiment 6), so fail at once.
            # A missing file stays retryable because it may simply land late.
            raise AirflowFailException(str(exc)) from exc

    @task
    def extract_order_increment(cfg: dict) -> dict:
        try:
            return extract.extract_increment(_cfg(cfg))
        except extract.SchemaChangeError as exc:
            raise AirflowFailException(str(exc)) from exc

    @task
    def validate_master(cfg: dict, source: str, meta: dict) -> dict:
        return etl_steps.validate_master(_cfg(cfg), source, meta)

    @task
    def validate_orders(cfg: dict, orders: dict, increment: dict, customers: dict, products: dict, stores: dict) -> dict:
        return etl_steps.validate_orders(_cfg(cfg), orders, increment,
                                         {"customers": customers, "products": products, "stores": stores})

    @task
    def validate_payments(cfg: dict, payments: dict, orders: dict) -> dict:
        return etl_steps.validate_payments(_cfg(cfg), payments, orders)

    @task
    def quarantine_failed_rows(cfg: dict, customers: dict, products: dict, stores: dict, orders: dict, payments: dict) -> dict:
        return etl_steps.quarantine(_cfg(cfg), [customers, products, stores, orders, payments])

    @task
    def transform(cfg: dict, customers: dict, products: dict, stores: dict, orders: dict, payments: dict,
                  quarantine: dict) -> dict:
        return etl_steps.transform_step(_cfg(cfg), {"customers": customers, "products": products,
                                                     "stores": stores, "orders": orders, "payments": payments})

    @task
    def load_warehouse(cfg: dict, transformed: dict) -> dict:
        return etl_steps.load_step(_cfg(cfg), transformed)

    @task
    def reconcile_payments(cfg: dict, payments: dict, orders: dict, loaded: dict) -> dict:
        return etl_steps.reconcile_step(_cfg(cfg), payments, orders)

    @task
    def build_reports(cfg: dict, reconciled: dict) -> dict:
        return etl_steps.report_step(_cfg(cfg))

    @task(trigger_rule="all_done", retries=0)
    def run_audit() -> dict:
        ctx = get_current_context()
        ti, run_id = ctx["ti"], ctx["run_id"]
        pull = lambda tid: ti.xcom_pull(task_ids=tid)  # None when that task failed or never ran
        cfg_dict = pull("start_run")
        cfg = _cfg(cfg_dict) if cfg_dict else RunConfig.from_params(run_id, ETL_DAG_ID, dict(ctx["params"]))
        extracted = [pull(f"extract_{s}") for s in BASE_SOURCES] + [pull("extract_order_increment")]
        rows_extracted = sum(m["rows"] for m in extracted if m) if any(extracted) else None
        q, ld = pull("quarantine_failed_rows"), pull("load_warehouse")
        states = audit.fetch_task_states(ETL_DAG_ID, run_id)
        row = audit.write_pipeline_run(
            cfg.warehouse, run_id=run_id, dag_id=ETL_DAG_ID,
            start_time=(cfg_dict or {}).get("start_time"), rows_extracted=rows_extracted,
            rows_loaded=(ld or {}).get("rows_loaded"), rows_quarantined=(q or {}).get("rows_quarantined"),
            task_states=states, warnings=(q or {}).get("dataset_warnings", []), params=cfg.to_dict(),
            audit_task_id="run_audit")
        if row["status"] == "FAILED":
            # Recorded first, then fail loudly so the DAG run is not shown as green.
            raise AirflowFailException(f"pipeline failed; failed tasks: {row['failed_tasks']}")
        etl_steps.cleanup_work_dir(cfg)
        return {k: row[k] for k in ("status", "rows_extracted", "rows_loaded", "rows_quarantined", "warnings")}

    cfg = start_run()
    ex = {s: extract_source.override(task_id=f"extract_{s}")(cfg, s) for s in BASE_SOURCES}
    inc = extract_order_increment(cfg)
    vm = {s: validate_master.override(task_id=f"validate_{s}")(cfg, s, ex[s])
          for s in ["customers", "products", "stores"]}
    vo = validate_orders(cfg, ex["orders"], inc, vm["customers"], vm["products"], vm["stores"])
    vp = validate_payments(cfg, ex["payments"], vo)
    q = quarantine_failed_rows(cfg, vm["customers"], vm["products"], vm["stores"], vo, vp)
    tr = transform(cfg, vm["customers"], vm["products"], vm["stores"], vo, vp, q)
    ld = load_warehouse(cfg, tr)
    rc = reconcile_payments(cfg, vp, vo, ld)
    rp = build_reports(cfg, rc)
    rp >> run_audit()


nammamart_etl_ashish_kamat()
