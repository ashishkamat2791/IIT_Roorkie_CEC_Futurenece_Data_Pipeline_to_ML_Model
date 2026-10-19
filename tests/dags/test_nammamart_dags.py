"""Structure tests for the NammaMart DAGs. Run inside Astro: astro dev pytest tests/dags/test_nammamart_dags.py"""

from datetime import timedelta

import pendulum
import pytest
from airflow.models import DagBag

DAG_IDS = ["nammamart_etl_ashish_kamat", "nammamart_elt_ashish_kamat"]


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(include_examples=False)


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_parses_without_errors(dagbag, dag_id):
    assert not [e for f, e in dagbag.import_errors.items() if "nammamart" in f]
    assert dag_id in dagbag.dags


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_schedule_is_9am_ist_mon_to_sat(dagbag, dag_id):
    from airflow.timetables.base import DataInterval, TimeRestriction
    from airflow.timetables.trigger import CronTriggerTimetable

    dag = dagbag.dags[dag_id]
    assert dag.timetable.expression == "0 9 * * 1-6"
    assert str(dag.timetable.timezone.name) == "Asia/Kolkata"
    assert dag.start_date == pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")  # stored as 2026-09-30T18:30Z
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    # Next run after Saturday 17 Oct 2026 09:00 IST is Monday 19 Oct 09:00 IST = 03:30 UTC (Sunday skipped).
    core = CronTriggerTimetable(dag.timetable.expression, timezone=dag.timetable.timezone)
    sat = pendulum.datetime(2026, 10, 17, 9, 0, tz="Asia/Kolkata")
    nxt = core.next_dagrun_info(last_automated_data_interval=DataInterval(sat, sat),
                                restriction=TimeRestriction(earliest=dag.start_date, latest=None, catchup=False))
    assert nxt.run_after == pendulum.datetime(2026, 10, 19, 3, 30, tz="UTC")


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_default_args(dagbag, dag_id):
    args = dagbag.dags[dag_id].default_args
    assert args["retries"] >= 2
    assert args["retry_delay"] == timedelta(minutes=1)
    assert args["execution_timeout"] == timedelta(minutes=15)


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_audit_runs_after_everything(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    audit = dag.get_task("run_audit")
    assert audit.trigger_rule == "all_done"
    assert audit.get_flat_relative_ids(upstream=True) == {t for t in dag.task_ids if t != "run_audit"}


def test_etl_extracts_in_parallel(dagbag):
    dag = dagbag.dags["nammamart_etl_ashish_kamat"]
    for s in ["orders", "payments", "customers", "products", "stores"]:
        assert dag.get_task(f"extract_{s}").upstream_task_ids == {"start_run"}
    assert {"validate_orders", "validate_payments"} <= dag.get_task("quarantine_failed_rows").upstream_task_ids


def test_elt_layers_in_order(dagbag):
    dag = dagbag.dags["nammamart_elt_ashish_kamat"]
    assert {"load_raw_orders", "load_raw_order_increment", "stg_customers", "stg_products",
            "stg_stores"} <= dag.get_task("stg_orders").upstream_task_ids
    assert dag.get_task("int_fact_orders").upstream_task_ids == {"record_dq_results", "start_run"}
    marts = {"mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
             "mart_payment_reconciliation", "mart_top_customers"}
    assert marts <= dag.get_task("int_fact_orders").downstream_task_ids
