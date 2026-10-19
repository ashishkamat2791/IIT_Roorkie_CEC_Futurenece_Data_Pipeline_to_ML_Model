"""
End-to-end and unit tests for the NammaMart ETL and ELT pipelines (Ashish Kamat).

Run locally (no Airflow needed):  .venv/bin/python -m pytest tests/nammamart -q
The DAG parse tests live in tests/dags/ and run inside the Astro containers.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from conftest import ROOT, TEST_DIR
from include.nammamart_ashish_kamat import extract, validate
from include.nammamart_ashish_kamat.config import RunConfig
from include.nammamart_ashish_kamat.masking import mask_email, mask_name, mask_phone

DATA = ROOT / "include" / "data" / "nammamart"
ORIG = ROOT / "assignment" / "nammamart_dataset"
FILES = ["orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv",
         "orders_2026-10-01_increment.csv"]
EXPECTED_ROWS = {"orders.csv": 2412, "payments.csv": 2228, "customers.csv": 405, "products.csv": 152,
                 "stores.csv": 15, "orders_2026-10-01_increment.csv": 160}
# One known-bad order per critical rule, taken from the profiling log.
CRITICAL_ORDERS = {
    "ORD002255": "orders_customer_id_not_null", "ORD001554": "orders_delivered_has_delivery_minutes",
    "ORD000403": "orders_order_ts_valid", "ORD000500": "orders_status_allowed",
    "ORD001812": "orders_payment_mode_allowed", "ORD002103": "orders_discount_pct_range",
    "ORD000462": "orders_quantity_positive", "ORD002305": "orders_amount_formula",
    "ORD000455": "orders_delivery_minutes_lt_120", "ORD000986": "orders_product_fk",
    "ORD001799": "orders_store_fk", "ORD002119": "orders_order_ts_not_future",
}
DIMENSIONS = {"Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness"}


def md5(p: Path) -> str:
    return hashlib.md5(p.read_bytes()).hexdigest()


# ------------------------------------------------------------- source data
@pytest.mark.parametrize("name", FILES)
def test_source_copies_are_unchanged(name):
    assert md5(DATA / name) == md5(ORIG / name)
    assert len(pd.read_csv(DATA / name, dtype=str, keep_default_na=False)) == EXPECTED_ROWS[name]


# ----------------------------------------------------------------- masking
def test_masking_rules():
    assert mask_email("test.user001@example.com") == "te***@example.com"
    assert mask_email("bad.email001example.com") is None          # malformed -> NULL, never partial
    assert mask_email("N/A") is None
    assert mask_phone("9000000012") == "90******12"
    assert mask_phone("9774349") is None                            # wrong length -> NULL
    assert mask_name("Test  User") == "T*** U***"
    assert mask_name("") is None


# --------------------------------------------------------- extract contract
def _cfg(tmp_dir: Path) -> RunConfig:
    return RunConfig(run_id="unit", dag_id="unit", data_dir=tmp_dir, work_dir=tmp_dir / "work")


def test_missing_file_fails_loudly(tmp_path):
    with pytest.raises(FileNotFoundError):
        extract.extract_source(_cfg(tmp_path), "products")


def test_schema_change_fails_loudly(tmp_path):
    df = pd.read_csv(DATA / "orders.csv", dtype=str, keep_default_na=False).rename(columns={"order_amount": "total"})
    df.to_csv(tmp_path / "orders.csv", index=False)
    with pytest.raises(extract.SchemaChangeError, match="missing=\\['order_amount'\\].*unexpected=\\['total'\\]"):
        extract.extract_source(_cfg(tmp_path), "orders")


def test_header_only_file_fails(tmp_path):
    (tmp_path / "stores.csv").write_text((DATA / "stores.csv").read_text().splitlines()[0] + "\n")
    with pytest.raises(ValueError, match="no rows"):
        extract.extract_source(_cfg(tmp_path), "stores")


# ------------------------------------------------------- validation edge cases
def _orders_df(rows: list[dict]) -> pd.DataFrame:
    base = dict(order_id="ORDX1", order_ts="2026-09-10 10:00:00", customer_id="C00001", store_id="S01",
                product_id="P0001", quantity="2", unit_price="100.0", discount_pct="10", order_amount="180.0",
                payment_mode="UPI", order_status="DELIVERED", channel="App", delivery_minutes="12")
    df = pd.DataFrame([{**base, **r} for r in rows])
    df.insert(0, "_row_num", range(1, len(df) + 1))
    df["_source_file"] = "orders.csv"
    return df


def _validate(df):
    from datetime import datetime
    out, res = validate.validate_orders(
        df, source_file="orders.csv", run_id="u", dag_id="u", cutoff=datetime(2026, 10, 1, 9),
        customer_ids={"C00001"}, product_ids={"P0001"}, store_ids={"S01"}, product_mrp={"P0001": 100.0},
        store_capacity={"S01": 300}, baseline=None)
    return out, {r["check_name"]: r for r in res}


def test_validation_edge_cases():
    out, res = _validate(_orders_df([
        {},                                                       # valid
        {"order_id": "ORDX2", "customer_id": ""},                 # null customer
        {"order_id": "ORDX3", "quantity": "abc"},                 # failed cast
        {"order_id": "ORDX4", "order_status": "SHIPPED"},         # invalid status
        {"order_id": "ORDX5", "order_ts": "2027-01-01 09:00:00"}, # future
        {"order_id": "ORDX6", "store_id": "S99"},                 # unknown store
        {"order_id": "ORDX7", "quantity": "-1", "order_amount": "-90.0"},
        {"order_id": "ORDX1"},                                    # exact duplicate of row 1
        {"order_id": "ORDX8", "order_status": "delivered", "payment_mode": "upi "},  # warnings only
    ]))
    st = dict(zip(out["order_id"] + ":" + out["_row_num"].astype(str), out["dq_status"]))
    assert st["ORDX1:1"] == "PASSED" and st["ORDX1:8"] == "FAILED"
    assert st["ORDX8:9"] == "WARNING"
    for oid, chk in [("ORDX2", "orders_customer_id_not_null"), ("ORDX3", "orders_numeric_fields_valid"),
                     ("ORDX4", "orders_status_allowed"), ("ORDX5", "orders_order_ts_not_future"),
                     ("ORDX6", "orders_store_fk"), ("ORDX7", "orders_quantity_positive")]:
        row = out[out["order_id"] == oid].iloc[0]
        assert row["dq_status"] == "FAILED" and chk in row["failed_checks"].split(";"), (oid, row["failure_reason"])
        assert row["failure_reason"].startswith(chk) or chk in row["failure_reason"]
    assert res["orders_order_id_unique"]["rows_failed"] == 1
    assert res["orders_volume_vs_baseline"]["status"] == "WARN"   # 9 rows vs 2412 baseline


def test_conflicting_duplicates_name_the_columns():
    out, _ = _validate(_orders_df([{}, {"quantity": "3", "order_amount": "270.0"}]))
    assert "differs from kept row 1 in quantity, order_amount" in out.iloc[1]["failure_reason"]


# ------------------------------------------------------------ ETL pipeline
def test_etl_happy_path(runs, con):
    r = runs["etl_1"]
    assert r["audit"]["status"] == "SUCCESS"
    assert r["extract"] == {"orders": 2412, "payments": 2228, "customers": 405, "products": 152, "stores": 15}
    assert r["validate"]["orders"] == {"PASSED": 2298, "WARNING": 10, "FAILED": 104}
    assert r["load"]["fact"]["inserted"] == r["load"]["fact"]["fact_rows_after"] == 2308
    assert r["quarantine"]["rows_quarantined"] == 167
    # after the increment, 10 rows come from the increment file and 2298 still from orders.csv
    assert con.execute("SELECT count(*) FROM etl_fact_orders WHERE source_file = 'orders.csv'").fetchone()[0] == 2298


def test_no_critical_order_reaches_finance(con):
    ids = con.execute("SELECT order_id FROM etl_fact_orders").df()["order_id"]
    assert not set(CRITICAL_ORDERS) & set(ids)
    assert ids.is_unique
    assert con.execute("SELECT count(*) FROM etl_fact_orders WHERE order_id = 'ORD000539'").fetchone()[0] == 1


def test_quarantine_files_have_reasons_and_no_raw_pii(runs):
    qdir = TEST_DIR / "quarantine"
    files = {p.name.split("_failed_")[0]: p for p in qdir.glob("*_failed_t_etl_1.csv")}
    assert {"orders", "payments", "customers", "products", "stores"} <= set(files)
    orders = pd.read_csv(files["orders"], dtype=str, keep_default_na=False)
    assert (orders["failure_reason"].str.len() > 0).all()
    for oid, chk in CRITICAL_ORDERS.items():
        assert chk in orders.loc[orders["order_id"] == oid, "failed_checks"].iloc[0]
    raw = pd.read_csv(DATA / "customers.csv", dtype=str, keep_default_na=False)
    text = files["customers"].read_text()
    assert not any(e in text for e in raw["email"] if "@" in e)
    assert not any(p in text for p in raw["phone"])


def test_pii_masked_in_reporting_tables(con):
    raw = pd.read_csv(DATA / "customers.csv", dtype=str, keep_default_na=False)
    for t in ["etl_dim_customer", "etl_top_customers", "stg_customers", "mart_top_customers"]:
        df = con.execute(f"SELECT * FROM {t}").df().astype(str)
        blob = " ".join(df.values.ravel())
        assert not any(p in blob for p in raw["phone"]), t
        assert not any(e in blob for e in raw["email"] if "@" in e), t
        assert not any(n in blob for n in raw["full_name"]), t
        assert df["email_masked"].map(lambda v: v in ("None", "nan", "<NA>") or re.fullmatch(r".{1,2}\*\*\*@.+", v)).all()


def test_all_six_dimensions_recorded_every_run(con):
    for dag in ["nammamart_etl_ashish_kamat", "nammamart_elt_ashish_kamat"]:
        per_run = con.execute("SELECT run_id, list(DISTINCT dimension) FROM dq_results WHERE dag_id = ? GROUP BY 1",
                              [dag]).fetchall()
        assert len(per_run) == 4
        for run_id, dims in per_run:
            assert set(dims) == DIMENSIONS, (dag, run_id)


def test_etl_idempotent_rerun(runs):
    f1, f2 = runs["etl_1"]["load"]["fact"], runs["etl_2"]["load"]["fact"]
    assert f1["inserted"] == 2308 and f2["inserted"] == 0 and f2["updated"] == 0
    assert f1["fact_rows_after"] == f2["fact_rows_after"] == 2308


def test_etl_increment(runs, con):
    f = runs["etl_inc"]["load"]["fact"]
    assert (f["inserted"], f["updated"]) == (150, 10)
    assert runs["etl_inc2"]["load"]["fact"]["inserted"] == 0 and runs["etl_inc2"]["load"]["fact"]["updated"] == 0
    rows = con.execute("SELECT order_status, previous_status, count(*) FROM etl_fact_orders "
                       "WHERE source_file LIKE 'orders_2026-10-01%' AND order_date < DATE '2026-10-01' GROUP BY ALL").fetchall()
    assert rows == [("RETURNED", "DELIVERED", 10)]


def test_reconciliation_covers_mismatch_types(con):
    reasons = {r[0] for r in con.execute("SELECT DISTINCT reason FROM etl_payment_reconciliation").fetchall()}
    assert {"AMOUNT_MISMATCH", "MISSING_PAYMENT", "PAYMENT_FAILED", "DUPLICATE_PAYMENT_RECORD", "ORPHAN_PAYMENT",
            "PAYMENT_FOR_REJECTED_ORDER", "RETURNED_NOT_REFUNDED"} <= reasons
    # cancelled orders without a payment are not mismatches
    assert con.execute("SELECT count(*) FROM etl_payment_reconciliation WHERE order_status = 'CANCELLED'").fetchone()[0] == 0


# ------------------------------------------------------------ ELT pipeline
def test_elt_raw_is_exact_copy(con):
    for name, table in [("orders.csv", "raw_orders"), ("payments.csv", "raw_payments"),
                        ("customers.csv", "raw_customers"), ("products.csv", "raw_products"), ("stores.csv", "raw_stores")]:
        src = pd.read_csv(DATA / name, dtype=str, keep_default_na=False)
        raw = con.execute(f"SELECT * EXCLUDE (_row_num, _source_file, _loaded_at, _run_id, _file_md5) FROM {table} "
                          "WHERE _run_id = 't_elt_1' AND _source_file = ? ORDER BY _row_num", [name]).df()
        assert raw.shape == src.shape, table
        assert (raw.values == src.values).all(), table          # blanks, 'UPI ', 'N/A' preserved byte-for-byte


def test_elt_raw_keeps_history(runs, con):
    n = con.execute("SELECT count(DISTINCT _run_id), count(*) FROM raw_orders WHERE _source_file = 'orders.csv'").fetchone()
    assert n == (4, 4 * 2412)
    assert runs["elt_inc"]["raw"]["raw_orders:orders_2026-10-01_increment.csv"][0] == 160


def test_elt_merge_and_no_duplicates(runs, con):
    m = runs["elt_inc"]["merges"][0]
    assert (m["inserted"], m["updated"], m["status_changed"]) == (150, 10, 10)
    assert con.execute("SELECT count(*), count(DISTINCT order_id) FROM stg_orders").fetchone() == (2550 - 0, 2550)
    prev = con.execute("SELECT count(*) FROM stg_orders WHERE previous_status = 'DELIVERED' "
                       "AND order_status = 'RETURNED'").fetchone()[0]
    assert prev == 10
    # the original DELIVERED rows are still auditable in raw
    assert con.execute("SELECT count(*) FROM raw_orders WHERE order_id = 'ORD000017' AND order_status = 'DELIVERED'").fetchone()[0] >= 1


def test_september_totals_match_between_etl_and_elt(runs):
    snap = runs["snapshot_sept"]
    assert snap["etl"] == snap["elt"]
    assert float(snap["etl"][1]) == pytest.approx(1134962.80)
    assert runs["etl_inc"]["report"]["september_net_revenue"] == runs["elt_inc"]["report"]["september_net_revenue"] == pytest.approx(1130769.10)


def test_all_business_answers_match(runs):
    import compare_etl_elt
    import sys
    argv = sys.argv
    sys.argv = ["compare", str(TEST_DIR / "wh.duckdb")]
    try:
        assert compare_etl_elt.main() == 0
    finally:
        sys.argv = argv


def test_pipeline_runs_audit(con):
    rows = con.execute("SELECT dag_id, status, rows_extracted FROM pipeline_runs ORDER BY end_time").fetchall()
    assert len(rows) == 8 and all(r[1] == "SUCCESS" for r in rows)
    assert ("nammamart_elt_ashish_kamat", "SUCCESS", 5372) in rows      # all six files incl. increment


def test_raw_load_retry_does_not_duplicate(runs):
    from include.nammamart_ashish_kamat import elt_steps
    cfg = RunConfig(run_id="t_elt_1", dag_id="nammamart_elt_ashish_kamat", warehouse=TEST_DIR / "wh.duckdb")
    meta = elt_steps.extract_check(cfg, "stores")
    elt_steps.load_raw(cfg, meta)    # simulate an Airflow retry of the same run
    c = duckdb.connect(str(TEST_DIR / "wh.duckdb"), read_only=True)
    assert c.execute("SELECT count(*) FROM raw_stores WHERE _run_id = 't_elt_1'").fetchone()[0] == 15
    c.close()


def test_dag_failure_callback_records_killed_run(runs):
    """A run killed by dagrun_timeout skips run_audit; the DAG callback must still record it."""
    from datetime import datetime
    from types import SimpleNamespace
    from include.nammamart_ashish_kamat import audit
    rel = (TEST_DIR / "cb.duckdb").relative_to(ROOT)
    ctx = {"dag_run": SimpleNamespace(run_id="scheduled__x", dag_id="nammamart_etl_ashish_kamat",
                                      start_date=datetime(2026, 10, 19, 3, 53, 56)),
           "params": {"warehouse_path": str(rel)}, "reason": "timed_out"}
    audit.record_dag_failure(ctx)
    audit.record_dag_failure(ctx)          # idempotent: the second call sees the existing row
    c = duckdb.connect(str(TEST_DIR / "cb.duckdb"), read_only=True)
    rows = c.execute("SELECT status, warnings FROM pipeline_runs").fetchall()
    hist = c.execute("SELECT count(*) FROM pipeline_run_history").fetchone()[0]
    c.close()
    assert rows == [("FAILED", "run killed before audit: timed_out")] and hist == 1
