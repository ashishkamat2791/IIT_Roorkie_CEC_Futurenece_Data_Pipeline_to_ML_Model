"""Bonus: SCD Type 2 customer dimension, both pipelines (local, no Airflow)."""

from __future__ import annotations

import shutil
import subprocess
import sys

import duckdb
import pytest

from conftest import ROOT
from include.nammamart_ashish_kamat.scd2 import plan_changes

import run_pipeline_local

BASE = ROOT / "include" / "tmp" / "pytest_scd2"
DAY2 = "include/experiments/exp11_scd2_day2/data"


def run(p, run_id, as_of, data_dir=None):
    args = [p, "--run-id", run_id, "--quiet", "--as-of", as_of, "--warehouse", str(BASE / f"{p}.duckdb"),
            "--quarantine-dir", str(BASE / "q"), "--work-dir", str(BASE / "w")]
    if data_dir:
        args += ["--data-dir", data_dir]
    return run_pipeline_local.main(args)


@pytest.fixture(scope="module")
def scd():
    shutil.rmtree(BASE, ignore_errors=True)
    BASE.mkdir(parents=True)
    subprocess.run([sys.executable, str(ROOT / "scripts" / "experiments.py"), "prepare", "exp11_scd2_day2"],
                   check=True, capture_output=True)
    out = {}
    for p in ("etl", "elt"):
        out[p] = [run(p, f"{p}_d1", "2026-10-01 09:00:00"),
                  run(p, f"{p}_d2", "2026-10-02 09:00:00", DAY2),
                  run(p, f"{p}_d2_rerun", "2026-10-02 09:00:00", DAY2),
                  run(p, f"{p}_d1_replay", "2026-10-01 09:00:00")]       # older batch replayed late
    yield out
    shutil.rmtree(BASE, ignore_errors=True)


def history(p):
    con = duckdb.connect(str(BASE / f"{p}.duckdb"), read_only=True)
    rows = con.execute(f"""SELECT customer_id, loyalty_tier, CAST(is_active AS VARCHAR), valid_from, valid_to,
                                  is_current, change_type FROM {p}_dim_customer_scd2 ORDER BY ALL""").fetchall()
    con.close()
    return rows


@pytest.mark.parametrize("p", ["etl", "elt"])
def test_scd2_tracks_changes_idempotently(scd, p):
    d1, d2, d2b, d1b = (r["scd2"] for r in scd[p])
    assert (d1["new"], d1["changed"], d1["current_total"]) == (400, 0, 400)
    assert (d2["new"], d2["changed"], d2["versions_total"]) == (0, 4, 404)
    assert (d2b["new"], d2b["changed"], d2b["versions_total"]) == (0, 0, 404)     # same batch again: no-op
    assert (d1b["new"], d1b["changed"], d1b["versions_total"]) == (0, 0, 404)     # older batch: ignored
    h = [r for r in history(p) if r[0] == "C00136"]
    assert [(r[1], r[5], r[6]) for r in h] == [("Bronze", False, "NEW"), ("Gold", True, "CHANGED")]
    assert str(h[0][4]) == str(h[1][3]) == "2026-10-02 09:00:00"                 # closed exactly when the new one opens


def test_scd2_etl_and_elt_identical(scd):
    assert history("etl") == history("elt")


def test_plan_changes_unit():
    import pandas as pd
    from datetime import datetime
    snap = pd.DataFrame([{"customer_id": "C1", "full_name_masked": "A***", "loyalty_tier": "Gold", "is_active": "true"}])
    cur = pd.DataFrame([{"customer_sk": "C1@x", "customer_id": "C1", "loyalty_tier": "Silver", "is_active": "true",
                         "valid_from": datetime(2026, 10, 1, 9)}])
    ins, closes = plan_changes(snap, cur, datetime(2026, 10, 2, 9))
    assert closes == ["C1@x"] and ins[0]["change_type"] == "CHANGED"
    assert plan_changes(snap, cur, datetime(2026, 10, 1, 9)) == ([], [])          # not newer -> no-op
