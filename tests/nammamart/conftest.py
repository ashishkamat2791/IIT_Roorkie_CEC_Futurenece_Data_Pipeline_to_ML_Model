"""Shared fixtures: run both pipelines (without Airflow) into an isolated warehouse."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import run_pipeline_local  # noqa: E402

TEST_DIR = ROOT / "include" / "tmp" / f"pytest_{os.getpid()}"


def run(pipeline: str, run_id: str, *extra: str) -> dict:
    return run_pipeline_local.main([
        pipeline, "--run-id", run_id, "--quiet",
        "--warehouse", str(TEST_DIR / "wh.duckdb"),
        "--quarantine-dir", str(TEST_DIR / "quarantine"),
        "--work-dir", str(TEST_DIR / "work"), *extra])


@pytest.fixture(scope="session")
def runs():
    """Happy path, rerun, increment, rerun of increment, for both pipelines (same order as the experiments)."""
    shutil.rmtree(TEST_DIR, ignore_errors=True)
    TEST_DIR.mkdir(parents=True)
    out = {}
    for p in ("etl", "elt"):
        out[f"{p}_1"] = run(p, f"t_{p}_1")
        out[f"{p}_2"] = run(p, f"t_{p}_2")
    out["snapshot_sept"] = snapshot()
    for p in ("etl", "elt"):
        out[f"{p}_inc"] = run(p, f"t_{p}_inc", "--increment")
        out[f"{p}_inc2"] = run(p, f"t_{p}_inc2", "--increment")
    yield out
    shutil.rmtree(TEST_DIR, ignore_errors=True)


def snapshot() -> dict:
    con = duckdb.connect(str(TEST_DIR / "wh.duckdb"), read_only=True)
    q = lambda t: con.execute(f"SELECT count(*), sum(net_revenue) FROM {t} WHERE order_date < DATE '2026-10-01' "
                              "AND order_status = 'DELIVERED'").fetchone()
    snap = {"etl": q("etl_fact_orders"), "elt": q("int_fact_orders")}
    con.close()
    return snap


@pytest.fixture
def con(runs):
    c = duckdb.connect(str(TEST_DIR / "wh.duckdb"), read_only=True)
    yield c
    c.close()
