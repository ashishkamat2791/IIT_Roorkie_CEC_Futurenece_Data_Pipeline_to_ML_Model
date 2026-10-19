"""
scripts/compare_etl_elt.py
--------------------------
Compares the ETL and ELT outputs for the six business questions, row by row,
plus the data-quality scorecard of the latest run of each DAG.

Usage:
  .venv/bin/python scripts/compare_etl_elt.py [warehouse.duckdb] [--period 2026-09]
Exit code 1 if any business answer differs.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]

# (question, ETL query, ELT query). Both sides return identical column sets.
def queries(period_filter: str):
    return [
        ("Q1 daily net revenue by zone and store",
         f"SELECT order_date, zone, store_id, delivered_orders, net_revenue FROM etl_daily_zone_revenue {period_filter} ORDER BY ALL",
         f"SELECT order_date, zone, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis {period_filter} ORDER BY ALL"),
        ("Q2 revenue and gross margin by category",
         "SELECT category, delivered_orders, units, net_revenue, cost_amount, gross_margin, margin_pct, orders_without_cost FROM etl_category_margin ORDER BY ALL",
         "SELECT category, delivered_orders, units, net_revenue, cost_amount, gross_margin, margin_pct, orders_without_cost FROM mart_category_margin ORDER BY ALL"),
        ("Q3 delivery SLA per store",
         "SELECT store_id, zone, delivered_orders, avg_delivery_minutes, pct_within_15_min FROM etl_delivery_sla ORDER BY ALL",
         "SELECT store_id, zone, delivered_orders, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY ALL"),
        ("Q4a revenue by payment mode + unreconciled",
         "SELECT * FROM etl_payment_mode_revenue ORDER BY ALL",
         "SELECT * FROM mart_payment_mode_revenue ORDER BY ALL"),
        ("Q4b reconciliation rows",
         "SELECT order_id, payment_ids, order_status, payment_status, payment_mode, expected_amount, actual_amount, unreconciled_amount, reason FROM etl_payment_reconciliation ORDER BY ALL",
         "SELECT order_id, payment_ids, order_status, payment_status, payment_mode, expected_amount, actual_amount, unreconciled_amount, reason FROM mart_payment_reconciliation ORDER BY ALL"),
        ("Q5 top 10 customers",
         "SELECT spend_rank, customer_id, full_name_masked, email_masked, phone_masked, loyalty_tier, delivered_orders, net_revenue FROM etl_top_customers ORDER BY ALL",
         "SELECT spend_rank, customer_id, full_name_masked, email_masked, phone_masked, loyalty_tier, delivered_orders, net_revenue FROM mart_top_customers ORDER BY ALL"),
        ("Q6 rejected rows by file and check (latest ETL run vs current ELT staging)",
         "SELECT source_file, check_name, rows_rejected FROM etl_rejected_rows WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id='nammamart_etl_ashish_kamat' AND status <> 'FAILED' ORDER BY end_time DESC LIMIT 1) ORDER BY ALL",
         "SELECT source_file, check_name, rows_rejected FROM mart_rejected_rows WHERE source_file IN (SELECT DISTINCT source_file FROM etl_rejected_rows WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id='nammamart_etl_ashish_kamat' AND status <> 'FAILED' ORDER BY end_time DESC LIMIT 1)) ORDER BY ALL"),
        ("Bonus SCD2 customer history (all versions)",
         "SELECT customer_id, loyalty_tier, is_active, valid_from, valid_to, is_current, change_type FROM etl_dim_customer_scd2 ORDER BY ALL",
         "SELECT customer_id, loyalty_tier, is_active, valid_from, valid_to, is_current, change_type FROM elt_dim_customer_scd2 ORDER BY ALL"),
        ("Fact rows (order_id, status, net_revenue, gross_margin, within_15_min)",
         "SELECT order_id, order_status, net_revenue, gross_margin, within_15_min, delivery_minutes FROM etl_fact_orders ORDER BY ALL",
         "SELECT order_id, order_status, net_revenue, gross_margin, within_15_min, delivery_minutes FROM int_fact_orders ORDER BY ALL"),
    ]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("warehouse", nargs="?", default=str(ROOT / "include" / "warehouse_ashish_kamat.duckdb"))
    ap.add_argument("--period", default=None, help="YYYY-MM to restrict Q1 to one month")
    a = ap.parse_args()
    con = duckdb.connect(a.warehouse, read_only=True)
    pf = f"WHERE strftime(order_date, '%Y-%m') = '{a.period}'" if a.period else ""
    ok = True
    print(f"warehouse: {a.warehouse}\n")
    for name, q_etl, q_elt in queries(pf):
        etl, elt = con.execute(q_etl).fetchall(), con.execute(q_elt).fetchall()
        only_etl = sorted(set(etl) - set(elt))
        only_elt = sorted(set(elt) - set(etl))
        same = not only_etl and not only_elt and len(etl) == len(elt)
        ok &= same
        print(f"{'MATCH' if same else 'DIFF '}  {name}: ETL rows={len(etl)} ELT rows={len(elt)}")
        for r in only_etl[:5]:
            print("        only ETL:", r)
        for r in only_elt[:5]:
            print("        only ELT:", r)
    print()
    tot = """SELECT count(*) FILTER (WHERE order_status='DELIVERED'), sum(net_revenue), sum(gross_margin),
                    count(*) FILTER (WHERE gross_margin IS NULL AND order_status='DELIVERED')
             FROM {t} WHERE strftime(order_date, '%Y-%m') = '2026-09'"""
    for label, t in [("ETL", "etl_fact_orders"), ("ELT", "int_fact_orders")]:
        r = con.execute(tot.format(t=t)).fetchone()
        print(f"September {label}: delivered_orders={r[0]} net_revenue={r[1]} gross_margin={r[2]} delivered_without_cost={r[3]}")
    print()
    dq = con.execute("""
        WITH latest AS (
            SELECT dag_id, arg_max(run_id, checked_at) AS run_id FROM dq_results GROUP BY dag_id)
        SELECT d.source_file, d.check_name, d.dimension, d.severity,
               max(rows_failed) FILTER (WHERE d.dag_id LIKE '%etl%') AS etl_failed,
               max(rows_failed) FILTER (WHERE d.dag_id LIKE '%elt%') AS elt_failed,
               max(rows_checked) FILTER (WHERE d.dag_id LIKE '%etl%') AS etl_checked,
               max(rows_checked) FILTER (WHERE d.dag_id LIKE '%elt%') AS elt_checked
        FROM dq_results d JOIN latest l USING (dag_id, run_id)
        GROUP BY ALL ORDER BY 1, 2""").fetchall()
    diff = [r for r in dq if (r[4], r[6]) != (r[5], r[7])]
    print(f"dq_results latest run per DAG: {len(dq)} (file, check) pairs, {len(diff)} with different counts")
    for r in diff:
        print("   ", r)
    print("\nRESULT:", "ETL and ELT business answers MATCH" if ok else "ETL and ELT business answers DIFFER")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
