"""Evidence for governance answers: lineage (E3), rejected-order audit (E8), change history (E9). Read-only."""
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
c = duckdb.connect(str(ROOT / "include" / "warehouse_ashish_kamat.duckdb"), read_only=True)
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30); pd.set_option("display.max_colwidth", 140)


def show(title, q, params=None):
    print(f"\n-- {title}\n{q.strip()}\n")
    print(c.execute(q, params or []).df().to_string(index=False))


print("=== E3 LINEAGE: mart_daily_store_kpis S01 on 2026-09-15")
show("Mart value", "SELECT order_date, zone, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis WHERE store_id = 'S01' AND order_date = DATE '2026-09-15'")
show("ELT: contributing rows -> staging version -> raw row", """
SELECT f.order_id, f.net_revenue, f.stg_row_key, r._source_file, r._row_num, r._run_id, r.order_amount AS raw_order_amount, r.order_status AS raw_status
FROM int_fact_orders f
JOIN raw_orders r ON r._source_file = f.source_file AND r._row_num = f.source_row_num AND r._run_id = f._raw_run_id
WHERE f.store_id = 'S01' AND f.order_date = DATE '2026-09-15' AND f.order_status = 'DELIVERED' ORDER BY 1""")
show("ETL: contributing fact rows with source_file + source_row_num", """
SELECT order_id, net_revenue, source_file, source_row_num, loaded_run_id FROM etl_fact_orders
WHERE store_id = 'S01' AND order_date = DATE '2026-09-15' AND order_status = 'DELIVERED' ORDER BY 1""")

print("\n=== E8 AUDIT: rejected order ORD000462")
show("ELT raw: exactly as received (every load kept)", "SELECT * FROM raw_orders WHERE order_id = 'ORD000462' ORDER BY _loaded_at")
show("ELT staging: rule broken", "SELECT order_id, dq_status, failed_checks, failure_reason, stg_row_key FROM stg_orders WHERE order_id = 'ORD000462'")
show("Not in either finance table", "SELECT (SELECT count(*) FROM etl_fact_orders WHERE order_id='ORD000462') AS in_etl_fact, (SELECT count(*) FROM int_fact_orders WHERE order_id='ORD000462') AS in_elt_fact")
show("CFO revenue total excludes it (sum over fact = sum over Q1 report)", "SELECT (SELECT sum(net_revenue) FROM etl_fact_orders WHERE order_status='DELIVERED') AS etl_fact_total, (SELECT sum(net_revenue) FROM etl_daily_zone_revenue) AS etl_report_total, (SELECT sum(net_revenue) FROM mart_daily_store_kpis) AS elt_mart_total")
show("Its payment is surfaced in reconciliation instead", "SELECT order_id, payment_ids, payment_status, actual_amount, reason FROM etl_payment_reconciliation WHERE order_id = 'ORD000462'")

print("\n=== E9 CHANGE HISTORY: September revenue as-originally-reported vs restated")
show("Restated (current state) vs as-originally-reported (versions from orders.csv only)", """
WITH original AS (
  SELECT sum(CASE WHEN order_status = 'DELIVERED' THEN order_amount ELSE 0 END) AS rev, count(*) FILTER (WHERE order_status='DELIVERED') AS n
  FROM stg_orders_versions WHERE _source_file = 'orders.csv' AND dq_status IN ('PASSED','WARNING') AND order_ts < TIMESTAMP '2026-10-01'
), restated AS (
  SELECT sum(net_revenue) AS rev, count(*) FILTER (WHERE order_status='DELIVERED') AS n FROM int_fact_orders WHERE order_date < DATE '2026-10-01'
)
SELECT o.n AS original_delivered, o.rev AS original_sept_revenue, r.n AS restated_delivered, r.rev AS restated_sept_revenue, o.rev - r.rev AS returned_value
FROM original o, restated r""")
show("The corrected orders and when the change arrived", "SELECT order_id, order_date, order_amount, previous_status, order_status, previous_stg_row_key, stg_row_key, last_merge_run_id FROM stg_orders WHERE previous_status IS NOT NULL ORDER BY order_id")
