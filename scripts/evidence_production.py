"""Evidence queries for experiments 1-3 on the production warehouse (read-only)."""
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
c = duckdb.connect(str(ROOT / "include" / "warehouse_ashish_kamat.duckdb"), read_only=True)


def show(title, q):
    print(f"\n-- {title}\n{q.strip()}\n")
    print(c.sql(q).df().to_string(index=False))


show("pipeline_runs", "SELECT dag_id, run_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status, failed_tasks, warnings FROM pipeline_runs ORDER BY dag_id, end_time")
show("ETL fact load log per run (exp01 insert, exp02 no-op, exp03 increment)", "SELECT run_id, source_file, inserted, updated, unchanged, skipped_older, deleted FROM etl_load_log ORDER BY loaded_at")
show("ELT MERGE log", "SELECT run_id, source_file, rows_in_file, inserted, updated, status_changed, skipped_older, extra_copies FROM elt_merge_log ORDER BY merged_at")
show("ELT raw history: rows per table per run (append-only)", "SELECT 'raw_orders' AS tbl, _run_id, _source_file, count(*) AS n FROM raw_orders GROUP BY ALL UNION ALL SELECT 'raw_payments', _run_id, _source_file, count(*) FROM raw_payments GROUP BY ALL UNION ALL SELECT 'raw_customers', _run_id, _source_file, count(*) FROM raw_customers GROUP BY ALL ORDER BY 1, 2, 3")
show("Fact / staging uniqueness", "SELECT 'etl_fact_orders' AS tbl, count(*) AS n_rows, count(DISTINCT order_id) AS n_ids FROM etl_fact_orders UNION ALL SELECT 'stg_orders', count(*), count(DISTINCT order_id) FROM stg_orders UNION ALL SELECT 'int_fact_orders', count(*), count(DISTINCT order_id) FROM int_fact_orders")
show("Corrected orders in both pipelines", "SELECT e.order_id, e.order_status AS etl_status, e.previous_status AS etl_prev, e.updated_run_id, s.order_status AS elt_status, s.previous_status AS elt_prev, s.stg_row_key AS elt_version FROM etl_fact_orders e JOIN stg_orders s USING (order_id) WHERE e.previous_status IS NOT NULL ORDER BY 1")
show("Original DELIVERED rows still auditable in raw (ORD000017)", "SELECT order_id, order_status, _source_file, _run_id, _row_num FROM raw_orders WHERE order_id = 'ORD000017' ORDER BY _loaded_at, _source_file")
show("dq_results scorecard trend (failed rows per dimension per run)", "SELECT dag_id, run_id, dimension, sum(rows_failed) AS rows_failed, count(*) FILTER (WHERE status <> 'PASS') AS checks_not_passing FROM dq_results GROUP BY ALL ORDER BY dag_id, min(checked_at), dimension")
