"""
Renders REAL query results (production warehouse, read-only) and the real
quarantine CSV to simple HTML pages for screenshot evidence. Nothing is typed
in by hand: every cell comes from DuckDB / the CSV at render time.

  .venv/bin/python scripts/screenshots/render_query_results.py <out_dir>
"""

import html
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=True)
WH = ROOT / "include" / "warehouse_ashish_kamat.duckdb"
con = duckdb.connect(str(WH), read_only=True)

PAGES = {
    "dq_results": ("SELECT dag_id, run_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at "
                   "FROM dq_results WHERE run_id IN ('exp03_increment_etl', 'exp03_increment_elt') AND status <> 'PASS' "
                   "ORDER BY dag_id, source_file, check_name"),
    "pipeline_runs": ("SELECT run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status "
                      "FROM pipeline_runs ORDER BY dag_id, end_time"),
    "payment_reconciliation": ("SELECT reason, count(*) AS mismatches, sum(expected_amount) AS expected, "
                               "sum(actual_amount) AS actual, sum(unreconciled_amount) AS unreconciled "
                               "FROM etl_payment_reconciliation GROUP BY reason ORDER BY reason; "
                               "SELECT order_id, payment_ids, order_status, payment_status, payment_mode, expected_amount, "
                               "actual_amount, unreconciled_amount, reason FROM mart_payment_reconciliation "
                               "WHERE reason IN ('AMOUNT_MISMATCH','RETURNED_NOT_REFUNDED','DUPLICATE_PAYMENT_RECORD','ORPHAN_PAYMENT') "
                               "ORDER BY reason, order_id LIMIT 30"),
}


def page(title: str, blocks: list[tuple[str, pd.DataFrame]]) -> str:
    css = ("body{font:13px Menlo,monospace;margin:18px;background:#fff;color:#111}h1{font:600 16px sans-serif}"
           "table{border-collapse:collapse;margin:8px 0 18px}td,th{border:1px solid #bbb;padding:3px 7px;white-space:nowrap}"
           "th{background:#eef}pre{background:#f6f6f6;padding:6px;white-space:pre-wrap}")
    body = [f"<h1>{html.escape(title)}</h1><div>source: {html.escape(str(WH.relative_to(ROOT)))} (read-only) · "
            f"rendered {datetime.now():%Y-%m-%d %H:%M:%S}</div>"]
    for sql, df in blocks:
        body.append(f"<pre>{html.escape(sql)}</pre>")
        body.append(df.to_html(index=False, na_rep="NULL", border=0))
    return f"<!doctype html><html><head><meta charset='utf-8'><style>{css}</style></head><body>{''.join(body)}</body></html>"


for name, sql in PAGES.items():
    blocks = [(q.strip(), con.execute(q).df()) for q in sql.split(";") if q.strip()]
    (OUT / f"{name}.html").write_text(page(name, blocks))

qdir = ROOT / "include" / "quarantine" / "ashish_kamat"
qf = sorted(qdir.glob("orders_failed_scheduled__*.csv"))[0]
q = pd.read_csv(qf, dtype=str, keep_default_na=False)
cols = ["run_id", "source_row_num", "order_id", "order_ts", "customer_id", "store_id", "quantity", "order_amount",
        "order_status", "dq_status", "failure_reason"]
(OUT / "quarantine_file.html").write_text(page(f"quarantine file: {qf.relative_to(ROOT)} ({len(q)} rows, first 30 shown)",
                                               [(f"head -31 {qf.name}", q[cols].head(30))]))
print("rendered", sorted(p.name for p in OUT.glob("*.html")))
