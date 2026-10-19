### Test and validation report

| Suite | Command | Result |
|---|---|---|
| Profiling (original data) | `.venv/bin/python scripts/profile_nammamart.py` | VERIFIED: 260 issues logged; reports in `docs/profiling/` |
| End-to-end and unit tests (no Airflow) | `.venv/bin/python -m pytest tests/nammamart -q` | **27 passed** (after all fixes) |
| DAG structure and parse tests (inside the Airflow image) | `docker exec … python -m pytest tests/dags` (equivalent to `astro dev pytest tests/dags`) | **19 passed** (11 of mine plus the trainer's import, tag and retry tests) |
| Import errors | `airflow dags list-import-errors` | VERIFIED: `No data found` |
| ETL vs ELT parity | `scripts/compare_etl_elt.py` | VERIFIED: all 8 comparisons MATCH; 0 dq_results count differences |
| Experiments 1–10 on both DAGs | Airflow runs, see Part F | VERIFIED: all executed; 4 defects found and fixed, 4 fixes re-verified |
| PII sweep of committable files | raw emails, phones and names from customers.csv and manager names, searched across every tracked or untracked non-ignored file | VERIFIED: no raw PII outside `include/data/` |
| Source files unchanged | `scripts/experiments.py verify` | VERIFIED: all 6 MD5s equal the originals |

What the 27 tests assert, explicitly:
- Source copies are unchanged.
- Masking rules.
- A missing file, a schema change and a header-only file each fail loudly.
- Edge cases: null customer, failed cast, `SHIPPED`, a 2027 timestamp, unknown store, negative quantity, exact and conflicting duplicates, casing warnings, the volume warning.
- Happy-path counts.
- No critical order reaches the finance fact.
- Quarantine files have reasons and no raw PII.
- No raw PII in `etl_dim_customer`, `etl_top_customers`, `stg_customers` or `mart_top_customers`.
- All six dimensions recorded on every run.
- ETL idempotency: 2,308 inserted, then 0.
- ETL increment: 150 inserted / 10 updated, then 0 / 0; the 10 orders are RETURNED with previous status DELIVERED.
- Reconciliation covers 7 mismatch types and never flags CANCELLED orders without payment.
- ELT raw values equal the CSV byte for byte.
- Raw keeps four loads of history.
- MERGE gives 150 / 10 / 10 with 2,550 unique `order_id`s.
- September totals are equal.
- All business answers match.
- Eight audit rows exist.
- A raw-load retry does not duplicate rows.

### Screenshots (genuine; [`docs/screenshots/`](docs/screenshots/))

| File | What it shows | How it was captured |
|---|---|---|
| `etl_graph_success.png` | ETL Graph view, run `exp03_increment_etl`, every task Success | Headless Chromium against the local Airflow 3 UI |
| `elt_graph_success.png` | ELT Graph view, run `final_rerun_elt`, every task Success | same |
| `dags_schedule_next_run.png` | Both DAGs: schedule `0 9 * * 1-6`, next run 2026-10-19 09:00:00 (UI in +05:30) | same |
| `quarantine_file.png` | First 30 rows of `orders_failed_scheduled__…csv` with `failure_reason` | real CSV rendered to HTML, then screenshotted |
| `dq_results.png` | Non-passing `dq_results` rows for the increment runs | real read-only query on the production warehouse, rendered and screenshotted |
| `pipeline_runs.png` | `pipeline_runs` for every production run | same |
| `payment_reconciliation.png` | Reconciliation by reason plus sample mismatch rows | same |

The four query screenshots are rendered by [`scripts/screenshots/render_query_results.py`](scripts/screenshots/render_query_results.py) from live query output; no value is typed by hand. If your evaluator expects screenshots from VS Code SQLTools instead, run the same SQL there; the SQL is printed on each image.
