# Runbook: NammaMart ETL and ELT (Ashish Kamat)

## 1. Start the environment

```bash
astro dev start            # Docker Desktop must be running
astro dev run dags list-import-errors   # expect: "No data found"
```

Astro prints the Airflow UI URL. The default is `http://localhost:8080` (admin/admin). If another Astro project already uses 8080/5432, set different ports first: `astro config set webserver.port 8081 && astro config set postgres.port 5433`. On this machine the UI was assigned `http://localhost:16852`, because another project held 8080.

Nothing extra to install: the DAGs need only `pandas`, `duckdb` and `pendulum`, which `requirements.txt` already lists or the runtime image already includes.

## 2. Run the pipelines

Both DAGs are scheduled at **09:00 Asia/Kolkata, Monday to Saturday** (`0 9 * * 1-6`). Unpause them in the UI, or:

```bash
astro dev run dags unpause nammamart_etl_ashish_kamat
astro dev run dags unpause nammamart_elt_ashish_kamat
```

Manual runs:

```bash
astro dev run dags trigger nammamart_etl_ashish_kamat
astro dev run dags trigger nammamart_elt_ashish_kamat
# Incremental next-day file (orders_2026-10-01_increment.csv):
astro dev run dags trigger nammamart_etl_ashish_kamat --conf '{"apply_increment": true}'
astro dev run dags trigger nammamart_elt_ashish_kamat --conf '{"apply_increment": true}'
```

In the UI, use **Trigger DAG w/ config** and tick `apply_increment`.

| Param | Default | Purpose |
|---|---|---|
| `apply_increment` | false | Also load the incremental order file |
| `increment_file` | `orders_2026-10-01_increment.csv` | Must match `orders_YYYY-MM-DD_increment.csv`; its cut-off is D+1 09:00 |
| `as_of` | `2026-10-01 09:00:00` | Batch cut-off (IST) used by the September timeliness checks; pinned so replays are reproducible |
| `expect_reduced_volume` | false | Suppress the volume-drop alert for an intentional small test run (recorded as `SKIPPED`) |
| `orders_baseline` | null | Override the volume baseline (default: median of the last 7 passing runs, else 2,412) |
| `data_dir`, `warehouse_path`, `quarantine_dir` | null | Isolated experiment inputs and outputs; paths must stay inside `include/` |

## 3. Outputs

| What | Where |
|---|---|
| Warehouse (both pipelines, plus `dq_results` and `pipeline_runs`) | `include/warehouse_ashish_kamat.duckdb` (git-ignored: `raw_customers` holds PII) |
| Quarantine files (PII masked) | `include/quarantine/ashish_kamat/<file>_failed_<run_id>.csv` |
| Task logs | Airflow UI → run → task → Logs (`[extract]`, `[validate]`, `[dq]`, `[quarantine]`, `[load]`, `[reconcile]`, `[report]`, `[audit]` lines) |

Query the warehouse while no DAG is running (DuckDB allows one writer):

```bash
.venv/bin/python -c "import duckdb; c=duckdb.connect('include/warehouse_ashish_kamat.duckdb', read_only=True); print(c.sql('SELECT * FROM pipeline_runs ORDER BY end_time'))"
```

## 4. Reproduce the evidence

```bash
uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python pandas==2.3.3 duckdb==1.5.6 pendulum pytest tabulate
.venv/bin/python scripts/profile_nammamart.py          # docs/profiling/*
.venv/bin/python -m pytest tests/nammamart -q            # 27 end-to-end + unit tests (no Airflow needed)
astro dev pytest tests/dags                              # DAG structure tests inside the Airflow image
.venv/bin/python scripts/compare_etl_elt.py              # ETL vs ELT row-by-row comparison
.venv/bin/python scripts/business_questions.py           # docs/business_questions.md
.venv/bin/python scripts/evidence_production.py          # experiments 1-3 evidence
.venv/bin/python scripts/evidence_governance.py          # lineage / audit / change-history evidence
scripts/airflow_run_evidence.sh <dag_id> <run_id>        # task states, tries, durations from Airflow metadata
```

`scripts/run_pipeline_local.py etl|elt --run-id X [--increment] --warehouse include/tmp/x.duckdb` runs the same step functions without Airflow (used by the tests).

## 5. Observability experiments (isolated)

```bash
.venv/bin/python scripts/experiments.py prepare exp05_bad_rows       # builds include/experiments/exp05_bad_rows/data
C=$(.venv/bin/python scripts/experiments.py conf exp05_bad_rows etl)
astro dev run dags trigger nammamart_etl_ashish_kamat --run-id exp05_bad_rows_etl --conf "$C"
.venv/bin/python scripts/experiments.py evidence exp05_bad_rows etl
.venv/bin/python scripts/experiments.py restore exp05_bad_rows
.venv/bin/python scripts/experiments.py verify   # proves include/data/nammamart is byte-identical to the originals
```

Bonus SCD-2 test: run once with defaults, then with `prepare exp11_scd2_day2` data and `--conf '{"as_of": "2026-10-02 09:00:00", …}'`.

Data-quality trend chart: `.venv/bin/python scripts/dq_trend_chart.py` writes `docs/dq_trend.html`.

Experiments: `exp04_missing_file`, `exp05_bad_rows`, `exp06_schema_change`, `exp07_volume_drop`, `exp08_drift`, `exp08b_drift_consistent`, `exp09_freshness`.

## 6. Recovery (experiment 10)

Fix the cause, then in the UI open the failed run → failed task → **Clear** → tick *Downstream* and *Only failed*. Successful upstream tasks are not re-run: they keep their XComs, and the ETL work files are kept for failed runs. The API equivalent used in testing:

`POST /api/v2/dags/{dag_id}/clearTaskInstances {"dag_run_id": ..., "only_failed": true, "include_downstream": true, "reset_dag_runs": true}`

## 7. Alerts and keeping the laptop awake

On a laptop, macOS sleep stops the scheduler. On 19 Oct 2026 the 09:00 run started 23 minutes late and then timed out. While demonstrating locally, keep the machine plugged in and awake (`caffeinate -i`). Production needs an always-on host.


Set an Airflow Variable `SLACK_WEBHOOK_URL` (UI → Admin → Variables, or a secrets backend) to receive a Slack message when a task fails after its last retry. Without the Variable, the alert is only written to the task log. Never put the URL in code, `airflow_settings.yaml` or Git.
