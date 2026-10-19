-- Shared observability tables used by both DAGs.
-- dq_results: one row per check, per source file, per run (the quality scorecard).
-- rows_checked / rows_failed count the population named in `population`
-- (source rows of that file in that run, unless stated otherwise).
CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR   NOT NULL,
    dag_id        VARCHAR   NOT NULL,
    check_name    VARCHAR   NOT NULL,
    dimension     VARCHAR   NOT NULL,   -- Completeness | Uniqueness | Validity | Accuracy | Consistency | Timeliness
    severity      VARCHAR   NOT NULL,   -- Critical | Warning
    rows_checked  BIGINT    NOT NULL,
    rows_failed   BIGINT    NOT NULL,
    status        VARCHAR   NOT NULL,   -- PASS | FAIL (critical rows failed) | WARN (warning rows failed)
    checked_at    TIMESTAMP NOT NULL,
    source_file   VARCHAR   NOT NULL,
    population    VARCHAR,
    details       VARCHAR,
    PRIMARY KEY (run_id, dag_id, check_name, source_file)
);

-- pipeline_runs: one row per DAG run, written by the trigger_rule=all_done audit task.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR   NOT NULL,
    dag_id            VARCHAR   NOT NULL,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    rows_extracted    BIGINT,
    rows_loaded       BIGINT,
    rows_quarantined  BIGINT,
    status            VARCHAR,          -- SUCCESS | SUCCESS_WITH_WARNINGS | FAILED
    failed_tasks      VARCHAR,
    warnings          VARCHAR,
    task_states       VARCHAR,          -- JSON {task_id: state}
    params            VARCHAR,          -- JSON of the run configuration
    PRIMARY KEY (dag_id, run_id)
);

-- Added after experiments 4/10: root cause separated from tasks that only failed upstream.
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS upstream_failed_tasks VARCHAR;

-- Append-only history of every audit write. pipeline_runs holds the latest state of a run;
-- a run that failed and was recovered with Clear keeps its FAILED attempt here (experiment 10).
CREATE TABLE IF NOT EXISTS pipeline_run_history (
    run_id VARCHAR, dag_id VARCHAR, recorded_at TIMESTAMP, start_time TIMESTAMP, end_time TIMESTAMP,
    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR,
    failed_tasks VARCHAR, upstream_failed_tasks VARCHAR, warnings VARCHAR, task_states VARCHAR
);
