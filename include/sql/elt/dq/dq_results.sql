-- Publish this run's quality scorecard into dq_results (shared with the ETL DAG).
-- rows_checked / rows_failed count staging rows of the named source file inside the
-- check's population; each source row is counted once (no joins fan rows out).
DELETE FROM dq_results WHERE run_id = {{ run_id }} AND dag_id = {{ dag_id }};

INSERT INTO dq_results
SELECT {{ run_id }}, {{ dag_id }}, check_name, dimension, severity,
       count(*) FILTER (WHERE in_population)              AS rows_checked,
       count(*) FILTER (WHERE failed)                     AS rows_failed,
       CASE WHEN count(*) FILTER (WHERE failed) = 0 THEN 'PASS'
            WHEN severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END AS status,
       CAST({{ checked_at }} AS TIMESTAMP), source_file, any_value(population), ''
FROM stg_dq_row_checks
GROUP BY check_name, dimension, severity, source_file
UNION ALL
SELECT {{ run_id }}, {{ dag_id }}, check_name, dimension, severity, rows_checked, rows_failed, status,
       CAST({{ checked_at }} AS TIMESTAMP), source_file, population, details
FROM stg_dq_dataset_checks;
