-- MART Q6: rows rejected in the current staging snapshot, by source file and failed check.
-- A row that broke two rules counts under both checks; 'ALL (distinct rows)' counts each row once.
-- Rejected = failed a critical check (FAILED, or FAILED_ATTRIBUTE for dimension rows).
CREATE OR REPLACE TABLE mart_rejected_rows AS
WITH failed AS (
    SELECT _source_file AS source_file, failed_checks FROM stg_orders_versions WHERE failed_checks <> ''
    UNION ALL SELECT _source_file, failed_checks FROM stg_payments  WHERE failed_checks <> ''
    UNION ALL SELECT _source_file, failed_checks FROM stg_customers WHERE failed_checks <> ''
    UNION ALL SELECT _source_file, failed_checks FROM stg_products  WHERE failed_checks <> ''
    UNION ALL SELECT _source_file, failed_checks FROM stg_stores    WHERE failed_checks <> ''
)
SELECT {{ run_id }} AS run_id, source_file, check_name, count(*) AS rows_rejected
FROM (SELECT source_file, unnest(string_split(failed_checks, ';')) AS check_name FROM failed)
GROUP BY ALL
UNION ALL
SELECT {{ run_id }}, source_file, 'ALL (distinct rows)', count(*) FROM failed GROUP BY ALL
ORDER BY source_file, rows_rejected DESC, check_name;
