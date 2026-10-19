-- STAGING: stores. Same rules as validate.py::validate_stores.
-- manager_name (employee personal data) is not carried into staging.

CREATE OR REPLACE TEMP TABLE _store AS
WITH latest AS (
    SELECT _source_file, arg_max(_run_id, _loaded_at) AS run_id
    FROM raw_stores WHERE _source_file = 'stores.csv' GROUP BY 1
), src AS (
    SELECT r.* FROM raw_stores r JOIN latest l ON r._source_file = l._source_file AND r._run_id = l.run_id
), grp AS (
    SELECT trim(store_id) AS k,
           list_filter([CASE WHEN count(DISTINCT store_id) > 1 THEN 'store_id' END,
                        CASE WHEN count(DISTINCT store_name) > 1 THEN 'store_name' END,
                        CASE WHEN count(DISTINCT zone) > 1 THEN 'zone' END,
                        CASE WHEN count(DISTINCT area) > 1 THEN 'area' END,
                        CASE WHEN count(DISTINCT pincode) > 1 THEN 'pincode' END,
                        CASE WHEN count(DISTINCT open_date) > 1 THEN 'open_date' END,
                        CASE WHEN count(DISTINCT manager_name) > 1 THEN 'manager_name' END,
                        CASE WHEN count(DISTINCT capacity_orders_per_day) > 1 THEN 'capacity_orders_per_day' END],
                       lambda x: x IS NOT NULL) AS diff_cols
    FROM src GROUP BY 1
)
SELECT s.*,
       s._source_file || ':' || s._row_num AS stg_row_key,
       trim(s.store_id) AS sid,
       upper(left(trim(s.zone), 1)) || lower(substr(trim(s.zone), 2)) AS zone_std,
       CASE WHEN regexp_full_match(trim(s.capacity_orders_per_day), '-?[0-9]+(\.[0-9]+)?')
            THEN CAST(trim(s.capacity_orders_per_day) AS DECIMAL(18,4)) END AS cap_num,
       try_strptime(trim(s.open_date), '%Y-%m-%d')::DATE AS open_dt,
       row_number() OVER (PARTITION BY trim(s.store_id) ORDER BY s._row_num) AS dup_rank,
       first_value(s._row_num) OVER (PARTITION BY trim(s.store_id) ORDER BY s._row_num) AS kept_row,
       g.diff_cols
FROM src s LEFT JOIN grp g ON g.k = trim(s.store_id);

DELETE FROM stg_dq_row_checks WHERE stg_table = 'stg_stores';
INSERT INTO stg_dq_row_checks
SELECT 'stg_stores', _source_file, stg_row_key, seq, check_name, dimension, severity, action, nullify_col,
       coalesce(in_pop, FALSE), coalesce(failed, FALSE) AND coalesce(in_pop, FALSE), detail, population
FROM (
    SELECT *, 1 AS seq, 'stores_store_id_not_null' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity, 'reject' AS action,
           NULL AS nullify_col, TRUE AS in_pop, sid = '' AS failed, 'store_id is blank' AS detail, 'source rows of stores.csv' AS population FROM _store
    UNION ALL
    SELECT *, 2, 'stores_zone_present', 'Completeness', 'Critical', 'nullify', 'zone',
           TRUE, trim(zone) = '', 'zone is blank', 'source rows of stores.csv' FROM _store
    UNION ALL
    SELECT *, 3, 'stores_store_id_unique', 'Uniqueness', 'Critical', 'reject', NULL,
           sid <> '', dup_rank > 1,
           CASE WHEN len(diff_cols) = 0 THEN 'exact duplicate of row ' || kept_row
                ELSE 'duplicate store_id; differs from kept row ' || kept_row || ' in ' || array_to_string(diff_cols, ', ') END,
           'rows with non-blank store_id' FROM _store
    UNION ALL
    SELECT *, 4, 'stores_zone_allowed', 'Validity', 'Critical', 'nullify', 'zone',
           trim(zone) <> '', zone_std NOT IN ('North', 'South', 'East', 'West'),
           'zone ''' || zone || ''' not North/South/East/West', 'rows with zone' FROM _store
    UNION ALL
    SELECT *, 5, 'stores_pincode_format', 'Validity', 'Warning', 'warn', NULL,
           TRUE, NOT regexp_full_match(trim(pincode), '56[0-9]{4}'), 'pincode ''' || pincode || ''' invalid',
           'source rows of stores.csv' FROM _store
    UNION ALL
    SELECT *, 6, 'stores_capacity_positive', 'Accuracy', 'Critical', 'nullify', 'capacity_orders_per_day',
           TRUE, cap_num IS NULL OR cap_num <= 0,
           'capacity_orders_per_day ''' || capacity_orders_per_day || ''' <= 0', 'source rows of stores.csv' FROM _store
    UNION ALL
    SELECT *, 7, 'stores_name_matches_area', 'Consistency', 'Warning', 'warn', NULL,
           TRUE, trim(store_name) <> 'NammaMart ' || trim(area), 'store_name does not match area',
           'source rows of stores.csv' FROM _store
    UNION ALL
    SELECT *, 8, 'stores_open_date_not_future', 'Timeliness', 'Critical', 'nullify', 'open_date',
           TRUE, open_dt IS NULL OR open_dt > CAST({{ cutoff }} AS TIMESTAMP),
           'open_date ''' || open_date || ''' invalid or after cut-off', 'source rows of stores.csv' FROM _store
);

CREATE OR REPLACE TABLE stg_stores AS
WITH agg AS (
    SELECT stg_row_key,
           bool_or(failed AND severity = 'Critical' AND action = 'reject')  AS is_rejected,
           bool_or(failed AND severity = 'Critical')                         AS has_critical,
           bool_or(failed AND severity = 'Warning')                          AS has_warning,
           list(DISTINCT nullify_col) FILTER (WHERE failed AND action = 'nullify') AS nullify_cols,
           string_agg(DISTINCT check_name, ';' ORDER BY check_name) FILTER (WHERE failed AND severity = 'Critical') AS failed_checks,
           string_agg(check_name || ': ' || detail, ' | ' ORDER BY check_seq) FILTER (WHERE failed AND severity = 'Critical') AS failure_reason,
           string_agg(DISTINCT check_name, ';' ORDER BY check_name) FILTER (WHERE failed AND severity = 'Warning') AS warning_checks
    FROM stg_dq_row_checks WHERE stg_table = 'stg_stores' GROUP BY stg_row_key
)
SELECT s.sid                                                   AS store_id,
       nullif(trim(s.store_name), '')                          AS store_name,
       CASE WHEN list_contains(a.nullify_cols, 'zone') THEN 'Unknown' ELSE s.zone_std END AS zone,
       nullif(trim(s.area), '')                                AS area,
       nullif(trim(s.pincode), '')                             AS pincode,
       CASE WHEN list_contains(a.nullify_cols, 'open_date') THEN NULL ELSE s.open_dt END AS open_date,
       CASE WHEN list_contains(a.nullify_cols, 'capacity_orders_per_day') THEN NULL
            ELSE CAST(trunc(s.cap_num) AS INTEGER) END         AS capacity_orders_per_day,
       CASE WHEN a.is_rejected THEN 'FAILED' WHEN a.has_critical THEN 'FAILED_ATTRIBUTE'
            WHEN a.has_warning THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       coalesce(a.failed_checks, '')  AS failed_checks,
       coalesce(a.failure_reason, '') AS failure_reason,
       coalesce(a.warning_checks, '') AS warning_checks,
       s.dup_rank, s._source_file, s._row_num, s._run_id AS _raw_run_id, s._loaded_at AS _raw_loaded_at, s.stg_row_key
FROM _store s JOIN agg a USING (stg_row_key)
ORDER BY s._row_num;
