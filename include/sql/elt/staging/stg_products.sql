-- STAGING: products. Same rules as validate.py::validate_products.
-- Duplicate product_id: the current catalogue row wins over a superseded '(old)' row, then file order.

CREATE OR REPLACE TEMP TABLE _prod AS
WITH latest AS (
    SELECT _source_file, arg_max(_run_id, _loaded_at) AS run_id
    FROM raw_products WHERE _source_file = 'products.csv' GROUP BY 1
), src AS (
    SELECT r.* FROM raw_products r JOIN latest l ON r._source_file = l._source_file AND r._run_id = l.run_id
), grp AS (
    SELECT trim(product_id) AS k,
           list_filter([CASE WHEN count(DISTINCT product_id) > 1 THEN 'product_id' END,
                        CASE WHEN count(DISTINCT product_name) > 1 THEN 'product_name' END,
                        CASE WHEN count(DISTINCT category) > 1 THEN 'category' END,
                        CASE WHEN count(DISTINCT sub_category) > 1 THEN 'sub_category' END,
                        CASE WHEN count(DISTINCT brand) > 1 THEN 'brand' END,
                        CASE WHEN count(DISTINCT mrp) > 1 THEN 'mrp' END,
                        CASE WHEN count(DISTINCT cost_price) > 1 THEN 'cost_price' END,
                        CASE WHEN count(DISTINCT unit) > 1 THEN 'unit' END,
                        CASE WHEN count(DISTINCT is_perishable) > 1 THEN 'is_perishable' END,
                        CASE WHEN count(DISTINCT launch_date) > 1 THEN 'launch_date' END],
                       lambda x: x IS NOT NULL) AS diff_cols
    FROM src GROUP BY 1
), typed AS (
    SELECT s.*,
           s._source_file || ':' || s._row_num AS stg_row_key,
           trim(s.product_id) AS pid,
           CASE WHEN regexp_full_match(trim(s.mrp), '-?[0-9]+(\.[0-9]+)?') THEN CAST(trim(s.mrp) AS DECIMAL(18,4)) END AS mrp_num,
           CASE WHEN regexp_full_match(trim(s.cost_price), '-?[0-9]+(\.[0-9]+)?') THEN CAST(trim(s.cost_price) AS DECIMAL(18,4)) END AS cost_num,
           try_strptime(trim(s.launch_date), '%Y-%m-%d')::DATE AS launch_dt,
           (CASE WHEN ends_with(trim(s.product_name), '(old)') THEN 1000000 ELSE 0 END) + s._row_num AS keep_order,
           g.diff_cols
    FROM src s LEFT JOIN grp g ON g.k = trim(s.product_id)
)
SELECT *,
       row_number() OVER (PARTITION BY pid ORDER BY keep_order) AS dup_rank,
       first_value(_row_num) OVER (PARTITION BY pid ORDER BY keep_order) AS kept_row,
       count(DISTINCT trim(category)) FILTER (WHERE trim(category) <> '') OVER (PARTITION BY trim(sub_category)) AS cats_for_sub
FROM typed;

DELETE FROM stg_dq_row_checks WHERE stg_table = 'stg_products';
INSERT INTO stg_dq_row_checks
SELECT 'stg_products', _source_file, stg_row_key, seq, check_name, dimension, severity, action, nullify_col,
       coalesce(in_pop, FALSE), coalesce(failed, FALSE) AND coalesce(in_pop, FALSE), detail, population
FROM (
    SELECT *, 1 AS seq, 'products_product_id_not_null' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity, 'reject' AS action,
           NULL AS nullify_col, TRUE AS in_pop, pid = '' AS failed, 'product_id is blank' AS detail, 'source rows of products.csv' AS population FROM _prod
    UNION ALL
    SELECT *, 2, 'products_category_present', 'Completeness', 'Critical', 'nullify', 'category',
           TRUE, trim(category) = '', 'category blank (sub_category ' || sub_category || ')', 'source rows of products.csv' FROM _prod
    UNION ALL
    SELECT *, 3, 'products_product_id_unique', 'Uniqueness', 'Critical', 'reject', NULL,
           pid <> '', dup_rank > 1,
           CASE WHEN len(diff_cols) = 0 THEN 'exact duplicate of row ' || kept_row
                ELSE 'duplicate product_id; differs from kept row ' || kept_row || ' in ' || array_to_string(diff_cols, ', ') END,
           'rows with non-blank product_id' FROM _prod
    UNION ALL
    SELECT *, 4, 'products_prices_numeric', 'Validity', 'Critical', 'nullify', 'cost_price',
           TRUE, mrp_num IS NULL OR cost_num IS NULL, 'mrp or cost_price not numeric', 'source rows of products.csv' FROM _prod
    UNION ALL
    SELECT *, 5, 'products_launch_date_valid', 'Validity', 'Critical', 'nullify', 'launch_date',
           TRUE, launch_dt IS NULL, 'launch_date ''' || launch_date || ''' not YYYY-MM-DD', 'source rows of products.csv' FROM _prod
    UNION ALL
    SELECT *, 6, 'products_mrp_positive', 'Accuracy', 'Critical', 'nullify', 'mrp',
           TRUE, mrp_num <= 0, 'mrp ' || mrp || ' <= 0', 'source rows of products.csv' FROM _prod
    UNION ALL
    SELECT *, 7, 'products_cost_price_positive', 'Accuracy', 'Critical', 'nullify', 'cost_price',
           TRUE, cost_num <= 0, 'cost_price ' || cost_price || ' <= 0', 'source rows of products.csv' FROM _prod
    UNION ALL
    SELECT *, 8, 'products_cost_le_mrp', 'Accuracy', 'Critical', 'nullify', 'cost_price',
           mrp_num > 0, cost_num > mrp_num, 'cost_price ' || cost_price || ' > mrp ' || mrp, 'rows with positive mrp' FROM _prod
    UNION ALL
    SELECT *, 9, 'products_subcategory_single_category', 'Consistency', 'Warning', 'warn', NULL,
           trim(category) <> '', cats_for_sub > 1, 'sub_category ' || sub_category || ' maps to several categories',
           'rows with category' FROM _prod
    UNION ALL
    SELECT *, 10, 'products_launch_not_future', 'Timeliness', 'Critical', 'nullify', 'launch_date',
           TRUE, launch_dt > CAST({{ cutoff }} AS TIMESTAMP),
           'launch_date ' || launch_date || ' after batch cut-off ' || {{ cutoff }}, 'source rows of products.csv' FROM _prod
);

CREATE OR REPLACE TABLE stg_products AS
WITH agg AS (
    SELECT stg_row_key,
           bool_or(failed AND severity = 'Critical' AND action = 'reject')  AS is_rejected,
           bool_or(failed AND severity = 'Critical')                         AS has_critical,
           bool_or(failed AND severity = 'Warning')                          AS has_warning,
           list(DISTINCT nullify_col) FILTER (WHERE failed AND action = 'nullify') AS nullify_cols,
           string_agg(DISTINCT check_name, ';' ORDER BY check_name) FILTER (WHERE failed AND severity = 'Critical') AS failed_checks,
           string_agg(check_name || ': ' || detail, ' | ' ORDER BY check_seq) FILTER (WHERE failed AND severity = 'Critical') AS failure_reason,
           string_agg(DISTINCT check_name, ';' ORDER BY check_name) FILTER (WHERE failed AND severity = 'Warning') AS warning_checks
    FROM stg_dq_row_checks WHERE stg_table = 'stg_products' GROUP BY stg_row_key
)
SELECT p.pid                                                                 AS product_id,
       nullif(trim(p.product_name), '')                                      AS product_name,
       CASE WHEN list_contains(a.nullify_cols, 'category') THEN 'Unknown' ELSE trim(p.category) END AS category,
       nullif(trim(p.sub_category), '')                                      AS sub_category,
       nullif(trim(p.brand), '')                                             AS brand,
       CASE WHEN list_contains(a.nullify_cols, 'mrp') THEN NULL ELSE CAST(p.mrp_num AS DECIMAL(12,2)) END AS mrp,
       CASE WHEN list_contains(a.nullify_cols, 'cost_price') THEN NULL ELSE CAST(p.cost_num AS DECIMAL(12,2)) END AS cost_price,
       abs(p.mrp_num)                                                        AS mrp_source_abs,  -- shelf price reference for drift checks
       nullif(trim(p.unit), '')                                              AS unit,
       CASE upper(trim(p.is_perishable)) WHEN 'TRUE' THEN TRUE WHEN 'FALSE' THEN FALSE END AS is_perishable,
       CASE WHEN list_contains(a.nullify_cols, 'launch_date') THEN NULL ELSE p.launch_dt END AS launch_date,
       CASE WHEN a.is_rejected THEN 'FAILED' WHEN a.has_critical THEN 'FAILED_ATTRIBUTE'
            WHEN a.has_warning THEN 'WARNING' ELSE 'PASSED' END              AS dq_status,
       coalesce(a.failed_checks, '')  AS failed_checks,
       coalesce(a.failure_reason, '') AS failure_reason,
       coalesce(a.warning_checks, '') AS warning_checks,
       p.dup_rank, p._source_file, p._row_num, p._run_id AS _raw_run_id, p._loaded_at AS _raw_loaded_at, p.stg_row_key
FROM _prod p JOIN agg a USING (stg_row_key)
ORDER BY p._row_num;
