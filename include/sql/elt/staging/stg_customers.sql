-- STAGING: customers. SQL-only cleaning of the latest raw load of customers.csv.
-- Same rules as include/nammamart_ashish_kamat/validate.py::validate_customers.
-- Raw name/email/phone are read here but only masked values are persisted.

CREATE OR REPLACE TEMP TABLE _cust AS
WITH latest AS (
    SELECT _source_file, arg_max(_run_id, _loaded_at) AS run_id
    FROM raw_customers WHERE _source_file = 'customers.csv' GROUP BY 1
), src AS (
    SELECT r.* FROM raw_customers r JOIN latest l ON r._source_file = l._source_file AND r._run_id = l.run_id
), grp AS (   -- which columns differ between copies of the same customer_id
    SELECT trim(customer_id) AS k,
           list_filter([CASE WHEN count(DISTINCT customer_id) > 1 THEN 'customer_id' END,
                        CASE WHEN count(DISTINCT full_name) > 1 THEN 'full_name' END,
                        CASE WHEN count(DISTINCT email) > 1 THEN 'email' END,
                        CASE WHEN count(DISTINCT phone) > 1 THEN 'phone' END,
                        CASE WHEN count(DISTINCT city) > 1 THEN 'city' END,
                        CASE WHEN count(DISTINCT area) > 1 THEN 'area' END,
                        CASE WHEN count(DISTINCT pincode) > 1 THEN 'pincode' END,
                        CASE WHEN count(DISTINCT signup_date) > 1 THEN 'signup_date' END,
                        CASE WHEN count(DISTINCT loyalty_tier) > 1 THEN 'loyalty_tier' END,
                        CASE WHEN count(DISTINCT is_active) > 1 THEN 'is_active' END],
                       lambda x: x IS NOT NULL) AS diff_cols
    FROM src GROUP BY 1
)
SELECT s.*,
       s._source_file || ':' || s._row_num                         AS stg_row_key,
       trim(s.customer_id)                                         AS cid,
       try_strptime(trim(s.signup_date), '%Y-%m-%d')::DATE         AS signup_dt,
       upper(left(trim(s.loyalty_tier), 1)) || lower(substr(trim(s.loyalty_tier), 2)) AS tier_std,
       row_number() OVER (PARTITION BY trim(s.customer_id) ORDER BY s._row_num) AS dup_rank,
       first_value(s._row_num) OVER (PARTITION BY trim(s.customer_id) ORDER BY s._row_num) AS kept_row,
       g.diff_cols
FROM src s LEFT JOIN grp g ON g.k = trim(s.customer_id);

DELETE FROM stg_dq_row_checks WHERE stg_table = 'stg_customers';
INSERT INTO stg_dq_row_checks
SELECT 'stg_customers', _source_file, stg_row_key, seq, check_name, dimension, severity, action, nullify_col,
       coalesce(in_pop, FALSE), coalesce(failed, FALSE) AND coalesce(in_pop, FALSE), detail, population
FROM (
    SELECT *, 1 AS seq, 'customers_customer_id_not_null' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity, 'reject' AS action,
           NULL AS nullify_col, TRUE AS in_pop, cid = '' AS failed, 'customer_id is blank' AS detail, 'source rows of customers.csv' AS population FROM _cust
    UNION ALL
    SELECT *, 2, 'customers_city_present', 'Completeness', 'Warning', 'warn', NULL,
           TRUE, trim(city) = '', 'city is blank', 'source rows of customers.csv' FROM _cust
    UNION ALL
    SELECT *, 3, 'customers_customer_id_unique', 'Uniqueness', 'Critical', 'reject', NULL,
           cid <> '', dup_rank > 1,
           CASE WHEN len(diff_cols) = 0 THEN 'exact duplicate of row ' || kept_row
                ELSE 'duplicate customer_id; differs from kept row ' || kept_row || ' in ' || array_to_string(diff_cols, ', ') END,
           'rows with non-blank customer_id' FROM _cust
    UNION ALL
    SELECT *, 4, 'customers_email_format', 'Validity', 'Warning', 'warn', NULL,
           TRUE, NOT regexp_full_match(trim(email), '[^@\s]+@[^@\s]+\.[A-Za-z]{2,}'),
           'email is not name@domain.tld (value withheld: PII)', 'source rows of customers.csv' FROM _cust
    UNION ALL
    SELECT *, 5, 'customers_phone_format', 'Validity', 'Warning', 'warn', NULL,
           TRUE, NOT regexp_full_match(trim(phone), '[6-9][0-9]{9}'),
           'phone has ' || length(trim(phone)) || ' digits, expected 10 starting 6-9 (value withheld: PII)',
           'source rows of customers.csv' FROM _cust
    UNION ALL
    SELECT *, 6, 'customers_pincode_format', 'Validity', 'Warning', 'warn', NULL,
           TRUE, NOT regexp_full_match(trim(pincode), '56[0-9]{4}'),
           'pincode ''' || pincode || ''' is not a 6-digit Bengaluru pincode', 'source rows of customers.csv' FROM _cust
    UNION ALL
    SELECT *, 7, 'customers_signup_date_valid', 'Validity', 'Critical', 'nullify', 'signup_date',
           TRUE, signup_dt IS NULL, 'signup_date ''' || signup_date || ''' not YYYY-MM-DD', 'source rows of customers.csv' FROM _cust
    UNION ALL
    SELECT *, 8, 'customers_loyalty_tier_allowed', 'Validity', 'Critical', 'nullify', 'loyalty_tier',
           TRUE, tier_std NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum'),
           'loyalty_tier ''' || loyalty_tier || ''' not allowed', 'source rows of customers.csv' FROM _cust
    UNION ALL
    SELECT *, 9, 'customers_is_active_flag', 'Validity', 'Warning', 'warn', NULL,
           TRUE, trim(is_active) NOT IN ('Y', 'N'), 'is_active ''' || is_active || ''' not Y/N', 'source rows of customers.csv' FROM _cust
    UNION ALL
    SELECT *, 10, 'customers_loyalty_tier_casing', 'Consistency', 'Warning', 'warn', NULL,
           TRUE, tier_std IN ('Bronze', 'Silver', 'Gold', 'Platinum') AND loyalty_tier <> tier_std,
           'loyalty_tier ''' || loyalty_tier || ''' standardised to ' || tier_std, 'source rows of customers.csv' FROM _cust
    UNION ALL
    SELECT *, 11, 'customers_signup_not_future', 'Timeliness', 'Critical', 'nullify', 'signup_date',
           TRUE, signup_dt > CAST({{ cutoff }} AS TIMESTAMP),
           'signup_date ' || signup_date || ' after batch cut-off ' || {{ cutoff }}, 'source rows of customers.csv' FROM _cust
);

CREATE OR REPLACE TABLE stg_customers AS
WITH agg AS (
    SELECT stg_row_key,
           bool_or(failed AND severity = 'Critical' AND action = 'reject')  AS is_rejected,
           bool_or(failed AND severity = 'Critical')                         AS has_critical,
           bool_or(failed AND severity = 'Warning')                          AS has_warning,
           list(DISTINCT nullify_col) FILTER (WHERE failed AND action = 'nullify') AS nullify_cols,
           string_agg(DISTINCT check_name, ';' ORDER BY check_name) FILTER (WHERE failed AND severity = 'Critical') AS failed_checks,
           string_agg(check_name || ': ' || detail, ' | ' ORDER BY check_seq) FILTER (WHERE failed AND severity = 'Critical') AS failure_reason,
           string_agg(DISTINCT check_name, ';' ORDER BY check_name) FILTER (WHERE failed AND severity = 'Warning') AS warning_checks
    FROM stg_dq_row_checks WHERE stg_table = 'stg_customers' GROUP BY stg_row_key
)
SELECT c.cid                                                                   AS customer_id,
       -- PII masking (same rules as include/nammamart_ashish_kamat/masking.py)
       nullif(array_to_string(list_transform(list_filter(regexp_split_to_array(trim(c.full_name), '\s+'), lambda w: w <> ''),
                                             lambda w: left(w, 1) || '***'), ' '), '') AS full_name_masked,
       CASE WHEN regexp_full_match(trim(c.email), '[^@\s]+@[^@\s]+\.[A-Za-z]{2,}')
            THEN left(split_part(trim(c.email), '@', 1), 2) || '***@' || split_part(trim(c.email), '@', 2) END AS email_masked,
       CASE WHEN regexp_full_match(trim(c.phone), '[6-9][0-9]{9}')
            THEN left(trim(c.phone), 2) || '******' || right(trim(c.phone), 2) END AS phone_masked,
       nullif(trim(c.city), '')                                                AS city,
       nullif(trim(c.area), '')                                                AS area,
       CASE WHEN regexp_full_match(trim(c.pincode), '56[0-9]{4}') THEN trim(c.pincode) END AS pincode,
       CASE WHEN list_contains(a.nullify_cols, 'signup_date') THEN NULL ELSE c.signup_dt END AS signup_date,
       CASE WHEN list_contains(a.nullify_cols, 'loyalty_tier') THEN NULL ELSE c.tier_std END AS loyalty_tier,
       CASE upper(trim(c.is_active)) WHEN 'Y' THEN TRUE WHEN 'N' THEN FALSE END AS is_active,
       CASE WHEN a.is_rejected THEN 'FAILED' WHEN a.has_critical THEN 'FAILED_ATTRIBUTE'
            WHEN a.has_warning THEN 'WARNING' ELSE 'PASSED' END                AS dq_status,
       coalesce(a.failed_checks, '')                                            AS failed_checks,
       coalesce(a.failure_reason, '')                                           AS failure_reason,
       coalesce(a.warning_checks, '')                                           AS warning_checks,
       c.dup_rank,
       c._source_file, c._row_num, c._run_id AS _raw_run_id, c._loaded_at AS _raw_loaded_at, c.stg_row_key
FROM _cust c JOIN agg a USING (stg_row_key)
ORDER BY c._row_num;
