-- STAGING: payments. Same rules as validate.py::validate_payments.
-- Orders reference = stg_orders (current version of every order_id, valid or not).

CREATE OR REPLACE TEMP TABLE _pay AS
WITH latest AS (
    SELECT _source_file, arg_max(_run_id, _loaded_at) AS run_id
    FROM raw_payments WHERE _source_file = 'payments.csv' GROUP BY 1
), src AS (
    SELECT r.* FROM raw_payments r JOIN latest l ON r._source_file = l._source_file AND r._run_id = l.run_id
), grp AS (
    SELECT trim(payment_id) AS k,
           list_filter([CASE WHEN count(DISTINCT payment_id) > 1 THEN 'payment_id' END,
                        CASE WHEN count(DISTINCT order_id) > 1 THEN 'order_id' END,
                        CASE WHEN count(DISTINCT payment_ts) > 1 THEN 'payment_ts' END,
                        CASE WHEN count(DISTINCT payment_mode) > 1 THEN 'payment_mode' END,
                        CASE WHEN count(DISTINCT amount_paid) > 1 THEN 'amount_paid' END,
                        CASE WHEN count(DISTINCT payment_status) > 1 THEN 'payment_status' END,
                        CASE WHEN count(DISTINCT gateway_ref) > 1 THEN 'gateway_ref' END],
                       lambda x: x IS NOT NULL) AS diff_cols
    FROM src GROUP BY 1
)
SELECT p.*,
       p._source_file || ':' || p._row_num                                  AS stg_row_key,
       trim(p.payment_id)                                                   AS pid,
       trim(p.order_id)                                                     AS oid,
       try_strptime(trim(p.payment_ts), '%Y-%m-%d %H:%M:%S')                AS pts,
       CASE WHEN regexp_full_match(trim(p.amount_paid), '-?[0-9]+(\.[0-9]+)?') THEN CAST(trim(p.amount_paid) AS DECIMAL(38,4)) END AS paid,
       upper(trim(p.payment_status))                                        AS pstatus,
       CASE lower(trim(p.payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'wallet' THEN 'Wallet' END                                 AS mode_std,
       o.order_id IS NOT NULL                                               AS order_known,
       o.order_amount AS o_amount, o.payment_mode AS o_mode, o.order_ts AS o_ts,
       row_number() OVER (PARTITION BY trim(p.payment_id) ORDER BY p._row_num) AS dup_rank,
       first_value(p._row_num) OVER (PARTITION BY trim(p.payment_id) ORDER BY p._row_num) AS kept_row,
       g.diff_cols
FROM src p
LEFT JOIN grp g ON g.k = trim(p.payment_id)
LEFT JOIN stg_orders o ON o.order_id = trim(p.order_id);

DELETE FROM stg_dq_row_checks WHERE stg_table = 'stg_payments';
INSERT INTO stg_dq_row_checks
SELECT 'stg_payments', _source_file, stg_row_key, seq, check_name, dimension, severity, action, NULL,
       coalesce(in_pop, FALSE), coalesce(failed, FALSE) AND coalesce(in_pop, FALSE), detail, population
FROM (
    SELECT _source_file, stg_row_key, 1 AS seq, 'payments_payment_id_not_null' AS check_name, 'Completeness' AS dimension,
           'Critical' AS severity, 'reject' AS action, TRUE AS in_pop, pid = '' AS failed,
           'payment_id is blank' AS detail, 'source rows of payments.csv' AS population FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 2, 'payments_order_id_not_null', 'Completeness', 'Critical', 'reject',
           TRUE, oid = '', 'order_id is blank', 'source rows of payments.csv' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 3, 'payments_gateway_ref_present', 'Completeness', 'Warning', 'warn',
           mode_std IS NOT NULL AND mode_std <> 'Cash on Delivery', trim(gateway_ref) = '',
           'non-COD payment without gateway_ref', 'non-COD payments' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 4, 'payments_payment_id_unique', 'Uniqueness', 'Critical', 'reject',
           pid <> '', dup_rank > 1,
           CASE WHEN len(diff_cols) = 0 THEN 'exact duplicate of row ' || kept_row
                ELSE 'duplicate payment_id; differs from kept row ' || kept_row || ' in ' || array_to_string(diff_cols, ', ') END,
           'rows with non-blank payment_id' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 5, 'payments_payment_ts_valid', 'Validity', 'Critical', 'reject',
           TRUE, pts IS NULL, 'payment_ts ''' || payment_ts || ''' is not YYYY-MM-DD HH:MM:SS', 'source rows of payments.csv' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 6, 'payments_amount_numeric', 'Validity', 'Critical', 'reject',
           TRUE, paid IS NULL, 'amount_paid ''' || amount_paid || ''' not numeric', 'source rows of payments.csv' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 7, 'payments_status_allowed', 'Validity', 'Critical', 'reject',
           TRUE, pstatus NOT IN ('SUCCESS', 'FAILED', 'REFUNDED'), 'payment_status ''' || payment_status || ''' not allowed',
           'source rows of payments.csv' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 8, 'payments_payment_mode_allowed', 'Validity', 'Critical', 'reject',
           TRUE, mode_std IS NULL, 'payment_mode ''' || payment_mode || ''' not mappable', 'source rows of payments.csv' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 9, 'payments_success_amount_positive', 'Accuracy', 'Critical', 'reject',
           pstatus = 'SUCCESS', paid <= 0, 'SUCCESS payment amount ' || amount_paid || ' <= 0', 'SUCCESS payments' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 10, 'payments_order_fk', 'Consistency', 'Critical', 'reject',
           oid <> '', NOT order_known, 'order_id ' || order_id || ' not in orders', 'rows with non-blank order_id' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 11, 'payments_success_amount_matches_order', 'Consistency', 'Critical', 'reject',
           pstatus = 'SUCCESS' AND order_known, abs(paid - o_amount) > 0.01,
           'amount_paid ' || amount_paid || ' != order_amount ' || o_amount, 'SUCCESS payments with a known order' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 12, 'payments_mode_matches_order', 'Consistency', 'Warning', 'warn',
           order_known, mode_std IS NOT NULL AND o_mode IS NOT NULL AND mode_std <> o_mode,
           'payment_mode ' || mode_std || ' != order payment_mode ' || o_mode, 'payments with a known order' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 13, 'payments_payment_mode_casing', 'Consistency', 'Warning', 'warn',
           TRUE, mode_std IS NOT NULL AND payment_mode <> mode_std, 'payment_mode ''' || payment_mode || ''' standardised',
           'source rows of payments.csv' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 14, 'payments_payment_ts_not_future', 'Timeliness', 'Critical', 'reject',
           TRUE, pts > CAST({{ cutoff }} AS TIMESTAMP), 'payment_ts ' || payment_ts || ' after batch cut-off ' || {{ cutoff }},
           'source rows of payments.csv' FROM _pay
    UNION ALL
    SELECT _source_file, stg_row_key, 15, 'payments_paid_after_order', 'Timeliness', 'Warning', 'warn',
           pts IS NOT NULL AND o_ts IS NOT NULL, pts < o_ts, 'payment_ts earlier than order_ts',
           'payments with valid payment_ts and order_ts' FROM _pay
);

CREATE OR REPLACE TABLE stg_payments AS
WITH agg AS (
    SELECT stg_row_key,
           bool_or(failed AND severity = 'Critical') AS is_rejected,
           bool_or(failed AND severity = 'Warning')  AS has_warning,
           string_agg(DISTINCT check_name, ';' ORDER BY check_name) FILTER (WHERE failed AND severity = 'Critical') AS failed_checks,
           string_agg(check_name || ': ' || detail, ' | ' ORDER BY check_seq) FILTER (WHERE failed AND severity = 'Critical') AS failure_reason,
           string_agg(DISTINCT check_name, ';' ORDER BY check_name) FILTER (WHERE failed AND severity = 'Warning') AS warning_checks
    FROM stg_dq_row_checks WHERE stg_table = 'stg_payments' GROUP BY stg_row_key
)
SELECT p.pid AS payment_id, p.oid AS order_id, p.pts AS payment_ts, p.mode_std AS payment_mode,
       CAST(p.paid AS DECIMAL(12,2)) AS amount_paid, p.pstatus AS payment_status,
       nullif(trim(p.gateway_ref), '') AS gateway_ref,
       CASE WHEN a.is_rejected THEN 'FAILED' WHEN a.has_warning THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       coalesce(a.failed_checks, '')  AS failed_checks,
       coalesce(a.failure_reason, '') AS failure_reason,
       coalesce(a.warning_checks, '') AS warning_checks,
       p.dup_rank, p._source_file, p._row_num, p._run_id AS _raw_run_id, p._loaded_at AS _raw_loaded_at, p.stg_row_key
FROM _pay p JOIN agg a USING (stg_row_key)
ORDER BY p._row_num;
