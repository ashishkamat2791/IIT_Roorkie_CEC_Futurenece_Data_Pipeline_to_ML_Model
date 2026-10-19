-- STAGING: quality rules for every order row version (same rules as validate.py::validate_orders).
-- Masters: keys of stg_customers / stg_products / stg_stores rows that were not rejected.
-- Each row is checked against its own file's cut-off (batch_cutoff).

CREATE OR REPLACE TEMP TABLE _ov AS
SELECT v.*,
       cust.customer_id IS NOT NULL                                        AS customer_known,
       prod.product_id IS NOT NULL                                         AS product_known,
       st.store_id IS NOT NULL                                             AS store_known,
       v.unit_price / CASE WHEN prod.mrp_source_abs > 0 THEN prod.mrp_source_abs END AS price_ratio,
       prod.mrp_source_abs,
       (v.order_status = 'DELIVERED')                                      AS is_delivered,
       trim(v.src_delivery_minutes) = ''                                   AS dm_blank
FROM stg_orders_versions v
LEFT JOIN (SELECT DISTINCT customer_id FROM stg_customers WHERE dq_status <> 'FAILED') cust ON cust.customer_id = v.customer_id
LEFT JOIN (SELECT product_id, any_value(mrp_source_abs) AS mrp_source_abs FROM stg_products
           WHERE dq_status <> 'FAILED' GROUP BY product_id) prod ON prod.product_id = v.product_id
LEFT JOIN (SELECT DISTINCT store_id FROM stg_stores WHERE dq_status <> 'FAILED') st ON st.store_id = v.store_id;

DELETE FROM stg_dq_row_checks WHERE stg_table = 'stg_orders';
INSERT INTO stg_dq_row_checks
SELECT 'stg_orders', _source_file, stg_row_key, seq, check_name, dimension, severity, action, NULL,
       coalesce(in_pop, FALSE), coalesce(failed, FALSE) AND coalesce(in_pop, FALSE), detail, population
FROM (
    SELECT _source_file, stg_row_key, 1 AS seq, 'orders_order_id_not_null' AS check_name, 'Completeness' AS dimension,
           'Critical' AS severity, 'reject' AS action, TRUE AS in_pop, order_id = '' AS failed,
           'order_id is blank' AS detail, 'source rows of ' || _source_file AS population FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 2, 'orders_customer_id_not_null', 'Completeness', 'Critical', 'reject',
           TRUE, customer_id = '', 'customer_id is blank', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 3, 'orders_delivered_has_delivery_minutes', 'Completeness', 'Critical', 'reject',
           is_delivered, is_delivered AND dm_blank, 'DELIVERED order has no delivery_minutes', 'DELIVERED rows' FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 4, 'orders_order_id_unique', 'Uniqueness', 'Critical', 'reject',
           order_id <> '', dup_rank > 1,
           CASE WHEN len(diff_cols) = 0 THEN 'exact duplicate of row ' || kept_row
                ELSE 'duplicate order_id; differs from kept row ' || kept_row || ' in ' || array_to_string(diff_cols, ', ') END,
           'rows with non-blank order_id' FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 5, 'orders_order_ts_valid', 'Validity', 'Critical', 'reject',
           TRUE, order_ts IS NULL, 'order_ts ''' || src_order_ts || ''' is not YYYY-MM-DD HH:MM:SS', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 6, 'orders_status_allowed', 'Validity', 'Critical', 'reject',
           TRUE, order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED'),
           'order_status ''' || src_order_status || ''' not in DELIVERED/CANCELLED/RETURNED', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 7, 'orders_payment_mode_allowed', 'Validity', 'Critical', 'reject',
           TRUE, payment_mode IS NULL,
           'payment_mode ''' || src_payment_mode || ''' not mappable to UPI/Card/Cash on Delivery/Wallet', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 8, 'orders_numeric_fields_valid', 'Validity', 'Critical', 'reject',
           TRUE, quantity IS NULL OR unit_price IS NULL OR discount_pct IS NULL OR order_amount IS NULL
                 OR (NOT dm_blank AND delivery_minutes_num IS NULL),
           'quantity/unit_price/discount_pct/order_amount/delivery_minutes not numeric', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 9, 'orders_discount_pct_range', 'Validity', 'Critical', 'reject',
           TRUE, discount_pct < 0 OR discount_pct > 100, 'discount_pct ' || src_discount_pct || ' outside 0-100',
           'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 10, 'orders_quantity_positive', 'Accuracy', 'Critical', 'reject',
           TRUE, quantity_num <= 0, 'quantity ' || src_quantity || ' <= 0', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 11, 'orders_unit_price_positive', 'Accuracy', 'Critical', 'reject',
           TRUE, unit_price <= 0, 'unit_price ' || src_unit_price || ' <= 0', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 12, 'orders_amount_formula', 'Accuracy', 'Critical', 'reject',
           TRUE, abs(expected_amount - order_amount) > 0.01,
           'order_amount ' || src_order_amount || ' != qty*price*(1-disc/100) = ' || expected_amount, 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 13, 'orders_delivery_minutes_lt_120', 'Accuracy', 'Critical', 'reject',
           TRUE, delivery_minutes_num >= 120 OR delivery_minutes_num <= 0,
           'delivery_minutes ' || src_delivery_minutes || ' not in 1-119', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 14, 'orders_delivery_minutes_normal_range', 'Accuracy', 'Warning', 'warn',
           delivery_minutes_num IS NOT NULL,
           delivery_minutes_num > 0 AND delivery_minutes_num < 120 AND (delivery_minutes_num < 8 OR delivery_minutes_num > 35),
           'delivery_minutes ' || src_delivery_minutes || ' outside 8-35', 'rows with delivery_minutes' FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 15, 'orders_unit_price_vs_mrp', 'Accuracy', 'Warning', 'warn',
           price_ratio IS NOT NULL, price_ratio < 0.5 OR price_ratio > 1.5,
           'unit_price ' || src_unit_price || ' vs mrp ' || mrp_source_abs, 'rows whose product has a positive mrp' FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 16, 'orders_customer_fk', 'Consistency', 'Critical', 'reject',
           customer_id <> '', NOT customer_known, 'customer_id ' || src_customer_id || ' not in customer master',
           'rows with non-blank customer_id' FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 17, 'orders_product_fk', 'Consistency', 'Critical', 'reject',
           TRUE, NOT product_known, 'product_id ' || src_product_id || ' not in product master', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 18, 'orders_store_fk', 'Consistency', 'Critical', 'reject',
           TRUE, NOT store_known, 'store_id ' || src_store_id || ' not in store master', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 19, 'orders_status_casing', 'Consistency', 'Warning', 'warn',
           TRUE, order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED') AND src_order_status <> order_status,
           'order_status ''' || src_order_status || ''' standardised to ' || order_status, 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 20, 'orders_payment_mode_casing', 'Consistency', 'Warning', 'warn',
           TRUE, payment_mode IS NOT NULL AND src_payment_mode <> payment_mode,
           'payment_mode ''' || src_payment_mode || ''' standardised', 'source rows of ' || _source_file FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 21, 'orders_cancelled_without_delivery', 'Consistency', 'Warning', 'warn',
           order_status = 'CANCELLED', order_status = 'CANCELLED' AND NOT dm_blank,
           'CANCELLED order has delivery_minutes', 'CANCELLED rows' FROM _ov
    UNION ALL
    SELECT _source_file, stg_row_key, 22, 'orders_order_ts_not_future', 'Timeliness', 'Critical', 'reject',
           TRUE, order_ts > batch_cutoff, 'order_ts ' || src_order_ts || ' after batch cut-off ' || batch_cutoff,
           'source rows of ' || _source_file FROM _ov
    UNION ALL  -- added after experiment 8b: 10x away from catalogue mrp = scale/unit error
    SELECT _source_file, stg_row_key, 23, 'orders_unit_price_gross_outlier', 'Accuracy', 'Critical', 'reject',
           price_ratio IS NOT NULL, price_ratio < 0.1 OR price_ratio > 10,
           'unit_price ' || src_unit_price || ' is outside 0.1-10.0x of mrp ' || mrp_source_abs,
           'rows whose product has a positive mrp' FROM _ov
);

-- File-level checks: freshness, volume, price distribution per store, store capacity.
DELETE FROM stg_dq_dataset_checks WHERE stg_table = 'stg_orders';
INSERT INTO stg_dq_dataset_checks
WITH f AS (
    SELECT _source_file, any_value(batch_cutoff) AS cutoff, count(*) AS n,
           max(order_ts) FILTER (WHERE order_ts <= batch_cutoff) AS latest
    FROM _ov GROUP BY 1
), fresh AS (
    SELECT *, date_diff('second', latest, cutoff) / 3600.0 AS age_h FROM f
)
SELECT 'stg_orders', _source_file, 'orders_freshness', 'Timeliness', 'Warning', 1,
       CASE WHEN latest IS NULL OR age_h > 24 THEN 1 ELSE 0 END,
       CASE WHEN latest IS NULL OR age_h > 24 THEN 'WARN' ELSE 'PASS' END,
       CASE WHEN latest IS NULL THEN 'no valid order_ts'
            ELSE 'latest valid order_ts ' || latest || ' is ' || round(age_h, 1) || 'h before cut-off ' || cutoff
                 || ' (threshold 24h)' END,
       'dataset (1 = whole file)'
FROM fresh
UNION ALL
SELECT 'stg_orders', _source_file, 'orders_volume_vs_baseline', 'Completeness', 'Warning', n,
       CASE WHEN {{ expect_reduced_volume }} THEN 0 WHEN 1 - n / base > 0.9 THEN 1 ELSE 0 END,
       CASE WHEN {{ expect_reduced_volume }} THEN 'SKIPPED' WHEN 1 - n / base > 0.9 THEN 'WARN' ELSE 'PASS' END,
       'rows=' || n || ' baseline=' || base || ' (' || CASE WHEN {{ baseline }} > 0 THEN 'history median' ELSE 'documented default' END
       || ') drop=' || round(100 * (1 - n / base), 1) || '% threshold=90%'
       || CASE WHEN {{ expect_reduced_volume }} THEN ' | suppressed: run flagged expect_reduced_volume' ELSE '' END,
       'orders file row count vs baseline'
FROM (SELECT *, CASE WHEN {{ baseline }} > 0 THEN {{ baseline }} ELSE 2412 END::DOUBLE AS base FROM f) WHERE _source_file = 'orders.csv'
UNION ALL
SELECT 'stg_orders', _source_file, 'orders_store_price_distribution', 'Accuracy', 'Warning', count(*),
       count(*) FILTER (WHERE r > 1.5 OR r < 1 / 1.5),
       CASE WHEN count(*) FILTER (WHERE r > 1.5 OR r < 1 / 1.5) > 0 THEN 'WARN' ELSE 'PASS' END,
       'stores with mean unit_price/mrp outside [0.67, 1.5]: '
       || coalesce(string_agg(store_id || '=' || round(r, 2), ', ' ORDER BY store_id) FILTER (WHERE r > 1.5 OR r < 1 / 1.5), 'none'),
       'stores (mean unit_price/mrp per store)'
FROM (SELECT _source_file, store_id, avg(price_ratio) AS r FROM _ov WHERE price_ratio IS NOT NULL GROUP BY 1, 2)
GROUP BY _source_file
UNION ALL
SELECT 'stg_orders', d._source_file, 'orders_store_daily_capacity', 'Consistency', 'Warning',
       count(*) FILTER (WHERE s.capacity_orders_per_day > 0),
       count(*) FILTER (WHERE d.n > s.capacity_orders_per_day AND s.capacity_orders_per_day > 0),
       CASE WHEN count(*) FILTER (WHERE d.n > s.capacity_orders_per_day AND s.capacity_orders_per_day > 0) > 0 THEN 'WARN' ELSE 'PASS' END,
       'store-days over capacity: ' || count(*) FILTER (WHERE d.n > s.capacity_orders_per_day AND s.capacity_orders_per_day > 0)
       || '; max orders/store/day=' || max(d.n),
       'store-days with a known positive capacity'
FROM (SELECT _source_file, store_id, CAST(order_ts AS DATE) AS day, count(*) AS n FROM _ov
      WHERE order_ts IS NOT NULL GROUP BY 1, 2, 3) d
LEFT JOIN (SELECT store_id, any_value(capacity_orders_per_day) AS capacity_orders_per_day FROM stg_stores
           WHERE dq_status <> 'FAILED' GROUP BY 1) s USING (store_id)
GROUP BY d._source_file;

-- Persist the row-level outcome on every version.
CREATE OR REPLACE TEMP TABLE _ov_out AS
WITH agg AS (
    SELECT stg_row_key,
           bool_or(failed AND severity = 'Critical') AS is_rejected,
           bool_or(failed AND severity = 'Warning')  AS has_warning,
           string_agg(DISTINCT check_name, ';' ORDER BY check_name) FILTER (WHERE failed AND severity = 'Critical') AS failed_checks,
           string_agg(check_name || ': ' || detail, ' | ' ORDER BY check_seq) FILTER (WHERE failed AND severity = 'Critical') AS failure_reason,
           string_agg(DISTINCT check_name, ';' ORDER BY check_name) FILTER (WHERE failed AND severity = 'Warning') AS warning_checks
    FROM stg_dq_row_checks WHERE stg_table = 'stg_orders' GROUP BY stg_row_key
)
SELECT v.*,
       CASE WHEN a.is_rejected THEN 'FAILED' WHEN a.has_warning THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       coalesce(a.failed_checks, '')  AS failed_checks,
       coalesce(a.failure_reason, '') AS failure_reason,
       coalesce(a.warning_checks, '') AS warning_checks
FROM stg_orders_versions v JOIN agg a USING (stg_row_key);

CREATE OR REPLACE TABLE stg_orders_versions AS SELECT * FROM _ov_out ORDER BY _source_file, _row_num;
