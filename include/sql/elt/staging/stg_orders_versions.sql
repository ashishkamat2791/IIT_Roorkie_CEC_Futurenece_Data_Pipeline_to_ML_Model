-- STAGING: every row of the latest load of orders.csv, typed and standardised.
-- stg_orders_versions keeps every source row (including bad rows and duplicate copies) so
-- dq checks see exactly what each file contained; stg_orders (one row per order_id) is built
-- from it by stg_orders_base.sql + stg_orders_merge.sql.
-- typed_orders(file, cutoff) is reused by stg_orders_versions_increment.sql in the same session.

CREATE OR REPLACE TEMP MACRO typed_orders(file, cutoff) AS TABLE
WITH latest AS (
    SELECT arg_max(_run_id, _loaded_at) AS run_id FROM raw_orders WHERE _source_file = file
), src AS (
    SELECT r.* FROM raw_orders r, latest l WHERE r._source_file = file AND r._run_id = l.run_id
), grp AS (
    SELECT trim(order_id) AS k,
           list_filter([CASE WHEN count(DISTINCT order_id) > 1 THEN 'order_id' END,
                        CASE WHEN count(DISTINCT order_ts) > 1 THEN 'order_ts' END,
                        CASE WHEN count(DISTINCT customer_id) > 1 THEN 'customer_id' END,
                        CASE WHEN count(DISTINCT store_id) > 1 THEN 'store_id' END,
                        CASE WHEN count(DISTINCT product_id) > 1 THEN 'product_id' END,
                        CASE WHEN count(DISTINCT quantity) > 1 THEN 'quantity' END,
                        CASE WHEN count(DISTINCT unit_price) > 1 THEN 'unit_price' END,
                        CASE WHEN count(DISTINCT discount_pct) > 1 THEN 'discount_pct' END,
                        CASE WHEN count(DISTINCT order_amount) > 1 THEN 'order_amount' END,
                        CASE WHEN count(DISTINCT payment_mode) > 1 THEN 'payment_mode' END,
                        CASE WHEN count(DISTINCT order_status) > 1 THEN 'order_status' END,
                        CASE WHEN count(DISTINCT channel) > 1 THEN 'channel' END,
                        CASE WHEN count(DISTINCT delivery_minutes) > 1 THEN 'delivery_minutes' END],
                       lambda x: x IS NOT NULL) AS diff_cols
    FROM src GROUP BY 1
), typed AS (
    SELECT
        s._source_file || ':' || s._row_num                                   AS stg_row_key,
        trim(s.order_id)                                                      AS order_id,
        try_strptime(trim(s.order_ts), '%Y-%m-%d %H:%M:%S')                   AS order_ts,
        trim(s.customer_id)                                                   AS customer_id,
        trim(s.store_id)                                                      AS store_id,
        trim(s.product_id)                                                    AS product_id,
        CASE WHEN regexp_full_match(trim(s.quantity), '-?[0-9]+') THEN TRY_CAST(trim(s.quantity) AS INTEGER) END AS quantity,
        CASE WHEN regexp_full_match(trim(s.quantity), '-?[0-9]+(\.[0-9]+)?') THEN CAST(trim(s.quantity) AS DECIMAL(38,4)) END AS quantity_num,
        CASE WHEN regexp_full_match(trim(s.unit_price), '-?[0-9]+(\.[0-9]+)?') THEN CAST(trim(s.unit_price) AS DECIMAL(38,4)) END AS unit_price,
        CASE WHEN regexp_full_match(trim(s.discount_pct), '-?[0-9]+(\.[0-9]+)?') THEN CAST(trim(s.discount_pct) AS DECIMAL(38,4)) END AS discount_pct,
        CASE WHEN regexp_full_match(trim(s.order_amount), '-?[0-9]+(\.[0-9]+)?') THEN CAST(trim(s.order_amount) AS DECIMAL(38,4)) END AS order_amount,
        CASE lower(trim(s.payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
             WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
             WHEN 'wallet' THEN 'Wallet' END                                  AS payment_mode,
        upper(trim(s.order_status))                                           AS order_status,
        nullif(trim(s.channel), '')                                           AS channel,
        CASE WHEN regexp_full_match(trim(s.delivery_minutes), '-?[0-9]+(\.[0-9]+)?') THEN CAST(trim(s.delivery_minutes) AS DECIMAL(38,4)) END AS delivery_minutes_num,
        s.order_ts AS src_order_ts, s.customer_id AS src_customer_id, s.store_id AS src_store_id,
        s.product_id AS src_product_id, s.quantity AS src_quantity, s.unit_price AS src_unit_price,
        s.discount_pct AS src_discount_pct, s.order_amount AS src_order_amount, s.payment_mode AS src_payment_mode,
        s.order_status AS src_order_status, s.delivery_minutes AS src_delivery_minutes,
        CAST(cutoff AS TIMESTAMP)                                             AS batch_cutoff,
        s._source_file, s._row_num, s._run_id AS _raw_run_id, s._loaded_at AS _raw_loaded_at,
        g.diff_cols
    FROM src s LEFT JOIN grp g ON g.k = trim(s.order_id)
)
SELECT t.*,
       -- exact decimal arithmetic, rounded half away from zero to 2 dp (= Python ROUND_HALF_UP)
       CAST(round(t.quantity_num * t.unit_price * (100 - t.discount_pct), 0) / 100 AS DECIMAL(18,2)) AS expected_amount,
       row_number() OVER (PARTITION BY t.order_id ORDER BY t._row_num)        AS dup_rank,
       first_value(t._row_num) OVER (PARTITION BY t.order_id ORDER BY t._row_num) AS kept_row
FROM typed t;

CREATE OR REPLACE TABLE stg_orders_versions AS
SELECT * FROM typed_orders('orders.csv', {{ cutoff }});
