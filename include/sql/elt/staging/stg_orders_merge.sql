-- STAGING: incremental MERGE of one order file into stg_orders, keyed on order_id.
--   new order_id                          -> INSERT
--   existing order_id, same/newer batch    -> UPDATE (corrections such as DELIVERED -> RETURNED)
--   existing order_id, older batch         -> skipped (a replayed old file never undoes a correction)
-- raw_orders keeps both the original and the corrected rows, so history stays auditable.

CREATE OR REPLACE TEMP TABLE _inc AS
SELECT order_id, order_ts, CAST(order_ts AS DATE) AS order_date, customer_id, store_id, product_id, quantity,
       TRY_CAST(unit_price AS DECIMAL(12,2)) AS unit_price, TRY_CAST(discount_pct AS DECIMAL(5,2)) AS discount_pct,
       TRY_CAST(order_amount AS DECIMAL(12,2)) AS order_amount, TRY_CAST(expected_amount AS DECIMAL(12,2)) AS expected_amount,
       payment_mode, order_status, channel, CAST(trunc(delivery_minutes_num) AS INTEGER) AS delivery_minutes,
       dq_status, failed_checks, failure_reason, warning_checks, batch_cutoff, stg_row_key,
       _source_file, _row_num, _raw_run_id, _raw_loaded_at
FROM stg_orders_versions
WHERE _source_file = {{ source_file }} AND order_id <> '' AND dup_rank = 1;

INSERT INTO elt_merge_log
SELECT {{ run_id }}, {{ source_file }},
       (SELECT count(*) FROM stg_orders_versions WHERE _source_file = {{ source_file }}),
       count(*) FILTER (WHERE t.order_id IS NULL),
       count(*) FILTER (WHERE t.order_id IS NOT NULL AND s.batch_cutoff >= t.batch_cutoff),
       count(*) FILTER (WHERE t.order_id IS NOT NULL AND s.batch_cutoff >= t.batch_cutoff
                          AND s.order_status IS DISTINCT FROM t.order_status),
       count(*) FILTER (WHERE t.order_id IS NOT NULL AND s.batch_cutoff < t.batch_cutoff),
       (SELECT count(*) FROM stg_orders_versions WHERE _source_file = {{ source_file }} AND dup_rank > 1 AND order_id <> ''),
       current_timestamp
FROM _inc s LEFT JOIN stg_orders t USING (order_id)
ON CONFLICT (run_id, source_file) DO UPDATE SET
    rows_in_file = excluded.rows_in_file, inserted = excluded.inserted, updated = excluded.updated,
    status_changed = excluded.status_changed, skipped_older = excluded.skipped_older,
    extra_copies = excluded.extra_copies, merged_at = excluded.merged_at;

MERGE INTO stg_orders AS t
USING _inc AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.batch_cutoff >= t.batch_cutoff THEN UPDATE SET
    order_ts = s.order_ts, order_date = s.order_date, customer_id = s.customer_id, store_id = s.store_id,
    product_id = s.product_id, quantity = s.quantity, unit_price = s.unit_price, discount_pct = s.discount_pct,
    order_amount = s.order_amount, expected_amount = s.expected_amount, payment_mode = s.payment_mode,
    order_status = s.order_status, channel = s.channel, delivery_minutes = s.delivery_minutes,
    dq_status = s.dq_status, failed_checks = s.failed_checks, failure_reason = s.failure_reason,
    warning_checks = s.warning_checks, batch_cutoff = s.batch_cutoff, stg_row_key = s.stg_row_key,
    _source_file = s._source_file, _row_num = s._row_num, _raw_run_id = s._raw_run_id,
    _raw_loaded_at = s._raw_loaded_at, previous_status = t.order_status,
    previous_stg_row_key = t.stg_row_key, last_merge_run_id = {{ run_id }}
WHEN NOT MATCHED THEN INSERT VALUES (
    s.order_id, s.order_ts, s.order_date, s.customer_id, s.store_id, s.product_id, s.quantity, s.unit_price,
    s.discount_pct, s.order_amount, s.expected_amount, s.payment_mode, s.order_status, s.channel,
    s.delivery_minutes, s.dq_status, s.failed_checks, s.failure_reason, s.warning_checks, s.batch_cutoff,
    s.stg_row_key, s._source_file, s._row_num, s._raw_run_id, s._raw_loaded_at, NULL, NULL, {{ run_id }});
