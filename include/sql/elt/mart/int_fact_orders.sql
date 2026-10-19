-- MART (intermediate): finance-grade order facts from staging.
-- Only current order versions that passed every critical check (PASSED / WARNING).
-- Same derivations as the ETL fact: revenue only for DELIVERED, after discount;
-- margin = revenue - quantity x cost_price (NULL when the product cost failed a check).
CREATE OR REPLACE TABLE int_fact_orders AS
SELECT o.order_id, o.order_ts, o.order_date, o.customer_id, o.store_id, o.product_id, o.quantity,
       o.unit_price, o.discount_pct, o.order_amount, o.payment_mode, o.order_status, o.channel,
       o.delivery_minutes,
       CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END::DECIMAL(12,2) AS net_revenue,
       CASE WHEN o.order_status = 'DELIVERED' AND p.cost_price IS NOT NULL
            THEN o.quantity * p.cost_price END::DECIMAL(12,2)                             AS cost_amount,
       CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount - o.quantity * p.cost_price
            ELSE 0 END::DECIMAL(12,2)                                                     AS gross_margin,
       CASE WHEN o.order_status = 'DELIVERED' AND o.delivery_minutes IS NOT NULL
            THEN o.delivery_minutes <= 15 END                                             AS within_15_min,
       o.dq_status, o.warning_checks AS dq_warnings,
       o._source_file AS source_file, o._row_num AS source_row_num, o.stg_row_key,
       o._raw_run_id, o.batch_cutoff, o.previous_status, o.last_merge_run_id
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_products WHERE dq_status <> 'FAILED') p USING (product_id)
WHERE o.dq_status IN ('PASSED', 'WARNING');
