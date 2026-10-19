-- MART Q4 (part): one row per order/payment mismatch. Same policy as etl_load.reconcile():
--   payment side : DUPLICATE_PAYMENT_RECORD, UNUSABLE_PAYMENT_RECORD, ORPHAN_PAYMENT, PAYMENT_FOR_REJECTED_ORDER
--   order side   : MISSING_PAYMENT, PAYMENT_FAILED, NO_SUCCESS_PAYMENT, DUPLICATE_PAYMENT, AMOUNT_MISMATCH,
--                  PAYMENT_MODE_MISMATCH, PAYMENT_ON_CANCELLED_ORDER, RETURNED_NOT_REFUNDED
-- CANCELLED orders without a payment are expected and are not mismatches.
-- actual_amount counts money captured (SUCCESS rows only).
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH p AS (
    SELECT *,
           CASE WHEN payment_status = 'SUCCESS' AND amount_paid IS NOT NULL THEN amount_paid ELSE 0 END AS captured,
           list_contains(string_split(failed_checks, ';'), 'payments_payment_id_unique')           AS is_extra,
           (payment_id = '' OR order_id = '' OR amount_paid IS NULL)                             AS unusable
    FROM stg_payments
), usable AS (
    SELECT * FROM p WHERE NOT is_extra AND NOT unusable
), per_order AS (
    SELECT order_id,
           count(*)                                                  AS n_pay,
           count(*) FILTER (WHERE payment_status = 'SUCCESS')        AS n_success,
           bool_or(payment_status = 'FAILED')                        AS has_failed,
           bool_or(payment_status = 'REFUNDED')                      AS has_refunded,
           sum(captured)                                             AS paid,
           string_agg(payment_id, ',' ORDER BY payment_id)           AS ids,
           string_agg(DISTINCT payment_status, ',' ORDER BY payment_status) AS statuses,
           max(amount_paid) FILTER (WHERE payment_status = 'SUCCESS') AS success_amount,
           max(payment_mode) FILTER (WHERE payment_status = 'SUCCESS') AS success_mode
    FROM usable GROUP BY order_id
), order_side AS (
    SELECT f.order_id, po.ids AS payment_ids, f.order_status, po.statuses AS payment_status, f.payment_mode,
           CASE
             WHEN f.order_status = 'DELIVERED' AND po.order_id IS NULL THEN 'MISSING_PAYMENT'
             WHEN f.order_status = 'DELIVERED' AND po.n_success = 0 AND po.has_failed THEN 'PAYMENT_FAILED'
             WHEN f.order_status = 'DELIVERED' AND po.n_success = 0 THEN 'NO_SUCCESS_PAYMENT'
             WHEN f.order_status = 'DELIVERED' AND po.n_success > 1 THEN 'DUPLICATE_PAYMENT'
             WHEN f.order_status = 'DELIVERED' AND abs(po.success_amount - f.order_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
             WHEN f.order_status = 'DELIVERED' AND po.success_mode IS DISTINCT FROM f.payment_mode THEN 'PAYMENT_MODE_MISMATCH'
             WHEN f.order_status = 'CANCELLED' AND po.paid > 0 THEN 'PAYMENT_ON_CANCELLED_ORDER'
             WHEN f.order_status = 'RETURNED' AND po.paid > 0 AND NOT po.has_refunded THEN 'RETURNED_NOT_REFUNDED'
           END AS reason,
           CASE WHEN f.order_status = 'DELIVERED' THEN f.order_amount ELSE 0 END AS expected_amount,
           CASE
             WHEN f.order_status = 'DELIVERED' AND (po.order_id IS NULL OR po.n_success = 0) THEN 0
             WHEN f.order_status = 'DELIVERED' AND po.n_success > 1 THEN po.paid
             WHEN f.order_status = 'DELIVERED' THEN po.success_amount
             ELSE coalesce(po.paid, 0)
           END AS actual_amount,
           CASE WHEN po.n_success > 1 THEN po.n_success || ' SUCCESS payments'
                WHEN f.order_status = 'RETURNED' THEN 'order returned but gateway shows no refund'
                WHEN po.success_mode IS DISTINCT FROM f.payment_mode AND po.n_success = 1 THEN 'gateway mode ' || po.success_mode
           END AS detail
    FROM int_fact_orders f LEFT JOIN per_order po USING (order_id)
), payment_side AS (
    SELECT order_id, payment_id AS payment_ids, NULL AS order_status, payment_status, payment_mode,
           0 AS expected_amount, captured AS actual_amount, 'DUPLICATE_PAYMENT_RECORD' AS reason,
           'extra copy at source row ' || _row_num || ' would double count' AS detail
    FROM p WHERE is_extra
    UNION ALL
    SELECT nullif(order_id, ''), nullif(payment_id, ''), NULL, payment_status, payment_mode,
           NULL, captured, 'UNUSABLE_PAYMENT_RECORD', failed_checks
    FROM p WHERE unusable AND NOT is_extra
    UNION ALL
    SELECT u.order_id, u.payment_id, NULL, u.payment_status, u.payment_mode, NULL, u.captured,
           CASE WHEN v.order_id IS NOT NULL THEN 'PAYMENT_FOR_REJECTED_ORDER' ELSE 'ORPHAN_PAYMENT' END,
           CASE WHEN v.order_id IS NOT NULL THEN 'order quarantined; payment not matched to revenue'
                ELSE 'order_id not present in any order file' END
    FROM usable u
    LEFT JOIN (SELECT DISTINCT order_id FROM stg_orders_versions) v USING (order_id)
    WHERE u.order_id NOT IN (SELECT order_id FROM int_fact_orders)
), unioned AS (
    SELECT * FROM order_side WHERE reason IS NOT NULL
    UNION ALL BY NAME
    SELECT * FROM payment_side
)
SELECT order_id, payment_ids, order_status, payment_status, payment_mode,
       CAST(expected_amount AS DECIMAL(12,2))                                         AS expected_amount,
       CAST(actual_amount AS DECIMAL(12,2))                                           AS actual_amount,
       CAST(actual_amount - coalesce(expected_amount, 0) AS DECIMAL(12,2))            AS difference,
       CAST(abs(actual_amount - coalesce(expected_amount, 0)) AS DECIMAL(12,2))       AS unreconciled_amount,
       reason, detail, {{ run_id }} AS run_id, current_timestamp AS reconciled_at
FROM unioned
ORDER BY reason, order_id;
