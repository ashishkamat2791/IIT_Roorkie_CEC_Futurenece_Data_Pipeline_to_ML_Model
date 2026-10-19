-- MART Q4: revenue by payment mode plus the amount that does not reconcile with the gateway.
CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
WITH rev AS (
    SELECT payment_mode, count(*) AS delivered_orders, sum(net_revenue) AS net_revenue
    FROM int_fact_orders WHERE order_status = 'DELIVERED' GROUP BY payment_mode
), unrec AS (
    SELECT coalesce(payment_mode, 'Unknown') AS payment_mode,
           count(*) AS mismatches, sum(unreconciled_amount) AS unreconciled_amount
    FROM mart_payment_reconciliation GROUP BY 1
)
SELECT coalesce(rev.payment_mode, unrec.payment_mode) AS payment_mode,
       coalesce(rev.delivered_orders, 0)             AS delivered_orders,
       coalesce(rev.net_revenue, 0)                  AS net_revenue,
       coalesce(unrec.mismatches, 0)                 AS mismatches,
       coalesce(unrec.unreconciled_amount, 0)        AS unreconciled_amount
FROM rev FULL OUTER JOIN unrec USING (payment_mode)
ORDER BY net_revenue DESC;
