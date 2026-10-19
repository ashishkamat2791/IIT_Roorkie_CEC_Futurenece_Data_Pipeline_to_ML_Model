-- ETL reporting tables, rebuilt from the finance-grade fact on every run.
-- Revenue = DELIVERED orders only, after discount (net_revenue).

-- Q1 Daily net revenue by zone and store
CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
SELECT f.order_date, s.zone, f.store_id, s.store_name,
       count(*)              AS delivered_orders,
       sum(f.net_revenue)    AS net_revenue
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY order_date, zone, store_id;

-- Q2 Revenue and gross margin by category (margin = revenue - quantity x cost_price).
-- orders_without_cost: delivered orders whose product cost failed a critical check (margin unknown).
CREATE OR REPLACE TABLE etl_category_margin AS
SELECT p.category,
       count(*)                                        AS delivered_orders,
       sum(f.quantity)                                 AS units,
       sum(f.net_revenue)                              AS net_revenue,
       sum(f.cost_amount)                              AS cost_amount,
       sum(f.gross_margin)                             AS gross_margin,
       round(100.0 * sum(f.gross_margin) / nullif(sum(f.net_revenue) FILTER (WHERE f.gross_margin IS NOT NULL), 0), 2) AS margin_pct,
       count(*) FILTER (WHERE f.gross_margin IS NULL) AS orders_without_cost
FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY net_revenue DESC;

-- Q3 Average delivery time per store and % of delivered orders within 15 minutes
CREATE OR REPLACE TABLE etl_delivery_sla AS
SELECT f.store_id, s.store_name, s.zone,
       count(*)                                              AS delivered_orders,
       round(avg(f.delivery_minutes), 2)                     AS avg_delivery_minutes,
       round(100.0 * avg(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 2) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY store_id;

-- Q4 Revenue by payment mode plus the amount that does not reconcile with the gateway
CREATE OR REPLACE TABLE etl_payment_mode_revenue AS
WITH rev AS (
    SELECT payment_mode, count(*) AS delivered_orders, sum(net_revenue) AS net_revenue
    FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY payment_mode
), unrec AS (
    SELECT coalesce(payment_mode, 'Unknown') AS payment_mode,
           count(*) AS mismatches, sum(unreconciled_amount) AS unreconciled_amount
    FROM etl_payment_reconciliation GROUP BY 1
)
SELECT coalesce(rev.payment_mode, unrec.payment_mode) AS payment_mode,
       coalesce(rev.delivered_orders, 0)             AS delivered_orders,
       coalesce(rev.net_revenue, 0)                  AS net_revenue,
       coalesce(unrec.mismatches, 0)                 AS mismatches,
       coalesce(unrec.unreconciled_amount, 0)        AS unreconciled_amount
FROM rev FULL OUTER JOIN unrec USING (payment_mode)
ORDER BY net_revenue DESC;

-- Q5 Top 10 customers by spend (masked identifiers only)
CREATE OR REPLACE TABLE etl_top_customers AS
SELECT row_number() OVER (ORDER BY sum(f.net_revenue) DESC, f.customer_id) AS spend_rank,
       f.customer_id, c.full_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
       count(*) AS delivered_orders, sum(f.net_revenue) AS net_revenue
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY f.customer_id, c.full_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier
ORDER BY spend_rank
LIMIT 10;
