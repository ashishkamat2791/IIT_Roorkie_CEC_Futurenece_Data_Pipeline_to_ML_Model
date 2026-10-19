-- MART Q2: revenue and gross margin by product category.
-- orders_without_cost: delivered orders whose product cost failed a critical check (margin unknown).
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       count(*)                                        AS delivered_orders,
       sum(f.quantity)                                 AS units,
       sum(f.net_revenue)                              AS net_revenue,
       sum(f.cost_amount)                              AS cost_amount,
       sum(f.gross_margin)                             AS gross_margin,
       round(100.0 * sum(f.gross_margin) / nullif(sum(f.net_revenue) FILTER (WHERE f.gross_margin IS NOT NULL), 0), 2) AS margin_pct,
       count(*) FILTER (WHERE f.gross_margin IS NULL) AS orders_without_cost
FROM int_fact_orders f
JOIN (SELECT * FROM stg_products WHERE dq_status <> 'FAILED') p USING (product_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY net_revenue DESC;
