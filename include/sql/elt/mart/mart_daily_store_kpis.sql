-- MART Q1: daily net revenue by zone and store (DELIVERED only, after discount).
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT f.order_date, s.zone, f.store_id, s.store_name,
       count(*) FILTER (WHERE f.order_status = 'DELIVERED')                 AS delivered_orders,
       count(*) FILTER (WHERE f.order_status = 'CANCELLED')                 AS cancelled_orders,
       count(*) FILTER (WHERE f.order_status = 'RETURNED')                  AS returned_orders,
       coalesce(sum(f.net_revenue) FILTER (WHERE f.order_status = 'DELIVERED'), 0)  AS net_revenue,
       sum(f.gross_margin) FILTER (WHERE f.order_status = 'DELIVERED')      AS gross_margin,
       round(avg(f.delivery_minutes) FILTER (WHERE f.order_status = 'DELIVERED'), 2) AS avg_delivery_minutes
FROM int_fact_orders f
JOIN (SELECT * FROM stg_stores WHERE dq_status <> 'FAILED') s USING (store_id)
GROUP BY ALL
HAVING count(*) FILTER (WHERE f.order_status = 'DELIVERED') > 0
ORDER BY f.order_date, s.zone, f.store_id;
