-- MART Q3: average delivery time per store and % of delivered orders within 15 minutes.
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT f.store_id, s.store_name, s.zone,
       count(*)                                                            AS delivered_orders,
       round(avg(f.delivery_minutes), 2)                                   AS avg_delivery_minutes,
       round(100.0 * avg(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 2)  AS pct_within_15_min
FROM int_fact_orders f
JOIN (SELECT * FROM stg_stores WHERE dq_status <> 'FAILED') s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY f.store_id;
