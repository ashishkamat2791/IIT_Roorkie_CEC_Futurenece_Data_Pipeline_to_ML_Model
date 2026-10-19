-- MART Q5: top 10 customers by spend, with loyalty tier. Masked identifiers only.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT row_number() OVER (ORDER BY sum(f.net_revenue) DESC, f.customer_id) AS spend_rank,
       f.customer_id, c.full_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
       count(*) AS delivered_orders, sum(f.net_revenue) AS net_revenue
FROM int_fact_orders f
JOIN (SELECT * FROM stg_customers WHERE dq_status <> 'FAILED') c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY f.customer_id, c.full_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier
ORDER BY spend_rank
LIMIT 10;
