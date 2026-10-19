-- MART (bonus): SCD Type 2 customer dimension keeping loyalty-tier (and is_active) history.
-- Same rules as include/nammamart_ashish_kamat/scd2.py:
--   no current version                                   -> insert NEW, valid_from = batch cut-off
--   current version, newer batch, tracked attrs changed  -> close it, insert CHANGED
--   same values, or a batch not newer than the current   -> no-op (idempotent re-runs)
CREATE TABLE IF NOT EXISTS elt_dim_customer_scd2 (
    customer_sk       VARCHAR PRIMARY KEY,
    customer_id       VARCHAR NOT NULL,
    full_name_masked  VARCHAR,
    loyalty_tier      VARCHAR,
    is_active         BOOLEAN,
    valid_from        TIMESTAMP NOT NULL,
    valid_to          TIMESTAMP,
    is_current        BOOLEAN NOT NULL,
    change_type       VARCHAR NOT NULL,
    loaded_run_id     VARCHAR NOT NULL
);

CREATE OR REPLACE TEMP TABLE _scd_plan AS
SELECT s.customer_id, s.full_name_masked, s.loyalty_tier, s.is_active,
       c.customer_sk AS current_sk,
       CASE WHEN c.customer_id IS NULL THEN 'NEW' ELSE 'CHANGED' END AS change_type
FROM (SELECT * FROM stg_customers WHERE dq_status <> 'FAILED') s
LEFT JOIN (SELECT * FROM elt_dim_customer_scd2 WHERE is_current) c USING (customer_id)
WHERE c.customer_id IS NULL
   OR (CAST({{ as_of }} AS TIMESTAMP) > c.valid_from
       AND (s.loyalty_tier IS DISTINCT FROM c.loyalty_tier OR s.is_active IS DISTINCT FROM c.is_active));

UPDATE elt_dim_customer_scd2
SET valid_to = CAST({{ as_of }} AS TIMESTAMP), is_current = FALSE
WHERE customer_sk IN (SELECT current_sk FROM _scd_plan WHERE current_sk IS NOT NULL);

INSERT INTO elt_dim_customer_scd2
SELECT customer_id || '@' || strftime(CAST({{ as_of }} AS TIMESTAMP), '%Y-%m-%dT%H:%M:%S'),
       customer_id, full_name_masked, loyalty_tier, is_active,
       CAST({{ as_of }} AS TIMESTAMP), NULL, TRUE, change_type, {{ run_id }}
FROM _scd_plan;
