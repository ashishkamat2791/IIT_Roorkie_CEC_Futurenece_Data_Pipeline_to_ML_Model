-- STAGING: stg_orders = one current row per order_id, seeded from the September snapshot.
-- Extra copies of a duplicated order_id and rows with a blank order_id stay only in
-- stg_orders_versions (dq_status FAILED), where they remain traceable to raw.
CREATE OR REPLACE TABLE stg_orders (
    order_id              VARCHAR PRIMARY KEY,
    order_ts              TIMESTAMP,
    order_date            DATE,
    customer_id           VARCHAR,
    store_id              VARCHAR,
    product_id            VARCHAR,
    quantity              INTEGER,
    unit_price            DECIMAL(12,2),
    discount_pct          DECIMAL(5,2),
    order_amount          DECIMAL(12,2),
    expected_amount       DECIMAL(12,2),
    payment_mode          VARCHAR,
    order_status          VARCHAR,
    channel               VARCHAR,
    delivery_minutes      INTEGER,
    dq_status             VARCHAR NOT NULL,
    failed_checks         VARCHAR,
    failure_reason        VARCHAR,
    warning_checks        VARCHAR,
    batch_cutoff          TIMESTAMP NOT NULL,
    stg_row_key           VARCHAR NOT NULL,   -- <source_file>:<row_num> of the version in use
    _source_file          VARCHAR NOT NULL,
    _row_num              INTEGER NOT NULL,
    _raw_run_id           VARCHAR NOT NULL,
    _raw_loaded_at        TIMESTAMP NOT NULL,
    previous_status       VARCHAR,            -- status before the latest MERGE update
    previous_stg_row_key  VARCHAR,            -- version replaced by the latest MERGE update
    last_merge_run_id     VARCHAR
);

INSERT INTO stg_orders
SELECT order_id, order_ts, CAST(order_ts AS DATE), customer_id, store_id, product_id, quantity,
       TRY_CAST(unit_price AS DECIMAL(12,2)), TRY_CAST(discount_pct AS DECIMAL(5,2)),
       TRY_CAST(order_amount AS DECIMAL(12,2)), TRY_CAST(expected_amount AS DECIMAL(12,2)),
       payment_mode, order_status, channel, CAST(trunc(delivery_minutes_num) AS INTEGER),
       dq_status, failed_checks, failure_reason, warning_checks, batch_cutoff, stg_row_key,
       _source_file, _row_num, _raw_run_id, _raw_loaded_at, NULL, NULL, NULL
FROM stg_orders_versions
WHERE _source_file = 'orders.csv' AND order_id <> '' AND dup_rank = 1;
