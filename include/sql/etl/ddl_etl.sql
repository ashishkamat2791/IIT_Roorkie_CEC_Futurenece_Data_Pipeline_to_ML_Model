-- ETL star schema (finance-grade: only rows that passed every critical check).
CREATE TABLE IF NOT EXISTS etl_dim_customer (
    customer_id       VARCHAR PRIMARY KEY,
    full_name_masked  VARCHAR,
    email_masked      VARCHAR,
    phone_masked      VARCHAR,
    city              VARCHAR,
    area              VARCHAR,
    pincode           VARCHAR,
    signup_date       DATE,
    loyalty_tier      VARCHAR,
    is_active         BOOLEAN,
    dq_status         VARCHAR NOT NULL,
    dq_issues         VARCHAR,
    source_file       VARCHAR NOT NULL,
    source_row_num    INTEGER NOT NULL,
    loaded_run_id     VARCHAR NOT NULL,
    loaded_at         TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_dim_product (
    product_id      VARCHAR PRIMARY KEY,
    product_name    VARCHAR,
    category        VARCHAR NOT NULL,
    sub_category    VARCHAR,
    brand           VARCHAR,
    mrp             DECIMAL(12,2),
    cost_price      DECIMAL(12,2),
    unit            VARCHAR,
    is_perishable   BOOLEAN,
    launch_date     DATE,
    dq_status       VARCHAR NOT NULL,
    dq_issues       VARCHAR,
    source_file     VARCHAR NOT NULL,
    source_row_num  INTEGER NOT NULL,
    loaded_run_id   VARCHAR NOT NULL,
    loaded_at       TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_dim_store (
    store_id                 VARCHAR PRIMARY KEY,
    store_name               VARCHAR,
    zone                     VARCHAR NOT NULL,
    area                     VARCHAR,
    pincode                  VARCHAR,
    open_date                DATE,
    capacity_orders_per_day  INTEGER,
    dq_status                VARCHAR NOT NULL,
    dq_issues                VARCHAR,
    source_file              VARCHAR NOT NULL,
    source_row_num           INTEGER NOT NULL,
    loaded_run_id            VARCHAR NOT NULL,
    loaded_at                TIMESTAMP NOT NULL
);

-- Grain: one row per order_id (each order has one product line).
-- batch_cutoff orders versions: a re-run of an older file never overwrites a newer correction.
CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id          VARCHAR PRIMARY KEY,
    order_ts          TIMESTAMP NOT NULL,
    order_date        DATE NOT NULL,
    customer_id       VARCHAR NOT NULL,
    store_id          VARCHAR NOT NULL,
    product_id        VARCHAR NOT NULL,
    quantity          INTEGER NOT NULL CHECK (quantity > 0),
    unit_price        DECIMAL(12,2) NOT NULL,
    discount_pct      DECIMAL(5,2) NOT NULL CHECK (discount_pct BETWEEN 0 AND 100),
    order_amount      DECIMAL(12,2) NOT NULL,
    payment_mode      VARCHAR NOT NULL,
    order_status      VARCHAR NOT NULL CHECK (order_status IN ('DELIVERED','CANCELLED','RETURNED')),
    channel           VARCHAR,
    delivery_minutes  INTEGER,
    net_revenue       DECIMAL(12,2) NOT NULL,
    cost_amount       DECIMAL(12,2),
    gross_margin      DECIMAL(12,2),
    within_15_min     BOOLEAN,
    dq_status         VARCHAR NOT NULL,
    dq_warnings       VARCHAR,
    source_file       VARCHAR NOT NULL,
    source_row_num    INTEGER NOT NULL,
    batch_cutoff      TIMESTAMP NOT NULL,
    loaded_run_id     VARCHAR NOT NULL,
    loaded_at         TIMESTAMP NOT NULL,
    updated_run_id    VARCHAR,
    previous_status   VARCHAR
);

-- Rows removed from the fact because a newer version of the order failed a critical check.
CREATE TABLE IF NOT EXISTS etl_fact_orders_removed (
    order_id        VARCHAR,
    removed_run_id  VARCHAR,
    removed_at      TIMESTAMP,
    failed_checks   VARCHAR,
    old_row         VARCHAR
);

-- Per-run history of incremental merges into the fact.
CREATE TABLE IF NOT EXISTS etl_load_log (
    run_id       VARCHAR,
    table_name   VARCHAR,
    source_file  VARCHAR,
    inserted     BIGINT,
    updated      BIGINT,
    unchanged    BIGINT,
    skipped_older BIGINT,
    deleted      BIGINT,
    loaded_at    TIMESTAMP,
    PRIMARY KEY (run_id, table_name, source_file)
);

-- Rejected-row counts by run, file and failed check (business question 6).
CREATE TABLE IF NOT EXISTS etl_rejected_rows (
    run_id       VARCHAR,
    source_file  VARCHAR,
    check_name   VARCHAR,
    severity     VARCHAR,
    rows_rejected BIGINT,
    quarantine_file VARCHAR,
    PRIMARY KEY (run_id, source_file, check_name)
);

-- Bonus: SCD Type 2 customer dimension keeping loyalty-tier (and is_active) history.
-- One row per customer version; valid_from/valid_to are batch cut-offs, so a re-run of the
-- same batch is a no-op and an older batch never rewrites newer history.
CREATE TABLE IF NOT EXISTS etl_dim_customer_scd2 (
    customer_sk       VARCHAR PRIMARY KEY,   -- customer_id || '@' || valid_from
    customer_id       VARCHAR NOT NULL,
    full_name_masked  VARCHAR,
    loyalty_tier      VARCHAR,
    is_active         BOOLEAN,
    valid_from        TIMESTAMP NOT NULL,
    valid_to          TIMESTAMP,             -- NULL while current
    is_current        BOOLEAN NOT NULL,
    change_type       VARCHAR NOT NULL,      -- NEW | CHANGED
    loaded_run_id     VARCHAR NOT NULL
);
