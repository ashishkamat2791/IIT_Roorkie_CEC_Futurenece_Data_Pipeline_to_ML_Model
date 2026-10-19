-- RAW layer: exact copy of every source row as text, append-only, never cleaned or deleted.
-- Lineage columns: _row_num (1 = first data row, CSV line = _row_num + 1), _source_file,
-- _loaded_at, _run_id, _file_md5. Access: data engineering + compliance only (contains PII).
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR, order_ts VARCHAR, customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity VARCHAR, unit_price VARCHAR, discount_pct VARCHAR, order_amount VARCHAR,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes VARCHAR,
    _row_num INTEGER, _source_file VARCHAR, _loaded_at TIMESTAMP, _run_id VARCHAR, _file_md5 VARCHAR
);
CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id VARCHAR, order_id VARCHAR, payment_ts VARCHAR, payment_mode VARCHAR,
    amount_paid VARCHAR, payment_status VARCHAR, gateway_ref VARCHAR,
    _row_num INTEGER, _source_file VARCHAR, _loaded_at TIMESTAMP, _run_id VARCHAR, _file_md5 VARCHAR
);
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id VARCHAR, full_name VARCHAR, email VARCHAR, phone VARCHAR, city VARCHAR, area VARCHAR,
    pincode VARCHAR, signup_date VARCHAR, loyalty_tier VARCHAR, is_active VARCHAR,
    _row_num INTEGER, _source_file VARCHAR, _loaded_at TIMESTAMP, _run_id VARCHAR, _file_md5 VARCHAR
);
CREATE TABLE IF NOT EXISTS raw_products (
    product_id VARCHAR, product_name VARCHAR, category VARCHAR, sub_category VARCHAR, brand VARCHAR,
    mrp VARCHAR, cost_price VARCHAR, unit VARCHAR, is_perishable VARCHAR, launch_date VARCHAR,
    _row_num INTEGER, _source_file VARCHAR, _loaded_at TIMESTAMP, _run_id VARCHAR, _file_md5 VARCHAR
);
CREATE TABLE IF NOT EXISTS raw_stores (
    store_id VARCHAR, store_name VARCHAR, zone VARCHAR, area VARCHAR, pincode VARCHAR,
    open_date VARCHAR, manager_name VARCHAR, capacity_orders_per_day VARCHAR,
    _row_num INTEGER, _source_file VARCHAR, _loaded_at TIMESTAMP, _run_id VARCHAR, _file_md5 VARCHAR
);

-- Row-level check outcomes for every staging table (feeds dq_status, dq_results and question 6).
CREATE TABLE IF NOT EXISTS stg_dq_row_checks (
    stg_table VARCHAR, source_file VARCHAR, stg_row_key VARCHAR, check_seq INTEGER,
    check_name VARCHAR, dimension VARCHAR, severity VARCHAR, action VARCHAR, nullify_col VARCHAR,
    in_population BOOLEAN, failed BOOLEAN, detail VARCHAR, population VARCHAR
);
-- File-level checks (freshness, volume, distribution drift, capacity).
CREATE TABLE IF NOT EXISTS stg_dq_dataset_checks (
    stg_table VARCHAR, source_file VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, details VARCHAR, population VARCHAR
);
-- Per-run MERGE statistics for the incremental order load.
CREATE TABLE IF NOT EXISTS elt_merge_log (
    run_id VARCHAR, source_file VARCHAR, rows_in_file BIGINT, inserted BIGINT, updated BIGINT,
    status_changed BIGINT, skipped_older BIGINT, extra_copies BIGINT, merged_at TIMESTAMP,
    PRIMARY KEY (run_id, source_file)
);
