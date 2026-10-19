-- STAGING: add the latest raw load of one incremental order file to stg_orders_versions.
-- Its timeliness cut-off is the 09:00 IST run after the file's business date.
INSERT INTO stg_orders_versions
SELECT * FROM typed_orders({{ source_file }}, {{ cutoff }});
