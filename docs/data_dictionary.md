# Data dictionary: NammaMart warehouse (`include/warehouse_ashish_kamat.duckdb`)

Types come from `DESCRIBE` on the live warehouse. **PII** marks personal data under the DPDP Act, 2023. `Masked` means only a masked form is stored. Money is `DECIMAL(12,2)` in rupees. Timestamps are IST wall-clock values exactly as exported; no timezone conversion is applied.

The ELT pipeline publishes the same business columns in `int_fact_orders`, `stg_customers`, `stg_products` and `stg_stores`. It adds lineage columns such as `stg_row_key`, `_raw_run_id` and `previous_stg_row_key`.

## `etl_fact_orders` (grain: one row per `order_id`; finance-grade)

Only the current version of each order is stored, and only if it passed every Critical check.

| Column | Type | Meaning | Source | Allowed values / rule | PII |
|---|---|---|---|---|---|
| order_id | VARCHAR PK | Order identifier | orders.csv / increment `order_id` (trimmed) | unique, not null | No |
| order_ts | TIMESTAMP | When the order was placed | `order_ts` | `YYYY-MM-DD HH:MM:SS`, not after the batch cut-off | No |
| order_date | DATE | Business date of the order | derived from `order_ts` | | No |
| customer_id | VARCHAR | Customer key (pseudonymous) | `customer_id` | exists in customer master | Indirect (pseudonymous ID) |
| store_id | VARCHAR | Dark store | `store_id` | exists in store master | No |
| product_id | VARCHAR | Product | `product_id` | exists in product master | No |
| quantity | INTEGER | Units ordered | `quantity` | integer > 0 | No |
| unit_price | DECIMAL(12,2) | Selling price per unit | `unit_price` | > 0 | No |
| discount_pct | DECIMAL(5,2) | Discount percent | `discount_pct` | 0–100 | No |
| order_amount | DECIMAL(12,2) | Order value after discount | `order_amount` | = round(qty × price × (1 − disc/100), 2) ± 0.01 | No |
| payment_mode | VARCHAR | Payment mode | `payment_mode`, standardised | UPI, Card, Cash on Delivery, Wallet (`upi`, `UPI `→UPI, `CARD`→Card, `cod`→Cash on Delivery) | No |
| order_status | VARCHAR | Latest known status | `order_status`, upper-cased; the increment overrides it | DELIVERED, CANCELLED, RETURNED | No |
| channel | VARCHAR | Ordering channel | `channel` | App, Web | No |
| delivery_minutes | INTEGER | Delivery time | `delivery_minutes` | required if DELIVERED; 1–119 (warning outside 8–35) | No |
| net_revenue | DECIMAL(12,2) | Recognised revenue | derived | `order_amount` if DELIVERED, else 0 | No |
| cost_amount | DECIMAL(12,2) | quantity × cost_price | derived with `etl_dim_product` | NULL if the product's cost failed a check | No |
| gross_margin | DECIMAL(12,2) | net_revenue − quantity × cost_price | derived | DELIVERED only; 0 for CANCELLED/RETURNED; NULL if cost unknown | No |
| within_15_min | BOOLEAN | Delivered in 15 minutes or less | derived | NULL unless DELIVERED | No |
| dq_status | VARCHAR | Row quality outcome | validation | PASSED, WARNING | No |
| dq_warnings | VARCHAR | Warning checks that fired | validation | check names, `;`-separated | No |
| source_file, source_row_num | VARCHAR, INTEGER | Lineage to the exact CSV row (CSV line = row_num + 1) | extraction | | No |
| batch_cutoff | TIMESTAMP | Cut-off of the batch that supplied this version | config / increment file name | orders the versions | No |
| loaded_run_id, loaded_at, updated_run_id, previous_status | | Airflow run that inserted or last changed the row, and the status before that change | load | | No |

## `etl_dim_customer` (grain: one row per `customer_id`)

| Column | Type | Meaning | Source | Allowed values / rule | PII |
|---|---|---|---|---|---|
| customer_id | VARCHAR PK | Customer key | customers.csv | first file occurrence kept when an ID is duplicated | Indirect |
| full_name_masked | VARCHAR | Initials only (`T*** U***`) | `full_name` | raw name never stored | Yes (masked) |
| email_masked | VARCHAR | `te***@example.com` | `email` | NULL if the email is malformed | Yes (masked) |
| phone_masked | VARCHAR | `90******12` | `phone` | NULL unless 10 digits starting 6–9 | Yes (masked) |
| city, area | VARCHAR | Location | `city`, `area` | blank city → NULL (warning) | Yes (quasi-identifier, low granularity) |
| pincode | VARCHAR | 6-digit pincode | `pincode` | `56xxxx`, else NULL | Yes (quasi-identifier) |
| signup_date | DATE | Sign-up date | `signup_date` | NULL if in the future (Critical, attribute-level) | No |
| loyalty_tier | VARCHAR | Tier | `loyalty_tier`, title-cased | Bronze, Silver, Gold, Platinum | No |
| is_active | BOOLEAN | Active flag | `is_active` | Y/N | No |
| dq_status, dq_issues | VARCHAR | Quality outcome | validation | PASSED, WARNING, FAILED_ATTRIBUTE | No |
| source_file, source_row_num, loaded_run_id, loaded_at | | Lineage | | | No |

## `etl_dim_product` (grain: one row per `product_id`)

| Column | Type | Meaning | Source | Allowed values / rule | PII |
|---|---|---|---|---|---|
| product_id | VARCHAR PK | Product key | products.csv | the current row wins over a superseded `(old)` row | No |
| product_name, sub_category, brand, unit | VARCHAR | Catalogue attributes | same-named columns | trimmed | No |
| category | VARCHAR | Category | `category` | 6 categories; blank → `Unknown` (Critical, attribute-level) | No |
| mrp | DECIMAL(12,2) | Maximum retail price | `mrp` | > 0, else NULL | No |
| cost_price | DECIMAL(12,2) | Unit cost | `cost_price` | > 0 and ≤ mrp, else NULL (margin unknown) | No |
| is_perishable | BOOLEAN | Perishable flag | `is_perishable` | True/False | No |
| launch_date | DATE | Launch date | `launch_date` | not in the future | No |
| dq_status, dq_issues, lineage | | as above | | | No |

## `etl_dim_store` (grain: one row per `store_id`)

| Column | Type | Meaning | Source | Allowed values / rule | PII |
|---|---|---|---|---|---|
| store_id | VARCHAR PK | Store key | stores.csv | unique | No |
| store_name, area, pincode | VARCHAR | Store attributes | same-named columns | | No |
| zone | VARCHAR | Zone | `zone` | North, South, East, West; blank → `Unknown` | No |
| open_date | DATE | Opening date | `open_date` | not in the future | No |
| capacity_orders_per_day | INTEGER | Daily capacity | `capacity_orders_per_day` | > 0, else NULL | No |
| (not published) manager_name | | Store manager | `manager_name` | kept only in raw (restricted) | **Yes**: employee personal data |

## Other tables

| Table | Grain | Purpose |
|---|---|---|
| `etl_payment_reconciliation`, `mart_payment_reconciliation` | one row per mismatch | `order_id`, `payment_ids`, statuses, `expected_amount`, `actual_amount` (money captured; SUCCESS only), `difference`, `unreconciled_amount`, `reason` |
| `etl_daily_zone_revenue`, `mart_daily_store_kpis` | date × store | Q1 |
| `etl_category_margin`, `mart_category_margin` | category | Q2 |
| `etl_delivery_sla`, `mart_delivery_sla` | store | Q3 |
| `etl_payment_mode_revenue`, `mart_payment_mode_revenue` | payment mode | Q4 |
| `etl_top_customers`, `mart_top_customers` | rank 1–10 | Q5 (masked identifiers only) |
| `etl_rejected_rows`, `mart_rejected_rows` | run × file × check | Q6 |
| `raw_*` | source row × load | exact text copy plus `_row_num`, `_source_file`, `_loaded_at`, `_run_id`, `_file_md5`; **contains PII** |
| `stg_*`, `stg_orders_versions` | source row / current order | typed, standardised, masked, `dq_status`, `failed_checks`, `failure_reason` |
| `stg_dq_row_checks`, `stg_dq_dataset_checks` | row × check | outcome of every check on every staging row |
| `dq_results` | run × check × file | quality scorecard (PK `run_id, dag_id, check_name, source_file`) |
| `pipeline_runs` | run | run audit, latest state (PK `dag_id, run_id`); `failed_tasks` = root cause, `upstream_failed_tasks` = collateral |
| `pipeline_run_history` | audit write | append-only history: a FAILED attempt survives a later recovery |
| `etl_dim_customer_scd2`, `elt_dim_customer_scd2` (bonus) | customer version | SCD Type 2: `customer_sk`, `customer_id`, `full_name_masked`, `loyalty_tier`, `is_active`, `valid_from`, `valid_to` (NULL = current), `is_current`, `change_type` (NEW/CHANGED), `loaded_run_id`. Masked name only |
| `etl_load_log`, `elt_merge_log`, `etl_fact_orders_removed` | run | insert/update counts and removed stale versions |
