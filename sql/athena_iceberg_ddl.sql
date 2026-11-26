-- Athena DDL to expose the Gold layer as Apache Iceberg tables (Athena engine v3).
-- Replace <bucket> and <database>. Not executed in this repo's local tests.

CREATE DATABASE IF NOT EXISTS <database>;

CREATE TABLE <database>.daily_sales (
  order_date       date,
  orders           bigint,
  revenue          decimal(18,2),
  avg_order_value  decimal(18,2)
)
PARTITIONED BY (month(order_date))
LOCATION 's3://<bucket>/lake/iceberg/daily_sales/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet');

CREATE TABLE <database>.customer_summary (
  customer_id       string,
  country           string,
  lifetime_orders   bigint,
  lifetime_value    decimal(18,2),
  first_order_date  date,
  last_order_date   date
)
LOCATION 's3://<bucket>/lake/iceberg/customer_summary/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet');

-- Idempotent upsert from a staging table of freshly computed Gold rows:
MERGE INTO <database>.daily_sales t
USING <database>.daily_sales_stage s
ON t.order_date = s.order_date
WHEN MATCHED THEN UPDATE SET orders = s.orders, revenue = s.revenue, avg_order_value = s.avg_order_value
WHEN NOT MATCHED THEN INSERT (order_date, orders, revenue, avg_order_value)
  VALUES (s.order_date, s.orders, s.revenue, s.avg_order_value);
