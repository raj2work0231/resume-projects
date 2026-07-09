-- =============================================================================
-- Project 4 – Enterprise Multi-Source ETL Pipeline
-- File: sql/01_create_tables.sql + 02_reporting_views.sql
-- =============================================================================

-- Bronze raw orders
CREATE TABLE IF NOT EXISTS catalog.bronze.orders_raw (
    order_id        STRING,
    customer_id     STRING,
    product_id      STRING,
    amount          DOUBLE,
    status          STRING,
    order_date_str  STRING,
    channel         STRING,
    region          STRING,
    fulfilled_date  DATE,
    row_hash        STRING,
    ingest_ts       TIMESTAMP DEFAULT current_timestamp(),
    ingest_date     DATE      DEFAULT current_date()
)
USING DELTA PARTITIONED BY (ingest_date)
TBLPROPERTIES ('quality' = 'bronze');

-- Silver cleansed orders
CREATE TABLE IF NOT EXISTS catalog.silver.orders (
    order_id         STRING NOT NULL,
    customer_id      STRING,
    product_id       STRING,
    amount           DOUBLE,
    status           STRING,
    order_date       DATE,
    channel          STRING,
    region           STRING,
    fulfilled_date   DATE,
    is_fulfilled     BOOLEAN,
    row_hash         STRING,
    ingest_ts        TIMESTAMP,
    ingest_date      DATE,
    silver_ts        TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA PARTITIONED BY (order_date)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'silver');

-- Gold curated orders
CREATE TABLE IF NOT EXISTS catalog.gold.orders_curated (
    order_id          STRING,
    customer_id       STRING,
    customer_name     STRING,
    customer_tier     STRING,
    product_id        STRING,
    product_name      STRING,
    product_category  STRING,
    amount            DOUBLE,
    status            STRING,
    order_date        DATE,
    channel           STRING,
    region            STRING,
    fulfilled_date    DATE,
    is_fulfilled      BOOLEAN,
    days_to_fulfil    INT,
    gold_ts           TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA PARTITIONED BY (order_date)
TBLPROPERTIES ('quality' = 'gold');

-- Gold daily summary
CREATE TABLE IF NOT EXISTS catalog.gold.orders_daily_summary (
    order_date           DATE,
    region               STRING,
    channel              STRING,
    product_category     STRING,
    customer_tier        STRING,
    order_count          BIGINT,
    completed_revenue    DOUBLE,
    pending_revenue      DOUBLE,
    failed_orders        BIGINT,
    avg_fulfilment_days  DOUBLE,
    unique_customers     BIGINT,
    load_ts              TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA PARTITIONED BY (order_date)
TBLPROPERTIES ('quality' = 'gold');

-- =============================================================================
-- REPORTING VIEWS
-- =============================================================================

-- Current day order status
CREATE OR REPLACE VIEW catalog.reporting.v_today_orders AS
SELECT
    order_id, customer_id, customer_name, product_name,
    amount, status, channel, region, order_date, is_fulfilled, days_to_fulfil
FROM catalog.gold.orders_curated
WHERE order_date = current_date();

-- Revenue by channel (last 30 days)
CREATE OR REPLACE VIEW catalog.reporting.v_revenue_by_channel AS
SELECT
    order_date,
    channel,
    SUM(completed_revenue)  AS completed_revenue,
    SUM(order_count)        AS total_orders,
    AVG(avg_fulfilment_days) AS avg_days
FROM catalog.gold.orders_daily_summary
WHERE order_date >= current_date() - INTERVAL 30 DAYS
GROUP BY order_date, channel
ORDER BY order_date DESC;

-- Failed orders requiring action
CREATE OR REPLACE VIEW catalog.reporting.v_failed_orders AS
SELECT
    order_id, customer_id, customer_name, amount, region, order_date
FROM catalog.gold.orders_curated
WHERE status = 'FAILED'
  AND order_date >= current_date() - INTERVAL 7 DAYS
ORDER BY amount DESC;
