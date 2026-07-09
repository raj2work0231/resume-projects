-- =============================================================================
-- Project 2 – Real-Time Kafka Streaming Pipeline (Multiplexer Pattern)
-- File: sql/01_create_streaming_tables.sql
-- =============================================================================

-- ============================================================================
-- 1. TOPIC ROUTING CONFIG TABLE (metadata-driven routing)
-- ============================================================================
CREATE TABLE IF NOT EXISTS catalog.config.topic_routing (
    kafka_topic        STRING  NOT NULL,   -- Kafka topic name
    target_table       STRING  NOT NULL,   -- Downstream Delta table (catalog.schema.table)
    scd_type           STRING  NOT NULL,   -- SCD1 or SCD2
    business_key_cols  STRING  NOT NULL,   -- Comma-separated PK columns
    is_active          BOOLEAN DEFAULT true
)
USING DELTA
TBLPROPERTIES ('quality' = 'config');

-- Seed routing config
INSERT INTO catalog.config.topic_routing VALUES
  ('events.orders',   'catalog.silver.orders',        'SCD1', 'order_id',   true),
  ('events.payments', 'catalog.silver.payments',      'SCD2', 'payment_id', true),
  ('events.accounts', 'catalog.silver.dim_accounts',  'SCD2', 'account_id', true),
  ('events.products', 'catalog.silver.dim_products',  'SCD1', 'product_id', true);

-- ============================================================================
-- 2. MULTIPLEXER DELTA TABLE (central CDC routing hub)
-- ============================================================================
CREATE TABLE IF NOT EXISTS catalog.bronze.kafka_multiplexer (
    msg_id               STRING    NOT NULL,  -- Unique message ID (UUID)
    kafka_topic          STRING,              -- Source Kafka topic
    target_table         STRING,              -- Resolved downstream table
    scd_type             STRING,              -- SCD1 / SCD2 (from routing config)
    business_key_cols    STRING,              -- Comma-separated PK columns
    operation            STRING,              -- INSERT / UPDATE / DELETE
    payload              STRING,              -- Full JSON payload
    row_hash             STRING,              -- MD5 of payload for change detection
    kafka_partition      INT,
    kafka_offset         BIGINT,
    kafka_event_ts       TIMESTAMP,
    ingest_ts            TIMESTAMP DEFAULT current_timestamp(),
    ingest_date          DATE      DEFAULT current_date(),
    processed_flag       BOOLEAN   DEFAULT false,
    processed_ts         TIMESTAMP
)
USING DELTA
PARTITIONED BY (ingest_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);

-- ============================================================================
-- 3. SAMPLE DOWNSTREAM TABLES
-- ============================================================================

-- SCD Type 1 downstream (latest-wins)
CREATE TABLE IF NOT EXISTS catalog.silver.orders (
    order_id         STRING NOT NULL,
    customer_id      STRING,
    product_id       STRING,
    quantity         INT,
    amount           DOUBLE,
    status           STRING,
    order_ts         TIMESTAMP,
    row_hash         STRING,
    updated_at       TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'silver');

-- SCD Type 2 downstream (full audit history)
CREATE TABLE IF NOT EXISTS catalog.silver.dim_accounts (
    account_id       STRING NOT NULL,
    customer_id      STRING,
    region           STRING,
    account_type     STRING,
    status           STRING,
    row_hash         STRING,
    eff_from         TIMESTAMP,
    eff_to           TIMESTAMP,
    crnt_flag        BOOLEAN,
    inserted_at      TIMESTAMP DEFAULT current_timestamp(),
    updated_at       TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES ('quality' = 'silver');

-- DEAD LETTER (unparseable / unroutable records)
CREATE TABLE IF NOT EXISTS catalog.bronze.kafka_dead_letter (
    kafka_topic       STRING,
    raw_payload       STRING,
    quarantine_reason STRING,
    quarantine_ts     TIMESTAMP DEFAULT current_timestamp(),
    ingest_date       DATE      DEFAULT current_date()
)
USING DELTA PARTITIONED BY (ingest_date);
    transaction_id     STRING,
    account_id         STRING,
    customer_id        STRING,
    amount             DOUBLE,
    currency           STRING,
    transaction_type   STRING,
    merchant_id        STRING,
    status             STRING,
    channel            STRING,
    event_ts           TIMESTAMP,
    region             STRING,
    -- Kafka metadata
    kafka_topic        STRING,
    kafka_partition    INT,
    kafka_offset       BIGINT,
    kafka_event_ts     TIMESTAMP,
    kafka_message_key  STRING,
    -- Pipeline
    row_hash           STRING,
    ingest_ts          TIMESTAMP DEFAULT current_timestamp(),
    ingest_date        DATE      DEFAULT current_date()
)
USING DELTA PARTITIONED BY (ingest_date)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze');

-- SILVER
CREATE TABLE IF NOT EXISTS catalog.silver.transactions (
    transaction_id     STRING NOT NULL,
    account_id         STRING NOT NULL,
    customer_id        STRING,
    amount             DOUBLE,
    currency           STRING,
    transaction_type   STRING,
    merchant_id        STRING,
    status             STRING,
    channel            STRING,
    event_ts           TIMESTAMP,
    event_date         DATE,
    region             STRING,
    row_hash           STRING,
    ingest_ts          TIMESTAMP,
    ingest_date        DATE,
    silver_processed_ts TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA PARTITIONED BY (event_date)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'silver');

-- GOLD FACT
CREATE TABLE IF NOT EXISTS catalog.gold.transactions_daily (
    account_id          STRING,
    event_date          DATE,
    transaction_type    STRING,
    currency            STRING,
    status              STRING,
    transaction_count   BIGINT,
    total_amount        DOUBLE,
    avg_amount          DOUBLE,
    max_amount          DOUBLE,
    min_amount          DOUBLE,
    unique_merchants    BIGINT,
    latest_event_ts     TIMESTAMP,
    load_ts             TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA PARTITIONED BY (event_date)
TBLPROPERTIES ('quality' = 'gold');

-- GOLD DIMENSION (SCD2)
CREATE TABLE IF NOT EXISTS catalog.gold.dim_account_history (
    account_id   STRING NOT NULL,
    customer_id  STRING,
    region       STRING,
    channel      STRING,
    row_hash     STRING,
    eff_from     TIMESTAMP,
    eff_to       TIMESTAMP,
    crnt_flag    BOOLEAN,
    inserted_at  TIMESTAMP DEFAULT current_timestamp(),
    updated_at   TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES ('quality' = 'gold');

-- DEAD LETTER
CREATE TABLE IF NOT EXISTS catalog.bronze.transactions_dead_letter (
    raw_payload        STRING,
    quarantine_reason  STRING,
    quarantine_ts      TIMESTAMP DEFAULT current_timestamp(),
    ingest_date        DATE      DEFAULT current_date()
)
USING DELTA PARTITIONED BY (ingest_date);

-- MONITORING
CREATE TABLE IF NOT EXISTS catalog.monitoring.dq_metrics (
    batch_id   BIGINT,
    topic      STRING,
    total      BIGINT,
    valid      BIGINT,
    invalid    BIGINT,
    dq_score   DOUBLE,
    metric_ts  TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;
