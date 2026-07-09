-- =============================================================================
-- Project 3 – AutoLoader File-Notification Multiplexer Pipeline
-- File: sql/01_metadata_tables.sql
-- =============================================================================

-- ============================================================================
-- 1. DATASET CONFIG — one row per file-based dataset (metadata-driven routing)
-- ============================================================================
CREATE TABLE IF NOT EXISTS catalog.control.dataset_config (
    dataset_id        STRING  NOT NULL  COMMENT 'Unique dataset identifier',
    source_path       STRING  NOT NULL  COMMENT 'ADLS Gen2 landing path (monitored by AutoLoader)',
    target_table      STRING  NOT NULL  COMMENT 'Downstream Silver Delta table',
    file_format       STRING  DEFAULT 'json' COMMENT 'File format: json / csv / parquet',
    scd_type          STRING  DEFAULT 'SCD1' COMMENT 'SCD1 (latest-wins) or SCD2 (full history)',
    business_key_cols STRING            COMMENT 'Comma-separated business key columns',
    hash_cols         STRING            COMMENT 'Comma-separated columns for MD5 row hash',
    operation_col     STRING            COMMENT 'Payload column carrying I/U/D op type (optional; defaults INSERT)',
    checkpoint_path   STRING            COMMENT 'Shared AutoLoader + downstream checkpoint base path',
    schema_path       STRING            COMMENT 'AutoLoader schema inference location',
    reader_options    STRING            COMMENT 'JSON map of additional cloudFiles reader options',
    is_active         BOOLEAN DEFAULT true,
    created_at        TIMESTAMP DEFAULT current_timestamp(),
    updated_at        TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Metadata config driving the AutoLoader multiplexer pipeline per dataset';

-- Sample config rows
INSERT INTO catalog.control.dataset_config
    (dataset_id, source_path, target_table, file_format, scd_type,
     business_key_cols, hash_cols, operation_col, checkpoint_path, schema_path, is_active)
VALUES
    ('orders',
     'abfss://raw@storage.dfs.core.windows.net/orders/',
     'catalog.silver.orders',
     'json', 'SCD1', 'order_id', 'order_id,customer_id,amount,status',
     'op_type',
     'abfss://ckpt@storage.dfs.core.windows.net/orders/',
     'abfss://schemas@storage.dfs.core.windows.net/orders/', true),
    ('customers',
     'abfss://raw@storage.dfs.core.windows.net/customers/',
     'catalog.silver.customers',
     'csv', 'SCD2', 'customer_id', 'customer_id,name,email,region',
     '',
     'abfss://ckpt@storage.dfs.core.windows.net/customers/',
     'abfss://schemas@storage.dfs.core.windows.net/customers/', true);

-- ============================================================================
-- 2. MULTIPLEXER DELTA TABLE — central routing hub for all file-based datasets
-- ============================================================================
CREATE TABLE IF NOT EXISTS catalog.control.file_multiplexer (
    dataset_id     STRING    NOT NULL  COMMENT 'Source dataset identifier',
    target_table   STRING    NOT NULL  COMMENT 'Downstream Silver Delta table',
    operation      STRING    NOT NULL  COMMENT 'INSERT / UPDATE / DELETE',
    source_file    STRING              COMMENT 'ADLS Gen2 source file path',
    row_hash       STRING              COMMENT 'MD5 hash of record payload',
    ingest_ts      TIMESTAMP DEFAULT current_timestamp(),
    ingest_date    DATE      DEFAULT current_date()
)
USING DELTA
PARTITIONED BY (ingest_date, dataset_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'control'
)
COMMENT 'Centralised routing hub: AutoLoader lands all datasets here before SCD downstream streaming';

-- ============================================================================
-- 3. FILE TRACKING TABLE — per-file ingestion status and recovery
-- ============================================================================
CREATE TABLE IF NOT EXISTS catalog.control.file_tracking (
    file_path    STRING    NOT NULL,
    dataset_id   STRING    NOT NULL,
    status       STRING    NOT NULL  COMMENT 'PENDING / PROCESSING / DONE / FAILED',
    file_size    BIGINT,
    error        STRING,
    retry_count  INT       DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Tracks ingestion status of every file detected by AutoLoader';

-- ============================================================================
-- 4. SCHEMA VERSION AUDIT LOG
-- ============================================================================
CREATE TABLE IF NOT EXISTS catalog.control.schema_version_log (
    table_name   STRING,
    dataset_id   STRING,
    new_columns  STRING  COMMENT 'JSON array of newly added columns',
    dropped_cols STRING  COMMENT 'JSON array of removed columns',
    type_changes STRING  COMMENT 'JSON map of type changes',
    detected_ts  TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Audit log of schema evolution events detected by AutoLoader per dataset';

-- Dataset configuration registry
CREATE TABLE IF NOT EXISTS catalog.control.dataset_config (
    dataset_id        STRING  NOT NULL  COMMENT 'Unique dataset identifier',
    source_path       STRING  NOT NULL  COMMENT 'ADLS Gen2 source landing path',
    bronze_table      STRING  NOT NULL  COMMENT 'Target Bronze Delta table',
    silver_table      STRING            COMMENT 'Target Silver Delta table',
    file_format       STRING  DEFAULT 'json' COMMENT 'File format: json/csv/parquet',
    scd_type          STRING  DEFAULT 'SCD1' COMMENT 'SCD1 or SCD2',
    business_key_cols STRING            COMMENT 'Comma-separated business key columns',
    hash_cols         STRING            COMMENT 'Comma-separated columns for row hash',
    checkpoint_path   STRING            COMMENT 'AutoLoader checkpoint location',
    schema_path       STRING            COMMENT 'AutoLoader schema inference location',
    trigger_mode      STRING  DEFAULT 'realtime' COMMENT 'realtime or batch',
    reader_options    STRING            COMMENT 'JSON of additional reader options',
    is_active         BOOLEAN DEFAULT true,
    created_at        TIMESTAMP DEFAULT current_timestamp(),
    updated_at        TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Central configuration table driving the metadata-driven AutoLoader framework';

-- File tracking table
CREATE TABLE IF NOT EXISTS catalog.control.file_tracking (
    file_path    STRING    NOT NULL,
    dataset_id   STRING    NOT NULL,
    status       STRING    NOT NULL  COMMENT 'PENDING / PROCESSING / DONE / FAILED',
    file_size    BIGINT,
    error        STRING,
    retry_count  INT       DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Tracks ingestion status of each file detected by AutoLoader';

-- Schema version audit log
CREATE TABLE IF NOT EXISTS catalog.control.schema_version_log (
    table_name   STRING,
    dataset_id   STRING,
    new_columns  STRING  COMMENT 'JSON array of newly added columns',
    dropped_cols STRING  COMMENT 'JSON array of removed columns',
    type_changes STRING  COMMENT 'JSON map of type changes',
    detected_ts  TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Audit log of schema evolution events per dataset';

-- Sample config rows
INSERT INTO catalog.control.dataset_config
    (dataset_id, source_path, bronze_table, silver_table, file_format, scd_type,
     business_key_cols, hash_cols, checkpoint_path, schema_path, trigger_mode, is_active)
VALUES
    ('orders',    'abfss://raw@storage.dfs.core.windows.net/orders/',
     'catalog.bronze.orders', 'catalog.silver.orders',
     'json', 'SCD1', 'order_id', 'order_id,customer_id,amount,status',
     'abfss://ckpt@storage.dfs.core.windows.net/orders/', 'abfss://schemas@storage.dfs.core.windows.net/orders/',
     'realtime', true),
    ('customers', 'abfss://raw@storage.dfs.core.windows.net/customers/',
     'catalog.bronze.customers', 'catalog.silver.customers',
     'csv', 'SCD2', 'customer_id', 'customer_id,name,email,region',
     'abfss://ckpt@storage.dfs.core.windows.net/customers/', 'abfss://schemas@storage.dfs.core.windows.net/customers/',
     'batch', true);
