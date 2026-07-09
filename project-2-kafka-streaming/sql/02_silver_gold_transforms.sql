-- =============================================================================
-- Project 2 – Real-Time Kafka Streaming Pipeline (Multiplexer Pattern)
-- File: sql/02_silver_gold_transforms.sql
-- Operational and reporting queries on Multiplexer + Downstream tables
-- =============================================================================

-- ----------------------------------------------------------------------------
-- 1. Multiplexer pipeline health — unprocessed record counts by target table
-- ----------------------------------------------------------------------------
SELECT
    target_table,
    operation,
    COUNT(*)                           AS pending_records,
    MIN(ingest_ts)                     AS oldest_pending_ts,
    MAX(ingest_ts)                     AS latest_pending_ts,
    DATEDIFF(MINUTE, MIN(ingest_ts), current_timestamp()) AS lag_minutes
FROM catalog.bronze.kafka_multiplexer
WHERE processed_flag = false
GROUP BY target_table, operation
ORDER BY lag_minutes DESC;

-- ----------------------------------------------------------------------------
-- 2. Multiplexer throughput — records processed per hour (last 24 hours)
-- ----------------------------------------------------------------------------
SELECT
    DATE_TRUNC('HOUR', processed_ts)   AS processing_hour,
    target_table,
    COUNT(*)                           AS records_processed
FROM catalog.bronze.kafka_multiplexer
WHERE processed_flag = true
  AND processed_ts   >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY 1, 2
ORDER BY 1 DESC, records_processed DESC;

-- ----------------------------------------------------------------------------
-- 3. SCD1 downstream — current active orders
-- ----------------------------------------------------------------------------
SELECT
    order_id,
    customer_id,
    product_id,
    amount,
    status,
    order_ts,
    updated_at
FROM catalog.silver.orders
WHERE status != 'CANCELLED'
ORDER BY order_ts DESC
LIMIT 100;

-- ----------------------------------------------------------------------------
-- 4. SCD2 downstream — current active account records
-- ----------------------------------------------------------------------------
SELECT
    account_id,
    customer_id,
    region,
    account_type,
    status,
    eff_from
FROM catalog.silver.dim_accounts
WHERE crnt_flag = true
ORDER BY account_id;

-- ----------------------------------------------------------------------------
-- 5. SCD2 audit history — full version trail for a specific account
-- ----------------------------------------------------------------------------
SELECT
    account_id,
    customer_id,
    region,
    account_type,
    status,
    eff_from,
    eff_to,
    crnt_flag
FROM catalog.silver.dim_accounts
WHERE account_id = '<<account_id>>'
ORDER BY eff_from;

-- ----------------------------------------------------------------------------
-- 6. Routing config status — active topics and their SCD type
-- ----------------------------------------------------------------------------
SELECT
    kafka_topic,
    target_table,
    scd_type,
    business_key_cols,
    is_active
FROM catalog.config.topic_routing
ORDER BY kafka_topic;
