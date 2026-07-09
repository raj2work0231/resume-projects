-- =============================================================================
-- Project 1 – Geo-Location Intelligence Platform
-- File: sql/01_create_delta_tables.sql
--
-- Purpose : DDL for all Delta tables in the Geo Intelligence pipeline.
--           Bronze / Silver / Gold layers + control tables.
--           Uses Unity Catalog 3-level namespace: catalog.schema.table
-- =============================================================================

-- ----------------------------------------------------------------------------
-- BRONZE LAYER — Raw network events (append-only)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS catalog.bronze.network_events_raw (
    event_id         STRING        NOT NULL COMMENT 'Unique event identifier from source system',
    device_id        STRING        NOT NULL COMMENT 'Anonymised device identifier',
    cell_site_id     STRING                 COMMENT 'Source cell site tower ID',
    latitude         DOUBLE                 COMMENT 'GPS latitude of event',
    longitude        DOUBLE                 COMMENT 'GPS longitude of event',
    event_type       STRING                 COMMENT 'Type of network event (attach/detach/handover)',
    signal_strength  DOUBLE                 COMMENT 'Signal strength in dBm',
    event_ts         TIMESTAMP              COMMENT 'Event timestamp from source',
    technology       STRING                 COMMENT 'Network technology: 4G / 5G / WiFi',
    data_usage_mb    DOUBLE                 COMMENT 'Data consumed in MB',
    -- Audit columns
    row_hash         STRING                 COMMENT 'MD5 hash of key event fields',
    source_file      STRING                 COMMENT 'Source file path (AutoLoader metadata)',
    source_modified  TIMESTAMP              COMMENT 'Source file last modified time',
    ingest_ts        TIMESTAMP DEFAULT current_timestamp() COMMENT 'Pipeline ingest timestamp',
    ingest_date      DATE      DEFAULT current_date()      COMMENT 'Partition key'
)
USING DELTA
PARTITIONED BY (ingest_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true',
    'quality'                          = 'bronze'
)
COMMENT 'Bronze layer: raw network events ingested via AutoLoader file-notification mode';

-- ----------------------------------------------------------------------------
-- SILVER LAYER — Standardised, validated, deduplicated events
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS catalog.silver.network_events_clean (
    event_id            STRING     NOT NULL,
    device_id           STRING     NOT NULL,
    cell_site_id        STRING,
    latitude            DOUBLE     NOT NULL,
    longitude           DOUBLE     NOT NULL,
    event_type          STRING,
    signal_strength     DOUBLE,
    event_ts            TIMESTAMP  NOT NULL,
    technology          STRING,
    data_usage_mb       DOUBLE,
    -- Silver enrichment
    h3_index            STRING              COMMENT 'Uber H3 index at resolution 11',
    h3_resolution       INT                 COMMENT 'H3 resolution level',
    h3_neighbours       ARRAY<STRING>       COMMENT 'K-ring=1 neighbour H3 cells',
    -- Audit
    row_hash            STRING,
    ingest_ts           TIMESTAMP,
    ingest_date         DATE,
    silver_processed_ts TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (ingest_date)
CLUSTER BY (h3_index, cell_site_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed'       = 'true',
    'quality'                          = 'silver'
)
COMMENT 'Silver layer: cleansed and H3-indexed network events';

-- ----------------------------------------------------------------------------
-- GOLD LAYER — Audience segments with SCD Type 2 history
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS catalog.gold.geospatial_audience_segments_history (
    -- Business key
    device_id               STRING    NOT NULL,
    h3_index                STRING    NOT NULL,
    -- Attributes
    cell_site_id            STRING,
    primary_poi_category    STRING,
    nearby_poi_categories   ARRAY<STRING>,
    nearby_poi_count        BIGINT,
    osm_area_type           STRING,
    segments                ARRAY<STRING>  COMMENT 'Audience segment labels',
    -- SCD2 columns
    row_hash                STRING         COMMENT 'MD5 hash to detect attribute changes',
    eff_from                TIMESTAMP      COMMENT 'Effective start of this record version',
    eff_to                  TIMESTAMP      COMMENT 'Effective end; NULL or 9999-12-31 = current',
    crnt_flag               BOOLEAN        COMMENT 'TRUE = current active record',
    -- Audit
    ingest_date             DATE,
    inserted_at             TIMESTAMP DEFAULT current_timestamp(),
    updated_at              TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (ingest_date)
CLUSTER BY (h3_index, crnt_flag)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed'       = 'true',
    'quality'                          = 'gold'
)
COMMENT 'Gold layer: audience segments per device+H3 cell with SCD Type 2 history';

-- ----------------------------------------------------------------------------
-- CONTROL TABLE — Campaign configuration
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS catalog.control.campaign_config (
    campaign_id     STRING    NOT NULL,
    campaign_name   STRING    NOT NULL,
    geo_radius_km   DOUBLE,
    h3_resolution   INT       DEFAULT 11,
    target_segments ARRAY<STRING>,
    start_date      DATE,
    end_date        DATE,
    run_date        DATE,
    is_active       BOOLEAN   DEFAULT true,
    created_at      TIMESTAMP DEFAULT current_timestamp(),
    updated_at      TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES ('quality' = 'control')
COMMENT 'Campaign configuration and scheduling control table';

-- ----------------------------------------------------------------------------
-- CONTROL TABLE — Campaign execution status tracking
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS catalog.control.campaign_execution_status (
    campaign_id    STRING     NOT NULL,
    status         STRING     NOT NULL  COMMENT 'SUBMITTED | RUNNING | SUCCESS | FAILED',
    error_message  STRING,
    run_ts         TIMESTAMP,
    updated_at     TIMESTAMP  DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES ('quality' = 'control')
COMMENT 'Per-campaign execution status updated by the orchestration notebook';

-- ----------------------------------------------------------------------------
-- REFERENCE TABLE — Cell sites with Voronoi polygons
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS catalog.reference.cell_sites (
    cell_site_id              STRING  NOT NULL,
    latitude                  DOUBLE,
    longitude                 DOUBLE,
    h3_index                  STRING,
    voronoi_polygon_wkt        STRING  COMMENT 'WKT Voronoi polygon for cell site coverage',
    sub_voronoi_polygon_wkt    STRING  COMMENT 'WKT sub-Voronoi for fine-grain indoor splitting',
    site_type                 STRING,
    technology_band           STRING,
    region                    STRING
)
USING DELTA
TBLPROPERTIES ('quality' = 'reference')
COMMENT 'Cell site reference with pre-computed Voronoi polygons';
