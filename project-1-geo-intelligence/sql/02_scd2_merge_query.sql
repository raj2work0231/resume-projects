-- =============================================================================
-- Project 1 – Geo-Location Intelligence Platform
-- File: sql/02_scd2_merge_query.sql
--
-- Purpose : SparkSQL implementation of SCD Type 2 MERGE INTO for the
--           Silver POI reference table (monthly batch load).
--           Run this after staging new POI data into the _staging table.
-- =============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Expire old active records where attributes have changed
-- ----------------------------------------------------------------------------
MERGE INTO catalog.silver.poi_reference_history  AS target
USING (
    SELECT s.*
    FROM   catalog.silver.poi_reference_staging s
    INNER JOIN catalog.silver.poi_reference_history t
        ON  s.poi_id    = t.poi_id
        AND t.crnt_flag = true
        AND s.row_hash <> t.row_hash   -- attribute change detected
) AS changed_records
ON  target.poi_id    = changed_records.poi_id
AND target.crnt_flag = true
WHEN MATCHED THEN UPDATE SET
    target.crnt_flag  = false,
    target.eff_to     = current_timestamp(),
    target.updated_at = current_timestamp();

-- ----------------------------------------------------------------------------
-- STEP 2: Insert new / updated records as the current active version
-- ----------------------------------------------------------------------------
INSERT INTO catalog.silver.poi_reference_history
SELECT
    s.poi_id,
    s.poi_name,
    s.poi_category,
    s.latitude,
    s.longitude,
    s.poi_type,
    s.h3_index,
    s.polygon_wkt,
    s.row_hash,
    current_timestamp()                AS eff_from,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS eff_to,
    true                               AS crnt_flag,
    s.load_date,
    current_timestamp()                AS inserted_at,
    current_timestamp()                AS updated_at
FROM catalog.silver.poi_reference_staging s
-- Insert new records (no existing active version)
LEFT JOIN catalog.silver.poi_reference_history t
    ON  s.poi_id    = t.poi_id
    AND t.crnt_flag = true
WHERE t.poi_id IS NULL          -- NEW records
   OR s.row_hash <> t.row_hash; -- CHANGED records (just expired in step 1)

-- ----------------------------------------------------------------------------
-- STEP 3: Verify SCD2 integrity — no duplicate active records
-- ----------------------------------------------------------------------------
SELECT
    poi_id,
    COUNT(*) AS active_versions
FROM catalog.silver.poi_reference_history
WHERE crnt_flag = true
GROUP BY poi_id
HAVING COUNT(*) > 1
ORDER BY active_versions DESC
LIMIT 20;
-- Expected: 0 rows returned (no violations)

-- ----------------------------------------------------------------------------
-- STEP 4: Useful reporting queries on SCD2 history table
-- ----------------------------------------------------------------------------

-- Current active POI records
SELECT
    poi_id,
    poi_name,
    poi_category,
    poi_type,
    latitude,
    longitude,
    eff_from
FROM catalog.silver.poi_reference_history
WHERE crnt_flag = true
ORDER BY poi_category, poi_name;

-- Full version history for a specific POI
SELECT
    poi_id,
    poi_name,
    poi_category,
    poi_type,
    eff_from,
    eff_to,
    crnt_flag
FROM catalog.silver.poi_reference_history
WHERE poi_id = '<<poi_id>>'
ORDER BY eff_from;

-- POI category distribution (current active)
SELECT
    poi_category,
    COUNT(*)                                                        AS poi_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)             AS pct
FROM catalog.silver.poi_reference_history
WHERE crnt_flag = true
GROUP BY poi_category
ORDER BY poi_count DESC;

-- ----------------------------------------------------------------------------
-- STEP 5: OPTIMIZE + VACUUM maintenance
-- ----------------------------------------------------------------------------
OPTIMIZE catalog.silver.poi_reference_history
    WHERE load_date >= current_date() - INTERVAL 60 DAYS;

VACUUM catalog.silver.poi_reference_history RETAIN 168 HOURS;
