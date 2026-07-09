-- =============================================================================
-- Project 5 – OPEX Analytics &amp; Contract Cost Analysis
-- File: sql/01_analytics_queries.sql
-- =============================================================================

-- Total OPEX cost by category and year
SELECT
    asset_category,
    billing_year,
    region,
    SUM(cost_amount)            AS total_cost,
    COUNT(asset_id)             AS asset_count,
    AVG(cost_variance_pct)      AS avg_variance_pct,
    SUM(CASE WHEN is_overspend THEN 1 ELSE 0 END) AS overspend_count,
    SUM(CASE WHEN is_anomaly   THEN 1 ELSE 0 END) AS anomaly_count
FROM catalog.gold.opex_cost_attribution
GROUP BY asset_category, billing_year, region
ORDER BY billing_year DESC, total_cost DESC;

-- Top suppliers by overspend
SELECT
    supplier_id,
    asset_category,
    COUNT(*)                    AS invoice_count,
    SUM(cost_amount)            AS total_billed,
    SUM(benchmark_cost)         AS total_benchmark,
    SUM(cost_variance)          AS total_variance,
    ROUND(AVG(cost_variance_pct), 2) AS avg_overspend_pct
FROM catalog.gold.opex_cost_attribution
WHERE is_overspend = true
GROUP BY supplier_id, asset_category
ORDER BY total_variance DESC
LIMIT 20;

-- Geospatial cost density — top 10 H3 cells by total cost (for Power BI map)
SELECT
    h3_index,
    asset_category,
    billing_year,
    total_cost,
    asset_count,
    cost_per_asset,
    centroid_lat,
    centroid_lng,
    cost_rank,
    cost_decile
FROM catalog.gold.opex_geo_analytics
WHERE h3_resolution = 7
  AND billing_year = 2024
  AND cost_decile = 1      -- top 10% highest-cost cells
ORDER BY total_cost DESC;

-- Anomaly investigation — records flagged as statistical outliers
SELECT
    asset_id,
    asset_category,
    supplier_id,
    cost_amount,
    benchmark_cost,
    cost_variance,
    cost_variance_pct,
    z_score,
    region,
    billing_period
FROM catalog.gold.opex_cost_attribution
WHERE is_anomaly = true
  AND billing_year >= 2023
ORDER BY z_score DESC
LIMIT 50;

-- Asset age vs cost trend
SELECT
    age_category,
    asset_category,
    COUNT(asset_id)      AS asset_count,
    AVG(cost_amount)     AS avg_cost,
    SUM(cost_amount)     AS total_cost,
    SUM(CASE WHEN is_overspend THEN 1 ELSE 0 END) AS overspend_assets
FROM catalog.gold.opex_cost_attribution
WHERE billing_year = 2024
GROUP BY age_category, asset_category
ORDER BY age_category, total_cost DESC;

-- Cost centre summary for budgeting
SELECT
    cost_centre_id,
    cost_centre_name,
    division,
    billing_year,
    SUM(cost_amount)        AS total_cost,
    COUNT(DISTINCT asset_id) AS unique_assets,
    SUM(cost_variance)      AS total_variance
FROM catalog.gold.opex_cost_attribution
GROUP BY cost_centre_id, cost_centre_name, division, billing_year
ORDER BY billing_year DESC, total_cost DESC;

-- Contract cost analysis — contracts flagged for recalculation
SELECT
    supplier_id,
    invoice_type,
    billing_period,
    COUNT(*)                                              AS contract_lines,
    SUM(theoretical_cost)                                 AS total_theoretical,
    SUM(actual_invoiced_cost)                             AS total_actual,
    ROUND(AVG(variance_pct) * 100, 2)                    AS avg_variance_pct,
    SUM(CASE WHEN contract_action = 'RECALCULATE' THEN 1 ELSE 0 END) AS recalculate_count
FROM catalog.gold.contract_cost_analysis
GROUP BY supplier_id, invoice_type, billing_period
ORDER BY avg_variance_pct DESC;
