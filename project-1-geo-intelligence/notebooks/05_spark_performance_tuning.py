# =============================================================================
# Project 1 – Geo-Location Intelligence Platform
# Notebook: 05_spark_performance_tuning.py
#
# Purpose : Demonstrate Spark performance tuning configurations applied to
#           geospatial join-heavy workloads — AQE, Photon, Liquid Clustering,
#           optimise-write, ZORDER, and cluster sizing recommendations.
#
# Stack   : Azure Databricks | PySpark | Delta Lake | AQE | Photon
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("spark_tuning")
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# ---------------------------------------------------------------------------
# 1. ADAPTIVE QUERY EXECUTION (AQE) CONFIGURATION
# AQE dynamically re-plans joins and coalesces shuffle partitions at runtime.
# ---------------------------------------------------------------------------

def configure_aqe(spark: SparkSession) -> None:
    """Enable and tune Adaptive Query Execution settings."""
    spark.conf.set("spark.sql.adaptive.enabled",                        "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",     "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "4")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes",   "128MB")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled",               "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",  "5")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
    # Auto broadcast: broadcast tables smaller than 10MB automatically
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",              "10MB")
    logger.info("AQE configuration applied.")

# COMMAND ----------
# ---------------------------------------------------------------------------
# 2. PHOTON VECTORIZED ENGINE
# Photon is enabled at the cluster level (use Photon-enabled cluster type).
# Below: SQL-level hints to ensure vectorized reads.
# ---------------------------------------------------------------------------

def photon_optimized_read(table_name: str, spark: SparkSession) -> DataFrame:
    """
    Read a Delta table using Photon-compatible query patterns.
    Avoid Python UDFs in hot paths; prefer native Spark/SQL expressions.
    """
    return spark.sql(f"""
        SELECT
            device_id,
            h3_index,
            cell_site_id,
            primary_poi_category,
            osm_area_type,
            ingest_date,
            COUNT(*) AS event_count,
            AVG(signal_strength) AS avg_signal
        FROM {table_name}
        WHERE ingest_date >= current_date() - INTERVAL 7 DAYS
          AND h3_index IS NOT NULL
        GROUP BY device_id, h3_index, cell_site_id, primary_poi_category, osm_area_type, ingest_date
    """)

# COMMAND ----------
# ---------------------------------------------------------------------------
# 3. OPTIMISE-WRITE — reduce small files on Delta append workloads
# ---------------------------------------------------------------------------

def configure_optimise_write(spark: SparkSession) -> None:
    """
    Enable optimized writes to auto-compact small files during write.
    Critical for streaming and incremental append patterns.
    """
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",          "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled",            "true")
    spark.conf.set("spark.databricks.delta.autoCompact.minNumFiles",        "50")
    spark.conf.set("spark.databricks.delta.optimizeWrite.binSize",          "512MB")
    logger.info("Optimise-write configuration applied.")

# COMMAND ----------
# ---------------------------------------------------------------------------
# 4. LIQUID CLUSTERING — replaces static ZORDER / partitioning
# Liquid Clustering auto-optimises data layout at write time using
# chosen cluster keys without requiring manual OPTIMIZE runs.
# ---------------------------------------------------------------------------

def apply_liquid_clustering(table_name: str, cluster_keys: list) -> None:
    """
    Alter an existing Delta table to use Liquid Clustering.
    Cluster keys should align with the most common filter/join predicates.
    """
    keys_str = ", ".join(cluster_keys)
    spark.sql(f"ALTER TABLE {table_name} CLUSTER BY ({keys_str})")
    logger.info(f"Liquid Clustering applied to {table_name} on keys: {keys_str}")


# Apply to Geo Intelligence Gold table
# apply_liquid_clustering(
#     "catalog.gold.geospatial_adcampaign_segments_history",
#     cluster_keys=["h3_index", "campaign_id", "ingest_date"]
# )

# COMMAND ----------
# ---------------------------------------------------------------------------
# 5. LEGACY ZORDER (for non-Liquid-Clustering tables)
# ---------------------------------------------------------------------------

def run_zorder_optimize(table_name: str, zorder_cols: list, predicate: str = None) -> None:
    """
    Run OPTIMIZE with ZORDER on specified columns.
    Optionally constrain to a partition predicate to reduce scope.
    """
    zorder_str = ", ".join(zorder_cols)
    predicate_clause = f"WHERE {predicate}" if predicate else ""
    sql = f"OPTIMIZE {table_name} {predicate_clause} ZORDER BY ({zorder_str})"
    logger.info(f"Running: {sql}")
    spark.sql(sql)
    logger.info("OPTIMIZE ZORDER complete.")


# COMMAND ----------
# ---------------------------------------------------------------------------
# 6. VACUUM — remove old Delta versions to reclaim storage
# ---------------------------------------------------------------------------

def vacuum_delta_table(table_name: str, retention_hours: int = 168) -> None:
    """
    Remove files older than retention_hours from the Delta table.
    Default: 168 hours (7 days) — minimum safe value for time-travel.
    """
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
    logger.info(f"VACUUM complete on {table_name}, retention={retention_hours}h")

# COMMAND ----------
# ---------------------------------------------------------------------------
# 7. GEOSPATIAL JOIN OPTIMISATION
# Spatial joins (point-in-polygon) are expensive. Key techniques:
#  a) Pre-index both sides with H3 so joins reduce to hash joins on strings
#  b) Broadcast small reference tables (cell sites, POIs)
#  c) Filter by bounding box before polygon containment check
# ---------------------------------------------------------------------------

def optimized_spatial_join(
    events_df: DataFrame,
    reference_df: DataFrame,
    join_key: str = "h3_index"
) -> DataFrame:
    """
    Efficient H3-based spatial join.
    Both sides must have h3_index pre-computed at the same resolution.
    """
    # Determine if reference can be broadcast (< 10MB rule of thumb)
    ref_count = reference_df.count()
    if ref_count < 500_000:
        logger.info(f"Broadcasting reference table ({ref_count:,} rows)")
        return events_df.join(
            F.broadcast(reference_df),
            on=join_key,
            how="left"
        )
    else:
        logger.info("Large reference — using shuffle join with AQE skew handling")
        return events_df.join(reference_df, on=join_key, how="left")

# COMMAND ----------
# ---------------------------------------------------------------------------
# 8. CLUSTER SIZING RECOMMENDATIONS (documentation)
# ---------------------------------------------------------------------------

CLUSTER_SIZING = """
Recommended Databricks cluster configuration for Geo-Intelligence workloads:

  Node type      : Standard_DS4_v2 (28 GB RAM, 8 vCPUs) — driver + workers
  Auto-scaling   : min 2 workers, max 8 workers
  Photon         : ENABLED (use Photon-enabled runtime)
  Runtime        : Databricks Runtime 14.x LTS (Photon)
  Spot instances : Worker nodes on spot for cost savings (with retries)
  Shuffle partitions  : spark.sql.shuffle.partitions = 200 (tune per data size)
  Executor memory : spark.executor.memory = 6g (leave headroom for off-heap)

Geospatial join tips:
  - Use H3 hash joins instead of polygon containment where possible
  - Cache reference tables (cell sites, POIs) in memory before join loops
  - Use repartition(200, 'h3_index') before heavy join operations
  - Avoid Python UDFs in hot paths — prefer Spark native SQL or Scala UDFs
"""

print(CLUSTER_SIZING)

# COMMAND ----------
# ---------------------------------------------------------------------------
# 9. APPLY ALL TUNING SETTINGS — single entry point
# ---------------------------------------------------------------------------

def apply_all_tuning(spark: SparkSession) -> None:
    configure_aqe(spark)
    configure_optimise_write(spark)
    logger.info("All performance tuning settings applied.")


# Uncomment to apply:
# apply_all_tuning(spark)
