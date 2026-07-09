# =============================================================================
# Project 5 – OPEX Analytics
# Notebook: 03_h3_geospatial_analytics.py
#
# Purpose : Apply Uber H3 hexagonal indexing to OPEX assets and compute
#           geospatial cost density metrics for nationwide infrastructure
#           dashboards in Power BI.
#
# Stack   : Azure Databricks | PySpark | Uber H3 | GeoPandas
# =============================================================================

# %pip install h3 geopandas shapely

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, DoubleType, StructType, StructField
import h3
import logging

logger = logging.getLogger("h3_opex_analytics")
spark  = SparkSession.builder.getOrCreate()

GOLD_OPEX_TABLE      = "catalog.gold.opex_cost_attribution"
GOLD_GEO_TABLE       = "catalog.gold.opex_geo_analytics"
H3_RESOLUTION_WIDE   = 7    # ~5.1km edge — regional view
H3_RESOLUTION_LOCAL  = 9    # ~174m edge — local density view

# ---------------------------------------------------------------------------
# H3 UDFs
# ---------------------------------------------------------------------------

@F.udf(returnType=StringType())
def to_h3(lat: float, lon: float, res: int) -> str:
    if lat is None or lon is None:
        return None
    try:
        return h3.latlng_to_cell(lat, lon, res)
    except Exception:
        return None


@F.udf(returnType=StructType([
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True)
]))
def h3_centroid(h3_index: str):
    """Return the centroid lat/lng of an H3 cell."""
    if h3_index is None:
        return None
    lat, lng = h3.cell_to_latlng(h3_index)
    return (lat, lng)


@F.udf(returnType=ArrayType(StringType()))
def h3_boundary_wkt(h3_index: str) -> list:
    """Return the boundary coordinates of an H3 cell as a list of lat/lng pairs."""
    if h3_index is None:
        return []
    boundary = h3.cell_to_boundary(h3_index)
    return [f"{lat},{lng}" for lat, lng in boundary]

# ---------------------------------------------------------------------------
# STEP 1: Assign H3 index at two resolutions
# ---------------------------------------------------------------------------

def assign_h3_indices(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .withColumn("h3_r7",  to_h3(F.col("latitude"), F.col("longitude"), F.lit(H3_RESOLUTION_WIDE)))
        .withColumn("h3_r9",  to_h3(F.col("latitude"), F.col("longitude"), F.lit(H3_RESOLUTION_LOCAL)))
    )

# ---------------------------------------------------------------------------
# STEP 2: Aggregate OPEX costs by H3 cell
# ---------------------------------------------------------------------------

def aggregate_by_h3(df: DataFrame, resolution_col: str, resolution: int) -> DataFrame:
    """
    Aggregate cost, asset count, and anomaly count per H3 hex cell.
    """
    agg = (
        df
        .groupBy(
            F.col(resolution_col).alias("h3_index"),
            F.col("asset_category"),
            F.col("billing_year"),
            F.col("region")
        )
        .agg(
            F.sum("cost_amount").alias("total_cost"),
            F.count("asset_id").alias("asset_count"),
            F.sum(F.when(F.col("is_overspend"), F.lit(1)).otherwise(F.lit(0))).alias("overspend_count"),
            F.sum(F.when(F.col("is_anomaly"),   F.lit(1)).otherwise(F.lit(0))).alias("anomaly_count"),
            F.avg("cost_variance_pct").alias("avg_variance_pct"),
            F.countDistinct("supplier_id").alias("unique_suppliers")
        )
    )

    # Attach H3 centroid for map rendering
    centroid = h3_centroid(F.col("h3_index"))
    return (
        agg
        .withColumn("centroid",       centroid)
        .withColumn("centroid_lat",   F.col("centroid.lat"))
        .withColumn("centroid_lng",   F.col("centroid.lng"))
        .withColumn("cost_per_asset", F.round(F.col("total_cost") / F.col("asset_count"), 2))
        .withColumn("h3_resolution",  F.lit(resolution))
        .withColumn("geo_ts",         F.current_timestamp())
        .drop("centroid")
    )

# ---------------------------------------------------------------------------
# STEP 3: Regional cost density ranking
# ---------------------------------------------------------------------------

def compute_cost_rank(df: DataFrame) -> DataFrame:
    """
    Rank H3 cells by cost density within each asset_category and billing_year.
    Useful for identifying highest-spend infrastructure zones.
    """
    from pyspark.sql.window import Window
    w = Window.partitionBy("asset_category", "billing_year").orderBy(F.col("total_cost").desc())
    return (
        df
        .withColumn("cost_rank",    F.rank().over(w))
        .withColumn("cost_decile",  F.ntile(10).over(w))  # 1=top 10%, 10=bottom 10%
    )

# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def run_geo_analytics(spark: SparkSession) -> None:
    logger.info("Loading Gold OPEX data …")
    opex_df = spark.table(GOLD_OPEX_TABLE)

    logger.info("Assigning H3 indices …")
    geo_df = assign_h3_indices(opex_df)

    logger.info("Aggregating at H3 resolution 7 (regional) …")
    r7_df = aggregate_by_h3(geo_df, "h3_r7", H3_RESOLUTION_WIDE)
    r7_df = compute_cost_rank(r7_df)

    logger.info("Aggregating at H3 resolution 9 (local) …")
    r9_df = aggregate_by_h3(geo_df, "h3_r9", H3_RESOLUTION_LOCAL)
    r9_df = compute_cost_rank(r9_df)

    # Combine both resolutions into one table
    combined = r7_df.unionByName(r9_df)

    logger.info(f"Writing geo analytics to {GOLD_GEO_TABLE} …")
    combined.write.format("delta").mode("overwrite") \
            .partitionBy("billing_year", "h3_resolution") \
            .saveAsTable(GOLD_GEO_TABLE)

    logger.info(f"Geo analytics complete. Total H3 cells: {combined.count():,}")


if __name__ == "__main__":
    run_geo_analytics(spark)
